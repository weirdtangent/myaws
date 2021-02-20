package myaws

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"graystorm.com/mylog"
)

// format of SecretString for RDS connection
type dbCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     int    `json:port`
}

func AWSConnect(r string, proj string) (*session.Session, error) {
	return session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String(r)},
		Profile: proj,
	})
}

// try to get value of key from aws secret
func AWSGetSecretKV(sess *session.Session, secret string, key string) (*string, error) {
	// get service into secrets manager
	svc := secretsmanager.New(sess)

	// go get the secret we need
	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secret),
	}

	result, err := svc.GetSecretValue(input)
	if err != nil {
		mylog.Error.Fatal(err)
	}

	var keyvalues map[string]string
	err = json.Unmarshal([]byte(*result.SecretString), &keyvalues)
	if err != nil {
		mylog.Error.Fatal(err)
	}
	for thiskey, thisvalue := range keyvalues {
		if thiskey == key {
			return &thisvalue, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("Key %s not found in secret %s", key, secret))
}

// try to connect to RDS after getting key value from secret
func DBConnect(sess *session.Session, credSecret string, table string) (*sqlx.DB, error) {
	dbCreds, err := awsGetDBCredentials(sess, credSecret)
	if err != nil {
		mylog.Error.Fatal(err)
	}

  AuroraConnection := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		dbCreds.Username,
		dbCreds.Password,
		dbCreds.Host,
		dbCreds.Port,
		table)

	return sqlx.Open("mysql", AuroraConnection)
}

// INTERNAL

func awsGetDBCredentials(sess *session.Session, key string) (*dbCredentials, error) {
	// get service into secrets manager
	svc := secretsmanager.New(sess)

	// go get the secret we need
	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(key),
	}

	result, err := svc.GetSecretValue(input)
	if err != nil {
		mylog.Error.Fatal(err)
	}

	// unmarshal json
	var dbCreds dbCredentials
	err = json.Unmarshal([]byte(*result.SecretString), &dbCreds)
	if err != nil {
		mylog.Error.Fatal(err)
	}

	return &dbCreds, nil
}
