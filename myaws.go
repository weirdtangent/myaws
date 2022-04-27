package myaws

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/aws/aws-sdk-go/service/sts"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

func AWSConfig(r string) (*aws.Config, error) {
	return &aws.Config{Region: aws.String(r)}, nil
}

func AWSConnect(r string, proj string) (*session.Session, error) {
	return session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String(r)},
		Profile: proj,
	})
}

func AWSMustConnect(r string, proj string) *session.Session {
	awssess, err := session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String(r)},
		Profile: proj,
	})
	if err != nil {
		log.Fatal().Err(err)
	}
	return awssess
}

func AWSAccount(awssess *session.Session) (*string, *string, error) {
	svc := sts.New(session.New())
	input := &sts.GetCallerIdentityInput{}

	region := "us-east-1"

	result, err := svc.GetCallerIdentity(input)
	if err != nil {
		return nil, nil, err
	}
	return &region, result.Account, nil
}

// try to get value of key from aws secret
func AWSGetSecretKV(awssess *session.Session, secret string, key string) (*string, error) {
	// get service into secrets manager
	svc := secretsmanager.New(awssess)

	// go get the secret we need
	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secret),
	}

	result, err := svc.GetSecretValue(input)
	if err != nil {
		log.Fatal().Err(err)
	}

	var keyvalues map[string]string
	err = json.Unmarshal([]byte(*result.SecretString), &keyvalues)
	if err != nil {
		log.Fatal().Err(err)
	}
	for thiskey, thisvalue := range keyvalues {
		if thiskey == key {
			return &thisvalue, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("Key %s not found in secret %s", key, secret))
}

func AWSGetSecretValue(awssess *session.Session, secret string) (*string, error) {
	// get service into secrets manager
	svc := secretsmanager.New(awssess)

	// go get the secret we need
	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secret),
	}

	result, err := svc.GetSecretValue(input)
	if err != nil {
		log.Fatal().Err(err)
	}

	var decodedBinarySecret string
	if result.SecretString != nil {
		return result.SecretString, nil
	} else {
		decodedBinarySecretBytes := make([]byte, base64.StdEncoding.DecodedLen(len(result.SecretBinary)))
		len, err := base64.StdEncoding.Decode(decodedBinarySecretBytes, result.SecretBinary)
		if err != nil {
			log.Error().Err(err).Msg("Failed Base64 Decode")
			return nil, err
		}
		decodedBinarySecret = string(decodedBinarySecretBytes[:len])
	}

	return &decodedBinarySecret, nil
}

// try to connect to RDS after getting key value from secret
func DBConnect(awssess *session.Session, credSecret string, database string) (*sqlx.DB, error) {
	rdbsConnection, err := AWSGetSecretKV(awssess, credSecret, "rdbs_connection")
	if err != nil {
		log.Fatal().Err(err)
	}

	connection := fmt.Sprintf("%s/%s", *rdbsConnection, database)

	return sqlx.Open("mysql", connection)
}

// must connect to RDS after getting key value from secret
func DBMustConnect(awssess *session.Session, credSecret string) *sqlx.DB {
	rdbsConnection, err := AWSGetSecretKV(awssess, credSecret, "rdbs_connection")
	if err != nil {
		log.Fatal().Err(err)
	}

	db := sqlx.MustOpen("mysql", *rdbsConnection)

	_, err = db.Exec("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
	if err != nil {
		log.Fatal().Err(err)
	}

	return db
}

// try to connect to DDB
func DDBConnect(awssess *session.Session) (*dynamodb.DynamoDB, error) {
	return dynamodb.New(awssess), nil
}
