//go:build infra

// Package dynamoidem implements pipeline.Idempotency on DynamoDB (or the free
// DynamoDB Local emulator) using a conditional write:
//
//	PutItem(event_id) IF attribute_not_exists(event_id)
//
// Table design: partition key event_id (string); expires_at (number) is a TTL
// attribute so idempotency records age out instead of growing forever.
package dynamoidem

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Store struct {
	c     *dynamodb.Client
	table string
	ttl   time.Duration
}

// New connects to endpoint (e.g. http://localhost:8000 for DynamoDB Local) and
// creates the table if needed. Credentials are dummy values for local use.
func New(ctx context.Context, endpoint, table string) (*Store, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")))
	if err != nil {
		return nil, err
	}
	s := &Store{table: table, ttl: 7 * 24 * time.Hour,
		c: dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(endpoint) })}

	_, err = s.c.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(table),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("event_id"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("event_id"), KeyType: types.KeyTypeHash}},
	})
	var inUse *types.ResourceInUseException
	if err != nil && !errors.As(err, &inUse) {
		return nil, err
	}
	return s, nil
}

// Claim returns false if event_id was already claimed.
func (s *Store) Claim(ctx context.Context, id string) (bool, error) {
	_, err := s.c.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]types.AttributeValue{
			"event_id":   &types.AttributeValueMemberS{Value: id},
			"expires_at": &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Add(s.ttl).Unix(), 10)},
		},
		ConditionExpression: aws.String("attribute_not_exists(event_id)"),
	})
	var dup *types.ConditionalCheckFailedException
	if errors.As(err, &dup) {
		return false, nil
	}
	return err == nil, err
}

// Release deletes the claim so a failed event can be retried.
func (s *Store) Release(ctx context.Context, id string) error {
	_, err := s.c.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.table),
		Key:       map[string]types.AttributeValue{"event_id": &types.AttributeValueMemberS{Value: id}},
	})
	return err
}
