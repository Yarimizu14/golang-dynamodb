package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func main() {
	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Using the Config value, create the DynamoDB client
	svc := dynamodb.NewFromConfig(cfg)

	// Build the request with its input parameters
	resp, err := svc.ListTables(context.TODO(), &dynamodb.ListTablesInput{
		Limit: aws.Int32(5),
	})
	if err != nil {
		log.Fatalf("failed to list tables, %v", err)
	}

	fmt.Println("Tables:")
	for _, tableName := range resp.TableNames {
		fmt.Println(tableName)
	}

	tableName := "golang-example"

	// Scan
	log.Printf("\n##################\n Scan \n##################\n")
	{
		ctx := context.Background()
		input := dynamodb.ScanInput{
			TableName: &tableName,
		}
		output, err := svc.Scan(ctx, &input)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Printf("output %d \n", output.Count)
		if output.ConsumedCapacity != nil {
			log.Printf("ConsumedCapacity %v \n", *output.ConsumedCapacity)
		}
		log.Printf("ScannedCount %d \n", output.ScannedCount)

		for _, item := range output.Items {
			for k, v := range item {
				log.Printf("Item key: %s --> %v \n", k, v)
			}
		}
	}

	// GetItem
	log.Printf("\n##################\n GetItem \n##################\n")
	{
		ctx := context.Background()
		input := dynamodb.GetItemInput{
			TableName: &tableName,
			Key: map[string]types.AttributeValue{
				"dummy": &types.AttributeValueMemberS{
					Value: "hoge",
				},
			},
		}
		output, err := svc.GetItem(ctx, &input)
		if err != nil {
			log.Fatal(err.Error())
		}
		// log.Printf("ResultMetadata %v \n", output.ResultMetadata)

		for k, v := range output.Item {
			log.Printf("Item key: %s --> %v \n", k, v)
		}
	}

	// PutItem
	log.Printf("\n##################\n PutItem \n##################\n")
	{
		now := time.Now()
		ctx := context.Background()
		input := dynamodb.PutItemInput{
			TableName: &tableName,
			Item: map[string]types.AttributeValue{
				"dummy": &types.AttributeValueMemberS{
					Value: fmt.Sprintf("dummy-%d", now.Unix()),
				},
				"timestamp": &types.AttributeValueMemberS{
					Value: now.String(),
				},
			},
		}
		output, err := svc.PutItem(ctx, &input)
		if err != nil {
			log.Fatal(err.Error())
		}

		for k, v := range output.Attributes {
			log.Printf("Item key: %s --> %v \n", k, v)
		}
	}

	// BatchWriteItemInput
	log.Printf("\n##################\n BatchWriteItemInput \n##################\n")
	{
		now := time.Now()
		ctx := context.Background()
		input := dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: {
					{
						PutRequest: &types.PutRequest{
							Item: map[string]types.AttributeValue{
								"dummy": &types.AttributeValueMemberS{
									Value: fmt.Sprintf("dummy-batch-%d", now.Unix()),
								},
								"timestamp": &types.AttributeValueMemberS{
									Value: now.String(),
								},
							},
						},
					},
				},
			},
		}
		_, err := svc.BatchWriteItem(ctx, &input)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	// DeleteItem
	log.Printf("\n##################\n DeleteItem \n##################\n")
	{
		ctx := context.Background()
		input := dynamodb.DeleteItemInput{
			TableName: &tableName,
			Key: map[string]types.AttributeValue{
				"dummy": &types.AttributeValueMemberS{
					Value: "fuga",
				},
			},
		}
		output, err := svc.DeleteItem(ctx, &input)
		if err != nil {
			log.Fatal(err.Error())
		}

		for k, v := range output.Attributes {
			log.Printf("Item key: %s --> %v \n", k, v)
		}
	}

	// Query
	// UpdateItem
	// BatchGetItem
	// ExecuteStatement
	// ExecuteTransaction
	// TransactGetItems
	// TransactWriteItems
}
