package utils

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// DDBItem is a container for the item's schema
type DDBItem struct {
	IngestorName      string `json:"ingestor_name"`
	ExecutionTime     string `json:"execution_time"`
	LastExecutionDate string `json:"last_execution_as_date"`
	DataLoad          string `json:"data_load"`
	SrcSys            string `json:"src_sys"`
	Status            string `json:"status"`
	StatusMessage     string `json:"status_message"`
	TableName         string `json:"table_name"`
	TotalRows         int    `json:"total_rows"`
}

// CreateDDBSession creates the AWS session object to DynamoDB service
func CreateDDBSession(region string) *dynamodb.DynamoDB {

	awscfg := &aws.Config{}
	awscfg.WithRegion(region)
	sess := session.Must(session.NewSession(awscfg))
	svc := dynamodb.New(sess)

	return svc
}

// QueryDDBItems invoke a query for a given item and timestamp to a DynamoDB table
func QueryDDBItems(ddbSvc *dynamodb.DynamoDB, table string, hashKey string, sortKey string, ingestorName string, timestamp string) (*dynamodb.QueryOutput, error) {

	// Construct the Key Condition builder
	keyCond := expression.Key(hashKey).
		Equal(expression.Value(ingestorName)).
		And(expression.Key(sortKey).LessThanEqual(expression.Value(timestamp)))

	// Build query expression
	expr, err := expression.NewBuilder().
		WithKeyCondition(keyCond).
		Build()
	if err != nil {
		return nil, err
	}

	// Build query input parameters
	params := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		TableName:                 aws.String(table),
		Limit:                     aws.Int64(1),
	}

	res, err := ddbSvc.Query(params)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// WriteDDBToCSV accepts a slice of Rows object and convert
// the elements into CSV records to be flushed to a file
func WriteDDBToCSV(res []*dynamodb.QueryOutput, fname string) error {

	// Create a file object for the writer object to flush the buffer
	fpath := fmt.Sprintf("./result/%s", fname)
	f, err := os.Create(fpath)
	log.Printf("Writing DDB result set to %s\n", fpath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Create a writer object with predefined header
	writer := csv.NewWriter(f)
	err = writer.Write([]string{"table_name_ddb", "last_exec_as_date", "exec_time", "status", "total_rows"})
	if err != nil {
		return err
	}
	defer writer.Flush()

	for _, i := range res {
		for _, v := range i.Items {
			var s []string
			item := DDBItem{}
			err = dynamodbattribute.UnmarshalMap(v, &item)
			if err != nil {
				return err
			}
			s = append(s, item.TableName, item.LastExecutionDate, item.ExecutionTime, item.Status, strconv.Itoa(item.TotalRows))
			err = writer.Write(s)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
