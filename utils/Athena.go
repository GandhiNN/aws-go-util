package utils

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

// RowCount is a struct to store row count query result
type Rows struct {
	Table string
	Count int
}

// CreateRowObject accepts a slice of table names and
// row count result and return a slice of Rows struct
func CreateAthenaRowObject(table []string, rowCount [][]interface{}) ([]Rows, error) {

	var r []Rows
	for i, _ := range table {
		tabName := rowCount[i][0].(string)
		rc, err := strconv.Atoi(rowCount[i][1].(string)) // convert str interface{} to int
		if err != nil {
			return nil, err
		}
		tableRowCount := &Rows{
			Table: tabName,
			Count: rc,
		}
		r = append(r, *tableRowCount)
	}
	return r, nil
}

// WriteAthenaToCSV accepts a slice of Rows object and convert
// the elements into CSV records to be flushed to a file
func WriteAthenaToCSV(rs []Rows, fname string) error {

	// Create a file object for the writer object to flush the buffer
	fpath := fmt.Sprintf("./result/%s", fname)
	f, err := os.Create(fpath)
	log.Printf("Writing Athena result set to %s\n", fpath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Create a writer object with predefined header
	writer := csv.NewWriter(f)
	err = writer.Write([]string{"table_name_athena", "row_count"})
	if err != nil {
		return err
	}
	defer writer.Flush()

	for _, v := range rs {
		var s []string
		s = append(s, v.Table, strconv.Itoa(v.Count))
		err = writer.Write(s)
		if err != nil {
			return err
		}
	}
	log.Println("Done writing CSV file!")
	return nil
}

// CreateAthenaSession creates the AWS session object towards Athena service
func CreateAthenaSession(region string) *athena.Athena {

	awscfg := &aws.Config{}
	awscfg.WithRegion(region)

	sess := session.Must(session.NewSession(awscfg))

	svc := athena.New(sess, aws.NewConfig().WithRegion(region))
	return svc
}

// SetupAthenaContext create a context to Athena services in defined region
// It accepts the query statement, DB name, athena output bucket name
// and returns a pointer to Athena's query execution input and output object
func SetupAthenaContext(query string, dbName string, bucketName string, svc *athena.Athena) (*athena.GetQueryExecutionInput, *athena.GetQueryExecutionOutput, *athena.StartQueryExecutionOutput) {

	var s athena.StartQueryExecutionInput
	s.SetQueryString(query)

	var q athena.QueryExecutionContext
	q.SetDatabase(dbName)
	s.SetQueryExecutionContext(&q)

	var r athena.ResultConfiguration
	r.SetOutputLocation(bucketName)
	s.SetResultConfiguration(&r)

	result, err := svc.StartQueryExecution(&s)
	if err != nil {
		log.Fatal("Cannot start an Athena query context : ", err)
	}
	log.Printf("StartQueryExecution result with query ID: %s\n", *result.QueryExecutionId)

	var qri athena.GetQueryExecutionInput
	qri.SetQueryExecutionId(*result.QueryExecutionId)

	var qrop *athena.GetQueryExecutionOutput

	return &qri, qrop, result
}

// InvokeAthenaQuery triggers an Athena query invocation
func InvokeAthenaQuery(svc *athena.Athena, res *athena.StartQueryExecutionOutput, qri *athena.GetQueryExecutionInput, qrop *athena.GetQueryExecutionOutput, duration int) ([][]interface{}, error) {

	waitDuration := time.Duration(duration) * time.Second
	var rc [][]interface{}
	for {
		qrop, err := svc.GetQueryExecution(qri)
		if err != nil {
			return nil, err
		}
		if *qrop.QueryExecution.Status.State == "RUNNING" {
			log.Println("Waiting...Query Status = ", *qrop.QueryExecution.Status.State)
			time.Sleep(waitDuration)
		} else if *qrop.QueryExecution.Status.State == "QUEUED" {
			log.Println("Waiting...Query Status = ", *qrop.QueryExecution.Status.State)
			time.Sleep(waitDuration)
		} else if *qrop.QueryExecution.Status.State == "SUCCEEDED" {
			log.Println("Query exits with status = ", *qrop.QueryExecution.Status.State)
			var ip athena.GetQueryResultsInput
			ip.SetQueryExecutionId(*res.QueryExecutionId)

			op, err := svc.GetQueryResults(&ip)
			if err != nil {
				return nil, err
			}
			rc = ParseAthenaOutput(op)
			return rc, nil
		} else {
			log.Println("Query status is : ", *qrop.QueryExecution.Status.State)
			time.Sleep(waitDuration)
		}
	}
}

// ParseAthenaOutput parse Athena SDK output to human-readable format
func ParseAthenaOutput(op *athena.GetQueryResultsOutput) [][]interface{} {

	var rc [][]interface{}
	for i := range op.ResultSet.Rows {
		if i == 0 {
			continue
		}
		var temp []interface{}
		for j := range op.ResultSet.Rows[i].Data {
			temp = append(temp, *op.ResultSet.Rows[i].Data[j].VarCharValue)
		}
		rc = append(rc, temp)
	}
	return rc
}

// RowCountQueryBuilder takes a slice of strings and build Athena-compliant SQL statement for row count query
func RowCountQueryBuilder(sc []string) string {

	var rc strings.Builder
	for i, str := range sc {
		fmt.Fprintf(&rc, "select '%s', count(*) from %s", str, str)
		if i != len(sc)-1 {
			fmt.Fprintf(&rc, " %s ", "union all")
		}
	}
	return rc.String()
}
