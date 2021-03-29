package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/GandhiNN/aws-go-api/utils"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func main() {

	// CLI flag
	var env string
	var svc string
	var tables string
	var output string
	var ddbPrefix string

	flag.StringVar(&env, "env", "dev", "Environment (dev|qa|prd)")
	flag.StringVar(&svc, "svc", "athena", "AWS service to be used (athena|ddb)")
	flag.StringVar(&tables, "tables", "tables.csv", "Path to list of tables to be queried, relative to `./config`")
	flag.StringVar(&ddbPrefix, "ddbPrefix", "pipe-", "Prefix for item in DDB tables")
	flag.StringVar(&output, "output", "results.csv", "Path to save the query result, relative to `./result`")

	flag.Parse()

	// Set configuration
	awsConfig, err := utils.ReadConfig("el", map[string]interface{}{
		"region":             "eu-west-1",
		"athenaDB":           "defaultDB",
		"athenaOutputBucket": "defaultBucket",
	})
	if err != nil {
		log.Fatal("Cannot load configuration file : ", err)
	}
	awsRegion := awsConfig.GetString("aws.region")
	athenaDB := awsConfig.GetString(fmt.Sprintf("aws.athena.%s.database", env))
	athenaOutputBucket := awsConfig.GetString(fmt.Sprintf("aws.athena.%s.outputBucket", env))
	ddbTable := awsConfig.GetString(fmt.Sprintf("aws.ddb.%s.table", env))

	// Set path for file output
	filepath := fmt.Sprintf("./config/%s", tables)

	if svc == "athena" {
		log.Println("Running Athena-based Validation...")
		runAthena(awsRegion, athenaDB, athenaOutputBucket, filepath, output)
		log.Println("Done running validation!")
	} else if svc == "ddb" {
		log.Println("Running DDB-based Validation...")
		runDDB(awsRegion, ddbTable, filepath, output, ddbPrefix)
		log.Println("Done running validation!")
	}
}

// runDDB is a private function run DDB related tasks
func runDDB(region string, ddbTable string, tpath string, fout string, ddbPrefix string) {

	// Create session object to be used
	ddbSvc := utils.CreateDDBSession(region)

	// Load items to be queried
	listOfTables := utils.TableReader(tpath, "ddb")

	var listOfIngName []string
	for _, i := range listOfTables {
		ingName := fmt.Sprintf("%s_%s", ddbPrefix, i)
		listOfIngName = append(listOfIngName, ingName)
	}
	tNow := utils.GetTimeNowEpoch()

	// Query items and write to CSV
	var qop []*dynamodb.QueryOutput
	for _, i := range listOfIngName {
		res, err := utils.QueryDDBItems(
			ddbSvc,
			ddbTable,
			"ingestor_name",
			"execution_time",
			i,
			tNow,
		)
		if err != nil {
			log.Fatal(err)
		}
		qop = append(qop, res)
	}
	err := utils.WriteDDBToCSV(qop, fout)
	if err != nil {
		log.Fatal()
	}
	log.Println("Done writing DDB query result")
}

// runAthena is a private function to run Athena related tasks
func runAthena(region string, athenaDB string, outputBucket string, filepath string, output string) {

	// Create session object to be used
	svc := utils.CreateAthenaSession(region)

	// Read list of table
	sc := utils.TableReader(filepath, "athena")

	// Build row count query
	rcq := utils.RowCountQueryBuilder(sc)

	// Build Athena context
	qri, qrop, result := utils.SetupAthenaContext(rcq, athenaDB, outputBucket, svc)

	rc, err := utils.InvokeAthenaQuery(svc, result, qri, qrop, 2)
	if err != nil {
		log.Fatalln("Cannot invoke query to Athena : ", err)
	}

	// Convert the result set into Rows object
	rr, err := utils.CreateAthenaRowObject(sc, rc)
	if err != nil {
		log.Fatal(err)
	}

	// Write Rows object to CSV
	err = utils.WriteAthenaToCSV(rr, output)
	if err != nil {
		log.Fatal(err)
	}
}
