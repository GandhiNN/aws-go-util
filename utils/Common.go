package utils

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

// GetTimeNowEpoch returns current time in epoch, milliseconds granularity
func GetTimeNowEpoch() string {

	now := time.Now()
	nanos := now.UnixNano()
	millis := nanos / 1000000

	return strconv.FormatInt(millis, 10)
}

// TableReader reads list of table from a csv file and returns as slice of strings
func TableReader(file string, svcName string) []string {

	// Open the file
	f, err := os.Open(file)
	if err != nil {
		log.Fatal("Could not open the csv file : ", err)
	}

	// Parse the file
	r := csv.NewReader(f)

	// Iterate through the records
	var s []string
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if svcName == "athena" {
			s = append(s, fmt.Sprintf("%s_%s", record[0], record[1]))
		} else {
			s = append(s, record[1])
		}
	}
	return s
}
