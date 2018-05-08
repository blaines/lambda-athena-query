package main

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"os"
	"time"
)

func main() {
	athenaDatabase := os.Getenv("ATHENA_DATABASE")
	athenaOutputBucket := os.Getenv("ATHENA_OUTPUT_BUCKET")
	athenaQuery := os.Getenv("ATHENA_QUERY")
	svc := athena.New(session.Must(session.NewSession()))

	// Function1
	// Set Database to query
	queryExecutionContext := &athena.QueryExecutionContext{Database: &athenaDatabase}

	// Set results of the query
	// SHA1/YYYY/MM/DD/UUID
	athenaComposedOutputLocation := fmt.Sprintf("s3://%s/%x", athenaOutputBucket, sha1.Sum([]byte(athenaQuery)))
	// fmt.Println(athenaComposedOutputLocation)
	resultConfiguration := &athena.ResultConfiguration{OutputLocation: &athenaComposedOutputLocation}

	// Create the StartQueryExecutionRequest to send to Athena which will start the query.
	startQueryExecutionInput := &athena.StartQueryExecutionInput{
		QueryExecutionContext: queryExecutionContext,
		ResultConfiguration:   resultConfiguration,
		QueryString:           &athenaQuery,
	}

	// Example sending a request using the StartQueryExecutionRequest method.
	reqa, respa := svc.StartQueryExecutionRequest(startQueryExecutionInput)

	erra := reqa.Send()
	if erra == nil {
		// fmt.Println(respa)
	} else {
		// fmt.Println(erra)
	}

	// Function2

	getQueryExecutionInput := &athena.GetQueryExecutionInput{
		QueryExecutionId: respa.QueryExecutionId,
	}

	queryExecuting := true

	for queryExecuting {
		reqb, respb := svc.GetQueryExecutionRequest(getQueryExecutionInput)

		errb := reqb.Send()
		if errb != nil {
			// fmt.Println(errb)
		} else {
			// fmt.Println(respb)
		}

		status := respb.QueryExecution.Status.State
		switch *status {
		case "RUNNING":
			queryExecuting = true
			time.Sleep(250 * time.Millisecond) // TODO: Environment variable
		case "SUCCEEDED":
			queryExecuting = false
		case "CANCELLED":
			queryExecuting = false
			fmt.Errorf("%s", status)
		case "FAILED":
			queryExecuting = false
			fmt.Errorf("%s", status)
		}
	}

	// Function3
	getQueryResultsInput := &athena.GetQueryResultsInput{
		// MaxResults:       1000,
		QueryExecutionId: respa.QueryExecutionId,
	}

	resultFunc := func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
		// fmt.Println(len(page.ResultSet.Rows), "Rows")
		resultMap := make(map[string][]map[string]string)
		var csvHeader []string

		for r, row := range page.ResultSet.Rows {
			csvBody := make(map[string]string)
			for c, column := range row.Data {
				if column.VarCharValue != nil {
					if r == 0 {
						csvHeader = append(csvHeader, *column.VarCharValue)
					} else {
						csvBody[csvHeader[c]] = *column.VarCharValue
					}
				}
			}
			if r != 0 {
				resultMap["result"] = append(resultMap["result"], csvBody)
			}
		}

		jsonString, _ := json.Marshal(resultMap)
		fmt.Println(string(jsonString))
		return !lastPage
	}

	err := svc.GetQueryResultsPages(getQueryResultsInput, resultFunc)
	if err != nil {
		fmt.Errorf("error")
	}
}
