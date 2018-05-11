package extract

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"time"
)

func getQueryExecutionResults(queryExecutionID string, resultStream chan []string) (string, error) {
	svc := athena.New(session.Must(session.NewSession()))
	getQueryResultsInput := &athena.GetQueryResultsInput{
		// MaxResults:       1000,
		QueryExecutionId: &queryExecutionID,
	}

	resultFunc := func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
		// fmt.Println(len(page.ResultSet.Rows), "Rows")
		resultMap := make(map[string][]map[string]string)
		var csvHeader []string

		for _, column := range page.ResultSet.ResultSetMetadata.ColumnInfo {
			// fmt.Println(c, column)
			csvHeader = append(csvHeader, *column.Name)
		}
		// fmt.Println("Header", len(csvHeader), csvHeader)
		resultStream <- csvHeader

		for n, row := range page.ResultSet.Rows {
			if n != 0 {
				// csvBody := make(map[string]string, len(csvHeader))
				csvLine := make([]string, len(csvHeader))
				for c, column := range row.Data {
					if column.VarCharValue == nil {
						// csvLine[c] = "<empty>"
					} else {
						// csvBody[csvHeader[c]] = *column.VarCharValue
						csvLine[c] = *column.VarCharValue
					}
				}
				// fmt.Println("Line", len(csvLine), csvLine)
				resultStream <- csvLine
				// resultMap["result"] = append(resultMap["result"], csvBody)
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

	// close(resultStream)

	return queryExecutionID, err
}

func GetExecutionResult(queryExecutionID string, resultStream chan []string) (string, error) {
	return getQueryExecutionResults(queryExecutionID, resultStream)
}

func Extract(athenaDatabase string, athenaOutputBucket string, athenaQuery string, checkInterval time.Duration, async bool, resultStream chan []string) (string, error) {
	svc := athena.New(session.Must(session.NewSession()))

	// Function1
	// Set Database to query
	queryExecutionContext := &athena.QueryExecutionContext{Database: &athenaDatabase}

	// Set results of the query
	// DB/SHA1/YYYY/MM/DD/UUID
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
		// fmt.Println("StartQueryExecutionRequest", respa)
	} else {
		fmt.Println("StartQueryExecutionRequest", erra)
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
			fmt.Println("GetQueryExecutionRequest", errb)
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

		for _, column := range page.ResultSet.ResultSetMetadata.ColumnInfo {
			// fmt.Println(c, column)
			csvHeader = append(csvHeader, *column.Name)
		}
		// fmt.Println("Header", len(csvHeader), csvHeader)
		resultStream <- csvHeader

		for n, row := range page.ResultSet.Rows {
			if n != 0 {
				csvBody := make(map[string]string, len(csvHeader))
				csvLine := make([]string, len(csvHeader))
				for c, column := range row.Data {
					if column.VarCharValue == nil {
						// csvLine[c] = "<empty>"
					} else {
						// csvBody[csvHeader[c]] = *column.VarCharValue
						csvLine[c] = *column.VarCharValue
					}
				}
				// fmt.Println("Line", len(csvLine), csvLine)
				resultStream <- csvLine
				resultMap["result"] = append(resultMap["result"], csvBody)
			}
		}

		// jsonString, _ := json.Marshal(resultMap)
		// fmt.Println(string(jsonString))
		return !lastPage
	}

	err := svc.GetQueryResultsPages(getQueryResultsInput, resultFunc)
	if err != nil {
		fmt.Errorf("error")
	}

	close(resultStream)

	return *respa.QueryExecutionId, err
}
