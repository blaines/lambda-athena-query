package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/blaines/lambda-athena-query"
	"os"
	"sync"
	"time"
)

var (
	// Version is set by Makefile ldflags
	Version = "undefined"
	// BuildDate is set by Makefile ldflags
	BuildDate string
	// GitCommit is set by Makefile ldflags
	GitCommit string
	// GitBranch is set by Makefile ldflags
	GitBranch string
	// GitSummary is set by Makefile ldflags
	GitSummary string
)

var (
	// ErrNameNotProvided is thrown when a name is not provided
	ErrNameNotProvided = errors.New("no name was provided in the HTTP body")
)

// HandleLambdaEvent is the Lambda function handler
func HandleLambdaEvent(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	queryString := request.Body
	asyncBool := false
	// if os.Getenv("ASYNC") == "true" {
	// 	asyncBool = true
	// }
	resultStream := make(chan []string)
	checkInterval, _ := time.ParseDuration(os.Getenv("CHECK_INTERVAL"))
	resultMap := make(map[string][]map[string]string)

	var wg sync.WaitGroup
	lineNo := 0
	go func() {
		var csvHeader []string
		for result := range resultStream {
			csvBody := make(map[string]string, len(csvHeader))
			wg.Add(1)
			if lineNo == 0 {
				csvHeader = result
			} else {
				for rowID, rowValue := range result {
					if rowValue != "" {
						csvBody[csvHeader[rowID]] = rowValue
					}
				}
				resultMap["result"] = append(resultMap["result"], csvBody)
			}

			lineNo++
			wg.Done()
		}
	}()

	qid, err := extract.Extract(os.Getenv("ATHENA_DATABASE"), os.Getenv("ATHENA_OUTPUT_BUCKET"), queryString, checkInterval, asyncBool, resultStream)
	fmt.Printf("Processing request data for request %s.\n", request.RequestContext.RequestID)
	fmt.Printf("Body size = %d.\n", len(request.Body))
	fmt.Println("Headers:")
	for key, value := range request.Headers {
		fmt.Printf("    %s: %s\n", key, value)
	}
	fmt.Println(qid, err)
	fmt.Println(resultMap)
	jsonString, _ := json.Marshal(resultMap)

	return events.APIGatewayProxyResponse{Body: string(jsonString), StatusCode: 200}, nil
}

func main() {
	lambda.Start(HandleLambdaEvent)
}
