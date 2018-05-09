package main

import (
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/blaines/lambda-athena-query"
	"os"
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

type MyEvent struct {
	Name string
	Age  int
}

type MyResponse struct {
	Message string
}

func HandleLambdaEvent(event MyEvent) (MyResponse, error) {
	asyncBool := false
	if os.Getenv("ASYNC") == "true" {
		asyncBool = true
	}
	checkInterval, _ := time.ParseDuration(os.Getenv("CHECK_INTERVAL"))
	extract.Extract(os.Getenv("ATHENA_DATABASE"), os.Getenv("ATHENA_OUTPUT_BUCKET"), "---QUERY---", checkInterval, asyncBool)
	return MyResponse{Message: fmt.Sprintf("%s is %d years old!", event.Name, event.Age)}, nil
}

func main() {
	lambda.Start(HandleLambdaEvent)
}
