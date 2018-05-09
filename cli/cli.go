package main

import (
	"fmt"
	"github.com/blaines/lambda-athena-query"
	"gopkg.in/urfave/cli.v1"
	"os"
	"sync"
	// "strings"
	"text/tabwriter"
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

func main() {
	app := cli.NewApp()
	app.Name = "aq"
	app.Usage = "Query AWS Athena"
	app.Version = Version
	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Printf("version=%s buildDate=%s sha=%s branch=%s (%s)\n", c.App.Version, BuildDate, GitCommit, GitBranch, GitSummary)
	}

	app.Commands = []cli.Command{
		{
			Name:  "extract",
			Usage: "Extract data from athena with a SQL query",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "query, q",
					Usage:  "query",
					EnvVar: "ATHENA_QUERY",
				},
				cli.StringFlag{
					Name:   "database, d",
					Usage:  "database",
					EnvVar: "ATHENA_DATABASE",
				},
				cli.StringFlag{
					Name:   "bucket, b",
					Usage:  "bucket",
					EnvVar: "ATHENA_OUTPUT_BUCKET",
				},
				cli.DurationFlag{
					Name:   "check-interval",
					Usage:  "Amount of time between check requests (example: 250ms, 1s)",
					Value:  250 * time.Millisecond,
					EnvVar: "CHECK_INTERVAL",
				},
				cli.BoolTFlag{
					Name:   "async, a",
					Usage:  "Asynchronous query, a query execution ID will be returned to use with the `result` command",
					EnvVar: "ASYNC_QUERY",
				},
			},
			Action: func(c *cli.Context) error {
				// TODO Need a reusable way to format streaming output data, it's repeated in the result command
				var wg sync.WaitGroup
				w := new(tabwriter.Writer)
				w.Init(os.Stdout, 0, 8, 2, ' ', 0)
				resultStream := make(chan []string)
				var header []string
				lineNo := 0
				go func() {
					for result := range resultStream {
						wg.Add(1)
						if lineNo == 0 {
							header = result
						} else {
							fmt.Fprintln(w)
							// fmt.Fprintln(w, fmt.Sprintf("Result %d", lineNo))
							fmt.Fprintln(w, fmt.Sprintf("%s\t%s", "key", "value"))
							for n, v := range result {
								fmt.Fprintln(w, fmt.Sprintf("%s\t%s", header[n], v))
								// fmt.Fprintln(w, strings.Join(result, "\t"))
							}
							w.Flush()
						}
						lineNo++
						wg.Done()
					}
				}()
				queryExecutionID, _ := extract.Extract(c.String("database"), c.String("bucket"), c.String("query"), c.Duration("check-interval"), c.BoolT("async"), resultStream)
				wg.Wait()
				fmt.Println("\nQuery execution ID:", queryExecutionID)
				return nil
			},
		},
		{
			Name:  "result",
			Usage: "Fetch the results from a query",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "query-execution-id, i",
					Usage:  "query-execution-id",
					EnvVar: "ATHENA_QUERY_EXECUTION_ID",
				},
			},
			Action: func(c *cli.Context) error {
				// TODO Need a reusable way to format streaming output data, it's the same as above
				var wg sync.WaitGroup
				w := new(tabwriter.Writer)
				w.Init(os.Stdout, 0, 8, 2, ' ', 0)
				resultStream := make(chan []string)
				var header []string
				lineNo := 0
				go func() {
					for result := range resultStream {
						wg.Add(1)
						if lineNo == 0 {
							header = result
						} else {
							fmt.Fprintln(w)
							// fmt.Fprintln(w, fmt.Sprintf("Result %d", lineNo))
							fmt.Fprintln(w, fmt.Sprintf("%s\t%s", "key", "value"))
							for n, v := range result {
								fmt.Fprintln(w, fmt.Sprintf("%s\t%s", header[n], v))
								// fmt.Fprintln(w, strings.Join(result, "\t"))
							}
							w.Flush()
						}
						lineNo++
						wg.Done()
					}
				}()

				queryExecutionID, _ := extract.GetExecutionResult(c.String("query-execution-id"), resultStream)

				wg.Wait()
				fmt.Println("\nQuery execution ID:", queryExecutionID)

				return nil
			},
		},
	}

	app.Flags = []cli.Flag{}

	app.Run(os.Args)
}
