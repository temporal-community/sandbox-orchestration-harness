package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	consumer "github.com/temporalio/sandbox-consumer-example"
)

func main() {
	steps := []consumer.Step{
		{Type: consumer.StepCreateFile, Path: "/tmp/hello.txt", Content: "hello, Temporal!"},
		{Type: consumer.StepReadFile, Path: "/tmp/hello.txt"},
		{Type: consumer.StepListFiles, Path: "/tmp"},
		{Type: consumer.StepListFiles, Path: "/tmp", Sandboxed: true},
		{Type: consumer.StepCreateFile, Path: "/tmp/hello.txt", Content: "hello, Temporal!", Sandboxed: true},
		{Type: consumer.StepListFiles, Path: "/tmp", Sandboxed: true},
	}

	opts := client.Options{
		HostPort:  getEnv("TEMPORAL_HOST_PORT", "localhost:7233"),
		Namespace: getEnv("TEMPORAL_NAMESPACE", "ephemeral-worker-poc"),
	}

	if apiKey := os.Getenv("TEMPORAL_API_KEY"); apiKey != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
		opts.ConnectionOptions.TLS = &tls.Config{}
	}

	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("unable to create Temporal client: %v", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{
			TaskQueue: consumer.TaskQueue,
		},
		consumer.FileOpsWorkflow,
		consumer.WorkflowInput{Steps: steps},
	)
	if err != nil {
		log.Fatalf("unable to start workflow: %v", err)
	}

	fmt.Printf("started WorkflowID=%s RunID=%s\n", run.GetID(), run.GetRunID())

	var result consumer.WorkflowResult
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}

	for i, r := range result.Results {
		if r != "" {
			fmt.Printf("step[%d]: %s\n", i, r)
		}
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
