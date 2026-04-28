package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	example "github.com/temporal-community/sandbox-orchestration-harness/examples/snapshot-fork"
)

func main() {
	opts := client.Options{
		HostPort:  getEnv("TEMPORAL_HOST_PORT", "localhost:7233"),
		Namespace: os.Getenv("TEMPORAL_NAMESPACE"),
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
			TaskQueue: example.TaskQueue,
		},
		example.SnapshotForkWorkflow,
	)
	if err != nil {
		log.Fatalf("unable to start workflow: %v", err)
	}

	fmt.Printf("started WorkflowID=%s RunID=%s\n", run.GetID(), run.GetRunID())

	var result example.WorkflowResult
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}

	fmt.Printf("origin sandbox files (expects: shared.txt only):\n%s\n", result.OriginFiles)
	fmt.Printf("fork-a sandbox files (expects: fork-a.txt + shared.txt):\n%s\n", result.ForkAFiles)
	fmt.Printf("fork-b sandbox files (expects: fork-b.txt + shared.txt):\n%s\n", result.ForkBFiles)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
