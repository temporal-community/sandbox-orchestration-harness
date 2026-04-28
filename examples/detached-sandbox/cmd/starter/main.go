package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	example "github.com/temporal-community/sandbox-orchestration-harness/examples/detached-sandbox"
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
		example.DetachedSandboxWorkflow,
	)
	if err != nil {
		log.Fatalf("unable to start workflow: %v", err)
	}

	fmt.Printf("started WorkflowID=%s RunID=%s\n", run.GetID(), run.GetRunID())

	var result example.CreatorResult
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalf("creator workflow failed: %v", err)
	}

	fmt.Printf("creator completed — sandbox %s still running, handoff workflow %s in progress\n",
		result.SandboxRef, result.HandoffWorkflowID)

	handoffRun := c.GetWorkflow(context.Background(), result.HandoffWorkflowID, "")
	var fileListing string
	if err := handoffRun.Get(context.Background(), &fileListing); err != nil {
		log.Fatalf("handoff workflow failed: %v", err)
	}

	fmt.Printf("handoff completed — file listing:\n%s\n", fileListing)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
