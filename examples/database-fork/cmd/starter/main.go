package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	example "github.com/temporal-community/sandbox-orchestration-harness/examples/database-fork"
)

func main() {
	cfg := example.Config{
		Template:   getEnv("CRAFTING_TEMPLATE", ""),
		Workspace:  getEnv("CRAFTING_WORKSPACE", "dev"),
		Dependency: getEnv("CRAFTING_DEPENDENCY", "db"),
		Folder:     os.Getenv("CRAFTING_FOLDER"),
	}
	if cfg.Template == "" {
		log.Fatal("set CRAFTING_TEMPLATE to the Crafting template to run against")
	}

	c, err := client.Dial(client.Options{
		HostPort:  getEnv("TEMPORAL_HOST_PORT", "localhost:7233"),
		Namespace: os.Getenv("TEMPORAL_NAMESPACE"),
	})
	if err != nil {
		log.Fatalf("unable to create Temporal client: %v", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{TaskQueue: example.TaskQueue},
		example.DatabaseForkWorkflow,
		cfg,
	)
	if err != nil {
		log.Fatalf("unable to start workflow: %v", err)
	}
	fmt.Printf("started WorkflowID=%s RunID=%s\n", run.GetID(), run.GetRunID())

	var result example.WorkflowResult
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}

	fmt.Println()
	fmt.Println("database rows, per sandbox")
	fmt.Printf("  origin (expects origin):        %s\n", result.OriginRows)
	fmt.Printf("  fork-a (expects origin,fork-a): %s\n", result.ForkARows)
	fmt.Printf("  fork-b (expects origin,fork-b): %s\n", result.ForkBRows)
	fmt.Println()
	fmt.Println("home directory files, per sandbox")
	fmt.Printf("  origin (expects shared):            %s\n", result.OriginFiles)
	fmt.Printf("  fork-a (expects fork-a,shared):     %s\n", result.ForkAFiles)
	fmt.Printf("  fork-b (expects fork-b,shared):     %s\n", result.ForkBFiles)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
