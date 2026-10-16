package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	example "github.com/temporal-community/sandbox-orchestration-harness/examples/database-fork"
	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func main() {
	provider, err := providerFromEnv()
	if err != nil {
		log.Fatal(err)
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
		provider,
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

// providerFromEnv reads the compute provider from SANDBOX_PROVIDER and its
// configuration from SANDBOX_PROVIDER_CONFIG, a JSON object of string values,
// so the example runs against any provider without being rebuilt.
func providerFromEnv() (sandbox.Provider, error) {
	typ := os.Getenv("SANDBOX_PROVIDER")
	if typ == "" {
		return sandbox.Provider{}, fmt.Errorf("set SANDBOX_PROVIDER to the compute provider to run against")
	}
	cfg := map[string]string{}
	if raw := os.Getenv("SANDBOX_PROVIDER_CONFIG"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return sandbox.Provider{}, fmt.Errorf("SANDBOX_PROVIDER_CONFIG must be a JSON object of strings: %w", err)
		}
	}
	return sandbox.Provider{Type: compute.ProviderType(typ), Config: cfg}, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
