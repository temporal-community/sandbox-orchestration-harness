package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"

	example "github.com/temporal-community/sandbox-orchestration-harness/examples/database-fork"
)

func main() {
	c, err := client.Dial(client.Options{
		HostPort:  getEnv("TEMPORAL_HOST_PORT", "localhost:7233"),
		Namespace: os.Getenv("TEMPORAL_NAMESPACE"),
	})
	if err != nil {
		log.Fatalf("unable to create Temporal client: %v", err)
	}
	defer c.Close()

	w := worker.New(c, example.TaskQueue, worker.Options{})
	w.RegisterWorkflow(example.DatabaseForkWorkflow)
	sandbox.Register(w, c)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker exited with error: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
