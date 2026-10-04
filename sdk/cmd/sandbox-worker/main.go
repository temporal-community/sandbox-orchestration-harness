// ABOUTME: Standalone worker that hosts SandboxWorkflow and its activities on a dedicated task queue.
// ABOUTME: Run it alongside workflows that use sandbox.WithTaskQueue, including workflows in other languages.
package main

import (
	"crypto/tls"
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
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

	taskQueue := getEnv("SANDBOX_TASK_QUEUE", sandbox.DefaultSandboxTaskQueue)
	w := worker.New(c, taskQueue, worker.Options{})
	sandbox.Register(w, c)

	log.Printf("sandbox worker polling task queue %q", taskQueue)
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
