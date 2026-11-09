package main

import (
	"crypto/tls"
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	sandboxnexus "github.com/temporal-community/sandbox-orchestration-harness/sdk/nexus"
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

	queue := getEnv("SANDBOX_TASK_QUEUE", sandboxnexus.DefaultTaskQueue)
	w := worker.New(c, queue, worker.Options{})

	if err := sandboxnexus.Register(w, c, sandboxnexus.Config{TaskQueue: queue}); err != nil {
		log.Fatalf("unable to register Nexus service: %v", err)
	}

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
