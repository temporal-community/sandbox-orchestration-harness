package main

import (
	"crypto/tls"
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	consumer "github.com/temporalio/ephemeral-workers-poc/consumer"
	sandbox "github.com/temporalio/ephemeral-workers-poc/sdk"
)

func main() {
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

	w := worker.New(c, consumer.TaskQueue, worker.Options{})

	w.RegisterWorkflow(consumer.FileOpsWorkflow)
	w.RegisterActivity(consumer.CreateFile)
	w.RegisterActivity(consumer.ReadFile)
	w.RegisterActivity(consumer.ListFiles)
	sandbox.RegisterHelperActivities(w, c)

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
