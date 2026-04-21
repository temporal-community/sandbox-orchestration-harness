package main

import (
	"crypto/tls"
	"log"
	"os"

	wfIface "github.com/temporalio/ephemeral-workers-poc/sdk/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	backend "github.com/temporalio/ephemeral-workers-poc/backend"
	_ "github.com/temporalio/ephemeral-workers-poc/backend/compute/agentcore"
	_ "github.com/temporalio/ephemeral-workers-poc/backend/compute/ecs"
	_ "github.com/temporalio/ephemeral-workers-poc/backend/compute/lambda"
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

	w := worker.New(c, wfIface.SandboxWorkflowTaskQueueName, worker.Options{})
	w.RegisterWorkflow(backend.SandboxWorkflow)
	w.RegisterActivity(backend.StartSandbox)
	w.RegisterActivity(backend.StopSandbox)

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
