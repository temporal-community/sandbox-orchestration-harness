package main

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"os"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	sandboxnexus "github.com/temporal-community/sandbox-orchestration-harness/sdk/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// Load the provider configuration for each profile name.
	data, err := os.ReadFile(os.Getenv("SANDBOX_PROFILES_FILE"))
	if err != nil {
		log.Fatal("read SANDBOX_PROFILES_FILE: ", err)
	}
	var profiles map[string]compute.ProviderDetails
	if err := json.Unmarshal(data, &profiles); err != nil {
		log.Fatal(err)
	}
	opts := client.Options{HostPort: env("TEMPORAL_HOST_PORT", "localhost:7233"), Namespace: env("TEMPORAL_NAMESPACE", "default")}
	if apiKey := os.Getenv("TEMPORAL_API_KEY"); apiKey != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
		opts.ConnectionOptions.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	queue := env("SANDBOX_TASK_QUEUE", sandboxnexus.DefaultTaskQueue)
	w := worker.New(c, queue, worker.Options{})
	if err := sandboxnexus.Register(w, c, sandboxnexus.Config{TaskQueue: queue, Profiles: profiles}); err != nil {
		log.Fatal(err)
	}
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
