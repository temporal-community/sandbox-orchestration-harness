package main

import (
	"crypto/tls"
	"log"
	"log/slog"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	example "github.com/temporal-community/sandbox-orchestration-harness/examples/explicit-suspend-resume"
	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
)

func main() {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(h))

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

	w := worker.New(c, example.TaskQueue, worker.Options{})

	w.RegisterWorkflow(example.ExplicitSuspendResumeWorkflow)
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
