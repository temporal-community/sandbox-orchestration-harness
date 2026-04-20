package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	consumer "github.com/temporalio/sandbox-consumer-example"
	"github.com/temporalio/sandbox-sdk-go/compute"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

var activeTasks int32

func main() {
	temporalOpts, err := buildClientOptions()
	if err != nil {
		log.Fatalf("build client options: %v", err)
	}

	// POST / — async task entrypoint (add_async_task equivalent)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		payload := compute.ComputeProviderInvocationPayload{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.TaskQueue == "" {
			http.Error(w, "taskQueue required", http.StatusBadRequest)
			return
		}

		atomic.AddInt32(&activeTasks, 1) // add_async_task
		go func() {
			defer atomic.AddInt32(&activeTasks, -1) // complete_async_task

			c, err := client.Dial(temporalOpts)
			if err != nil {
				log.Printf("dial temporal: %v", err)
				return
			}
			defer c.Close()

			wrk := worker.New(c, payload.TaskQueue, worker.Options{
				DeploymentOptions: worker.DeploymentOptions{UseVersioning: false},
			})
			wrk.RegisterActivity(consumer.CreateFile)
			wrk.RegisterActivity(consumer.ReadFile)
			wrk.RegisterActivity(consumer.ListFiles)

			if err := wrk.Run(worker.InterruptCh()); err != nil {
				log.Printf("worker error: %v", err)
			}
		}()

		w.WriteHeader(http.StatusAccepted)
	})

	// GET /ping — health status for AgentCore session management
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		status := "Healthy"
		if atomic.LoadInt32(&activeTasks) > 0 {
			status = "HealthyBusy"
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":%q}`, status)
	})

	port := getEnv("PORT", "8080")
	log.Printf("listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

func buildClientOptions() (client.Options, error) {
	opts := client.Options{
		HostPort:  getEnv("TEMPORAL_HOST_PORT", "localhost:7233"),
		Namespace: getEnv("TEMPORAL_NAMESPACE", "ephemeral-worker-poc"),
	}
	if os.Getenv("ENABLE_TLS") == "" {
		return opts, nil
	}

	tlsConfig := &tls.Config{InsecureSkipVerify: true}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return opts, fmt.Errorf("load aws config: %w", err)
	}
	svc := secretsmanager.NewFromConfig(cfg)

	tlsKey := os.Getenv("TLS_KEY")
	tlsCert := os.Getenv("TLS_CERT")
	if tlsKey != "" && tlsCert != "" {
		certSecret, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &tlsCert})
		if err != nil {
			return opts, fmt.Errorf("get tls cert: %w", err)
		}
		keySecret, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &tlsKey})
		if err != nil {
			return opts, fmt.Errorf("get tls key: %w", err)
		}
		cert, err := tls.X509KeyPair([]byte(*certSecret.SecretString), []byte(*keySecret.SecretString))
		if err != nil {
			return opts, fmt.Errorf("parse tls keypair: %w", err)
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
	}

	if apiKey := os.Getenv("API_KEY"); apiKey != "" {
		secret, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &apiKey})
		if err != nil {
			return opts, fmt.Errorf("get api key: %w", err)
		}
		opts.Credentials = client.NewAPIKeyStaticCredentials(*secret.SecretString)
	}

	opts.ConnectionOptions.TLS = tlsConfig
	return opts, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
