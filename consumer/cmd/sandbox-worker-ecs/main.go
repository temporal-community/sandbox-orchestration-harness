package main

import (
	"context"
	"crypto/tls"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	consumer "github.com/temporalio/ephemeral-workers-poc/consumer"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	taskQueue := os.Getenv("TQ_NAME")
	if taskQueue == "" {
		log.Fatal("TQ_NAME environment variable is required")
	}

	opts := client.Options{
		HostPort:  getEnv("TEMPORAL_HOST_PORT", "localhost:7233"),
		Namespace: getEnv("TEMPORAL_NAMESPACE", "ephemeral-worker-poc"),
	}

	if os.Getenv("ENABLE_TLS") != "" {
		tlsConfig := &tls.Config{InsecureSkipVerify: true}

		ctx := context.Background()
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			log.Fatalf("load aws config: %v", err)
		}
		svc := secretsmanager.NewFromConfig(cfg)

		tlsKey := os.Getenv("TLS_KEY")
		tlsCert := os.Getenv("TLS_CERT")
		if tlsKey != "" && tlsCert != "" {
			certSecret, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &tlsCert})
			if err != nil {
				log.Fatalf("get tls cert: %v", err)
			}
			keySecret, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &tlsKey})
			if err != nil {
				log.Fatalf("get tls key: %v", err)
			}
			cert, err := tls.X509KeyPair([]byte(*certSecret.SecretString), []byte(*keySecret.SecretString))
			if err != nil {
				log.Fatalf("parse tls keypair: %v", err)
			}
			tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
		}

		if apiKey := os.Getenv("API_KEY"); apiKey != "" {
			secret, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &apiKey})
			if err != nil {
				log.Fatalf("get api key: %v", err)
			}
			opts.Credentials = client.NewAPIKeyStaticCredentials(*secret.SecretString)
		}

		opts.ConnectionOptions.TLS = tlsConfig
	}

	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("dial temporal: %v", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning: false,
		},
	})
	w.RegisterActivity(consumer.CreateFile)
	w.RegisterActivity(consumer.ReadFile)
	w.RegisterActivity(consumer.ListFiles)

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
