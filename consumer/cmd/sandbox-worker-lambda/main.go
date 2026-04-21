package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	consumer "github.com/temporalio/ephemeral-workers-poc/consumer"
	"github.com/temporalio/ephemeral-workers-poc/sdk/compute"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/aws/lambdaworker"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func main() {
	lambdaworker.RunWorker(
		worker.WorkerDeploymentVersion{
			DeploymentName: "dummy",
			BuildID:        "dummy",
		},
		func(ctx *lambdaworker.Options) error {
			ctx.PerInvocationOptions = func(ctx context.Context, event json.RawMessage, options *lambdaworker.Options) error {
				payload := compute.ComputeProviderInvocationPayload{}
				if err := json.Unmarshal(event, &payload); err != nil || payload.TaskQueue == "" {
					payload.TaskQueue = os.Getenv("DEFAULT_TQ_NAME")
				}
				options.TaskQueue = payload.TaskQueue
				return nil
			}

			// need to set a default here for the moment
			ctx.TaskQueue = "dummy"

			// disable versioning for the ephemeral worker
			ctx.WorkerOptions.DeploymentOptions.UseVersioning = false
			ctx.WorkerOptions.DeploymentOptions.DefaultVersioningBehavior = workflow.VersioningBehaviorUnspecified

			enableTLS := os.Getenv("ENABLE_TLS")
			tlsKey := os.Getenv("TLS_KEY")
			tlsCert := os.Getenv("TLS_CERT")
			apiKey := os.Getenv("API_KEY")

			localCtx := context.Background()

			var tlsConfig *tls.Config
			var credentials client.Credentials

			if enableTLS != "" {
				tlsConfig = &tls.Config{InsecureSkipVerify: true}

				config, err := config.LoadDefaultConfig(localCtx)
				if err != nil {
					return err
				}
				svc := secretsmanager.NewFromConfig(config)

				if tlsKey != "" && tlsCert != "" {
					clientCert, err := svc.GetSecretValue(localCtx, &secretsmanager.GetSecretValueInput{SecretId: &tlsCert})
					if err != nil {
						return err
					}
					clientKey, err := svc.GetSecretValue(localCtx, &secretsmanager.GetSecretValueInput{SecretId: &tlsKey})
					if err != nil {
						return err
					}

					cert, err := tls.X509KeyPair([]byte(*clientCert.SecretString), []byte(*clientKey.SecretString))
					if err != nil {
						return err
					}
					tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
				}

				if apiKey != "" {
					apiKeyValue, err := svc.GetSecretValue(localCtx, &secretsmanager.GetSecretValueInput{SecretId: &apiKey})
					if err != nil {
						return err
					}
					credentials = client.NewAPIKeyStaticCredentials(*apiKeyValue.SecretString)
				}
			}
			ctx.ClientOptions.ConnectionOptions = client.ConnectionOptions{
				TLS: tlsConfig,
			}
			ctx.ClientOptions.Credentials = credentials

			ctx.RegisterActivity(consumer.CreateFile)
			ctx.RegisterActivity(consumer.ReadFile)
			ctx.RegisterActivity(consumer.ListFiles)

			return nil
		})
}
