package backend

import (
	"context"

	backendcompute "github.com/temporalio/ephemeral-workers-poc/backend/compute"
	sdkcompute "github.com/temporalio/ephemeral-workers-poc/sdk/compute"
)

type StartSandboxInput struct {
	ProviderType sdkcompute.ComputeProviderType
	Config       map[string]string

	TaskQueueName string
}

type StartSandboxOutput struct {
	Status *backendcompute.ComputeProviderStatus
}

type StopSandboxInput struct {
	ProviderType sdkcompute.ComputeProviderType
	Config       map[string]string
	Status       *backendcompute.ComputeProviderStatus
}

func StartSandbox(ctx context.Context, input StartSandboxInput) (StartSandboxOutput, error) {
	provider, err := backendcompute.Lookup(input.ProviderType, input.Config)
	if err != nil {
		return StartSandboxOutput{}, err
	}
	status, err := provider.Start(ctx, input.TaskQueueName)
	if err != nil {
		return StartSandboxOutput{}, err
	}
	return StartSandboxOutput{Status: status}, nil
}

func StopSandbox(ctx context.Context, input StopSandboxInput) error {
	provider, err := backendcompute.Lookup(input.ProviderType, input.Config)
	if err != nil {
		return err
	}
	return provider.Stop(ctx, input.Status)
}
