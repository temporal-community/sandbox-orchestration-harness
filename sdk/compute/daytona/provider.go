package daytona

import (
	"context"
	"errors"
	"fmt"

	daytonasdk "github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"
	daytonatypes "github.com/daytonaio/daytona/libs/sdk-go/pkg/types"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func init() {
	compute.Register(compute.ProviderTypeDaytona, newDaytonaProvider)
}

func newDaytonaProvider(config map[string]string) (compute.Provider, error) {
	var client *daytonasdk.Client
	var err error
	if region := config["region"]; region != "" {
		client, err = daytonasdk.NewClientWithConfig(&daytonatypes.DaytonaConfig{Target: region})
	} else {
		client, err = daytonasdk.NewClient()
	}
	if err != nil {
		return nil, fmt.Errorf("daytona: create client: %w", err)
	}
	image := config["image"]
	if image == "" {
		return nil, fmt.Errorf("daytona: image required")
	}
	return &daytonaProvider{client: client, image: image}, nil
}

type daytonaProvider struct {
	client *daytonasdk.Client
	image  string
}

// Start creates a Daytona sandbox. The task queue name is passed via the
// TEMPORAL_TASK_QUEUE env var; the container's entrypoint reads it and starts the worker.
func (p *daytonaProvider) Start(ctx context.Context, taskQueueName string) (*compute.ProviderStatus, error) {
	sandbox, err := p.client.Create(ctx, daytonatypes.ImageParams{
		SandboxBaseParams: daytonatypes.SandboxBaseParams{
			EnvVars: map[string]string{
				"TEMPORAL_TASK_QUEUE": taskQueueName,
			},
		},
		Image: p.image,
	})
	if err != nil {
		return nil, fmt.Errorf("daytona: create sandbox: %w", err)
	}
	return &compute.ProviderStatus{InstanceID: sandbox.ID}, nil
}

func (p *daytonaProvider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	sandbox, err := p.client.Get(ctx, status.InstanceID)
	if err != nil {
		var notFound *daytonaerrors.DaytonaNotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("daytona: get sandbox: %w", err)
	}
	if err := sandbox.Delete(ctx); err != nil {
		var notFound *daytonaerrors.DaytonaNotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("daytona: delete sandbox: %w", err)
	}
	return nil
}

func (p *daytonaProvider) Suspend(ctx context.Context, status *compute.ProviderStatus) error {
	sandbox, err := p.client.Get(ctx, status.InstanceID)
	if err != nil {
		return fmt.Errorf("daytona: get sandbox: %w", err)
	}
	if err := sandbox.Stop(ctx); err != nil {
		return fmt.Errorf("daytona: stop sandbox: %w", err)
	}
	return nil
}

func (p *daytonaProvider) Resume(ctx context.Context, status *compute.ProviderStatus) error {
	sandbox, err := p.client.Get(ctx, status.InstanceID)
	if err != nil {
		return fmt.Errorf("daytona: get sandbox: %w", err)
	}
	if err := sandbox.Start(ctx); err != nil {
		return fmt.Errorf("daytona: start sandbox: %w", err)
	}
	return nil
}

func (p *daytonaProvider) Snapshot(_ context.Context, _ *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("daytona: Snapshot: %w", errors.ErrUnsupported)
}

func (p *daytonaProvider) StartFromSnapshot(_ context.Context, _ string, _ *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	return nil, fmt.Errorf("daytona: StartFromSnapshot: %w", errors.ErrUnsupported)
}

func (p *daytonaProvider) DeleteSnapshot(_ context.Context, _ *compute.ProviderSnapshot) error {
	return fmt.Errorf("daytona: DeleteSnapshot: %w", errors.ErrUnsupported)
}

func (p *daytonaProvider) ExecuteCommand(ctx context.Context, status *compute.ProviderStatus, cmd string) (*compute.CommandResult, error) {
	sandbox, err := p.client.Get(ctx, status.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("daytona: get sandbox: %w", err)
	}
	result, err := sandbox.Process.ExecuteCommand(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("daytona: execute command: %w", err)
	}
	return &compute.CommandResult{
		Stdout:   result.Result,
		Stderr:   "", // Daytona only returns stdout
		ExitCode: int32(result.ExitCode),
	}, nil
}
