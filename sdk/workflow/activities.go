package workflow

import (
	"context"
	"errors"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/temporal"
)

func wrapUnsupported(err error) error {
	if errors.Is(err, errors.ErrUnsupported) {
		return temporal.NewNonRetryableApplicationError(err.Error(), errUnsupportedType, err)
	}
	return err
}

func wrapProviderConfig(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), "ProviderConfigError", err)
}

type StartSandboxInput struct {
	Provider      compute.ProviderDetails `json:"provider"`
	TaskQueueName string                  `json:"task_queue_name"`
}

type StartSandboxOutput struct {
	Status *compute.ProviderStatus `json:"status"`
}

type StopSandboxInput struct {
	Provider compute.ProviderDetails `json:"provider"`
	Status   *compute.ProviderStatus `json:"status"`
}

func StartSandbox(ctx context.Context, input StartSandboxInput) (StartSandboxOutput, error) {
	provider, err := compute.Lookup(input.Provider.Type, input.Provider.Config)
	if err != nil {
		return StartSandboxOutput{}, wrapProviderConfig(err)
	}
	status, err := provider.Start(ctx, input.TaskQueueName)
	if err != nil {
		return StartSandboxOutput{}, wrapUnsupported(err)
	}
	return StartSandboxOutput{Status: status}, nil
}

func StopSandbox(ctx context.Context, input StopSandboxInput) error {
	provider, err := compute.Lookup(input.Provider.Type, input.Provider.Config)
	if err != nil {
		return wrapProviderConfig(err)
	}
	return wrapUnsupported(provider.Stop(ctx, input.Status))
}

type SuspendSandboxInput struct {
	Provider compute.ProviderDetails `json:"provider"`
	Status   *compute.ProviderStatus `json:"status"`
}

func SuspendSandbox(ctx context.Context, input SuspendSandboxInput) error {
	provider, err := compute.Lookup(input.Provider.Type, input.Provider.Config)
	if err != nil {
		return wrapProviderConfig(err)
	}
	return wrapUnsupported(provider.Suspend(ctx, input.Status))
}

type ResumeSandboxInput struct {
	Provider compute.ProviderDetails `json:"provider"`
	Status   *compute.ProviderStatus `json:"status"`
}

func ResumeSandbox(ctx context.Context, input ResumeSandboxInput) error {
	provider, err := compute.Lookup(input.Provider.Type, input.Provider.Config)
	if err != nil {
		return wrapProviderConfig(err)
	}
	return wrapUnsupported(provider.Resume(ctx, input.Status))
}

type ExecuteCommandInput struct {
	Provider compute.ProviderDetails `json:"provider"`
	Status   *compute.ProviderStatus `json:"status"`
	Command  string                  `json:"command"`
}

type ExecuteCommandOutput struct {
	Result *compute.CommandResult `json:"result"`
}

func ExecuteCommand(ctx context.Context, input ExecuteCommandInput) (ExecuteCommandOutput, error) {
	provider, err := compute.Lookup(input.Provider.Type, input.Provider.Config)
	if err != nil {
		return ExecuteCommandOutput{}, wrapProviderConfig(err)
	}
	result, err := provider.ExecuteCommand(ctx, input.Status, input.Command)
	if err != nil {
		return ExecuteCommandOutput{}, wrapUnsupported(err)
	}
	return ExecuteCommandOutput{Result: result}, nil
}

type SnapshotSandboxInput struct {
	Provider compute.ProviderDetails `json:"provider"`
	Status   *compute.ProviderStatus `json:"status"`
}

type SnapshotSandboxOutput struct {
	SandboxState compute.SandboxPostSnapshotState `json:"sandbox_state"`
	Snapshot     *compute.ProviderSnapshot        `json:"snapshot"`
}

func SnapshotSandbox(ctx context.Context, input SnapshotSandboxInput) (SnapshotSandboxOutput, error) {
	provider, err := compute.Lookup(input.Provider.Type, input.Provider.Config)
	if err != nil {
		return SnapshotSandboxOutput{}, wrapProviderConfig(err)
	}
	state, snapshot, err := provider.Snapshot(ctx, input.Status)
	if err != nil {
		return SnapshotSandboxOutput{}, wrapUnsupported(err)
	}
	return SnapshotSandboxOutput{SandboxState: state, Snapshot: snapshot}, nil
}

type StartSandboxFromSnapshotInput struct {
	Provider      compute.ProviderDetails   `json:"provider"`
	TaskQueueName string                    `json:"task_queue_name"`
	Snapshot      *compute.ProviderSnapshot `json:"snapshot"`
}

type StartSandboxFromSnapshotOutput struct {
	Status *compute.ProviderStatus `json:"status"`
}

func StartSandboxFromSnapshot(ctx context.Context, input StartSandboxFromSnapshotInput) (StartSandboxFromSnapshotOutput, error) {
	provider, err := compute.Lookup(input.Provider.Type, input.Provider.Config)
	if err != nil {
		return StartSandboxFromSnapshotOutput{}, wrapProviderConfig(err)
	}
	status, err := provider.StartFromSnapshot(ctx, input.TaskQueueName, input.Snapshot)
	if err != nil {
		return StartSandboxFromSnapshotOutput{}, wrapUnsupported(err)
	}
	return StartSandboxFromSnapshotOutput{Status: status}, nil
}

type DeleteSnapshotInput struct {
	Provider compute.ProviderDetails   `json:"provider"`
	Snapshot *compute.ProviderSnapshot `json:"snapshot"`
}

func DeleteSnapshot(ctx context.Context, input DeleteSnapshotInput) error {
	provider, err := compute.Lookup(input.Provider.Type, input.Provider.Config)
	if err != nil {
		return wrapProviderConfig(err)
	}
	return wrapUnsupported(provider.DeleteSnapshot(ctx, input.Snapshot))
}
