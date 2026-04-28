// Package compute contains the abstraction for the different sandbox providers
package compute

import (
	"context"
	"time"
)

// NoIdleTimeout is a sentinel value for IdleTimeout meaning the sandbox will
// never be automatically suspended due to inactivity.
const NoIdleTimeout time.Duration = -1

// SandboxPostSnapshotState describes the sandbox's state after Snapshot returns.
type SandboxPostSnapshotState int

const (
	// SandboxPostSnapshotRunning means the sandbox is still running after the snapshot;
	// the workflow will not change its lifecycle state.
	SandboxPostSnapshotRunning SandboxPostSnapshotState = iota
	// SandboxPostSnapshotSuspended means the provider paused the sandbox during
	// snapshotting; the workflow transitions to SandboxLifecycleSuspendedWithSnapshot
	// and will restart from this snapshot on the next Resume.
	SandboxPostSnapshotSuspended
	// SandboxPostSnapshotDeleted means the compute resource was destroyed during
	// snapshotting; the workflow treats this as a terminal state and begins shutdown.
	// Do not return this value unless the underlying instance has been permanently destroyed.
	SandboxPostSnapshotDeleted
)

type (
	ProviderType string

	ProviderDetails struct {
		Type   ProviderType      `json:"type"`
		Config map[string]string `json:"config"`
	}

	ProviderStatus struct {
		InstanceID string `json:"instance_id"`
	}
	ProviderSnapshot struct {
		SnapshotID string `json:"snapshot_id"`
	}
	CommandResult struct {
		Stdout   string `json:"stdout"`
		Stderr   string `json:"stderr"`
		ExitCode int32  `json:"exit_code"`
	}

	// Provider is the interface that compute backends must implement to be used
	// as sandbox providers. Register a constructor via compute.Register (typically
	// from an init function) so the SDK can locate the provider by type name.
	//
	// Lifecycle contract:
	//   - Start must return a ProviderStatus whose InstanceID is sufficient to
	//     identify the sandbox in all subsequent calls (Stop, Suspend, ExecuteCommand, etc.).
	//   - taskQueueName passed to Start and StartFromSnapshot is the Temporal task
	//     queue the provisioned instance should poll; it must be passed to the worker
	//     process started inside the sandbox.
	//   - Suspend, Resume, Snapshot, StartFromSnapshot, and DeleteSnapshot must return
	//     errors.ErrUnsupported (or wrap it via fmt.Errorf("...: %w", errors.ErrUnsupported))
	//     when the operation is not supported. The workflow detects this via errors.Is,
	//     which traverses the error chain, and falls back to snapshot-based suspend for Suspend.
	//   - Snapshot returns a SandboxPostSnapshotState describing the sandbox's fate;
	//     see the constant documentation for the workflow's response to each value.
	Provider interface {
		Start(ctx context.Context, taskQueueName string) (*ProviderStatus, error)
		Stop(ctx context.Context, status *ProviderStatus) error

		Suspend(ctx context.Context, status *ProviderStatus) error
		Resume(ctx context.Context, status *ProviderStatus) error

		Snapshot(ctx context.Context, status *ProviderStatus) (SandboxPostSnapshotState, *ProviderSnapshot, error)
		StartFromSnapshot(ctx context.Context, taskQueueName string, snapshot *ProviderSnapshot) (*ProviderStatus, error)
		DeleteSnapshot(ctx context.Context, snapshot *ProviderSnapshot) error

		ExecuteCommand(ctx context.Context, status *ProviderStatus, cmd string) (*CommandResult, error)
	}
)

const (
	ProviderTypeAgentCoreRuntime ProviderType = "aws-agentcore-runtime"
	ProviderTypeDaytona          ProviderType = "daytona"
	ProviderTypeE2B              ProviderType = "e2b"
	ProviderTypeModal            ProviderType = "modal"
	ProviderTypeGKEAgentSandbox  ProviderType = "gke-agent-sandbox"
)
