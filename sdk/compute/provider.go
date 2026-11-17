// Package compute contains the abstraction for the different sandbox providers
package compute

import (
	"context"
	"fmt"
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
		InstanceID      string           `json:"instance_id"`
		ResourceBinding *ResourceBinding `json:"resource_binding,omitempty"`
	}
	// ResourceBinding carries nonsecret reconciliation context for a fresh resource.
	// It detects conflicting inputs in a trusted environment; it is not authentication.
	ResourceBinding struct {
		ProviderType        ProviderType `json:"provider_type"`
		TaskQueueName       string       `json:"task_queue_name"`
		InstanceID          string       `json:"instance_id"`
		ConfigurationDigest string       `json:"configuration_digest"`
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
	//   - Start must return a ProviderStatus that identifies the sandbox in all
	//     subsequent calls (Stop, Suspend, ExecuteCommand, etc.). Retain the complete
	//     returned status, including any resource binding required by the provider.
	//   - taskQueueName passed to Start and StartFromSnapshot is a stable input that
	//     providers may use for identity or Temporal Worker routing when configured.
	//     Provider configuration chooses the workload; Start need not start a Temporal Worker.
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
	ProviderTypeCrafting         ProviderType = "crafting"
	ProviderTypeDaytona          ProviderType = "daytona"
	ProviderTypeE2B              ProviderType = "e2b"
	ProviderTypeModal            ProviderType = "modal"
	ProviderTypeSmolCloud        ProviderType = "smol-cloud"
	ProviderTypeGKEAgentSandbox  ProviderType = "gke-agent-sandbox"
)

// ValidateResourceBinding checks the resource context available before provider construction.
// Provider-specific configuration and remote metadata checks remain with the provider.
func ValidateResourceBinding(details ProviderDetails, status *ProviderStatus) error {
	if status != nil && status.ResourceBinding != nil {
		binding := status.ResourceBinding
		if binding.ProviderType == "" || binding.TaskQueueName == "" || binding.InstanceID == "" || binding.ConfigurationDigest == "" {
			return fmt.Errorf("compute: incomplete resource binding")
		}
		if binding.ProviderType != details.Type || binding.InstanceID != status.InstanceID {
			return fmt.Errorf("compute: resource binding conflicts with provider or instance")
		}
	}
	policy, err := ResolvePolicy(details)
	if err != nil {
		return err
	}
	if details.Type == ProviderTypeSmolCloud && policy.Lifecycle == LifecycleFresh && (status == nil || status.ResourceBinding == nil) {
		return fmt.Errorf("compute: fresh resource binding is required")
	}
	return nil
}
