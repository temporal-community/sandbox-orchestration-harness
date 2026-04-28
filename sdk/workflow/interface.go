package workflow

import (
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

const (
	SandboxWorkflowType         = "SandboxWorkflow"
	SandboxInitUpdate           = "sandbox-init"
	SandboxExecuteCommandUpdate = "sandbox-execute-command"
	SandboxSuspendUpdate        = "sandbox-suspend"
	SandboxResumeUpdate         = "sandbox-resume"
	SandboxSnapshotUpdate       = "sandbox-snapshot"
	SandboxDeleteSnapshotUpdate = "sandbox-delete-snapshot"
	SandboxStopSignal           = "sandbox-stop"
	SandboxStateQuery           = "sandbox-state"

	errUnsupportedType = "ErrUnsupported"

	defaultOperationTimeout = 10 * time.Minute

	startSandboxTimeout      = defaultOperationTimeout
	stopSandboxTimeout       = defaultOperationTimeout
	suspendSandboxTimeout    = defaultOperationTimeout
	resumeSandboxTimeout     = defaultOperationTimeout
	snapshotSandboxTimeout   = defaultOperationTimeout
	startFromSnapshotTimeout = defaultOperationTimeout
	deleteSnapshotTimeout    = defaultOperationTimeout
	executeCommandTimeout    = defaultOperationTimeout
	idleAutoSuspendTimeout   = 5 * time.Minute
)

type SandboxExecuteCommandInput struct {
	Command           string `json:"command"`
	DisableAutoResume bool   `json:"disable_auto_resume"`
}

// SandboxLocalState is the mutable state carried by SandboxWorkflow throughout
// its lifetime. Only WorkflowID and RunID are populated when the workflow starts;
// ComputeProvider, Status, and IdleTimeout begin nil/zero and are set by the
// sandbox-init update handler, then updated by subsequent lifecycle operations.
type SandboxLocalState struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`

	ComputeProvider *compute.ProviderDetails `json:"compute_provider"`
	Status          *compute.ProviderStatus  `json:"status"`
	IdleTimeout     time.Duration            `json:"idle_timeout"` // zero → idleAutoSuspendTimeout; compute.NoIdleTimeout (-1) → no auto-suspend
}

// SandboxLifecycle is the lifecycle state of a sandbox, returned by SandboxStateQuery.
type SandboxLifecycle int

const (
	// SandboxLifecyclePending means the sandbox workflow has started but the init
	// update has not yet been processed.
	SandboxLifecyclePending SandboxLifecycle = iota
	// SandboxLifecycleRunning means the sandbox is initialized and accepting commands.
	SandboxLifecycleRunning
	// SandboxLifecycleSuspended means the sandbox is suspended via the provider's
	// native suspend mechanism and will resume with a native Resume call.
	SandboxLifecycleSuspended
	// SandboxLifecycleSuspendedWithSnapshot means the sandbox is suspended and the SDK
	// is holding an internal snapshot; resume will restart the sandbox from that snapshot.
	// This state arises when a provider suspends the sandbox as part of a user-initiated
	// Snapshot call (e.g. GKE), or as the fallback when a provider without native Suspend
	// support is suspended via snapshot+stop.
	SandboxLifecycleSuspendedWithSnapshot
	// SandboxLifecycleFailed means the init activity failed; the workflow is exiting.
	SandboxLifecycleFailed
	// SandboxLifecycleDeleted means the sandbox was deleted as a side-effect of a
	// snapshot operation; the workflow is exiting.
	SandboxLifecycleDeleted
)

// SandboxState is the result type for the SandboxStateQuery query.
type SandboxState struct {
	Lifecycle       SandboxLifecycle         `json:"lifecycle"`
	ComputeProvider *compute.ProviderDetails `json:"compute_provider"`
	Status          *compute.ProviderStatus  `json:"status"`
	IdleTimeout     time.Duration            `json:"idle_timeout"`
}

// SandboxInitInput is sent via update after the sandbox workflow starts,
// identifying the compute provider and associated configuration.
type SandboxInitInput struct {
	ComputeProvider compute.ProviderDetails   `json:"compute_provider"`
	IdleTimeout     time.Duration             `json:"idle_timeout"` // zero → idleAutoSuspendTimeout
	Snapshot        *compute.ProviderSnapshot `json:"snapshot"`     // nil → fresh start via Start; non-nil → StartFromSnapshot
}
