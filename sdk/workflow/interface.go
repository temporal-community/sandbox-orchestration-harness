package workflow

import (
	"github.com/temporalio/sandbox-sdk-go/compute"
)

const (
	SandboxWorkflowTaskQueueName = "sandbox-worker"
	SandboxWorkflowType          = "SandboxWorkflow"
	SandboxInitUpdate            = "sandbox-init"
	SandboxStopSignal            = "sandbox-stop"
)

// SandboxLocalState is passed when starting the sandbox workflow.
type SandboxLocalState struct {
	WorkflowID string
	RunID      string

	ComputeProvider *compute.ComputeProvider
}

// SandboxInitInput is sent via update after the sandbox workflow starts,
// identifying the parent workflow that owns this sandbox.
type SandboxInitInput struct {
	ComputeProvider compute.ComputeProvider
}
