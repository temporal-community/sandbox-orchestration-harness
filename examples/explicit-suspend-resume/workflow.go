package explicitsuspendresume

import (
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "explicit-suspend-resume-queue"

type WorkflowResult struct {
	Files string
}

// ExplicitSuspendResumeWorkflow creates a sandbox, writes a file, suspends the sandbox,
// resumes it, then lists files to confirm state survived the suspend/resume cycle.
// The sandbox is not stopped explicitly; Temporal's parent close policy sends a
// cancellation to the sandbox child workflow when this workflow completes, which
// triggers sandbox cleanup automatically.
func ExplicitSuspendResumeWorkflow(ctx workflow.Context) (WorkflowResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	sbx, err := sandbox.NewSandbox(ctx, sandbox.Provider{
		Type: compute.ProviderTypeModal,
		Config: map[string]string{
			"image": "ubuntu:26.04",
		},
	})
	if err != nil {
		return WorkflowResult{}, err
	}

	if _, err := sbx.ExecuteCommand(ctx, "mkdir -p /mnt/session"); err != nil {
		return WorkflowResult{}, err
	}

	if _, err := sbx.ExecuteCommand(ctx, "touch /mnt/session/suspend-resume-test.txt"); err != nil {
		return WorkflowResult{}, err
	}

	if err := sbx.Suspend(ctx); err != nil {
		return WorkflowResult{}, err
	}

	if err := sbx.Resume(ctx); err != nil {
		return WorkflowResult{}, err
	}

	listing, err := sbx.ExecuteCommand(ctx, "ls /mnt/session")
	if err != nil {
		return WorkflowResult{}, err
	}

	return WorkflowResult{Files: listing.Stdout}, nil
}
