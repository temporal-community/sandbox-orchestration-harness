package autosuspend

import (
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "auto-suspend-queue"

// WorkflowResult holds the file listing captured before and after the 2-minute wait.
type WorkflowResult struct {
	BeforeSuspend string
	AfterSuspend  string
}

// AutoSuspendWorkflow creates a sandbox, writes a file, waits 2 minutes,
// then verifies the file is still present after resume.
func AutoSuspendWorkflow(ctx workflow.Context) (WorkflowResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// 5 minutes covers the 2-minute sleep plus command execution on both sides.
		StartToCloseTimeout: 5 * time.Minute,
	})

	sbx, err := sandbox.NewSandbox(ctx, sandbox.Provider{
		Type: compute.ProviderTypeModal,
		Config: map[string]string{
			"image": "ubuntu:26.04",
		},
	}, sandbox.WithIdleTimeout(30*time.Second))
	if err != nil {
		return WorkflowResult{}, err
	}

	if _, err := sbx.ExecuteCommand(ctx, "mkdir -p /mnt/session"); err != nil {
		return WorkflowResult{}, err
	}

	if _, err := sbx.ExecuteCommand(ctx, "touch /mnt/session/persist.txt"); err != nil {
		return WorkflowResult{}, err
	}

	before, err := sbx.ExecuteCommand(ctx, "ls -la /mnt/session/persist.txt")
	if err != nil {
		return WorkflowResult{}, err
	}

	// The sandbox may auto-suspend during this wait.
	if err := workflow.Sleep(ctx, 2*time.Minute); err != nil {
		return WorkflowResult{}, err
	}

	after, err := sbx.ExecuteCommand(ctx, "ls -la /mnt/session/persist.txt")
	if err != nil {
		return WorkflowResult{}, err
	}

	if err := sbx.Stop(ctx); err != nil {
		return WorkflowResult{}, err
	}

	return WorkflowResult{
		BeforeSuspend: before.Stdout,
		AfterSuspend:  after.Stdout,
	}, nil
}
