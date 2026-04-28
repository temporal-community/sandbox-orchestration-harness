package sharedsandbox

import (
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "shared-sandbox-queue"

type ChildInput struct {
	SandboxRef string
	FileName   string
}

type WorkflowResult struct {
	Files string
}

// ChildFileWorkflow attaches to an existing sandbox and creates a single file in it.
func ChildFileWorkflow(ctx workflow.Context, input ChildInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	sbx, err := sandbox.AttachToSandbox(input.SandboxRef)
	if err != nil {
		return err
	}
	_, err = sbx.ExecuteCommand(ctx, "touch /tmp/"+input.FileName)
	return err
}

// SharedSandboxWorkflow creates a sandbox, writes a file, then delegates to two child
// workflows that each write their own file into the same sandbox. After both children
// complete it lists all files and returns the result.
func SharedSandboxWorkflow(ctx workflow.Context) (WorkflowResult, error) {
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

	if _, err := sbx.ExecuteCommand(ctx, "touch /tmp/parent.txt"); err != nil {
		return WorkflowResult{}, err
	}

	ref, err := sbx.Ref()
	if err != nil {
		return WorkflowResult{}, err
	}
	child1 := workflow.ExecuteChildWorkflow(ctx, ChildFileWorkflow, ChildInput{
		SandboxRef: ref,
		FileName:   "child1.txt",
	})
	child2 := workflow.ExecuteChildWorkflow(ctx, ChildFileWorkflow, ChildInput{
		SandboxRef: ref,
		FileName:   "child2.txt",
	})

	if err := child1.Get(ctx, nil); err != nil {
		return WorkflowResult{}, err
	}
	if err := child2.Get(ctx, nil); err != nil {
		return WorkflowResult{}, err
	}

	listing, err := sbx.ExecuteCommand(ctx, "ls /tmp/*.txt")
	if err != nil {
		return WorkflowResult{}, err
	}

	if err := sbx.Stop(ctx); err != nil {
		return WorkflowResult{}, err
	}

	return WorkflowResult{Files: listing.Stdout}, nil
}
