package detachedsandbox

import (
	"time"

	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "detached-sandbox-queue"

type HandoffInput struct {
	SandboxRef string
}

type CreatorResult struct {
	SandboxRef        string
	HandoffWorkflowID string
}

// HandoffWorkflow runs independently of the creator. It attaches to the still-running
// sandbox, lists the file left by the creator, then stops the sandbox.
func HandoffWorkflow(ctx workflow.Context, input HandoffInput) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	sbx, err := sandbox.AttachToSandbox(input.SandboxRef)
	if err != nil {
		return "", err
	}

	result, err := sbx.ExecuteCommand(ctx, "ls -la /tmp/handoff-test.txt")
	if err != nil {
		return "", err
	}

	if err := sbx.RequestStop(ctx); err != nil {
		return "", err
	}

	return result.Stdout, nil
}

// DetachedSandboxWorkflow creates a sandbox with CleanupDisabled so it survives the
// creator workflow closing, writes a file, then fires a HandoffWorkflow as an
// abandoned child (fire-and-forget) before returning.
func DetachedSandboxWorkflow(ctx workflow.Context) (CreatorResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	sbx, err := sandbox.NewSandbox(ctx, sandbox.Provider{
		Type: compute.ProviderTypeModal,
		Config: map[string]string{
			"image": "ubuntu:26.04",
		},
	}, sandbox.WithCleanup(sandbox.CleanupDisabled))
	if err != nil {
		return CreatorResult{}, err
	}

	if _, err := sbx.ExecuteCommand(ctx, "touch /tmp/handoff-test.txt"); err != nil {
		return CreatorResult{}, err
	}

	ref, err := sbx.Ref()
	if err != nil {
		return CreatorResult{}, err
	}
	handoffWorkflowID := "handoff-" + ref

	// Fire the handoff workflow as an abandoned child so it runs independently
	// of this (creator) workflow's lifecycle.
	future := workflow.ExecuteChildWorkflow(
		workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID:        handoffWorkflowID,
			ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
		}),
		HandoffWorkflow,
		HandoffInput{SandboxRef: ref},
	)
	// wait for the child workflow to start
	if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		return CreatorResult{
			SandboxRef:        ref,
			HandoffWorkflowID: handoffWorkflowID,
		}, err
	}

	return CreatorResult{
		SandboxRef:        ref,
		HandoffWorkflowID: handoffWorkflowID,
	}, nil
}
