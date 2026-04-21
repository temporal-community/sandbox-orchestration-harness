package sandbox

import (
	"context"

	wfIface "github.com/temporalio/ephemeral-workers-poc/sdk/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func RegisterHelperActivities(w worker.ActivityRegistry, c client.Client) error {
	a := sandboxActivities{client: c}

	w.RegisterActivity(a.SendSandboxInit)
	return nil
}

type sandboxActivities struct {
	client client.Client
}

func NewSandboxActivities(c client.Client) *sandboxActivities {
	return &sandboxActivities{client: c}
}

func (a *sandboxActivities) SendSandboxInit(ctx context.Context, sandboxID string, input wfIface.SandboxInitInput) error {
	handle, err := a.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   sandboxID,
		UpdateName:   wfIface.SandboxInitUpdate,
		Args:         []interface{}{input},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return err
	}
	return handle.Get(ctx, nil)
}
