package sandbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/agentcore"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/daytona"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/e2b"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/gkeagentsandbox"
	_ "github.com/temporal-community/sandbox-orchestration-harness/sdk/compute/modal"
	wfIface "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Register wires the whole sandbox SDK into a Temporal worker: it calls both
// RegisterSandboxWorkflow and RegisterClientActivities on w. Use it when the
// worker serves both the workflows that create sandboxes and the sandboxes
// themselves (the default, single-task-queue setup). The blank imports in this
// file ensure all five built-in compute providers are self-registered via their
// init functions before any activity runs.
func Register(w worker.Registry, c client.Client) {
	RegisterClientActivities(w, c)
	RegisterSandboxWorkflow(w)
}

// RegisterClientActivities registers the SendSandbox* activities, which forward
// a caller's requests to a SandboxWorkflow as workflow updates. They run on the
// sandbox's task queue (see WithTaskQueue). The Temporal client c is used to
// call UpdateWorkflow.
func RegisterClientActivities(w worker.Registry, c client.Client) {
	a := &sandboxActivities{client: c}
	w.RegisterActivity(a.SendSandboxInit)
	w.RegisterActivity(a.SendSandboxExecuteCommand)
	w.RegisterActivity(a.SendSandboxSuspend)
	w.RegisterActivity(a.SendSandboxResume)
	w.RegisterActivity(a.SendSandboxSnapshot)
	w.RegisterActivity(a.SendSandboxDeleteSnapshot)
}

// RegisterSandboxWorkflow registers SandboxWorkflow and the sandbox lifecycle
// activities (StartSandbox, StopSandbox, ExecuteCommand, etc.) that call the
// compute providers. The worker running these needs the providers' credentials.
func RegisterSandboxWorkflow(w worker.Registry) {
	w.RegisterWorkflow(wfIface.SandboxWorkflow)
	w.RegisterActivity(wfIface.StartSandbox)
	w.RegisterActivity(wfIface.StopSandbox)
	w.RegisterActivity(wfIface.SuspendSandbox)
	w.RegisterActivity(wfIface.ResumeSandbox)
	w.RegisterActivity(wfIface.SnapshotSandbox)
	w.RegisterActivity(wfIface.StartSandboxFromSnapshot)
	w.RegisterActivity(wfIface.DeleteSnapshot)
	w.RegisterActivity(wfIface.ExecuteCommand)
}

type sandboxActivities struct {
	client client.Client
}

type SendSandboxExecuteCommandInput struct {
	SandboxID         string `json:"sandbox_id"`
	UpdateID          string `json:"update_id"`
	Command           string `json:"command"`
	DisableAutoResume bool   `json:"disable_auto_resume"`
}

func (a *sandboxActivities) SendSandboxExecuteCommand(ctx context.Context, input SendSandboxExecuteCommandInput) (compute.CommandResult, error) {
	handle, err := a.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   input.SandboxID,
		UpdateID:     input.UpdateID,
		UpdateName:   wfIface.SandboxExecuteCommandUpdate,
		Args:         []any{wfIface.SandboxExecuteCommandInput{Command: input.Command, DisableAutoResume: input.DisableAutoResume}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return compute.CommandResult{}, err
	}
	var result compute.CommandResult
	if err := handle.Get(ctx, &result); err != nil {
		return result, wrapUpdateError(err)
	}
	return result, nil
}

type SendSandboxSuspendInput struct {
	SandboxID string `json:"sandbox_id"`
	UpdateID  string `json:"update_id"`
}

func (a *sandboxActivities) SendSandboxSuspend(ctx context.Context, input SendSandboxSuspendInput) error {
	handle, err := a.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   input.SandboxID,
		UpdateID:     input.UpdateID,
		UpdateName:   wfIface.SandboxSuspendUpdate,
		Args:         []any{struct{}{}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return err
	}
	if err := handle.Get(ctx, nil); err != nil {
		return wrapUpdateError(err)
	}
	return nil
}

type SendSandboxResumeInput struct {
	SandboxID string `json:"sandbox_id"`
	UpdateID  string `json:"update_id"`
}

func (a *sandboxActivities) SendSandboxResume(ctx context.Context, input SendSandboxResumeInput) error {
	handle, err := a.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   input.SandboxID,
		UpdateID:     input.UpdateID,
		UpdateName:   wfIface.SandboxResumeUpdate,
		Args:         []any{struct{}{}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return err
	}
	if err := handle.Get(ctx, nil); err != nil {
		return wrapUpdateError(err)
	}
	return nil
}

type SendSandboxInitInput struct {
	SandboxID       string                    `json:"sandbox_id"`
	UpdateID        string                    `json:"update_id"`
	ComputeProvider compute.ProviderDetails   `json:"compute_provider"`
	IdleTimeout     time.Duration             `json:"idle_timeout"`
	Snapshot        *compute.ProviderSnapshot `json:"snapshot"` // nil → fresh start
}

func (a *sandboxActivities) SendSandboxInit(ctx context.Context, input SendSandboxInitInput) error {
	handle, err := a.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   input.SandboxID,
		UpdateID:     input.UpdateID,
		UpdateName:   wfIface.SandboxInitUpdate,
		Args:         []any{wfIface.SandboxInitInput{ComputeProvider: input.ComputeProvider, IdleTimeout: input.IdleTimeout, Snapshot: input.Snapshot}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return err
	}
	if err := handle.Get(ctx, nil); err != nil {
		return wrapUpdateError(err)
	}
	return nil
}

type SendSandboxSnapshotInput struct {
	SandboxID string `json:"sandbox_id"`
	UpdateID  string `json:"update_id"`
}

func (a *sandboxActivities) SendSandboxSnapshot(ctx context.Context, input SendSandboxSnapshotInput) (*compute.ProviderSnapshot, error) {
	handle, err := a.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   input.SandboxID,
		UpdateID:     input.UpdateID,
		UpdateName:   wfIface.SandboxSnapshotUpdate,
		Args:         []any{struct{}{}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, err
	}
	var snapshot compute.ProviderSnapshot
	if err := handle.Get(ctx, &snapshot); err != nil {
		return nil, wrapUpdateError(err)
	}
	return &snapshot, nil
}

type SendSandboxDeleteSnapshotInput struct {
	SandboxID string                    `json:"sandbox_id"`
	UpdateID  string                    `json:"update_id"`
	Snapshot  *compute.ProviderSnapshot `json:"snapshot"`
}

func (a *sandboxActivities) SendSandboxDeleteSnapshot(ctx context.Context, input SendSandboxDeleteSnapshotInput) error {
	handle, err := a.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   input.SandboxID,
		UpdateID:     input.UpdateID,
		UpdateName:   wfIface.SandboxDeleteSnapshotUpdate,
		Args:         []any{input.Snapshot},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return err
	}
	if err := handle.Get(ctx, nil); err != nil {
		return wrapUpdateError(err)
	}

	return nil
}

// wrapUpdateError makes a workflow update error non-retryable at the activity
// level. Errors are made non-retryable to prevent activity-level retry attempts;
// the stable UpdateID (from sideEffectUUID) ensures that if the parent workflow
// re-executes this activity via replay, Temporal deduplicates the update so it
// is applied exactly once.
//
// If the error is already an ApplicationError (a domain error such as
// "AlreadySuspended"), its type is preserved so callers can distinguish them.
// A NotFound error (sandbox workflow gone) is surfaced as "SandboxNotFound".
// Any other error is wrapped under the generic "UpdateWorkflowFailure" type.
func wrapUpdateError(err error) error {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		return temporal.NewNonRetryableApplicationError(appErr.Message(), appErr.Type(), appErr)
	}
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return temporal.NewNonRetryableApplicationError("sandbox not found", "SandboxNotFound", err)
	}
	return temporal.NewNonRetryableApplicationError(err.Error(), "UpdateWorkflowFailure", err)
}

// sideEffectUUID generates a UUID that is recorded in the workflow history so
// it is stable across replays. Use it to produce a deterministic UpdateID for
// each UpdateWorkflow activity call so retries are idempotent.
func sideEffectUUID(ctx workflow.Context) (string, error) {
	se := workflow.SideEffect(ctx, func(workflow.Context) any { return uuid.New().String() })
	var id string
	if err := se.Get(&id); err != nil {
		return "", fmt.Errorf("sandbox: get side-effect update ID: %w", err)
	}
	return id, nil
}
