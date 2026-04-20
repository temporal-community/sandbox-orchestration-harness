package sandbox

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/temporalio/sandbox-sdk-go/compute"
	wfIface "github.com/temporalio/sandbox-sdk-go/workflow"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

type Sandbox interface {
	ExecuteActivity(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future

	Stop(ctx workflow.Context) error
}

type defaultSandbox struct {
	workflowId string
	runId      string
	sandboxId  string

	future     workflow.ChildWorkflowFuture
	activities *sandboxActivities
}

func NewSandbox(ctx workflow.Context, computeProvider compute.ComputeProvider) (Sandbox, error) {
	wfInfo := workflow.GetInfo(ctx)
	sandboxId := uuid.New()

	s := &defaultSandbox{
		sandboxId: sandboxId.String(),

		workflowId: wfInfo.WorkflowExecution.ID,
		runId:      wfInfo.WorkflowExecution.RunID,
		activities: &sandboxActivities{},
	}

	if err := s.start(ctx, computeProvider); err != nil {
		return nil, err
	}

	return s, nil
}

// start launches the sandbox workflow using sandboxId as its workflow ID,
// waits for it to be running, then sends an init signal so the sandbox
// knows which parent spawned it.
func (s *defaultSandbox) start(ctx workflow.Context, computeProvider compute.ComputeProvider) error {
	cwo := workflow.ChildWorkflowOptions{
		WorkflowID:        s.sandboxId,
		TaskQueue:         wfIface.SandboxWorkflowTaskQueueName,
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	}
	s.future = workflow.ExecuteChildWorkflow(
		workflow.WithChildOptions(ctx, cwo),
		wfIface.SandboxWorkflowType,
		wfIface.SandboxLocalState{WorkflowID: s.workflowId, RunID: s.runId},
	)

	// Wait for the sandbox workflow to be scheduled and running before
	// sending the init update so the signal is not lost.
	if err := s.future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		return err
	}

	// Send an init update to the running sandbox workflow. Using an activity
	// to call client.UpdateWorkflow ensures the parent blocks until the
	// sandbox's update handler has completed (request-response semantics).
	return workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Minute,
		}),
		s.activities.SendSandboxInit,
		s.sandboxId,
		wfIface.SandboxInitInput{ComputeProvider: computeProvider},
	).Get(ctx, nil)
}

func (s *defaultSandbox) ExecuteActivity(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
	priorActivityOptions := workflow.GetActivityOptions(ctx)
	ao := workflow.ActivityOptions{
		TaskQueue: s.taskQueueName(),

		ScheduleToCloseTimeout: priorActivityOptions.ScheduleToCloseTimeout,
		StartToCloseTimeout:    priorActivityOptions.StartToCloseTimeout,
		ScheduleToStartTimeout: priorActivityOptions.ScheduleToStartTimeout,
		HeartbeatTimeout:       priorActivityOptions.HeartbeatTimeout,
		WaitForCancellation:    priorActivityOptions.WaitForCancellation,
		ActivityID:             priorActivityOptions.ActivityID,
		RetryPolicy:            priorActivityOptions.RetryPolicy,
		DisableEagerExecution:  priorActivityOptions.DisableEagerExecution,
		Priority:               priorActivityOptions.Priority,
		Summary:                priorActivityOptions.Summary,
	}
	return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activity, args...)
}

func (s *defaultSandbox) Stop(ctx workflow.Context) error {
	if err := workflow.SignalExternalWorkflow(
		ctx, s.sandboxId, "",
		wfIface.SandboxStopSignal, nil,
	).Get(ctx, nil); err != nil {
		return err
	}

	return s.future.Get(ctx, nil)
}

func (s *defaultSandbox) taskQueueName() string {
	return fmt.Sprintf("sandbox-%s", s.sandboxId)
}
