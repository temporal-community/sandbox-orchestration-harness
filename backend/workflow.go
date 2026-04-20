package backend

import (
	"fmt"
	"time"

	backendcompute "github.com/temporalio/sandbox-backend/compute"
	"github.com/temporalio/sandbox-sdk-go/compute"
	wfIface "github.com/temporalio/sandbox-sdk-go/workflow"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type sandboxWorkflow struct {
	state           wfIface.SandboxLocalState
	initialized     bool
	cancelRequested bool
	status          *backendcompute.ComputeProviderStatus
}

// SandboxWorkflow is the long-lived workflow that represents a single
// lambda sandbox instance. Its workflow ID is always the sandboxId so the
// parent can address it directly.
func SandboxWorkflow(ctx workflow.Context, input wfIface.SandboxLocalState) error {
	return (&sandboxWorkflow{state: input}).run(ctx)
}

func (s *sandboxWorkflow) run(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)

	if err := workflow.SetUpdateHandlerWithOptions(ctx, wfIface.SandboxInitUpdate, s.handleInit, workflow.UpdateHandlerOptions{Validator: s.validateInit}); err != nil {
		return err
	}

	// Block until the init update has been processed.
	if err := workflow.Await(ctx, func() bool { return s.initialized }); err != nil {
		return err
	}

	logger.Info("sandbox initialised", "parentWorkflowID", s.state.WorkflowID, "parentRunID", s.state.RunID)
	if s.state.ComputeProvider != nil {
		logger.Info("compute provider configured", "type", s.state.ComputeProvider.Type)
	}

	s.listenToSignals(ctx)

	// Block until a stop signal arrives or the workflow is cancelled.
	workflow.Await(ctx, func() bool { return s.cancelRequested }) //nolint:errcheck

	cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
	workflow.ExecuteActivity(
		workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Minute,
		}),
		StopSandbox,
		StopSandboxInput{
			ProviderType: s.state.ComputeProvider.Type,
			Config:       s.state.ComputeProvider.Config,
			Status:       s.status,
		},
	).Get(cleanupCtx, nil) //nolint:errcheck

	return nil
}

func (s *sandboxWorkflow) validateInit(inp wfIface.SandboxInitInput) error {
	if inp.ComputeProvider.Type != compute.ComputeProviderTypeLambda && inp.ComputeProvider.Type != compute.ComputeProviderTypeECS {
		return temporal.NewApplicationError("Unknown compute provider type", "InvalidArgument")
	}

	return nil
}

func (s *sandboxWorkflow) handleInit(ctx workflow.Context, inp wfIface.SandboxInitInput) error {
	var out StartSandboxOutput
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Minute,
		}),
		StartSandbox,
		StartSandboxInput{
			ProviderType:  inp.ComputeProvider.Type,
			Config:        inp.ComputeProvider.Config,
			TaskQueueName: s.taskQueueName(ctx),
		},
	).Get(ctx, &out); err != nil {
		return err
	}
	s.status = out.Status
	s.state.ComputeProvider = &inp.ComputeProvider
	s.initialized = true
	return nil
}

// listenToSignals spawns a goroutine that sets cancelRequested when either
// the stop signal or context cancellation is received.
func (s *sandboxWorkflow) listenToSignals(ctx workflow.Context) {
	stopCh := workflow.GetSignalChannel(ctx, wfIface.SandboxStopSignal)

	workflow.Go(ctx, func(ctx workflow.Context) {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(stopCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			s.cancelRequested = true
		})
		selector.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			s.cancelRequested = true
		})
		selector.Select(ctx)
	})
}

func (s *sandboxWorkflow) taskQueueName(ctx workflow.Context) string {
	wfInfo := workflow.GetInfo(ctx)

	return fmt.Sprintf("sandbox-%s", wfInfo.WorkflowExecution.ID)
}
