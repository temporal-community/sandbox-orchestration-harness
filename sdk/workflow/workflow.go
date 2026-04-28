package workflow

import (
	"errors"
	"fmt"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type sandboxLifecycle int

const (
	sandboxPending   sandboxLifecycle = iota // before init update is processed
	sandboxRunning                           // initialized, not suspended
	sandboxSuspended                         // initialized, suspended
	sandboxFailed                            // init activity failed; workflow exits immediately
	sandboxDeleted                           // sandbox destroyed as a side-effect of a snapshot
)

type sandboxWorkflow struct {
	state           SandboxLocalState
	lifecycle       sandboxLifecycle
	cancelRequested bool
	cancelIdleTimer func() // cancels the in-flight idle timer; nil if none

	// suspendSnapshot is set when the sandbox was suspended via snapshot+stop
	// (fallback for providers without native Suspend), or when the provider
	// auto-suspended the sandbox as part of a user-initiated Snapshot call.
	// Non-nil means handleResume must restart via StartSandboxFromSnapshot.
	suspendSnapshot *compute.ProviderSnapshot
	// suspendSnapshotUserOwned is true when suspendSnapshot originated from a
	// user-initiated handleSnapshot call. The user holds a reference to this
	// snapshot; the SDK must never auto-delete it on resume or cleanup.
	suspendSnapshotUserOwned bool
}

// SandboxWorkflow is the long-lived workflow that represents a single
// sandbox instance. Its workflow ID is always the sandboxID so the
// parent can address it directly.
func SandboxWorkflow(ctx workflow.Context, input SandboxLocalState) error {
	return (&sandboxWorkflow{state: input}).run(ctx)
}

func (s *sandboxWorkflow) run(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)

	if err := workflow.SetQueryHandler(ctx, SandboxStateQuery, s.handleStateQuery); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, SandboxInitUpdate, s.handleInit, workflow.UpdateHandlerOptions{Validator: s.validateInit}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, SandboxExecuteCommandUpdate, s.handleExecuteCommand, workflow.UpdateHandlerOptions{Validator: s.validateExecuteCommand}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, SandboxSuspendUpdate, s.handleSuspend, workflow.UpdateHandlerOptions{Validator: s.validateSuspend}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, SandboxResumeUpdate, s.handleResume, workflow.UpdateHandlerOptions{Validator: s.validateResume}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, SandboxSnapshotUpdate, s.handleSnapshot, workflow.UpdateHandlerOptions{Validator: s.validateSnapshot}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, SandboxDeleteSnapshotUpdate, s.handleDeleteSnapshot, workflow.UpdateHandlerOptions{Validator: s.validateDeleteSnapshot}); err != nil {
		return err
	}

	// Block until the init update has been processed.
	if err := workflow.Await(ctx, func() bool { return s.lifecycle != sandboxPending }); err != nil {
		return err
	}
	if s.lifecycle == sandboxFailed {
		return nil
	}

	logger.Info("sandbox initialised", "parentWorkflowID", s.state.WorkflowID, "parentRunID", s.state.RunID)
	if s.state.ComputeProvider != nil {
		logger.Info("compute provider configured", "type", s.state.ComputeProvider.Type)
	}

	s.listenToSignals(ctx)

	// Block until a stop signal arrives, the workflow context is cancelled, or the
	// sandbox was deleted as a side-effect of a Snapshot call (handleSnapshot sets
	// cancelRequested = true for SandboxPostSnapshotDeleted).
	// Discard a cancellation error: returning it from the workflow function marks the
	// workflow as Failed rather than Cancelled in Temporal visibility.
	awaitErr := workflow.Await(ctx, func() bool { return s.cancelRequested })
	var canceledErr *temporal.CanceledError
	if errors.As(awaitErr, &canceledErr) {
		awaitErr = nil
	}
	s.cancelIdleTimerIfActive()

	cleanupCtx, cancelCleanup := workflow.NewDisconnectedContext(ctx)
	defer cancelCleanup()
	if s.state.ComputeProvider != nil && s.state.Status != nil {
		if s.suspendSnapshot != nil && !s.suspendSnapshotUserOwned {
			// Internal suspend snapshot: the sandbox was already stopped by suspendViaSnapshot.
			// Just delete the orphaned snapshot.
			if err := workflow.ExecuteActivity(
				workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{StartToCloseTimeout: deleteSnapshotTimeout}),
				DeleteSnapshot,
				DeleteSnapshotInput{
					Provider: *s.state.ComputeProvider,
					Snapshot: s.suspendSnapshot,
				},
			).Get(cleanupCtx, nil); err != nil {
				return err
			}
		} else {
			// Either no suspend snapshot, or a user-owned snapshot (the user holds a reference;
			// never auto-delete it). The compute resource may still be running or suspended, so stop it.
			if err := workflow.ExecuteActivity(
				workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{StartToCloseTimeout: stopSandboxTimeout}),
				StopSandbox,
				StopSandboxInput{
					Provider: *s.state.ComputeProvider,
					Status:   s.state.Status,
				},
			).Get(cleanupCtx, nil); err != nil {
				return err
			}
		}
	}

	return awaitErr
}

func (s *sandboxWorkflow) handleStateQuery() (SandboxState, error) {
	return SandboxState{
		Lifecycle:       s.publicLifecycle(),
		ComputeProvider: s.state.ComputeProvider,
		Status:          s.state.Status,
		IdleTimeout:     s.state.IdleTimeout,
	}, nil
}

func (s *sandboxWorkflow) publicLifecycle() SandboxLifecycle {
	switch s.lifecycle {
	case sandboxPending:
		return SandboxLifecyclePending
	case sandboxRunning:
		return SandboxLifecycleRunning
	case sandboxSuspended:
		if s.suspendSnapshot != nil {
			return SandboxLifecycleSuspendedWithSnapshot
		}
		return SandboxLifecycleSuspended
	case sandboxFailed:
		return SandboxLifecycleFailed
	case sandboxDeleted:
		return SandboxLifecycleDeleted
	default:
		panic(fmt.Sprintf("sandbox: unhandled lifecycle state %d in publicLifecycle", s.lifecycle))
	}
}

func (s *sandboxWorkflow) validateInit(inp SandboxInitInput) error {
	if s.lifecycle != sandboxPending {
		return temporal.NewApplicationError("sandbox already initialized", "AlreadyInitialized")
	}
	if !compute.IsRegistered(inp.ComputeProvider.Type) {
		return temporal.NewApplicationError("Unknown compute provider type", "InvalidArgument")
	}
	return nil
}

func (s *sandboxWorkflow) handleInit(ctx workflow.Context, inp SandboxInitInput) error {
	if inp.Snapshot != nil {
		var out StartSandboxFromSnapshotOutput
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: startFromSnapshotTimeout,
			}),
			StartSandboxFromSnapshot,
			StartSandboxFromSnapshotInput{
				Provider:      inp.ComputeProvider,
				TaskQueueName: s.taskQueueName(ctx),
				Snapshot:      inp.Snapshot,
			},
		).Get(ctx, &out); err != nil {
			s.lifecycle = sandboxFailed
			return err
		}
		s.state.Status = out.Status
	} else {
		var out StartSandboxOutput
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: startSandboxTimeout,
			}),
			StartSandbox,
			StartSandboxInput{
				Provider:      inp.ComputeProvider,
				TaskQueueName: s.taskQueueName(ctx),
			},
		).Get(ctx, &out); err != nil {
			s.lifecycle = sandboxFailed
			return err
		}
		s.state.Status = out.Status
	}
	s.state.ComputeProvider = &inp.ComputeProvider
	s.state.IdleTimeout = inp.IdleTimeout
	s.lifecycle = sandboxRunning
	return nil
}

func (s *sandboxWorkflow) validateExecuteCommand(inp SandboxExecuteCommandInput) error {
	if s.lifecycle == sandboxPending || s.lifecycle == sandboxFailed || s.lifecycle == sandboxDeleted {
		return temporal.NewApplicationError("sandbox not initialized", "NotInitialized")
	}
	if s.lifecycle == sandboxSuspended && inp.DisableAutoResume {
		return temporal.NewApplicationError("sandbox is suspended", "Suspended")
	}
	return nil
}

func (s *sandboxWorkflow) handleExecuteCommand(ctx workflow.Context, inp SandboxExecuteCommandInput) (*compute.CommandResult, error) {
	// Cancel any in-flight idle timer from the previous command.
	s.cancelIdleTimerIfActive()

	if s.lifecycle == sandboxSuspended {
		if err := s.handleResume(ctx, struct{}{}); err != nil {
			return nil, err
		}
	}

	var out ExecuteCommandOutput
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: executeCommandTimeout}),
		ExecuteCommand,
		ExecuteCommandInput{
			Provider: *s.state.ComputeProvider,
			Status:   s.state.Status,
			Command:  inp.Command,
		},
	).Get(ctx, &out)
	if err != nil {
		return nil, err
	}

	// Start an idle timer unless auto-suspend is disabled. If no further command
	// arrives within effectiveIdleTimeout, the sandbox is automatically suspended.
	if s.state.IdleTimeout == compute.NoIdleTimeout {
		return out.Result, nil
	}
	workflow.Go(ctx, func(gCtx workflow.Context) {
		timerCtx, cancel := workflow.WithCancel(gCtx)
		s.cancelIdleTimer = cancel
		if err := workflow.NewTimer(timerCtx, s.effectiveIdleTimeout()).Get(timerCtx, nil); err != nil {
			return // cancelled by the next command or workflow stop
		}

		if s.lifecycle != sandboxSuspended && !s.cancelRequested {
			if err := s.handleSuspend(gCtx, struct{}{}); err != nil {
				workflow.GetLogger(gCtx).Error("sandbox: idle auto-suspend failed; sandbox continues running",
					"error", err)
			}
		}
	})

	return out.Result, nil
}

func (s *sandboxWorkflow) validateSuspend(_ struct{}) error {
	if s.lifecycle == sandboxPending || s.lifecycle == sandboxFailed || s.lifecycle == sandboxDeleted {
		return temporal.NewApplicationError("sandbox not initialized", "NotInitialized")
	}
	if s.lifecycle == sandboxSuspended {
		return temporal.NewApplicationError("sandbox already suspended", "AlreadySuspended")
	}
	return nil
}

func (s *sandboxWorkflow) handleSuspend(ctx workflow.Context, _ struct{}) error {
	s.cancelIdleTimerIfActive()

	if s.state.Status == nil {
		return temporal.NewNonRetryableApplicationError("invalid sandbox state for suspend", "InvalidSandboxState", nil)
	}

	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: suspendSandboxTimeout}),
		SuspendSandbox,
		SuspendSandboxInput{Provider: *s.state.ComputeProvider, Status: s.state.Status},
	).Get(ctx, nil)
	if err != nil {
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) && appErr.Type() == errUnsupportedType {
			return s.suspendViaSnapshot(ctx)
		}
		return err
	}
	s.lifecycle = sandboxSuspended
	return nil
}

// suspendViaSnapshot is the fallback for providers that don't support native Suspend.
// It snapshots the sandbox and stores the snapshot for use by handleResume. StopSandbox
// is only called when the sandbox is still running after the snapshot — some providers
// already pause the sandbox as part of the snapshot operation.
func (s *sandboxWorkflow) suspendViaSnapshot(ctx workflow.Context) error {
	var snapOut SnapshotSandboxOutput
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: snapshotSandboxTimeout}),
		SnapshotSandbox,
		SnapshotSandboxInput{
			Provider: *s.state.ComputeProvider,
			Status:   s.state.Status,
		},
	).Get(ctx, &snapOut); err != nil {
		return err
	}
	// A nil snapshot is a provider bug: we cannot use snapshot-based suspend
	// without a snapshot to resume from.
	if snapOut.Snapshot == nil {
		return temporal.NewApplicationError(
			"provider returned a nil snapshot; cannot use snapshot-based suspend",
			"InvalidProviderBehavior",
		)
	}
	if snapOut.SandboxState == compute.SandboxPostSnapshotRunning {
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: stopSandboxTimeout}),
			StopSandbox,
			StopSandboxInput{
				Provider: *s.state.ComputeProvider,
				Status:   s.state.Status,
			},
		).Get(ctx, nil); err != nil {
			return err
		}
	}
	s.suspendSnapshot = snapOut.Snapshot
	s.lifecycle = sandboxSuspended
	return nil
}

func (s *sandboxWorkflow) validateSnapshot(_ struct{}) error {
	if s.lifecycle == sandboxPending || s.lifecycle == sandboxFailed || s.lifecycle == sandboxDeleted {
		return temporal.NewApplicationError("sandbox not initialized", "InvalidSandboxState")
	}
	if s.lifecycle == sandboxSuspended {
		return temporal.NewApplicationError("sandbox is currently suspended", "InvalidSandboxState")
	}
	return nil
}

func (s *sandboxWorkflow) handleSnapshot(ctx workflow.Context, _ struct{}) (*compute.ProviderSnapshot, error) {
	var out SnapshotSandboxOutput
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: snapshotSandboxTimeout}),
		SnapshotSandbox,
		SnapshotSandboxInput{
			Provider: *s.state.ComputeProvider,
			Status:   s.state.Status,
		},
	).Get(ctx, &out); err != nil {
		return nil, err
	}
	if out.Snapshot == nil {
		return nil, temporal.NewApplicationError("provider returned no error but also no snapshot", "InvalidProviderBehavior")
	}

	s.cancelIdleTimerIfActive()

	switch out.SandboxState {
	case compute.SandboxPostSnapshotSuspended:
		s.lifecycle = sandboxSuspended
		// Store the snapshot so handleResume can restart via StartSandboxFromSnapshot
		// for providers that suspend the sandbox as part of snapshotting (e.g. GKE).
		// Mark it user-owned so the SDK never auto-deletes it on resume or cleanup.
		s.suspendSnapshot = out.Snapshot
		s.suspendSnapshotUserOwned = true
	case compute.SandboxPostSnapshotDeleted:
		s.lifecycle = sandboxDeleted
		s.state.Status = nil // sandbox no longer exists; skip StopSandbox in cleanup
		s.cancelRequested = true
	}
	return out.Snapshot, nil
}

func (s *sandboxWorkflow) validateDeleteSnapshot(snapshot *compute.ProviderSnapshot) error {
	if s.lifecycle == sandboxPending || s.lifecycle == sandboxFailed || s.lifecycle == sandboxDeleted {
		return temporal.NewApplicationError("sandbox not initialized", "NotInitialized")
	}
	if snapshot == nil || snapshot.SnapshotID == "" {
		return temporal.NewNonRetryableApplicationError("invalid snapshot reference", "InvalidArgument", nil)
	}
	if s.state.ComputeProvider == nil {
		return temporal.NewNonRetryableApplicationError("invalid sandbox state", "InvalidSandboxState", nil)
	}
	return nil
}

func (s *sandboxWorkflow) handleDeleteSnapshot(ctx workflow.Context, snapshot *compute.ProviderSnapshot) error {
	if s.suspendSnapshot != nil && snapshot != nil && s.suspendSnapshot.SnapshotID == snapshot.SnapshotID {
		return temporal.NewApplicationError("snapshot is currently in use by this sandbox", "SnapshotInUse")
	}

	return workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: deleteSnapshotTimeout}),
		DeleteSnapshot,
		DeleteSnapshotInput{
			Provider: *s.state.ComputeProvider,
			Snapshot: snapshot,
		},
	).Get(ctx, nil)
}

func (s *sandboxWorkflow) validateResume(_ struct{}) error {
	if s.lifecycle == sandboxPending || s.lifecycle == sandboxFailed || s.lifecycle == sandboxDeleted {
		return temporal.NewApplicationError("sandbox not initialized", "NotInitialized")
	}
	if s.lifecycle != sandboxSuspended {
		return temporal.NewApplicationError("sandbox not suspended", "NotSuspended")
	}
	return nil
}

func (s *sandboxWorkflow) handleResume(ctx workflow.Context, _ struct{}) error {
	if s.suspendSnapshot != nil {
		var out StartSandboxFromSnapshotOutput
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: startFromSnapshotTimeout}),
			StartSandboxFromSnapshot,
			StartSandboxFromSnapshotInput{
				Provider:      *s.state.ComputeProvider,
				TaskQueueName: s.taskQueueName(ctx),
				Snapshot:      s.suspendSnapshot,
			},
		).Get(ctx, &out); err != nil {
			return err
		}
		s.state.Status = out.Status
		oldSnapshot := s.suspendSnapshot
		wasUserOwned := s.suspendSnapshotUserOwned
		s.suspendSnapshot = nil
		s.suspendSnapshotUserOwned = false
		s.lifecycle = sandboxRunning

		// Only delete the snapshot when it was created internally by suspendViaSnapshot.
		// User-owned snapshots (from an explicit Sandbox.Snapshot call) are the caller's
		// responsibility; auto-deleting them would invalidate any forks they intend to create.
		if !wasUserOwned {
			if err := workflow.ExecuteActivity(
				workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: deleteSnapshotTimeout}),
				DeleteSnapshot,
				DeleteSnapshotInput{
					Provider: *s.state.ComputeProvider,
					Snapshot: oldSnapshot,
				},
			).Get(ctx, nil); err != nil {
				workflow.GetLogger(ctx).Error("sandbox: failed to delete suspend snapshot after resume",
					"providerType", s.state.ComputeProvider.Type,
					"sandboxID", workflow.GetInfo(ctx).WorkflowExecution.ID,
					"snapshotID", oldSnapshot.SnapshotID,
					"error", err)
			}
		}
		return nil
	}
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: resumeSandboxTimeout}),
		ResumeSandbox,
		ResumeSandboxInput{Provider: *s.state.ComputeProvider, Status: s.state.Status},
	).Get(ctx, nil)
	if err != nil {
		return err
	}
	s.lifecycle = sandboxRunning
	return nil
}

// listenToSignals spawns a goroutine that sets cancelRequested when either
// the stop signal or context cancellation is received.
func (s *sandboxWorkflow) listenToSignals(ctx workflow.Context) {
	stopCh := workflow.GetSignalChannel(ctx, SandboxStopSignal)

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

		for !s.cancelRequested {
			selector.Select(ctx)
		}
	})
}

func (s *sandboxWorkflow) cancelIdleTimerIfActive() {
	if s.cancelIdleTimer != nil {
		s.cancelIdleTimer()
		s.cancelIdleTimer = nil
	}
}

func (s *sandboxWorkflow) effectiveIdleTimeout() time.Duration {
	if s.state.IdleTimeout > 0 {
		return s.state.IdleTimeout
	}
	return idleAutoSuspendTimeout
}

func (s *sandboxWorkflow) taskQueueName(ctx workflow.Context) string {
	wfInfo := workflow.GetInfo(ctx)

	return fmt.Sprintf("sandbox-%s", wfInfo.WorkflowExecution.ID)
}
