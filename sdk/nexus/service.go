// Package sandboxnexus exposes the existing sandbox workflow through Nexus.
package sandboxnexus

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	gen "github.com/temporal-community/sandbox-orchestration-harness/sdk/generated/nexus"
	sandboxworkflow "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
)

const DefaultTaskQueue = "sandbox-control"
const creationMemo = "sandbox.nexus.creation"

type Config struct {
	TaskQueue string
	Profiles  map[string]compute.ProviderDetails
}

// Register adds the existing SDK workflow, its activities, and the Nexus service.
func Register(w worker.Registry, c client.Client, cfg Config) error {
	service, err := NewService(c, cfg)
	if err != nil {
		return err
	}
	sandbox.Register(w, c)
	w.RegisterNexusService(service)
	return nil
}

// NewService forwards operations to SandboxWorkflow's existing updates and query.
// Updates return after acceptance. Temporal delivers their results by callback.
func NewService(c client.Client, cfg Config) (*nexus.Service, error) {
	if cfg.TaskQueue == "" {
		return nil, fmt.Errorf("Nexus task queue is required")
	}
	if len(cfg.Profiles) == 0 {
		return nil, fmt.Errorf("at least one sandbox profile is required")
	}
	profiles := make(map[string]compute.ProviderDetails, len(cfg.Profiles))
	for name, provider := range cfg.Profiles {
		if name == "" || !compute.IsRegistered(provider.Type) {
			return nil, fmt.Errorf("invalid sandbox profile %q (provider %q)", name, provider.Type)
		}
		provider.Config = maps.Clone(provider.Config)
		profiles[name] = provider
	}
	cfg.Profiles = profiles
	service := nexus.NewService(gen.SandboxControl.ServiceName)
	service.MustRegister(
		createOperation(gen.SandboxControl.Create, cfg, func(input gen.CreateInput) creation {
			return creation{SandboxID: input.SandboxID, Profile: input.Profile, IdleTimeoutSeconds: input.IdleTimeoutSecondsOrDefault()}
		}),
		createOperation(gen.SandboxControl.Fork, cfg, func(input gen.ForkInput) creation {
			return creation{SandboxID: input.SandboxID, Profile: input.Profile, IdleTimeoutSeconds: input.IdleTimeoutSecondsOrDefault(), SnapshotID: input.SnapshotID}
		}),
		updateOperation(gen.SandboxControl.Execute, sandboxworkflow.SandboxExecuteCommandUpdate, func(input gen.ExecuteInput) (string, any) {
			return input.SandboxID, sandboxworkflow.SandboxExecuteCommandInput{Command: input.Command, DisableAutoResume: input.DisableAutoResumeOrDefault()}
		}),
		updateOperation(gen.SandboxControl.Suspend, sandboxworkflow.SandboxSuspendUpdate, emptyArgument),
		updateOperation(gen.SandboxControl.Resume, sandboxworkflow.SandboxResumeUpdate, emptyArgument),
		updateOperation(gen.SandboxControl.Snapshot, sandboxworkflow.SandboxSnapshotUpdate, emptyArgument),
		updateOperation(gen.SandboxControl.DeleteSnapshot, sandboxworkflow.SandboxDeleteSnapshotUpdate, func(input gen.DeleteSnapshotInput) (string, any) {
			return input.SandboxID, &compute.ProviderSnapshot{SnapshotID: input.SnapshotID}
		}),
		deleteOperation(cfg.TaskQueue),
		nexus.NewSyncOperation(gen.SandboxControl.Describe.Name(), func(ctx context.Context, input gen.SandboxHandle, _ nexus.StartOperationOptions) (gen.SandboxDescription, error) {
			if err := input.Validate(); err != nil {
				return gen.SandboxDescription{}, badRequest(err.Error())
			}
			run, err := describeSandbox(ctx, c, input.SandboxID)
			if err != nil {
				return gen.SandboxDescription{}, err
			}
			value, err := c.QueryWorkflow(ctx, input.SandboxID, run.WorkflowExecution.RunID, sandboxworkflow.SandboxStateQuery)
			if err != nil {
				return gen.SandboxDescription{}, operationError(err)
			}
			var state sandboxworkflow.SandboxState
			if err := value.Get(&state); err != nil {
				return gen.SandboxDescription{}, err
			}
			states := []gen.SandboxDescriptionLifecycle{
				gen.SandboxDescriptionLifecyclePending, gen.SandboxDescriptionLifecycleRunning,
				gen.SandboxDescriptionLifecycleSuspended, gen.SandboxDescriptionLifecycleSuspendedWithSnapshot,
				gen.SandboxDescriptionLifecycleFailed, gen.SandboxDescriptionLifecycleDeleted,
			}
			result := gen.SandboxDescription{SandboxID: input.SandboxID, Lifecycle: states[state.Lifecycle]}
			if state.Status != nil {
				result.InstanceID = state.Status.InstanceID
			}
			return result, nil
		}),
	)
	return service, nil
}

// creation is stored in workflow memo to check repeated creation requests.
// Memo excludes provider configuration.
type creation struct {
	SandboxID          string
	Profile            string
	IdleTimeoutSeconds int64
	SnapshotID         string
}

// createOperation synchronously starts a new SandboxWorkflow and then asynchronously
// sends its init update. The handler returns async after the update is accepted by the workflow.
//
// TODO(long-nt-tran): Use async update-with-start-over-Nexus once available so we can do this atomically.
func createOperation[I interface{ Validate() error }](reference nexus.OperationReference[I, nexus.NoValue], cfg Config, request func(I) creation) nexus.Operation[I, nexus.NoValue] {
	return temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[I, nexus.NoValue]{
		Name: reference.Name(),
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input I, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[nexus.NoValue], error) {
			if err := input.Validate(); err != nil {
				return temporalnexus.TemporalOperationResult[nexus.NoValue]{}, badRequest(err.Error())
			}
			spec := request(input)
			provider, ok := cfg.Profiles[spec.Profile]
			if !ok {
				return temporalnexus.TemporalOperationResult[nexus.NoValue]{}, badRequest("unknown sandbox profile")
			}
			c := nc.GetWorkflowClient()
			// Start the same workflow used by the SDK. Its init update performs provisioning.
			_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
				ID: spec.SandboxID, TaskQueue: cfg.TaskQueue,
				WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
				WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				Memo:                     map[string]interface{}{creationMemo: spec},
			}, sandboxworkflow.SandboxWorkflow, sandboxworkflow.SandboxLocalState{})
			if err != nil {
				return temporalnexus.TemporalOperationResult[nexus.NoValue]{}, operationError(err)
			}
			run, err := describeSandbox(ctx, c, spec.SandboxID)
			if err != nil {
				return temporalnexus.TemporalOperationResult[nexus.NoValue]{}, err
			}
			var previous creation
			payload := run.Memo.GetFields()[creationMemo]
			if payload == nil {
				return temporalnexus.TemporalOperationResult[nexus.NoValue]{}, badRequest("sandbox_id already used by another creation request")
			}
			if err := run.GetMemoValue(creationMemo, &previous); err != nil {
				return temporalnexus.TemporalOperationResult[nexus.NoValue]{}, err
			}
			if previous != spec {
				return temporalnexus.TemporalOperationResult[nexus.NoValue]{}, badRequest("sandbox_id already used with different parameters")
			}
			init := sandboxworkflow.SandboxInitInput{ComputeProvider: provider, IdleTimeout: idleTimeout(spec.IdleTimeoutSeconds)}
			if spec.SnapshotID != "" {
				init.Snapshot = &compute.ProviderSnapshot{SnapshotID: spec.SnapshotID}
			}
			return startUpdate[nexus.NoValue](ctx, nc, spec.SandboxID, run.WorkflowExecution.RunID, sandboxworkflow.SandboxInitUpdate, "nexus-init", init)
		},
	})
}

// updateOperation forwards a Nexus operation to an existing update of the same name in SandboxWorkflow.
func updateOperation[I interface{ Validate() error }, O any](reference nexus.OperationReference[I, O], name string, argument func(I) (string, any)) nexus.Operation[I, O] {
	return temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[I, O]{
		Name: reference.Name(),
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input I, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[O], error) {
			if err := input.Validate(); err != nil {
				return temporalnexus.TemporalOperationResult[O]{}, badRequest(err.Error())
			}
			id, arg := argument(input)
			run, err := describeSandbox(ctx, nc.GetWorkflowClient(), id)
			if err != nil {
				return temporalnexus.TemporalOperationResult[O]{}, err
			}
			return startUpdate[O](ctx, nc, id, run.WorkflowExecution.RunID, name, "", arg)
		},
	})
}

// startUpdate sends an update to an existing workflow and returns after the update is accepted.
// This allows the handler to return async and yield while the update is processing.
// The update result is delivered by callback to the Nexus client.
func startUpdate[O any](ctx context.Context, nc temporalnexus.NexusClient, id, runID, name, updateID string, arg any) (temporalnexus.TemporalOperationResult[O], error) {
	// Pin RunID so the callback and start response use the same operation token.
	result, err := temporalnexus.StartUpdateWorkflow[O](ctx, nc, client.UpdateWorkflowOptions{
		WorkflowID: id, RunID: runID, UpdateID: updateID, UpdateName: name, Args: []any{arg},
		WaitForStage: client.WorkflowUpdateStageAccepted,
	})
	return result, operationError(err)
}

func deleteOperation(queue string) nexus.Operation[gen.SandboxHandle, nexus.NoValue] {
	return temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[gen.SandboxHandle, nexus.NoValue]{
		Name: gen.SandboxControl.Delete.Name(),
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input gen.SandboxHandle, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[nexus.NoValue], error) {
			if err := input.Validate(); err != nil {
				return temporalnexus.TemporalOperationResult[nexus.NoValue]{}, badRequest(err.Error())
			}
			c := nc.GetWorkflowClient()
			run, err := describeSandbox(ctx, c, input.SandboxID)
			if err != nil {
				return temporalnexus.TemporalOperationResult[nexus.NoValue]{}, err
			}
			runID := run.WorkflowExecution.RunID
			if run.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				if err := c.GetWorkflow(ctx, input.SandboxID, runID).Get(ctx, nil); err != nil {
					return temporalnexus.TemporalOperationResult[nexus.NoValue]{}, nexus.NewOperationFailedError(err.Error())
				}
				return temporalnexus.NewSyncResult[nexus.NoValue](nil), nil
			}
			// Attach the callback to the running sandbox workflow, then send its
			// existing stop signal. No new workflow or Delete update is needed.
			result, err := temporalnexus.StartUntypedWorkflow[nexus.NoValue](ctx, nc, client.StartWorkflowOptions{
				ID: input.SandboxID, TaskQueue: queue,
				WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
				WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			}, sandboxworkflow.SandboxWorkflow, sandboxworkflow.SandboxLocalState{})
			if err != nil {
				return result, operationError(err)
			}
			err = c.SignalWorkflow(ctx, input.SandboxID, runID, sandboxworkflow.SandboxStopSignal, nil)
			return result, operationError(err)
		},
	})
}

func describeSandbox(ctx context.Context, c client.Client, id string) (*client.WorkflowExecutionDescription, error) {
	run, err := c.DescribeWorkflow(ctx, id, "")
	if err != nil {
		return nil, operationError(err)
	}
	if run.WorkflowType.Name != sandboxworkflow.SandboxWorkflowType {
		return nil, badRequest("sandbox_id does not identify a SandboxWorkflow")
	}
	return run, nil
}

func emptyArgument(input gen.SandboxHandle) (string, any) { return input.SandboxID, struct{}{} }
func idleTimeout(seconds int64) time.Duration {
	if seconds == -1 {
		return compute.NoIdleTimeout
	}
	return time.Duration(seconds) * time.Second
}
func badRequest(message string) error {
	return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "%s", message)
}
func operationError(err error) error {
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "sandbox not found")
	}
	return err
}
