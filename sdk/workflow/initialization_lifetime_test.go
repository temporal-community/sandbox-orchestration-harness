package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

func TestControllerCancellationBeforeInitialization(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
	env.ExecuteWorkflow(SandboxWorkflow, SandboxLocalState{})
	if err := env.GetWorkflowError(); !temporal.IsCanceledError(err) {
		t.Fatalf("uninitialized controller result: %v", err)
	}
	env.AssertNotCalled(t, "StartSandbox", mock.Anything, mock.Anything)
	env.AssertNotCalled(t, "StopSandbox", mock.Anything, mock.Anything)
}

// Snapshot initialization keeps immediate cancellation and no ordinary Start claim.
func TestSnapshotInitializationCancellationLifetime(t *testing.T) {
	provider, _ := registerTestPolicy(t)
	provider.Config["lifecycle"] = ""
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.OnActivity(StartSandboxFromSnapshot, mock.Anything, mock.Anything).
		Return(func(context.Context, StartSandboxFromSnapshotInput) (StartSandboxFromSnapshotOutput, error) {
			return StartSandboxFromSnapshotOutput{Status: &compute.ProviderStatus{InstanceID: "snapshot-instance"}}, nil
		}).After(time.Hour)
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
	env.ExecuteWorkflow(func(ctx temporalworkflow.Context) error {
		s := &sandboxWorkflow{}
		err := s.handleInit(ctx, SandboxInitInput{ComputeProvider: provider, Snapshot: &compute.ProviderSnapshot{SnapshotID: "snapshot"}})
		if s.ordinaryStart || s.state.Status != nil || s.lifecycle != sandboxFailed {
			return temporal.NewNonRetryableApplicationError("snapshot acquired ordinary Start lifetime", "InvalidTestResult", nil)
		}
		return err
	})
	if err := env.GetWorkflowError(); !temporal.IsCanceledError(err) {
		t.Fatalf("snapshot cancellation result: %v", err)
	}
	env.AssertExpectations(t)
}
