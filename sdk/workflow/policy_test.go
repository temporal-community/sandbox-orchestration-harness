package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

var workflowPolicySequence atomic.Uint64

// registerTestPolicy separates pure admission from a constructor that records attempted allocation.
func registerTestPolicy(t *testing.T) (compute.ProviderDetails, *atomic.Int32) {
	t.Helper()
	typ := compute.ProviderType(fmt.Sprintf("%s-%d", t.Name(), workflowPolicySequence.Add(1)))
	calls := new(atomic.Int32)
	compute.Register(typ, func(map[string]string) (compute.Provider, error) {
		calls.Add(1)
		return nil, errors.New("constructor reached")
	})
	compute.RegisterPolicy(typ, func(config map[string]string) (compute.ExecutionPolicy, error) {
		if config["lifecycle"] == "invalid" {
			return compute.ExecutionPolicy{}, errors.New("invalid test lifecycle")
		}
		return compute.ExecutionPolicy{Lifecycle: compute.LifecyclePolicy(config["lifecycle"])}, nil
	})
	return compute.ProviderDetails{Type: typ, Config: map[string]string{"lifecycle": "fresh"}}, calls
}

func requirePolicyRejection(t *testing.T, err error, message string) {
	t.Helper()
	var applicationError *temporal.ApplicationError
	if !errors.As(err, &applicationError) || applicationError.Type() != errUnsupportedType || !applicationError.NonRetryable() || !strings.Contains(err.Error(), message) {
		t.Fatalf("rejection = %v, want nonretryable %s containing %q", err, errUnsupportedType, message)
	}
}

func TestControllerInitialPolicyAdmission(t *testing.T) {
	provider, calls := registerTestPolicy(t)
	for _, test := range []struct {
		name     string
		idle     time.Duration
		snapshot *compute.ProviderSnapshot
		message  string
	}{
		{name: "default idle", message: "requires NoIdleTimeout"},
		{name: "positive idle", idle: time.Minute, message: "requires NoIdleTimeout"},
		{name: "snapshot", idle: compute.NoIdleTimeout, snapshot: &compute.ProviderSnapshot{SnapshotID: "snapshot"}, message: "does not support restore"},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := SandboxInitInput{ComputeProvider: provider, IdleTimeout: test.idle, Snapshot: test.snapshot}
			controller := &sandboxWorkflow{}
			requirePolicyRejection(t, controller.validateInit(input), test.message)
			requirePolicyRejection(t, controller.handleInit(nil, input), test.message)
			if controller.state.ComputeProvider != nil || controller.state.Status != nil || controller.lifecycle != sandboxPending || calls.Load() != 0 {
				t.Fatal("rejected initialization changed state or constructed a provider")
			}
		})
	}
	controller := &sandboxWorkflow{}
	if err := controller.validateInit(SandboxInitInput{ComputeProvider: provider, IdleTimeout: compute.NoIdleTimeout}); err != nil {
		t.Fatalf("valid fresh initialization rejected: %v", err)
	}
	provider.Config["lifecycle"] = ""
	if err := controller.validateInit(SandboxInitInput{ComputeProvider: provider, Snapshot: &compute.ProviderSnapshot{SnapshotID: "snapshot"}}); err != nil {
		t.Fatalf("legacy initialization rejected: %v", err)
	}
	provider.Config["lifecycle"] = "invalid"
	for _, err := range []error{
		controller.validateInit(SandboxInitInput{ComputeProvider: provider}),
		controller.handleInit(nil, SandboxInitInput{ComputeProvider: provider}),
	} {
		var applicationError *temporal.ApplicationError
		if !errors.As(err, &applicationError) || applicationError.Type() != "ProviderConfigError" || !applicationError.NonRetryable() || !strings.Contains(err.Error(), "invalid test lifecycle") {
			t.Fatalf("invalid configuration rejection = %v", err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("initial admission constructed a provider")
	}
}

func TestControllerRetainedPolicyGuards(t *testing.T) {
	provider, _ := registerTestPolicy(t)
	snapshot := &compute.ProviderSnapshot{SnapshotID: "snapshot"}
	for _, test := range []struct {
		operation string
		lifecycle sandboxLifecycle
		validate  func(*sandboxWorkflow) error
		handle    func(*sandboxWorkflow) error
	}{
		{operation: "suspend", lifecycle: sandboxRunning, validate: func(s *sandboxWorkflow) error { return s.validateSuspend(struct{}{}) }, handle: func(s *sandboxWorkflow) error { return s.handleSuspend(nil, struct{}{}) }},
		{operation: "resume", lifecycle: sandboxSuspended, validate: func(s *sandboxWorkflow) error { return s.validateResume(struct{}{}) }, handle: func(s *sandboxWorkflow) error { return s.handleResume(nil, struct{}{}) }},
		{operation: "snapshot", lifecycle: sandboxRunning, validate: func(s *sandboxWorkflow) error { return s.validateSnapshot(struct{}{}) }, handle: func(s *sandboxWorkflow) error { _, err := s.handleSnapshot(nil, struct{}{}); return err }},
		{operation: "delete-snapshot", lifecycle: sandboxRunning, validate: func(s *sandboxWorkflow) error { return s.validateDeleteSnapshot(snapshot) }, handle: func(s *sandboxWorkflow) error { return s.handleDeleteSnapshot(nil, snapshot) }},
		{operation: "resume", lifecycle: sandboxSuspended, validate: func(s *sandboxWorkflow) error { return s.validateExecuteCommand(SandboxExecuteCommandInput{}) }, handle: func(s *sandboxWorkflow) error {
			_, err := s.handleExecuteCommand(nil, SandboxExecuteCommandInput{})
			return err
		}},
	} {
		t.Run(test.operation, func(t *testing.T) {
			canceled := false
			controller := &sandboxWorkflow{
				state:  SandboxLocalState{ComputeProvider: &provider, Status: &compute.ProviderStatus{InstanceID: "instance"}},
				policy: compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}, lifecycle: test.lifecycle,
				cancelIdleTimer: func() { canceled = true }, suspendSnapshot: snapshot,
			}
			requirePolicyRejection(t, test.validate(controller), "does not support "+test.operation)
			requirePolicyRejection(t, test.handle(controller), "does not support "+test.operation)
			if canceled || controller.lifecycle != test.lifecycle || controller.state.Status.InstanceID != "instance" || controller.suspendSnapshot != snapshot {
				t.Fatal("policy rejection changed lifecycle state")
			}
			controller.policy = compute.ExecutionPolicy{}
			if err := test.validate(controller); err != nil {
				t.Fatalf("zero policy rejected existing operation: %v", err)
			}
		})
	}
	controller := &sandboxWorkflow{policy: compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}, lifecycle: sandboxRunning}
	if err := controller.validateExecuteCommand(SandboxExecuteCommandInput{}); err != nil {
		t.Fatalf("fresh policy rejected command: %v", err)
	}
}

func TestReceivingActivityPolicyBeforeConstruction(t *testing.T) {
	provider, calls := registerTestPolicy(t)
	ctx := context.Background()
	for _, test := range []struct {
		operation compute.Operation
		run       func() error
	}{
		{operation: compute.OperationSuspend, run: func() error { return SuspendSandbox(ctx, SuspendSandboxInput{Provider: provider}) }},
		{operation: compute.OperationResume, run: func() error { return ResumeSandbox(ctx, ResumeSandboxInput{Provider: provider}) }},
		{operation: compute.OperationSnapshot, run: func() error { _, err := SnapshotSandbox(ctx, SnapshotSandboxInput{Provider: provider}); return err }},
		{operation: compute.OperationRestore, run: func() error {
			_, err := StartSandboxFromSnapshot(ctx, StartSandboxFromSnapshotInput{Provider: provider})
			return err
		}},
		{operation: compute.OperationDeleteSnapshot, run: func() error { return DeleteSnapshot(ctx, DeleteSnapshotInput{Provider: provider}) }},
	} {
		t.Run(string(test.operation), func(t *testing.T) {
			provider.Config["lifecycle"] = "fresh"
			before := calls.Load()
			requirePolicyRejection(t, test.run(), "does not support "+string(test.operation))
			if calls.Load() != before {
				t.Fatal("denied Activity constructed a provider")
			}
			provider.Config["lifecycle"] = "invalid"
			var applicationError *temporal.ApplicationError
			if err := test.run(); !errors.As(err, &applicationError) || applicationError.Type() != "ProviderConfigError" || !applicationError.NonRetryable() || !strings.Contains(err.Error(), "invalid test lifecycle") || calls.Load() != before {
				t.Fatalf("invalid Activity configuration rejection = %v, constructor calls = %d", err, calls.Load()-before)
			}
			provider.Config["lifecycle"] = ""
			if err := test.run(); err == nil || !strings.Contains(err.Error(), "constructor reached") || calls.Load() != before+1 {
				t.Fatalf("legacy Activity failed to reach provider: %v, constructor calls = %d", err, calls.Load()-before)
			}
		})
	}
}

func TestControllerInitRetainsResolvedPolicyAndConfiguration(t *testing.T) {
	provider, calls := registerTestPolicy(t)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.OnActivity(StartSandbox, mock.Anything, mock.MatchedBy(func(input StartSandboxInput) bool {
		return input.Provider.Type == provider.Type && input.Provider.Config["lifecycle"] == "fresh" && input.TaskQueueName != ""
	})).Return(StartSandboxOutput{Status: &compute.ProviderStatus{InstanceID: "instance"}}, nil).Once()
	env.ExecuteWorkflow(func(ctx temporalworkflow.Context) (string, error) {
		controller := &sandboxWorkflow{}
		input := SandboxInitInput{ComputeProvider: provider, IdleTimeout: compute.NoIdleTimeout}
		if err := controller.handleInit(ctx, input); err != nil {
			return "", err
		}
		input.ComputeProvider.Config["lifecycle"] = ""
		if controller.policy.Lifecycle != compute.LifecycleFresh || controller.state.ComputeProvider.Config["lifecycle"] != "fresh" || controller.state.IdleTimeout != compute.NoIdleTimeout || controller.lifecycle != sandboxRunning {
			return "", errors.New("controller lost resolved policy or copied configuration")
		}
		if err := controller.validateSnapshot(struct{}{}); err == nil || !strings.Contains(err.Error(), "does not support snapshot") {
			return "", fmt.Errorf("retained snapshot policy = %v", err)
		}
		return controller.state.Status.InstanceID, nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var instance string
	if err := env.GetWorkflowResult(&instance); err != nil || instance != "instance" {
		t.Fatalf("initialized instance = %q, %v", instance, err)
	}
	if calls.Load() != 0 {
		t.Fatal("mocked launch constructed a provider")
	}
	env.AssertExpectations(t)
}
