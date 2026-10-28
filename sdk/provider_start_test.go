package sandbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os/exec"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/mock"
	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	controller "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// This fake owns its workload configuration and idempotency. The SDK only
// transports provider configuration through the existing lifecycle Activities.
type nativeLaunch struct {
	Queue   string
	Config  map[string]string
	Attempt int32
}

type nativeResource struct {
	ID     string
	Output string
}

type nativeBoundary struct {
	mu             sync.Mutex
	Starts         []nativeLaunch
	Resources      map[string]nativeResource
	Stops          []string
	LoseFirstReply bool
}

type nativeProvider struct {
	boundary *nativeBoundary
	config   map[string]string
}

var _ compute.Provider = (*nativeProvider)(nil)

func registerNativeBoundary(b *nativeBoundary) compute.ProviderType {
	typ := compute.ProviderType("native-process-test-" + uuid.NewString())
	b.Resources = make(map[string]nativeResource)
	compute.Register(typ, func(config map[string]string) (compute.Provider, error) {
		if config["workload"] != "printf native-process" {
			return nil, fmt.Errorf("workload must select the supported native process")
		}
		return &nativeProvider{boundary: b, config: maps.Clone(config)}, nil
	})
	return typ
}

func (p *nativeProvider) Start(ctx context.Context, queue string) (*compute.ProviderStatus, error) {
	p.boundary.mu.Lock()
	defer p.boundary.mu.Unlock()
	b := p.boundary
	b.Starts = append(b.Starts, nativeLaunch{Queue: queue, Config: maps.Clone(p.config), Attempt: activity.GetInfo(ctx).Attempt})
	resource, exists := b.Resources[queue]
	if !exists {
		// Launch a bounded ordinary process, with no Temporal Worker or polling.
		out, err := exec.CommandContext(ctx, "sh", "-c", p.config["workload"]).Output()
		if err != nil {
			return nil, err
		}
		resource = nativeResource{ID: "resource-" + queue, Output: string(out)}
		b.Resources[queue] = resource
		if b.LoseFirstReply {
			return nil, errors.New("simulated lost allocation reply")
		}
	}
	return &compute.ProviderStatus{InstanceID: resource.ID}, nil
}

func (p *nativeProvider) Stop(_ context.Context, status *compute.ProviderStatus) error {
	p.boundary.mu.Lock()
	defer p.boundary.mu.Unlock()
	p.boundary.Stops = append(p.boundary.Stops, status.InstanceID)
	return nil
}
func (p *nativeProvider) ExecuteCommand(_ context.Context, status *compute.ProviderStatus, command string) (*compute.CommandResult, error) {
	p.boundary.mu.Lock()
	defer p.boundary.mu.Unlock()
	for _, resource := range p.boundary.Resources {
		if resource.ID == status.InstanceID && command == "read-native-output" {
			return &compute.CommandResult{Stdout: resource.Output}, nil
		}
	}
	return nil, errors.New("unknown resource or command")
}
func (*nativeProvider) Suspend(context.Context, *compute.ProviderStatus) error {
	return errors.ErrUnsupported
}
func (*nativeProvider) Resume(context.Context, *compute.ProviderStatus) error {
	return errors.ErrUnsupported
}
func (*nativeProvider) Snapshot(context.Context, *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	return compute.SandboxPostSnapshotRunning, nil, errors.ErrUnsupported
}
func (*nativeProvider) StartFromSnapshot(context.Context, string, *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	return nil, errors.ErrUnsupported
}
func (*nativeProvider) DeleteSnapshot(context.Context, *compute.ProviderSnapshot) error {
	return errors.ErrUnsupported
}

func startActivityWorkflow(ctx workflow.Context, input controller.StartSandboxInput) (controller.StartSandboxOutput, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Millisecond, MaximumAttempts: 3},
	})
	var output controller.StartSandboxOutput
	err := workflow.ExecuteActivity(ctx, controller.StartSandbox, input).Get(ctx, &output)
	return output, err
}

func TestExistingStartRetriesPreserveProviderConfiguration(t *testing.T) {
	b := &nativeBoundary{LoseFirstReply: true}
	typ := registerNativeBoundary(b)
	input := controller.StartSandboxInput{Provider: compute.ProviderDetails{Type: typ, Config: map[string]string{
		"workload": "printf native-process", "provider-owned-extra": "preserved",
	}}, TaskQueueName: "stable-resource-identity"}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(controller.StartSandbox)
	env.ExecuteWorkflow(startActivityWorkflow, input)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var output controller.StartSandboxOutput
	if err := env.GetWorkflowResult(&output); err != nil {
		t.Fatal(err)
	}
	if len(b.Starts) != 2 || len(b.Resources) != 1 {
		t.Fatalf("starts=%+v resources=%+v", b.Starts, b.Resources)
	}
	for i, launch := range b.Starts {
		if launch.Queue != input.TaskQueueName || !reflect.DeepEqual(launch.Config, input.Provider.Config) || launch.Attempt != int32(i+1) {
			t.Fatalf("retry %d changed provider input: %+v", i, launch)
		}
	}
	resource := b.Resources[input.TaskQueueName]
	if resource.Output != "native-process" || output.Status == nil || output.Status.InstanceID != resource.ID {
		t.Fatalf("native process result=%+v status=%+v", resource, output.Status)
	}
}

func TestExistingStartRejectsInvalidProviderBeforeAllocation(t *testing.T) {
	for _, name := range []string{"unknown", "missing-workload", "invalid-workload"} {
		t.Run(name, func(t *testing.T) {
			b := &nativeBoundary{}
			typ := registerNativeBoundary(b)
			config := map[string]string{}
			if name == "unknown" {
				typ = compute.ProviderType(uuid.NewString())
			}
			if name == "invalid-workload" {
				config["workload"] = "unsupported-process"
			}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterActivity(controller.StartSandbox)
			env.ExecuteWorkflow(startActivityWorkflow, controller.StartSandboxInput{Provider: compute.ProviderDetails{Type: typ, Config: config}, TaskQueueName: "unused"})
			var app *temporal.ApplicationError
			if !errors.As(env.GetWorkflowError(), &app) || app.Type() != "ProviderConfigError" || !app.NonRetryable() {
				t.Fatalf("expected nonretryable provider configuration error, got %v", env.GetWorkflowError())
			}
			if len(b.Starts) != 0 || len(b.Resources) != 0 {
				t.Fatalf("invalid configuration allocated: %+v", b)
			}
		})
	}
}

// SDK registration accepts dynamic function values; the recorder narrows that
// boundary to registered function names and rejects unexpected registration modes.
type registrationRecorder struct{ workflows, activities []string }

var _ worker.Registry = (*registrationRecorder)(nil)

func registeredName(fn any) string {
	name := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	return strings.TrimSuffix(name[strings.LastIndex(name, ".")+1:], "-fm")
}
func (r *registrationRecorder) RegisterWorkflow(fn any) {
	r.workflows = append(r.workflows, registeredName(fn))
}
func (r *registrationRecorder) RegisterActivity(fn any) {
	r.activities = append(r.activities, registeredName(fn))
}
func (*registrationRecorder) RegisterWorkflowWithOptions(any, workflow.RegisterOptions) {
	panic("unexpected workflow alias")
}
func (*registrationRecorder) RegisterDynamicWorkflow(any, workflow.DynamicRegisterOptions) {
	panic("unexpected dynamic workflow")
}
func (*registrationRecorder) RegisterActivityWithOptions(any, activity.RegisterOptions) {
	panic("unexpected activity alias")
}
func (*registrationRecorder) RegisterDynamicActivity(any, activity.DynamicRegisterOptions) {
	panic("unexpected dynamic activity")
}
func (*registrationRecorder) RegisterNexusService(*nexus.Service) { panic("unexpected Nexus service") }

func TestExistingSandboxRegistrationContract(t *testing.T) {
	r := &registrationRecorder{}
	sandbox.Register(r, nil)
	if !reflect.DeepEqual(r.workflows, []string{"SandboxWorkflow"}) {
		t.Fatalf("workflows: %v", r.workflows)
	}
	expected := []string{"SendSandboxInit", "SendSandboxExecuteCommand", "SendSandboxSuspend", "SendSandboxResume", "SendSandboxSnapshot", "SendSandboxDeleteSnapshot", "StartSandbox", "StopSandbox", "SuspendSandbox", "ResumeSandbox", "SnapshotSandbox", "StartSandboxFromSnapshot", "DeleteSnapshot", "ExecuteCommand"}
	slices.Sort(expected)
	slices.Sort(r.activities)
	if !reflect.DeepEqual(r.activities, expected) {
		t.Fatalf("activities: %v", r.activities)
	}
}

func TestExistingSandboxWireShapes(t *testing.T) {
	provider := compute.ProviderDetails{Type: "fake", Config: map[string]string{"workload": "native"}}
	status := &compute.ProviderStatus{InstanceID: "resource"}
	snapshot := &compute.ProviderSnapshot{SnapshotID: "snapshot"}
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"init", controller.SandboxInitInput{ComputeProvider: provider}, `{"compute_provider":{"type":"fake","config":{"workload":"native"}},"idle_timeout":0,"snapshot":null}`},
		{"start", controller.StartSandboxInput{Provider: provider, TaskQueueName: "stable"}, `{"provider":{"type":"fake","config":{"workload":"native"}},"task_queue_name":"stable"}`},
		{"start-output", controller.StartSandboxOutput{Status: status}, `{"status":{"instance_id":"resource"}}`},
		{"stop", controller.StopSandboxInput{Provider: provider, Status: status}, `{"provider":{"type":"fake","config":{"workload":"native"}},"status":{"instance_id":"resource"}}`},
		{"snapshot-output", controller.SnapshotSandboxOutput{SandboxState: compute.SandboxPostSnapshotRunning, Snapshot: snapshot}, `{"sandbox_state":0,"snapshot":{"snapshot_id":"snapshot"}}`},
		{"snapshot-start", controller.StartSandboxFromSnapshotInput{Provider: provider, TaskQueueName: "stable", Snapshot: snapshot}, `{"provider":{"type":"fake","config":{"workload":"native"}},"task_queue_name":"stable","snapshot":{"snapshot_id":"snapshot"}}`},
		{"snapshot-start-output", controller.StartSandboxFromSnapshotOutput{Status: status}, `{"status":{"instance_id":"resource"}}`},
		{"delete-snapshot", controller.DeleteSnapshotInput{Provider: provider, Snapshot: snapshot}, `{"provider":{"type":"fake","config":{"workload":"native"}},"snapshot":{"snapshot_id":"snapshot"}}`},
		{"state", controller.SandboxState{Lifecycle: controller.SandboxLifecycleRunning, ComputeProvider: &provider, Status: status}, `{"lifecycle":1,"compute_provider":{"type":"fake","config":{"workload":"native"}},"status":{"instance_id":"resource"},"idle_timeout":0}`},
		{"local-state", controller.SandboxLocalState{WorkflowID: "parent", RunID: "run"}, `{"workflow_id":"parent","run_id":"run","compute_provider":null,"status":null,"idle_timeout":0}`},
		{"send-init", sandbox.SendSandboxInitInput{SandboxID: "sandbox", UpdateID: "update", ComputeProvider: provider}, `{"SandboxID":"sandbox","UpdateID":"update","ComputeProvider":{"type":"fake","config":{"workload":"native"}},"IdleTimeout":0,"Snapshot":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("wire shape changed: %s", got)
			}
		})
	}
}

type optionForwardingInput struct {
	Provider compute.ProviderDetails
	Idle     time.Duration
	Snapshot *compute.ProviderSnapshot
}

func optionForwardingWorkflow(ctx workflow.Context, input optionForwardingInput) error {
	options := []sandbox.SandboxOption{sandbox.WithCleanup(sandbox.CleanupDisabled)}
	if input.Idle != 0 {
		options = append(options, sandbox.WithIdleTimeout(input.Idle))
	}
	if input.Snapshot != nil {
		options = append(options, sandbox.WithSnapshot(input.Snapshot))
	}
	_, err := sandbox.NewSandbox(ctx, input.Provider, options...)
	return err
}

func TestExistingSandboxOptionsForwardedUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name     string
		idle     time.Duration
		snapshot *compute.ProviderSnapshot
	}{
		{name: "default"},
		{name: "no-idle-timeout", idle: sandbox.NoIdleTimeout},
		{name: "snapshot", snapshot: &compute.ProviderSnapshot{SnapshotID: "existing-snapshot"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := optionForwardingInput{Provider: compute.ProviderDetails{Type: "option-forwarding-fake", Config: map[string]string{"opaque": "preserved"}}, Idle: tc.idle, Snapshot: tc.snapshot}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(controller.SandboxWorkflow)
			env.OnWorkflow(controller.SandboxWorkflow, mock.Anything, mock.Anything).Return(nil)
			var received []sandbox.SendSandboxInitInput
			env.RegisterActivityWithOptions(func(_ context.Context, value sandbox.SendSandboxInitInput) error {
				received = append(received, value)
				return nil
			}, activity.RegisterOptions{Name: "SendSandboxInit"})
			env.ExecuteWorkflow(optionForwardingWorkflow, input)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if len(received) != 1 {
				t.Fatalf("init calls=%d", len(received))
			}
			got := received[0]
			if got.SandboxID == "" || got.UpdateID == "" || got.IdleTimeout != tc.idle || !reflect.DeepEqual(got.Snapshot, tc.snapshot) || !reflect.DeepEqual(got.ComputeProvider, input.Provider) {
				t.Fatalf("options changed: %+v", got)
			}
		})
	}
}

func TestExistingControllerSnapshotStartRouting(t *testing.T) {
	b := &nativeBoundary{}
	typ := registerNativeBoundary(b)
	provider := compute.ProviderDetails{Type: typ, Config: map[string]string{"workload": "printf native-process", "opaque": "retained"}}
	snapshot := &compute.ProviderSnapshot{SnapshotID: "existing-snapshot"}
	status := &compute.ProviderStatus{InstanceID: "snapshot-resource"}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(controller.StartSandbox)
	env.RegisterActivity(controller.StartSandboxFromSnapshot)
	env.RegisterActivity(controller.StopSandbox)
	var starts []controller.StartSandboxFromSnapshotInput
	var stops []controller.StopSandboxInput
	env.OnActivity(controller.StartSandboxFromSnapshot, mock.Anything, mock.Anything).Return(func(_ context.Context, input controller.StartSandboxFromSnapshotInput) (controller.StartSandboxFromSnapshotOutput, error) {
		starts = append(starts, input)
		return controller.StartSandboxFromSnapshotOutput{Status: status}, nil
	})
	env.OnActivity(controller.StopSandbox, mock.Anything, mock.Anything).Return(func(_ context.Context, input controller.StopSandboxInput) error {
		stops = append(stops, input)
		return nil
	})
	completed := false
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(controller.SandboxInitUpdate, "snapshot-init", &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Errorf("snapshot init rejected: %v", err) },
			OnAccept: func() {},
			OnComplete: func(_ any, err error) {
				completed = true
				if err != nil {
					t.Errorf("snapshot init failed: %v", err)
				}
				env.SignalWorkflow(controller.SandboxStopSignal, nil)
			},
		}, controller.SandboxInitInput{ComputeProvider: provider, Snapshot: snapshot, IdleTimeout: sandbox.NoIdleTimeout})
	}, 0)
	env.ExecuteWorkflow(controller.SandboxWorkflow, controller.SandboxLocalState{WorkflowID: "parent", RunID: "run"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !completed || len(starts) != 1 || len(stops) != 1 || len(b.Starts) != 0 {
		t.Fatalf("completed=%v snapshot starts=%+v stops=%+v fresh starts=%+v", completed, starts, stops, b.Starts)
	}
	if !reflect.DeepEqual(starts[0].Provider, provider) || !reflect.DeepEqual(starts[0].Snapshot, snapshot) || !strings.HasPrefix(starts[0].TaskQueueName, "sandbox-") {
		t.Fatalf("snapshot route changed: %+v", starts[0])
	}
	if !reflect.DeepEqual(stops[0], controller.StopSandboxInput{Provider: provider, Status: status}) {
		t.Fatalf("snapshot cleanup lost provider/resource: %+v", stops[0])
	}
}
