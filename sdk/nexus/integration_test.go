package sandboxnexus

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	gen "github.com/temporal-community/sandbox-orchestration-harness/sdk/generated/nexus"
	sandboxworkflow "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	nexuspb "go.temporal.io/api/nexus/v1"
	operatorpb "go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// TestNexusIntegration runs real Nexus requests against an isolated Temporal server.
// Set SANDBOX_NEXUS_TEST_SERVER to enable it; normal unit test runs skip it.
// It creates a temporary endpoint and a worker with one Nexus handler slot.
// The worker runs the existing SandboxWorkflow and a fake compute provider.
// Provisioning is blocked while Describe runs. This checks that Create releases
// the handler slot before the provider finishes. The remaining calls check the
// generated contract, update callbacks, snapshot/fork, and cleanup completion.
// Repeated Create uses the same sandbox ID and must not provision it again.
// The test makes no requests to OpenAI or a real compute provider.
func TestNexusIntegration(t *testing.T) {
	address := os.Getenv("SANDBOX_NEXUS_TEST_SERVER")
	if address == "" {
		t.Skip("set SANDBOX_NEXUS_TEST_SERVER to run live callback tests")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	c, err := client.Dial(client.Options{HostPort: address, Namespace: "default"})
	require.NoError(t, err)
	defer c.Close()
	queue := "sandbox-test-" + uuid.NewString()
	endpoint, err := c.OperatorService().CreateNexusEndpoint(ctx, &operatorpb.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{Name: queue, Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{
			Worker: &nexuspb.EndpointTarget_Worker{Namespace: "default", TaskQueue: queue},
		}}},
	})
	require.NoError(t, err)
	defer func() {
		_, err := c.OperatorService().DeleteNexusEndpoint(context.Background(), &operatorpb.DeleteNexusEndpointRequest{
			Id: endpoint.Endpoint.Id, Version: endpoint.Endpoint.Version,
		})
		require.NoError(t, err)
	}()
	p := &integrationProvider{started: make(chan struct{}), release: make(chan struct{})}
	providerType := compute.ProviderType(queue)
	compute.Register(providerType, func(map[string]string) (compute.Provider, error) { return p, nil })
	w := worker.New(c, queue, worker.Options{MaxConcurrentNexusTaskExecutionSize: 1})
	require.NoError(t, Register(w, c, Config{TaskQueue: queue, Profiles: map[string]compute.ProviderDetails{"test": {Type: providerType}}}))
	require.NoError(t, w.Start())
	defer w.Stop()
	nc, err := c.NewNexusClient(client.NexusClientOptions{Endpoint: queue, Service: gen.SandboxControl.ServiceName})
	require.NoError(t, err)
	invoke := func(operation, input, output any) {
		t.Helper()
		handle, err := nc.ExecuteOperation(ctx, operation, input, client.StartNexusOperationOptions{
			ID: uuid.NewString(), ScheduleToCloseTimeout: time.Minute,
		})
		require.NoError(t, err)
		require.NoError(t, handle.Get(ctx, output))
	}
	sandbox := gen.SandboxHandle{SandboxID: "sandbox-test/" + uuid.NewString()}
	creation, err := nc.ExecuteOperation(ctx, gen.SandboxControl.Create, gen.CreateInput{SandboxID: sandbox.SandboxID, Profile: "test"},
		client.StartNexusOperationOptions{ID: uuid.NewString(), ScheduleToCloseTimeout: time.Minute})
	require.NoError(t, err)
	select {
	case <-p.started:
	case <-ctx.Done():
		t.Fatal("provisioning never started: ", ctx.Err())
	}
	// Block provisioning and call Describe with one Nexus handler slot. Describe
	// can complete only after the provisioning handler releases the slot.
	queryCtx, queryCancel := context.WithTimeout(ctx, 5*time.Second)
	defer queryCancel()
	describe, err := nc.ExecuteOperation(queryCtx, gen.SandboxControl.Describe, sandbox, client.StartNexusOperationOptions{ID: uuid.NewString()})
	require.NoError(t, err)
	var description gen.SandboxDescription
	require.NoError(t, describe.Get(queryCtx, &description))
	require.Equal(t, gen.SandboxDescriptionLifecyclePending, description.Lifecycle)
	close(p.release)
	require.NoError(t, creation.Get(ctx, nil))
	run, err := c.DescribeWorkflowExecution(ctx, sandbox.SandboxID, "")
	require.NoError(t, err)
	require.Equal(t, sandboxworkflow.SandboxWorkflowType, run.WorkflowExecutionInfo.Type.Name)
	// A second Create reuses the same init update and does not provision again.
	invoke(gen.SandboxControl.Create, gen.CreateInput{SandboxID: sandbox.SandboxID, Profile: "test"}, nil)
	var command gen.CommandResult
	invoke(gen.SandboxControl.Execute, gen.ExecuteInput{SandboxID: sandbox.SandboxID, Command: "print 42"}, &command)
	require.Equal(t, "42\n", command.Stdout)
	invoke(gen.SandboxControl.Suspend, sandbox, nil)
	invoke(gen.SandboxControl.Describe, sandbox, &description)
	require.Equal(t, gen.SandboxDescriptionLifecycleSuspended, description.Lifecycle)
	invoke(gen.SandboxControl.Resume, sandbox, nil)
	var snapshot gen.SnapshotHandle
	invoke(gen.SandboxControl.Snapshot, sandbox, &snapshot)
	fork := gen.SandboxHandle{SandboxID: "sandbox-test/" + uuid.NewString()}
	invoke(gen.SandboxControl.Fork, gen.ForkInput{SandboxID: fork.SandboxID, Profile: "test", SnapshotID: snapshot.SnapshotID}, nil)
	// DeleteSnapshot is the existing update on the source sandbox workflow.
	invoke(gen.SandboxControl.DeleteSnapshot, gen.DeleteSnapshotInput{SandboxID: sandbox.SandboxID, SnapshotID: snapshot.SnapshotID}, nil)
	invoke(gen.SandboxControl.Delete, fork, nil)
	invoke(gen.SandboxControl.Delete, sandbox, nil)
	// Cleanup and callback delivery have both finished before Delete returns.
	p.mu.Lock()
	require.Equal(t, 2, p.stops)
	require.Equal(t, 1, p.starts)
	p.mu.Unlock()
}

type integrationProvider struct {
	started       chan struct{}
	release       chan struct{}
	once          sync.Once
	mu            sync.Mutex
	starts, stops int
}

func (p *integrationProvider) Start(ctx context.Context, _ string) (*compute.ProviderStatus, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	p.starts++
	p.mu.Unlock()
	return &compute.ProviderStatus{InstanceID: uuid.NewString()}, nil
}
func (p *integrationProvider) Stop(context.Context, *compute.ProviderStatus) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stops++
	return nil
}
func (p *integrationProvider) Suspend(context.Context, *compute.ProviderStatus) error { return nil }
func (p *integrationProvider) Resume(context.Context, *compute.ProviderStatus) error  { return nil }
func (p *integrationProvider) Snapshot(context.Context, *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	return compute.SandboxPostSnapshotRunning, &compute.ProviderSnapshot{SnapshotID: uuid.NewString()}, nil
}
func (p *integrationProvider) StartFromSnapshot(context.Context, string, *compute.ProviderSnapshot) (*compute.ProviderStatus, error) {
	return &compute.ProviderStatus{InstanceID: uuid.NewString()}, nil
}
func (p *integrationProvider) DeleteSnapshot(context.Context, *compute.ProviderSnapshot) error {
	return nil
}
func (p *integrationProvider) ExecuteCommand(context.Context, *compute.ProviderStatus, string) (*compute.CommandResult, error) {
	return &compute.CommandResult{Stdout: "42\n"}, nil
}
