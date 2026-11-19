package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/temporal"
)

func TestReceivingBindingConflictBeforeConstruction(t *testing.T) {
	constructions, methods := new(atomic.Int32), new(atomic.Int32)
	typ := compute.ProviderType(fmt.Sprintf("binding-%d", workflowPolicySequence.Add(1)))
	compute.Register(typ, func(map[string]string) (compute.Provider, error) {
		constructions.Add(1)
		return &bindingRecordingProvider{calls: methods}, nil
	})
	details := compute.ProviderDetails{Type: typ}
	for _, conflict := range []string{"provider", "instance", "status-instance-empty", "provider-empty", "binding-instance-empty", "both-instances-empty", "queue", "digest"} {
		t.Run(conflict, func(t *testing.T) {
			status := &compute.ProviderStatus{InstanceID: "owned", ResourceBinding: &compute.ResourceBinding{
				ProviderType: details.Type, InstanceID: "owned", TaskQueueName: "queue", ConfigurationDigest: "digest",
			}}
			switch conflict {
			case "provider":
				status.ResourceBinding.ProviderType = compute.ProviderTypeDaytona
			case "instance":
				status.InstanceID = "other"
			case "status-instance-empty":
				status.InstanceID = ""
			case "provider-empty":
				status.ResourceBinding.ProviderType = ""
			case "binding-instance-empty":
				status.ResourceBinding.InstanceID = ""
			case "both-instances-empty":
				status.InstanceID, status.ResourceBinding.InstanceID = "", ""
			case "queue":
				status.ResourceBinding.TaskQueueName = ""
			case "digest":
				status.ResourceBinding.ConfigurationDigest = ""
			}
			ctx := context.Background()
			for _, invoke := range []func() error{
				func() error { return StopSandbox(ctx, StopSandboxInput{Provider: details, Status: status}) },
				func() error {
					_, err := ExecuteCommand(ctx, ExecuteCommandInput{Provider: details, Status: status})
					return err
				},
				func() error { return SuspendSandbox(ctx, SuspendSandboxInput{Provider: details, Status: status}) },
				func() error { return ResumeSandbox(ctx, ResumeSandboxInput{Provider: details, Status: status}) },
				func() error {
					_, err := SnapshotSandbox(ctx, SnapshotSandboxInput{Provider: details, Status: status})
					return err
				},
			} {
				var appErr *temporal.ApplicationError
				if err := invoke(); !errors.As(err, &appErr) || appErr.Type() != "ProviderConfigError" || !appErr.NonRetryable() || !errors.Is(appErr.Unwrap(), compute.ErrInvalidResourceBinding) {
					t.Fatalf("binding refusal: %v", err)
				}
			}
			if constructions.Load() != 0 || methods.Load() != 0 {
				t.Fatalf("invalid binding constructed provider %d times, dispatched %d methods", constructions.Load(), methods.Load())
			}
		})
	}
	// A provider without a policy resolver still receives its legacy status.
	if err := StopSandbox(context.Background(), StopSandboxInput{Provider: details, Status: &compute.ProviderStatus{InstanceID: "legacy"}}); err != nil || constructions.Load() != 1 || methods.Load() != 1 {
		t.Fatalf("legacy dispatch: err=%v constructions=%d methods=%d", err, constructions.Load(), methods.Load())
	}
	compute.RegisterPolicy(typ, func(map[string]string) (compute.ExecutionPolicy, error) {
		return compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}, nil
	})
	for _, status := range []*compute.ProviderStatus{nil, {InstanceID: "unrelated-fresh"}} {
		before := constructions.Load()
		if err := StopSandbox(context.Background(), StopSandboxInput{Provider: details, Status: status}); err != nil || constructions.Load() != before+1 || methods.Load() != before+1 {
			t.Fatalf("unrelated fresh dispatch: err=%v constructions=%d methods=%d", err, constructions.Load(), methods.Load())
		}
	}
}

type bindingRecordingProvider struct {
	compute.Provider
	calls *atomic.Int32
}

func (p *bindingRecordingProvider) Stop(context.Context, *compute.ProviderStatus) error {
	p.calls.Add(1)
	return nil
}
func (p *bindingRecordingProvider) Suspend(context.Context, *compute.ProviderStatus) error {
	p.calls.Add(1)
	return nil
}
func (p *bindingRecordingProvider) Resume(context.Context, *compute.ProviderStatus) error {
	p.calls.Add(1)
	return nil
}
func (p *bindingRecordingProvider) Snapshot(context.Context, *compute.ProviderStatus) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	p.calls.Add(1)
	return compute.SandboxPostSnapshotRunning, nil, nil
}
func (p *bindingRecordingProvider) ExecuteCommand(context.Context, *compute.ProviderStatus, string) (*compute.CommandResult, error) {
	p.calls.Add(1)
	return &compute.CommandResult{}, nil
}
