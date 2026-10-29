package sandbox_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sandbox "github.com/temporal-community/sandbox-orchestration-harness/sdk"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func TestFreshPolicyPublicAdmissionBeforeWorkflowEffects(t *testing.T) {
	typ := compute.ProviderType("fresh-admission-" + uuid.NewString())
	compute.RegisterPolicy(typ, func(map[string]string) (compute.ExecutionPolicy, error) {
		return compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}, nil
	})
	provider := compute.ProviderDetails{Type: typ}
	for _, tc := range []struct {
		name string
		opts []sandbox.SandboxOption
		want string
	}{
		{"default-idle", nil, "requires NoIdleTimeout"},
		{"zero-idle", []sandbox.SandboxOption{sandbox.WithIdleTimeout(0)}, "requires NoIdleTimeout"},
		{"positive-idle", []sandbox.SandboxOption{sandbox.WithIdleTimeout(time.Minute)}, "requires NoIdleTimeout"},
		{"snapshot", []sandbox.SandboxOption{sandbox.WithIdleTimeout(sandbox.NoIdleTimeout), sandbox.WithSnapshot(&compute.ProviderSnapshot{SnapshotID: "checkpoint"})}, "does not support restore"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A nil Workflow context exposes any effect reached before admission.
			_, err := sandbox.NewSandbox(nil, provider, tc.opts...)
			if !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want unsupported %q", err, tc.want)
			}
		})
	}
}

func TestPublicPolicyResolverErrorBeforeWorkflowEffects(t *testing.T) {
	typ := compute.ProviderType("invalid-admission-" + uuid.NewString())
	want := errors.New("invalid provider configuration")
	compute.RegisterPolicy(typ, func(map[string]string) (compute.ExecutionPolicy, error) {
		return compute.ExecutionPolicy{}, want
	})
	if _, err := sandbox.NewSandbox(nil, compute.ProviderDetails{Type: typ}); !errors.Is(err, want) {
		t.Fatalf("got %v, want configuration error", err)
	}
}
