package compute_test

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func TestExecutionPolicyOperations(t *testing.T) {
	operations := []compute.Operation{
		compute.OperationCommand, compute.OperationStop, compute.OperationSuspend,
		compute.OperationResume, compute.OperationSnapshot, compute.OperationRestore,
		compute.OperationDeleteSnapshot,
	}
	for _, operation := range operations {
		t.Run(string(operation), func(t *testing.T) {
			if err := (compute.ExecutionPolicy{}).ValidateOperation(operation); err != nil {
				t.Fatalf("zero policy rejects legacy operation: %v", err)
			}
			err := (compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}).ValidateOperation(operation)
			if operation == compute.OperationCommand || operation == compute.OperationStop {
				if err != nil {
					t.Fatalf("fresh policy rejects permitted operation: %v", err)
				}
				return
			}
			if !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "does not support "+string(operation)) {
				t.Fatalf("fresh rejection = %v, want unsupported %s", err, operation)
			}
		})
	}
	for _, policy := range []compute.LifecyclePolicy{compute.LifecycleStandard, compute.LifecycleFresh} {
		if err := (compute.ExecutionPolicy{Lifecycle: policy}).ValidateOperation("unknown"); err == nil || !strings.Contains(err.Error(), "unknown operation") {
			t.Fatalf("unknown operation rejection = %v", err)
		}
	}
	for _, operation := range operations {
		if err := (compute.ExecutionPolicy{Lifecycle: "unknown"}).ValidateOperation(operation); err == nil || !strings.Contains(err.Error(), "unknown lifecycle policy") {
			t.Fatalf("unknown policy with %s rejection = %v", operation, err)
		}
	}
}

func TestExecutionPolicyInitialOptions(t *testing.T) {
	for _, test := range []struct {
		name     string
		idle     time.Duration
		snapshot *compute.ProviderSnapshot
		message  string
	}{
		{name: "no idle", idle: compute.NoIdleTimeout},
		{name: "default idle", message: "requires NoIdleTimeout"},
		{name: "positive idle", idle: time.Minute, message: "requires NoIdleTimeout"},
		{name: "other negative idle", idle: -2, message: "requires NoIdleTimeout"},
		{name: "restore", idle: compute.NoIdleTimeout, snapshot: &compute.ProviderSnapshot{SnapshotID: "snapshot"}, message: "does not support restore"},
		{name: "empty snapshot", idle: compute.NoIdleTimeout, snapshot: &compute.ProviderSnapshot{}, message: "does not support restore"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := (compute.ExecutionPolicy{}).ValidateInitial(test.idle, test.snapshot); err != nil {
				t.Fatalf("zero policy changes initial options: %v", err)
			}
			err := (compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}).ValidateInitial(test.idle, test.snapshot)
			if test.message == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("rejection = %v, want %q", err, test.message)
			}
		})
	}
	if err := (compute.ExecutionPolicy{Lifecycle: "unknown"}).ValidateInitial(compute.NoIdleTimeout, nil); err == nil || !strings.Contains(err.Error(), "unknown lifecycle policy") {
		t.Fatalf("unknown initial policy rejection = %v", err)
	}
}

var policyTestSequence atomic.Uint64

func policyTestType(t *testing.T) compute.ProviderType {
	return compute.ProviderType(fmt.Sprintf("%s-%d", t.Name(), policyTestSequence.Add(1)))
}

func TestResolvePolicyDefaultAndIsolation(t *testing.T) {
	details := compute.ProviderDetails{Type: policyTestType(t), Config: map[string]string{"lifecycle": "fresh"}}
	policy, err := compute.ResolvePolicy(details)
	if err != nil || policy != (compute.ExecutionPolicy{}) {
		t.Fatalf("unregistered resolver = %+v, %v; want legacy zero", policy, err)
	}
	compute.RegisterPolicy(details.Type, func(config map[string]string) (compute.ExecutionPolicy, error) {
		if config["lifecycle"] != "fresh" {
			t.Errorf("resolver input = %v", config)
		}
		config["lifecycle"] = "modified"
		return compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}, nil
	})
	for range 2 {
		policy, err = compute.ResolvePolicy(details)
		if err != nil || policy.Lifecycle != compute.LifecycleFresh || details.Config["lifecycle"] != "fresh" {
			t.Fatalf("resolver result = %+v, %v; caller config = %v", policy, err, details.Config)
		}
	}
	if compute.IsRegistered(details.Type) {
		t.Fatal("policy registration also registered a provider constructor")
	}
}

func TestResolvePolicyRejectsResolverFailures(t *testing.T) {
	sentinel := errors.New("invalid configuration")
	for _, test := range []struct {
		name   string
		result compute.ExecutionPolicy
		err    error
	}{
		{name: "resolver error", result: compute.ExecutionPolicy{Lifecycle: compute.LifecycleFresh}, err: sentinel},
		{name: "invalid returned policy", result: compute.ExecutionPolicy{Lifecycle: "unknown"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			typ := policyTestType(t)
			compute.RegisterPolicy(typ, func(map[string]string) (compute.ExecutionPolicy, error) { return test.result, test.err })
			policy, err := compute.ResolvePolicy(compute.ProviderDetails{Type: typ})
			if err == nil || policy != (compute.ExecutionPolicy{}) {
				t.Fatalf("failed resolver = %+v, %v", policy, err)
			}
			if test.err != nil && !errors.Is(err, sentinel) {
				t.Fatalf("resolver error lost: %v", err)
			}
			if test.err == nil && !strings.Contains(err.Error(), "unknown lifecycle policy") {
				t.Fatalf("invalid policy rejection = %v", err)
			}
		})
	}
}

func TestRegisterPolicyRejectsInvalidRegistration(t *testing.T) {
	resolver := func(map[string]string) (compute.ExecutionPolicy, error) { return compute.ExecutionPolicy{}, nil }
	for _, test := range []struct {
		name string
		run  func(compute.ProviderType)
		want string
	}{
		{name: "nil", run: func(typ compute.ProviderType) { compute.RegisterPolicy(typ, nil) }, want: "nil policy resolver"},
		{name: "duplicate", run: func(typ compute.ProviderType) {
			compute.RegisterPolicy(typ, resolver)
			compute.RegisterPolicy(typ, resolver)
		}, want: "policy already registered"},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if value := recover(); value == nil || !strings.Contains(fmt.Sprint(value), test.want) {
					t.Errorf("panic = %v, want %q", value, test.want)
				}
			}()
			test.run(policyTestType(t))
		})
	}
}
