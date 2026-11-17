package compute

import (
	"errors"
	"fmt"
	"maps"
	"time"
)

// LifecyclePolicy limits operations for an admitted execution, independently of Worker bootstrap.
type LifecyclePolicy string

const (
	// LifecycleStandard preserves the provider's existing lifecycle operations.
	LifecycleStandard LifecyclePolicy = ""
	// LifecycleFresh permits commands and stop, without idle suspension or restoration.
	LifecycleFresh LifecyclePolicy = "fresh"
)

// Operation identifies an existing sandbox operation for policy admission.
type Operation string

const (
	OperationCommand        Operation = "command"
	OperationStop           Operation = "stop"
	OperationSuspend        Operation = "suspend"
	OperationResume         Operation = "resume"
	OperationSnapshot       Operation = "snapshot"
	OperationRestore        Operation = "restore"
	OperationDeleteSnapshot Operation = "delete-snapshot"
)

// ExecutionPolicy is the single owner of operation restrictions for a sandbox.
// Its zero value permits every existing operation.
type ExecutionPolicy struct {
	// Lifecycle restricts suspension, resumption and snapshots.
	Lifecycle LifecyclePolicy
	// NoBranching refuses snapshot and restore while other operations remain available.
	NoBranching bool
}

// InitialOptions holds the controller options that exist before a resource is allocated.
type InitialOptions struct {
	// IdleTimeout is the controller idle timeout; NoIdleTimeout disables idle suspension.
	IdleTimeout time.Duration
	// Snapshot selects an initial restore when it is not nil.
	Snapshot *ProviderSnapshot
}

// ValidateOperation rejects unknown policy and operation values and operations the policy excludes.
// Exclusions wrap errors.ErrUnsupported.
func (p ExecutionPolicy) ValidateOperation(operation Operation) error {
	if p.Lifecycle != LifecycleStandard && p.Lifecycle != LifecycleFresh {
		return fmt.Errorf("compute: unknown lifecycle policy %q", p.Lifecycle)
	}
	switch operation {
	case OperationCommand, OperationStop:
		return nil
	case OperationSuspend, OperationResume, OperationSnapshot, OperationRestore, OperationDeleteSnapshot:
	default:
		return fmt.Errorf("compute: unknown operation %q", operation)
	}
	if p.Lifecycle == LifecycleFresh {
		return fmt.Errorf("compute: fresh lifecycle does not support %s: %w", operation, errors.ErrUnsupported)
	}
	if p.NoBranching && (operation == OperationSnapshot || operation == OperationRestore) {
		return fmt.Errorf("compute: branching is disabled; %s is not supported: %w", operation, errors.ErrUnsupported)
	}
	return nil
}

// ValidateInitial checks the actual controller options before a resource is allocated.
// Fresh lifecycle requires NoIdleTimeout because it permits no idle suspension.
func (p ExecutionPolicy) ValidateInitial(options InitialOptions) error {
	if err := p.ValidateOperation(OperationCommand); err != nil {
		return err
	}
	if p.Lifecycle == LifecycleFresh && options.IdleTimeout != NoIdleTimeout {
		return fmt.Errorf("compute: fresh lifecycle requires NoIdleTimeout: %w", errors.ErrUnsupported)
	}
	if options.Snapshot != nil {
		return p.ValidateOperation(OperationRestore)
	}
	return nil
}

// PolicyResolver interprets only the configuration that determines an ExecutionPolicy.
// It runs in Workflow code, so it must not perform I/O or read credentials, clocks or mutable state,
// and it must return the same policy for the same configuration during replay. Other provider
// configuration is validated by the provider constructor inside Activities.
type PolicyResolver func(config map[string]string) (ExecutionPolicy, error)

var policies = map[ProviderType]PolicyResolver{}

// RegisterPolicy registers a policy resolver, conventionally during package initialization.
// Every Worker that runs sandbox Workflows must import the same provider packages so
// replay resolves the same policies.
// Providers without a resolver retain the zero policy. Duplicate or nil resolvers panic.
func RegisterPolicy(typ ProviderType, resolve PolicyResolver) {
	mu.Lock()
	defer mu.Unlock()
	if resolve == nil {
		panic("compute: nil policy resolver")
	}
	if _, exists := policies[typ]; exists {
		panic("compute: policy already registered for type " + string(typ))
	}
	policies[typ] = resolve
}

// ResolvePolicy reads configuration policy without constructing a provider or accessing credentials.
// The resolver receives its own map; providers without a resolver preserve existing behavior.
func ResolvePolicy(details ProviderDetails) (ExecutionPolicy, error) {
	mu.RLock()
	resolve := policies[details.Type]
	mu.RUnlock()
	if resolve == nil {
		return ExecutionPolicy{}, nil
	}
	policy, err := resolve(maps.Clone(details.Config))
	if err != nil {
		return ExecutionPolicy{}, err
	}
	if err := policy.ValidateOperation(OperationCommand); err != nil {
		return ExecutionPolicy{}, err
	}
	return policy, nil
}
