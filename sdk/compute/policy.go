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

// ExecutionPolicy is the resolved lifecycle restriction. Its zero value preserves existing behavior.
type ExecutionPolicy struct {
	Lifecycle LifecyclePolicy
}

// ValidateOperation rejects unknown policy/operation values and operations excluded by the policy.
func (p ExecutionPolicy) ValidateOperation(operation Operation) error {
	if p.Lifecycle != LifecycleStandard && p.Lifecycle != LifecycleFresh {
		return fmt.Errorf("compute: unknown lifecycle policy %q", p.Lifecycle)
	}
	switch operation {
	case OperationCommand, OperationStop:
		return nil
	case OperationSuspend, OperationResume, OperationSnapshot, OperationRestore, OperationDeleteSnapshot:
		if p.Lifecycle == LifecycleFresh {
			return fmt.Errorf("compute: fresh lifecycle does not support %s: %w", operation, errors.ErrUnsupported)
		}
		return nil
	default:
		return fmt.Errorf("compute: unknown operation %q", operation)
	}
}

// ValidateInitial checks actual controller options before a resource is allocated.
func (p ExecutionPolicy) ValidateInitial(idle time.Duration, snapshot *ProviderSnapshot) error {
	if err := p.ValidateOperation(OperationCommand); err != nil {
		return err
	}
	if p.Lifecycle == LifecycleFresh {
		if idle != NoIdleTimeout {
			return fmt.Errorf("compute: fresh lifecycle requires NoIdleTimeout: %w", errors.ErrUnsupported)
		}
		if snapshot != nil {
			return p.ValidateOperation(OperationRestore)
		}
	}
	return nil
}

// PolicyResolver interprets configuration without I/O, credentials, clocks or mutable state.
// A resolver must return the same policy for the same configuration during Workflow replay.
type PolicyResolver func(config map[string]string) (ExecutionPolicy, error)

var policies = map[ProviderType]PolicyResolver{}

// RegisterPolicy registers a pure configuration resolver, conventionally during package initialization.
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
