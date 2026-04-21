package compute

import (
	"fmt"
	"sync"

	sdkcompute "github.com/temporalio/ephemeral-workers-poc/sdk/compute"
)

// Constructor is a factory that receives the provider config and returns a ComputeProvider.
type Constructor func(config map[string]string) (ComputeProvider, error)

var (
	mu       sync.RWMutex
	registry = map[sdkcompute.ComputeProviderType]Constructor{}
)

// Register adds a constructor for the given provider type.
// Conventionally called from an init() function.
func Register(typ sdkcompute.ComputeProviderType, ctor Constructor) {
	mu.Lock()
	defer mu.Unlock()
	registry[typ] = ctor
}

// Lookup constructs and returns a ComputeProvider for the given type and config.
// Returns an error if no constructor has been registered for typ.
func Lookup(typ sdkcompute.ComputeProviderType, config map[string]string) (ComputeProvider, error) {
	mu.RLock()
	ctor, ok := registry[typ]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no compute provider registered for type %q", typ)
	}
	return ctor(config)
}
