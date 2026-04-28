package compute

import (
	"fmt"
	"sync"
)

// Constructor is a factory that receives the provider config and returns a Provider.
type Constructor func(config map[string]string) (Provider, error)

var (
	mu       sync.RWMutex
	registry = map[ProviderType]Constructor{}
)

// Register adds a constructor for the given provider type.
// Conventionally called from an init() function.
// Panics if typ has already been registered, following the database/sql driver convention.
func Register(typ ProviderType, ctor Constructor) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := registry[typ]; exists {
		panic("compute: provider already registered for type " + string(typ))
	}
	registry[typ] = ctor
}

// IsRegistered reports whether a constructor has been registered for typ.
func IsRegistered(typ ProviderType) bool {
	mu.RLock()
	_, ok := registry[typ]
	mu.RUnlock()
	return ok
}

// Lookup constructs and returns a Provider for the given type and config.
// Returns an error if no constructor has been registered for typ.
func Lookup(typ ProviderType, config map[string]string) (Provider, error) {
	mu.RLock()
	ctor, ok := registry[typ]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no compute provider registered for type %q", typ)
	}
	p, err := ctor(config)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("compute: constructor for type %q returned a nil provider", typ)
	}
	return p, nil
}
