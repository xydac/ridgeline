package sinks

import (
	"fmt"
	"sort"
	"sync"
)

// Factory returns a fresh uninitialized Sink. Implementations must
// return a new value on every call so the pipeline can Init one sink
// per connector instance without state bleeding between runs.
type Factory func() Sink

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register adds a factory to the process-wide native sink registry
// under name. Register panics if name is empty, f is nil, or a factory
// is already registered under name.
//
// Native sinks call Register from an init function in their package.
func Register(name string, f Factory) {
	if name == "" {
		panic("sinks: Register called with empty name")
	}
	if f == nil {
		panic("sinks: Register called with nil factory")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("sinks: duplicate registration for %q", name))
	}
	registry[name] = f
}

// New returns a fresh Sink registered under name. The returned sink
// has not been initialized; callers must call Init before Write.
func New(name string) (Sink, error) {
	registryMu.RLock()
	f, ok := registry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("sink type %q is not registered (known: %v)", name, List())
	}
	return f(), nil
}

// List returns the names of all registered sinks, sorted alphabetically.
func List() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]string, 0, len(registry))
	for name := range registry {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// reset clears the registry. Test-only.
func reset() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = map[string]Factory{}
}
