package sinks

import (
	"fmt"
	"sort"
	"sync"
)

var (
	registryMu sync.RWMutex
	registry   = map[string]Sink{}
)

// Register adds s to the process-wide native sink registry, keyed by
// s.Name(). Register panics if a sink with the same name is already
// registered.
//
// Native sinks call Register from an init function in their package.
func Register(s Sink) {
	if s == nil {
		panic("sinks: Register called with nil Sink")
	}
	name := s.Name()
	if name == "" {
		panic("sinks: Register called with empty sink name")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("sinks: duplicate registration for %q", name))
	}
	registry[name] = s
}

// Get returns the registered Sink with the given name, or (nil, false)
// if no such sink is registered.
func Get(name string) (Sink, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	s, ok := registry[name]
	return s, ok
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
	registry = map[string]Sink{}
}
