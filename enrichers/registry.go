package enrichers

import (
	"fmt"
	"sort"
	"sync"
)

var (
	registryMu sync.RWMutex
	registry   = map[string]Enricher{}
)

// Register adds e to the process-wide native enricher registry, keyed
// by e.Name(). Register panics if an enricher with the same name is
// already registered.
//
// Native enrichers call Register from an init function in their package.
func Register(e Enricher) {
	if e == nil {
		panic("enrichers: Register called with nil Enricher")
	}
	name := e.Name()
	if name == "" {
		panic("enrichers: Register called with empty enricher name")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("enrichers: duplicate registration for %q", name))
	}
	registry[name] = e
}

// Get returns the registered Enricher with the given name, or
// (nil, false) if no such enricher is registered.
func Get(name string) (Enricher, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	e, ok := registry[name]
	return e, ok
}

// List returns the names of all registered enrichers, sorted alphabetically.
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
	registry = map[string]Enricher{}
}
