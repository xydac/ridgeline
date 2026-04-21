package connectors

import (
	"fmt"
	"sort"
	"sync"
)

var (
	registryMu sync.RWMutex
	registry   = map[string]Connector{}
)

// Register adds c to the process-wide native connector registry, keyed
// by c.Spec().Name. Register panics if a connector with the same name
// is already registered, mirroring the behavior of database/sql.Register
// and similar Go conventions.
//
// Native connectors call Register from an init function in their
// package. Importing the package (typically via a side-effect import
// in cmd/ridgeline) makes the connector available.
func Register(c Connector) {
	if c == nil {
		panic("connectors: Register called with nil Connector")
	}
	name := c.Spec().Name
	if name == "" {
		panic("connectors: Register called with empty connector name")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("connectors: duplicate registration for %q", name))
	}
	registry[name] = c
}

// Get returns the registered Connector with the given name, or
// (nil, false) if no such connector is registered.
func Get(name string) (Connector, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	c, ok := registry[name]
	return c, ok
}

// List returns the names of all registered connectors, sorted
// alphabetically.
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
	registry = map[string]Connector{}
}
