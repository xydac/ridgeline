package pipeline

import (
	"context"
	"sync"

	"github.com/xydac/ridgeline/connectors"
)

// StateStore persists connector checkpoints between syncs.
//
// The pipeline calls Load once, before Connector.Extract, so the
// connector can resume from its previous cursor. It then calls Save
// every time the connector emits a StateMsg and the sink has flushed
// the records that preceded it.
//
// Implementations must be safe for concurrent use. Keys are opaque
// strings chosen by the caller (typically the connector instance name).
type StateStore interface {
	Load(ctx context.Context, key string) (connectors.State, error)
	Save(ctx context.Context, key string, state connectors.State) error
}

// MemoryStateStore is an in-process StateStore backed by a map. It is
// safe for concurrent use and is meant for dry runs, tests, and any
// deployment where durability across process restarts is not required.
type MemoryStateStore struct {
	mu    sync.RWMutex
	items map[string]connectors.State
}

// NewMemoryStateStore returns an empty MemoryStateStore.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{items: map[string]connectors.State{}}
}

// Load returns the state for key, or an empty State if none exists.
// Load never returns an error for MemoryStateStore.
func (m *MemoryStateStore) Load(_ context.Context, key string) (connectors.State, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.items[key]
	if !ok {
		return connectors.State{}, nil
	}
	// Return a copy so callers cannot mutate our stored state.
	out := make(connectors.State, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out, nil
}

// Save stores a copy of state under key, replacing any prior value.
func (m *MemoryStateStore) Save(_ context.Context, key string, state connectors.State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copy := make(connectors.State, len(state))
	for k, v := range state {
		copy[k] = v
	}
	m.items[key] = copy
	return nil
}
