package sinks

import (
	"context"

	"github.com/xydac/ridgeline/connectors"
)

// SinkConfig is the user-supplied configuration for one sink instance,
// loaded from ridgeline.yaml. Like ConnectorConfig, the shape is
// sink-specific and helpers below provide typed access.
type SinkConfig map[string]any

// String returns the string value at key, or "" if missing.
func (c SinkConfig) String(key string) string {
	if v, ok := c[key].(string); ok {
		return v
	}
	return ""
}

// Int returns the int value at key, or fallback if missing or not numeric.
func (c SinkConfig) Int(key string, fallback int) int {
	switch v := c[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return fallback
}

// Bool returns the bool value at key, or fallback if missing.
func (c SinkConfig) Bool(key string, fallback bool) bool {
	if v, ok := c[key].(bool); ok {
		return v
	}
	return fallback
}

// Sink is the contract every storage backend implements.
//
// Lifecycle:
//
//  1. Name returns a stable identifier used in config and logs.
//  2. Init is called once before any Write, with the resolved config.
//     Sinks that need to open files or connections do so here.
//  3. Write is called repeatedly with batches of Records for one
//     stream. The batch boundaries are chosen by the orchestrator
//     and may be small. Sinks should buffer internally if they need
//     larger writes for efficiency.
//  4. Flush is called between batches when the orchestrator wants to
//     guarantee durability before advancing connector state. Sinks
//     that buffer must drain those buffers in Flush.
//  5. Close is called once at shutdown. Sinks must release resources
//     and finalize any pending writes (close Parquet files, close
//     DuckDB connections, etc).
//
// Implementations must be safe for concurrent calls to Write from
// different goroutines.
type Sink interface {
	Name() string
	Init(ctx context.Context, cfg SinkConfig) error
	Write(ctx context.Context, stream string, records []connectors.Record) error
	Flush(ctx context.Context) error
	Close() error
}
