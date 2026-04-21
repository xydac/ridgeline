package enrichers

import (
	"context"

	"github.com/xydac/ridgeline/connectors"
)

// EnrichConfig is the user-supplied configuration for one enricher
// invocation, loaded from ridgeline.yaml.
type EnrichConfig map[string]any

// String returns the string value at key, or "" if missing.
func (c EnrichConfig) String(key string) string {
	if v, ok := c[key].(string); ok {
		return v
	}
	return ""
}

// Int returns the int value at key, or fallback if missing or not numeric.
func (c EnrichConfig) Int(key string, fallback int) int {
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

// StringSlice returns the []string value at key, or nil if missing.
func (c EnrichConfig) StringSlice(key string) []string {
	switch v := c[key].(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// Enricher transforms records by adding fields. The same record slice
// may be returned with mutated entries, or a new slice may be returned;
// callers must not assume in-place mutation.
//
// Implementations should be batch-aware: a single Enrich call may carry
// many records, and grouping LLM/model calls per batch is the main way
// enrichers stay efficient.
//
// Enrich must respect ctx cancellation and return ctx.Err() if cancelled
// mid-batch.
type Enricher interface {
	Name() string
	Enrich(ctx context.Context, cfg EnrichConfig, records []connectors.Record) ([]connectors.Record, error)
}
