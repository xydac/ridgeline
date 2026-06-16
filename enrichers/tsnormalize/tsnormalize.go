// Package tsnormalize provides an enricher that parses timestamp fields
// from various formats into a canonical UTC RFC 3339 string with
// nanosecond precision (trailing zeros omitted).
//
// Configuration keys:
//
//	ts_field  - source field containing the raw timestamp value
//	            (default: "timestamp")
//	out_field - destination field written with the normalized value
//	            (default: same as ts_field)
//
// Accepted input formats (tried in order):
//
//   - string: RFC 3339 with optional sub-second digits, RFC 3339 without
//     sub-seconds, "2006-01-02T15:04:05" (no timezone, treated as UTC),
//     "2006-01-02 15:04:05", date-only "2006-01-02"
//   - int, int64, float64: Unix epoch; values <= 1e10 are seconds,
//     larger values are milliseconds
//
// Sub-second precision from the input is preserved in the output.
// A whole-second input produces a whole-second RFC 3339 output (no
// trailing ".000000000").
//
// Records whose ts_field is missing, is an unsupported type, or cannot
// be parsed are passed through unchanged (partial-success behaviour).
// The enricher never returns an error for individual record failures;
// only context cancellation returns a non-nil error.
//
// Register the enricher by importing this package for its side effects:
//
//	import _ "github.com/xydac/ridgeline/enrichers/tsnormalize"
package tsnormalize

import (
	"context"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/enrichers"
)

func init() { enrichers.Register(&Enricher{}) }

// Enricher normalizes a timestamp field to UTC RFC 3339.
type Enricher struct{}

// Name returns the stable registered name of this enricher.
func (e *Enricher) Name() string { return "ts_normalize" }

// stringLayouts are the string timestamp formats tried in order.
var stringLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// epochThreshold distinguishes Unix seconds from Unix milliseconds.
// Values above this threshold are assumed to be milliseconds.
const epochThreshold = int64(1e10)

// parseTimestamp attempts to parse v as a timestamp. Returns the parsed
// time and true on success, or zero time and false on failure.
func parseTimestamp(v any) (time.Time, bool) {
	switch raw := v.(type) {
	case string:
		for _, layout := range stringLayouts {
			if t, err := time.Parse(layout, raw); err == nil {
				return t.UTC(), true
			}
		}
	case int:
		return fromEpoch(int64(raw)), true
	case int64:
		return fromEpoch(raw), true
	case float64:
		return fromEpoch(int64(raw)), true
	}
	return time.Time{}, false
}

func fromEpoch(n int64) time.Time {
	if n > epochThreshold {
		return time.UnixMilli(n).UTC()
	}
	return time.Unix(n, 0).UTC()
}

// Enrich reads cfg["ts_field"] (default "timestamp") from each record,
// parses it as a timestamp, and writes the UTC RFC 3339 string to
// cfg["out_field"] (default: same as ts_field). Records that have no
// parseable timestamp in ts_field are passed through unchanged.
func (e *Enricher) Enrich(ctx context.Context, cfg enrichers.EnrichConfig, recs []connectors.Record) ([]connectors.Record, error) {
	tsField := cfg.String("ts_field")
	if tsField == "" {
		tsField = "timestamp"
	}
	outField := cfg.String("out_field")
	if outField == "" {
		outField = tsField
	}

	for i := range recs {
		if err := ctx.Err(); err != nil {
			return recs[:i], err
		}
		v, ok := recs[i].Data[tsField]
		if !ok {
			continue
		}
		t, parsed := parseTimestamp(v)
		if !parsed {
			continue
		}
		recs[i].Data[outField] = t.Format(time.RFC3339Nano)
	}
	return recs, nil
}
