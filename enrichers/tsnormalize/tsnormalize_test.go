package tsnormalize_test

import (
	"context"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/enrichers"
	_ "github.com/xydac/ridgeline/enrichers/tsnormalize"
)

func TestTSNormalizeRegistered(t *testing.T) {
	if _, ok := enrichers.Get("ts_normalize"); !ok {
		t.Fatal("ts_normalize enricher not registered")
	}
}

func rec(data map[string]any) connectors.Record {
	return connectors.Record{Stream: "test", Timestamp: time.Now(), Data: data}
}

func TestTSNormalizeFormats(t *testing.T) {
	e, _ := enrichers.Get("ts_normalize")

	cases := []struct {
		name  string
		input any
		want  string
	}{
		{"rfc3339", "2024-03-15T10:30:00Z", "2024-03-15T10:30:00Z"},
		{"rfc3339nano millis", "2024-03-15T10:30:00.123Z", "2024-03-15T10:30:00.123Z"},
		{"rfc3339nano full", "2024-03-15T10:30:00.123456789Z", "2024-03-15T10:30:00.123456789Z"},
		{"rfc3339 with offset", "2024-03-15T12:30:00+02:00", "2024-03-15T10:30:00Z"},
		{"rfc3339 offset with millis", "2024-03-15T12:30:00.456+02:00", "2024-03-15T10:30:00.456Z"},
		{"datetime no tz", "2024-03-15T10:30:00", "2024-03-15T10:30:00Z"},
		{"datetime space", "2024-03-15 10:30:00", "2024-03-15T10:30:00Z"},
		{"date only", "2024-03-15", "2024-03-15T00:00:00Z"},
		{"unix seconds int", int(1710495000), "2024-03-15T09:30:00Z"},
		{"unix seconds int64", int64(1710495000), "2024-03-15T09:30:00Z"},
		{"unix seconds float64", float64(1710495000), "2024-03-15T09:30:00Z"},
		{"unix millis int64", int64(1710495000000), "2024-03-15T09:30:00Z"},
		{"unix millis float64", float64(1710495000000), "2024-03-15T09:30:00Z"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := []connectors.Record{rec(map[string]any{"timestamp": tc.input})}
			out, err := e.Enrich(context.Background(), enrichers.EnrichConfig{}, in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got, ok := out[0].Data["timestamp"].(string)
			if !ok {
				t.Fatalf("timestamp field missing or wrong type, got %T", out[0].Data["timestamp"])
			}
			if got != tc.want {
				t.Errorf("got %q; want %q", got, tc.want)
			}
		})
	}
}

// TestTSNormalizePartialSuccess verifies that unparseable rows pass through
// unchanged while parseable rows in the same batch are normalized.
func TestTSNormalizePartialSuccess(t *testing.T) {
	e, _ := enrichers.Get("ts_normalize")

	in := []connectors.Record{
		rec(map[string]any{"timestamp": "2024-03-15T10:30:00Z"}), // parseable
		rec(map[string]any{"timestamp": "not-a-date"}),           // unparseable - pass through
		rec(map[string]any{"timestamp": 1710495000}),             // parseable epoch
		rec(map[string]any{"other": "field"}),                    // missing ts_field - pass through
		rec(map[string]any{"timestamp": true}),                   // unsupported type - pass through
	}

	out, err := e.Enrich(context.Background(), enrichers.EnrichConfig{}, in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 5 {
		t.Fatalf("len = %d; want 5", len(out))
	}

	// parseable rows normalized
	if got := out[0].Data["timestamp"]; got != "2024-03-15T10:30:00Z" {
		t.Errorf("record[0] timestamp = %v; want 2024-03-15T10:30:00Z", got)
	}
	// unparseable string passed through unchanged
	if got := out[1].Data["timestamp"]; got != "not-a-date" {
		t.Errorf("record[1] timestamp = %v; want not-a-date (unchanged)", got)
	}
	// epoch int normalized
	if got := out[2].Data["timestamp"]; got != "2024-03-15T09:30:00Z" {
		t.Errorf("record[2] timestamp = %v; want 2024-03-15T09:30:00Z", got)
	}
	// missing field: record passes through with no timestamp key written
	if _, ok := out[3].Data["timestamp"]; ok {
		t.Error("record[3] should not have timestamp field added")
	}
	// unsupported type: passes through unchanged
	if got := out[4].Data["timestamp"]; got != true {
		t.Errorf("record[4] timestamp = %v; want true (unchanged bool)", got)
	}
}

func TestTSNormalizeCustomFields(t *testing.T) {
	e, _ := enrichers.Get("ts_normalize")

	in := []connectors.Record{
		rec(map[string]any{"created_at": "2024-03-15T10:30:00Z"}),
	}
	cfg := enrichers.EnrichConfig{"ts_field": "created_at", "out_field": "ts_utc"}
	out, err := e.Enrich(context.Background(), cfg, in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out[0].Data["ts_utc"]; got != "2024-03-15T10:30:00Z" {
		t.Errorf("ts_utc = %v; want 2024-03-15T10:30:00Z", got)
	}
	// original field unchanged
	if got := out[0].Data["created_at"]; got != "2024-03-15T10:30:00Z" {
		t.Errorf("created_at = %v; want original value (should not be modified when out_field differs)", got)
	}
}

// TestTSNormalizeContextCancel verifies that the enricher stops and returns
// a context error when ctx is cancelled mid-batch.
func TestTSNormalizeContextCancel(t *testing.T) {
	e, _ := enrichers.Get("ts_normalize")

	in := make([]connectors.Record, 10)
	for i := range in {
		in[i] = rec(map[string]any{"timestamp": "2024-03-15T10:30:00Z"})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	out, err := e.Enrich(ctx, enrichers.EnrichConfig{}, in)
	if err == nil {
		t.Fatal("expected context error; got nil")
	}
	// should have processed 0 records (cancelled before first)
	if len(out) != 0 {
		t.Errorf("expected 0 records on immediate cancel; got %d", len(out))
	}
}
