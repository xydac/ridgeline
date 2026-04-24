package parquet_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	pq "github.com/parquet-go/parquet-go"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/sinks"
	pqsink "github.com/xydac/ridgeline/sinks/parquet"
)

func TestSink_DeclaredSchemaWritesTypedColumns(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := pqsink.New()
	if err := s.Init(context.Background(), sinks.SinkConfig{"dir": dir, "run_id": "r"}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s.DeclareStream("search_analytics", connectors.Schema{Columns: []connectors.Column{
		{Name: "clicks", Type: connectors.Int},
		{Name: "impressions", Type: connectors.Int},
		{Name: "ctr", Type: connectors.Float},
		{Name: "position", Type: connectors.Float},
	}})

	ts := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)
	recs := []connectors.Record{{
		Stream:    "search_analytics",
		Timestamp: ts,
		Data: map[string]any{
			"clicks":      float64(42),
			"impressions": float64(1000),
			"ctr":         0.042,
			"position":    3.5,
			"query":       "ridgeline",
		},
	}}
	if err := s.Write(context.Background(), "search_analytics", recs); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, "r", "search_analytics.parquet")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	file, err := pq.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	schema := file.Schema()
	want := map[string]string{
		"stream":      "BYTE_ARRAY",
		"timestamp":   "INT64",
		"clicks":      "INT64",
		"impressions": "INT64",
		"ctr":         "DOUBLE",
		"position":    "DOUBLE",
		"data_json":   "BYTE_ARRAY",
	}
	have := map[string]string{}
	for _, col := range schema.Columns() {
		leaf, _ := schema.Lookup(col...)
		have[col[len(col)-1]] = leaf.Node.Type().Kind().String()
	}
	for name, kind := range want {
		if have[name] != kind {
			t.Errorf("column %q: want kind %s, got %q", name, kind, have[name])
		}
	}

	// Read raw row values through the low-level Rows API to verify
	// the physical types came out as declared.
	reader := pq.NewReader(file)
	defer reader.Close()
	row := make([]pq.Row, 1)
	n, err := reader.ReadRows(row)
	if err != nil && n == 0 {
		t.Fatalf("ReadRows: %v", err)
	}
	values := map[string]pq.Value{}
	for _, v := range row[0] {
		col := schema.Columns()[v.Column()]
		values[col[len(col)-1]] = v
	}
	if got := values["clicks"].Int64(); got != 42 {
		t.Errorf("clicks: want 42, got %d", got)
	}
	if got := values["impressions"].Int64(); got != 1000 {
		t.Errorf("impressions: want 1000, got %d", got)
	}
	if got := values["ctr"].Double(); got < 0.04 || got > 0.05 {
		t.Errorf("ctr: want ~0.042, got %v", got)
	}
	if got := values["position"].Double(); got < 3.4 || got > 3.6 {
		t.Errorf("position: want ~3.5, got %v", got)
	}
	if got := values["stream"].String(); got != "search_analytics" {
		t.Errorf("stream: want search_analytics, got %q", got)
	}
}

func TestSink_UndeclaredStreamKeepsUntypedShape(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := pqsink.New()
	if err := s.Init(context.Background(), sinks.SinkConfig{"dir": dir, "run_id": "r"}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	ts := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)
	recs := []connectors.Record{{Stream: "events", Timestamp: ts, Data: map[string]any{"k": "v"}}}
	if err := s.Write(context.Background(), "events", recs); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	path := filepath.Join(dir, "r", "events.parquet")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	stat, _ := f.Stat()
	file, err := pq.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	names := map[string]bool{}
	for _, col := range file.Schema().Columns() {
		names[col[len(col)-1]] = true
	}
	for _, want := range []string{"stream", "timestamp", "data_json"} {
		if !names[want] {
			t.Errorf("column %q missing from untyped parquet", want)
		}
	}
	for _, unwanted := range []string{"clicks", "impressions"} {
		if names[unwanted] {
			t.Errorf("column %q unexpectedly present in untyped parquet", unwanted)
		}
	}
}
