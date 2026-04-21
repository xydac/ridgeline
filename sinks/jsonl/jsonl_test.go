package jsonl_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/manifest"
	"github.com/xydac/ridgeline/sinks"
	"github.com/xydac/ridgeline/sinks/jsonl"
)

func newSink(t *testing.T, dir string) *jsonl.Sink {
	t.Helper()
	s := jsonl.New()
	if err := s.Init(context.Background(), sinks.SinkConfig{"dir": dir, "run_id": "run1"}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return s
}

func TestSink_WriteAndClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)

	t0 := time.Unix(1000, 0).UTC()
	t1 := time.Unix(2000, 0).UTC()
	if err := s.Write(context.Background(), "pages", []connectors.Record{
		{Stream: "pages", Timestamp: t1, Data: map[string]any{"url": "/a"}},
		{Stream: "pages", Timestamp: t0, Data: map[string]any{"url": "/b"}},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// File exists and has 2 lines.
	path := filepath.Join(dir, "run1", "pages.jsonl")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2", len(lines))
	}
	// Each line is valid JSON with stream, timestamp, data keys.
	for i, l := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("line %d not json: %v: %s", i, err, l)
		}
		if m["stream"] != "pages" {
			t.Errorf("line %d stream = %v, want pages", i, m["stream"])
		}
	}

	// Manifest has exactly one partition with correct time range.
	store := manifest.NewStore(filepath.Join(dir, "manifest.json"))
	m, err := store.Load()
	if err != nil {
		t.Fatalf("manifest load: %v", err)
	}
	if len(m.Partitions) != 1 {
		t.Fatalf("manifest parts = %d, want 1", len(m.Partitions))
	}
	p := m.Partitions[0]
	if p.Stream != "pages" || p.Format != "jsonl" || p.Rows != 2 {
		t.Errorf("partition wrong: %+v", p)
	}
	if !p.StartTime.Equal(t0) || !p.EndTime.Equal(t1) {
		t.Errorf("time range = [%s, %s], want [%s, %s]", p.StartTime, p.EndTime, t0, t1)
	}
	if p.SizeBytes <= 0 {
		t.Errorf("SizeBytes = %d, want > 0", p.SizeBytes)
	}
	// Path is relative to dir.
	if strings.HasPrefix(p.Path, "/") || strings.Contains(p.Path, dir) {
		t.Errorf("path should be relative, got %q", p.Path)
	}
}

func TestSink_MultipleStreams(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)
	_ = s.Write(context.Background(), "a", []connectors.Record{{Stream: "a", Data: map[string]any{"k": 1}}})
	_ = s.Write(context.Background(), "b", []connectors.Record{{Stream: "b", Data: map[string]any{"k": 2}}})
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	store := manifest.NewStore(filepath.Join(dir, "manifest.json"))
	m, _ := store.Load()
	if len(m.Partitions) != 2 {
		t.Fatalf("Partitions = %d, want 2", len(m.Partitions))
	}
	// Both files on disk.
	for _, p := range m.Partitions {
		if _, err := os.Stat(filepath.Join(dir, p.Path)); err != nil {
			t.Errorf("partition file missing: %v", err)
		}
	}
}

func TestSink_NoRecordsNoManifestEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	store := manifest.NewStore(filepath.Join(dir, "manifest.json"))
	m, _ := store.Load()
	if len(m.Partitions) != 0 {
		t.Errorf("Partitions = %d, want 0", len(m.Partitions))
	}
}

func TestSink_Init_MissingDir(t *testing.T) {
	t.Parallel()
	s := jsonl.New()
	if err := s.Init(context.Background(), sinks.SinkConfig{}); err == nil {
		t.Fatal("expected error for missing dir")
	}
}

func TestSink_Init_TwiceErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := jsonl.New()
	cfg := sinks.SinkConfig{"dir": dir}
	if err := s.Init(context.Background(), cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := s.Init(context.Background(), cfg); err == nil {
		t.Fatal("second Init should error")
	}
}

func TestSink_WriteBeforeInit(t *testing.T) {
	t.Parallel()
	s := jsonl.New()
	if err := s.Write(context.Background(), "s", nil); err == nil {
		t.Fatal("expected error writing before Init")
	}
}

func TestSink_WriteAfterClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)
	_ = s.Close()
	if err := s.Write(context.Background(), "s", []connectors.Record{{Stream: "s"}}); err == nil {
		t.Fatal("expected error writing after Close")
	}
}

func TestSink_EmptyStreamRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)
	defer s.Close()
	if err := s.Write(context.Background(), "", []connectors.Record{{Stream: ""}}); err == nil {
		t.Fatal("expected error for empty stream")
	}
}

func TestSink_Init_RejectsUnknownOption(t *testing.T) {
	t.Parallel()
	s := jsonl.New()
	err := s.Init(context.Background(), sinks.SinkConfig{"dirr": t.TempDir()})
	if err == nil {
		t.Fatal("expected error for typo'd option key")
	}
	// The did-you-mean hint must name the typo and the suggestion.
	if !strings.Contains(err.Error(), `"dirr"`) || !strings.Contains(err.Error(), `"dir"`) {
		t.Errorf("got %q, want did-you-mean for dirr -> dir", err)
	}
}

func TestSink_Init_RejectsUnrelatedUnknownOption(t *testing.T) {
	t.Parallel()
	s := jsonl.New()
	err := s.Init(context.Background(), sinks.SinkConfig{"dir": t.TempDir(), "totally_unknown": 42})
	if err == nil {
		t.Fatal("expected error for unknown option key")
	}
	if !strings.Contains(err.Error(), "totally_unknown") {
		t.Errorf("got %q, want mention of the unknown key", err)
	}
}

func TestSink_RegisteredInRegistry(t *testing.T) {
	t.Parallel()
	// The init func in jsonl.go should have registered "jsonl".
	if _, ok := sinks.Get(jsonl.Name); !ok {
		t.Errorf("jsonl sink not registered")
	}
}

func TestSink_DoubleCloseIsNoop(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)
	if err := s.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close #2: %v", err)
	}
}
