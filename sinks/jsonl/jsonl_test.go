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

func TestSink_RerunPrunesCoveredRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	t0 := time.Unix(1000, 0).UTC()
	t1 := time.Unix(2000, 0).UTC()
	t2 := time.Unix(3000, 0).UTC()

	// First run: land two records on the pages stream.
	s1 := jsonl.New()
	if err := s1.Init(ctx, sinks.SinkConfig{"dir": dir, "run_id": "run1"}); err != nil {
		t.Fatalf("Init 1: %v", err)
	}
	if err := s1.Write(ctx, "pages", []connectors.Record{
		{Stream: "pages", Timestamp: t0, Data: map[string]any{"k": "a"}},
		{Stream: "pages", Timestamp: t1, Data: map[string]any{"k": "b"}},
	}); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	manifestPath := filepath.Join(dir, "manifest.json")
	firstStat, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest after run 1: %v", err)
	}
	firstMtime := firstStat.ModTime()

	// Second run: identical window. Every record must be pruned, no
	// new file appears under run2, and the manifest is untouched.
	s2 := jsonl.New()
	if err := s2.Init(ctx, sinks.SinkConfig{"dir": dir, "run_id": "run2"}); err != nil {
		t.Fatalf("Init 2: %v", err)
	}
	if err := s2.Write(ctx, "pages", []connectors.Record{
		{Stream: "pages", Timestamp: t0, Data: map[string]any{"k": "a"}},
		{Stream: "pages", Timestamp: t1, Data: map[string]any{"k": "b"}},
	}); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "run2", "pages.jsonl")); !os.IsNotExist(err) {
		t.Errorf("run2/pages.jsonl should not exist, stat err = %v", err)
	}
	store := manifest.NewStore(manifestPath)
	m, err := store.Load()
	if err != nil {
		t.Fatalf("manifest load: %v", err)
	}
	if len(m.Partitions) != 1 {
		t.Errorf("Partitions = %d, want 1 (no new append)", len(m.Partitions))
	}
	secondStat, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest after run 2: %v", err)
	}
	if !secondStat.ModTime().Equal(firstMtime) {
		t.Errorf("manifest mtime changed on no-op re-run: %v -> %v", firstMtime, secondStat.ModTime())
	}

	// Third run: a record at t2 is outside the covered window, so it
	// must land. A duplicate at t1 must still be pruned.
	s3 := jsonl.New()
	if err := s3.Init(ctx, sinks.SinkConfig{"dir": dir, "run_id": "run3"}); err != nil {
		t.Fatalf("Init 3: %v", err)
	}
	if err := s3.Write(ctx, "pages", []connectors.Record{
		{Stream: "pages", Timestamp: t1, Data: map[string]any{"k": "b"}},
		{Stream: "pages", Timestamp: t2, Data: map[string]any{"k": "c"}},
	}); err != nil {
		t.Fatalf("Write 3: %v", err)
	}
	if err := s3.Close(); err != nil {
		t.Fatalf("Close 3: %v", err)
	}
	m, _ = store.Load()
	if len(m.Partitions) != 2 {
		t.Fatalf("Partitions = %d, want 2 after a genuinely new record", len(m.Partitions))
	}
	newPart := m.Partitions[1]
	if newPart.Rows != 1 {
		t.Errorf("new partition Rows = %d, want 1", newPart.Rows)
	}
	if !newPart.StartTime.Equal(t2) || !newPart.EndTime.Equal(t2) {
		t.Errorf("new partition window = [%s, %s], want [%s, %s]", newPart.StartTime, newPart.EndTime, t2, t2)
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
