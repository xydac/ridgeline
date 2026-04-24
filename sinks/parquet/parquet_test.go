package parquet_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pq "github.com/parquet-go/parquet-go"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/manifest"
	"github.com/xydac/ridgeline/sinks"
	pqsink "github.com/xydac/ridgeline/sinks/parquet"
)

func newSink(t *testing.T, dir string) *pqsink.Sink {
	t.Helper()
	s := pqsink.New()
	if err := s.Init(context.Background(), sinks.SinkConfig{"dir": dir, "run_id": "run1"}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return s
}

func TestSink_WriteReadBack(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)

	t0 := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	recs := []connectors.Record{
		{Stream: "pages", Timestamp: t1, Data: map[string]any{"url": "/a", "hits": float64(3)}},
		{Stream: "pages", Timestamp: t0, Data: map[string]any{"url": "/b", "hits": float64(1)}},
	}
	if err := s.Write(context.Background(), "pages", recs); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, "run1", "pages.parquet")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	r := pq.NewGenericReader[pqsink.Row](f)
	defer r.Close()
	out := make([]pqsink.Row, 2)
	n, err := r.Read(out)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("Read: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows read = %d, want 2", n)
	}
	if out[0].Stream != "pages" || out[1].Stream != "pages" {
		t.Errorf("stream column lost: %+v", out)
	}
	if out[0].Timestamp != t1.UnixMicro() || out[1].Timestamp != t0.UnixMicro() {
		t.Errorf("timestamps = %d,%d; want %d,%d", out[0].Timestamp, out[1].Timestamp, t1.UnixMicro(), t0.UnixMicro())
	}
	var m0, m1 map[string]any
	if err := json.Unmarshal([]byte(out[0].DataJSON), &m0); err != nil {
		t.Fatalf("row0 data_json: %v", err)
	}
	if err := json.Unmarshal([]byte(out[1].DataJSON), &m1); err != nil {
		t.Fatalf("row1 data_json: %v", err)
	}
	if m0["url"] != "/a" || m1["url"] != "/b" {
		t.Errorf("url values wrong: %v / %v", m0["url"], m1["url"])
	}

	// Manifest has the partition, with start = earlier timestamp, end = later.
	store := manifest.NewStore(filepath.Join(dir, "manifest.json"))
	mf, err := store.Load()
	if err != nil {
		t.Fatalf("manifest load: %v", err)
	}
	if len(mf.Partitions) != 1 {
		t.Fatalf("partitions = %d, want 1", len(mf.Partitions))
	}
	p := mf.Partitions[0]
	if p.Stream != "pages" {
		t.Errorf("stream = %q, want pages", p.Stream)
	}
	if p.Format != "parquet" {
		t.Errorf("format = %q, want parquet", p.Format)
	}
	if p.Rows != 2 {
		t.Errorf("rows = %d, want 2", p.Rows)
	}
	if !p.StartTime.Equal(t0) || !p.EndTime.Equal(t1) {
		t.Errorf("start/end = %v/%v, want %v/%v", p.StartTime, p.EndTime, t0, t1)
	}
	if p.SizeBytes != stat.Size() {
		t.Errorf("manifest size = %d, file size = %d", p.SizeBytes, stat.Size())
	}
}

func TestSink_MultipleStreams(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)

	now := time.Now().UTC()
	if err := s.Write(context.Background(), "pages", []connectors.Record{
		{Stream: "pages", Timestamp: now, Data: map[string]any{"k": "v"}},
	}); err != nil {
		t.Fatalf("Write pages: %v", err)
	}
	if err := s.Write(context.Background(), "events", []connectors.Record{
		{Stream: "events", Timestamp: now, Data: map[string]any{"k": "v"}},
		{Stream: "events", Timestamp: now, Data: map[string]any{"k": "v2"}},
	}); err != nil {
		t.Fatalf("Write events: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for _, stream := range []string{"pages", "events"} {
		if _, err := os.Stat(filepath.Join(dir, "run1", stream+".parquet")); err != nil {
			t.Errorf("expected %s.parquet to exist: %v", stream, err)
		}
	}
	store := manifest.NewStore(filepath.Join(dir, "manifest.json"))
	mf, err := store.Load()
	if err != nil {
		t.Fatalf("manifest load: %v", err)
	}
	if len(mf.Partitions) != 2 {
		t.Fatalf("partitions = %d, want 2", len(mf.Partitions))
	}
}

func TestSink_NilDataMarshalsToEmptyObject(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)
	if err := s.Write(context.Background(), "s", []connectors.Record{
		{Stream: "s", Timestamp: time.Now().UTC(), Data: nil},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	f, err := os.Open(filepath.Join(dir, "run1", "s.parquet"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	r := pq.NewGenericReader[pqsink.Row](f)
	defer r.Close()
	out := make([]pqsink.Row, 1)
	if _, err := r.Read(out); err != nil && err.Error() != "EOF" {
		t.Fatalf("Read: %v", err)
	}
	if out[0].DataJSON != "{}" {
		t.Errorf("nil Data should marshal to {}; got %q", out[0].DataJSON)
	}
}

func TestSink_InitRequiresDir(t *testing.T) {
	t.Parallel()
	s := pqsink.New()
	err := s.Init(context.Background(), sinks.SinkConfig{})
	if err == nil {
		t.Fatal("Init without dir: expected error")
	}
}

func TestSink_WriteBeforeInit(t *testing.T) {
	t.Parallel()
	s := pqsink.New()
	err := s.Write(context.Background(), "s", nil)
	if err == nil {
		t.Fatal("Write before Init: expected error")
	}
}

func TestSink_WriteAfterClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err := s.Write(context.Background(), "s", []connectors.Record{{Stream: "s", Timestamp: time.Now(), Data: map[string]any{"k": "v"}}})
	if err == nil {
		t.Fatal("Write after Close: expected error")
	}
}

func TestSink_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := newSink(t, dir)
	if err := s.Write(context.Background(), "s", []connectors.Record{
		{Stream: "s", Timestamp: time.Now().UTC(), Data: map[string]any{"k": "v"}},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
}

func TestSink_Init_RejectsUnknownOption(t *testing.T) {
	t.Parallel()
	s := pqsink.New()
	err := s.Init(context.Background(), sinks.SinkConfig{"dirr": t.TempDir()})
	if err == nil {
		t.Fatal("expected error for typo'd option key")
	}
	if !strings.Contains(err.Error(), `"dirr"`) || !strings.Contains(err.Error(), `"dir"`) {
		t.Errorf("got %q, want did-you-mean for dirr -> dir", err)
	}
}

func TestSink_Init_RejectsUnrelatedUnknownOption(t *testing.T) {
	t.Parallel()
	s := pqsink.New()
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
	s, err := sinks.New("parquet")
	if err != nil {
		t.Fatalf("sinks.New(\"parquet\"): %v", err)
	}
	if s.Name() != "parquet" {
		t.Errorf("Name() = %q, want parquet", s.Name())
	}
}

func TestSink_RerunPrunesCoveredRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	t0 := time.Unix(1000, 0).UTC()
	t1 := time.Unix(2000, 0).UTC()
	t2 := time.Unix(3000, 0).UTC()

	s1 := pqsink.New()
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

	// Second run: fully covered. No parquet file, no manifest append.
	s2 := pqsink.New()
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
	if _, err := os.Stat(filepath.Join(dir, "run2", "pages.parquet")); !os.IsNotExist(err) {
		t.Errorf("run2/pages.parquet should not exist, stat err = %v", err)
	}
	store := manifest.NewStore(manifestPath)
	m, err := store.Load()
	if err != nil {
		t.Fatalf("manifest load: %v", err)
	}
	if len(m.Partitions) != 1 {
		t.Errorf("Partitions = %d, want 1", len(m.Partitions))
	}
	secondStat, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest after run 2: %v", err)
	}
	if !secondStat.ModTime().Equal(firstMtime) {
		t.Errorf("manifest mtime changed on no-op re-run: %v -> %v", firstMtime, secondStat.ModTime())
	}

	// Third run: one new record outside the covered window lands.
	s3 := pqsink.New()
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
		t.Fatalf("Partitions = %d, want 2", len(m.Partitions))
	}
	if m.Partitions[1].Rows != 1 {
		t.Errorf("new partition Rows = %d, want 1", m.Partitions[1].Rows)
	}
	if !m.Partitions[1].StartTime.Equal(t2) || !m.Partitions[1].EndTime.Equal(t2) {
		t.Errorf("new partition window = [%s, %s], want [%s, %s]", m.Partitions[1].StartTime, m.Partitions[1].EndTime, t2, t2)
	}
}
