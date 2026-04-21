package manifest_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xydac/ridgeline/manifest"
)

func TestLoad_MissingFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := manifest.NewStore(filepath.Join(dir, "manifest.json"))
	m, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m.Version != manifest.Version {
		t.Errorf("Version = %d, want %d", m.Version, manifest.Version)
	}
	if len(m.Partitions) != 0 {
		t.Errorf("Partitions = %d, want 0", len(m.Partitions))
	}
}

func TestAppendThenLoad(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "manifest.json")
	s := manifest.NewStore(path)

	p1 := manifest.Partition{
		Stream:    "pages",
		Path:      "pages/0001.jsonl",
		Format:    "jsonl",
		Rows:      10,
		SizeBytes: 123,
		StartTime: time.Unix(100, 0).UTC(),
		EndTime:   time.Unix(200, 0).UTC(),
	}
	p2 := manifest.Partition{
		Stream:    "events",
		Path:      "events/0001.jsonl",
		Format:    "jsonl",
		Rows:      3,
		SizeBytes: 45,
		StartTime: time.Unix(150, 0).UTC(),
		EndTime:   time.Unix(175, 0).UTC(),
	}

	if err := s.Append(p1); err != nil {
		t.Fatalf("Append p1: %v", err)
	}
	if err := s.Append(p2); err != nil {
		t.Fatalf("Append p2: %v", err)
	}

	m, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(m.Partitions) != 2 {
		t.Fatalf("Partitions = %d, want 2", len(m.Partitions))
	}
	if m.Partitions[0].Stream != "pages" || m.Partitions[1].Stream != "events" {
		t.Errorf("order wrong: %+v", m.Partitions)
	}
	if m.Partitions[0].CreatedAt.IsZero() {
		t.Errorf("CreatedAt was not set")
	}
	if m.UpdatedAt.IsZero() {
		t.Errorf("UpdatedAt was not set")
	}

	// Validate raw JSON shape.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v, _ := raw["version"].(float64); int(v) != manifest.Version {
		t.Errorf("raw version = %v, want %d", raw["version"], manifest.Version)
	}
}

func TestAppend_PreservesCreatedAt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := manifest.NewStore(filepath.Join(dir, "manifest.json"))
	want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	p := manifest.Partition{Stream: "s", Path: "s/1", CreatedAt: want}
	if err := s.Append(p); err != nil {
		t.Fatalf("Append: %v", err)
	}
	m, _ := s.Load()
	if !m.Partitions[0].CreatedAt.Equal(want) {
		t.Errorf("CreatedAt = %v, want %v", m.Partitions[0].CreatedAt, want)
	}
}

func TestForStream(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := manifest.NewStore(filepath.Join(dir, "manifest.json"))
	_ = s.Append(manifest.Partition{Stream: "a", Path: "a/1"})
	_ = s.Append(manifest.Partition{Stream: "b", Path: "b/1"})
	_ = s.Append(manifest.Partition{Stream: "a", Path: "a/2"})
	m, _ := s.Load()
	got := m.ForStream("a")
	if len(got) != 2 {
		t.Fatalf("ForStream a = %d, want 2", len(got))
	}
	if got[0].Path != "a/1" || got[1].Path != "a/2" {
		t.Errorf("order wrong: %+v", got)
	}
	if len(m.ForStream("nope")) != 0 {
		t.Errorf("ForStream(nope) should be empty")
	}
}

func TestSave_AtomicOverwrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	s := manifest.NewStore(path)
	if err := s.Append(manifest.Partition{Stream: "s", Path: "s/1"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Save(manifest.Manifest{Partitions: []manifest.Partition{{Stream: "s", Path: "s/2"}}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	m, _ := s.Load()
	if len(m.Partitions) != 1 || m.Partitions[0].Path != "s/2" {
		t.Errorf("overwrite failed: %+v", m.Partitions)
	}
}

func TestLoad_CorruptFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := manifest.NewStore(path)
	if _, err := s.Load(); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestAppend_Concurrent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := manifest.NewStore(filepath.Join(dir, "manifest.json"))
	var wg sync.WaitGroup
	const n = 20
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.Append(manifest.Partition{Stream: "s", Path: filepath.Join("s", "p")})
			_ = i
		}(i)
	}
	wg.Wait()
	m, _ := s.Load()
	if len(m.Partitions) != n {
		t.Errorf("Partitions = %d, want %d", len(m.Partitions), n)
	}
}
