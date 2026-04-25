package sqlite_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/state/sqlite"
)

func TestOpen_MemoryAndRoundTrip(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	got, err := s.Load(ctx, "never-saved")
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Load empty: want zero-length state, got %v", got)
	}

	want := connectors.State{"cursor": "2026-04-20", "page": float64(42)}
	if err := s.Save(ctx, "myapp_gsc", want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err = s.Load(ctx, "myapp_gsc")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got["cursor"] != "2026-04-20" || got["page"] != float64(42) {
		t.Fatalf("Load: got %v", got)
	}
}

func TestSave_OverwritesExisting(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Save(ctx, "k", connectors.State{"v": 1.0}); err != nil {
		t.Fatalf("Save1: %v", err)
	}
	if err := s.Save(ctx, "k", connectors.State{"v": 2.0}); err != nil {
		t.Fatalf("Save2: %v", err)
	}
	got, err := s.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got["v"] != 2.0 {
		t.Fatalf("Load after overwrite: want 2, got %v", got["v"])
	}
}

func TestPersistsAcrossProcessRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ridgeline.db")
	ctx := context.Background()

	s1, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("Open1: %v", err)
	}
	if err := s1.Save(ctx, "persist", connectors.State{"cursor": "A"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close1: %v", err)
	}

	s2, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("Open2: %v", err)
	}
	defer s2.Close()
	got, err := s2.Load(ctx, "persist")
	if err != nil {
		t.Fatalf("Load after reopen: %v", err)
	}
	if got["cursor"] != "A" {
		t.Fatalf("after reopen: want cursor=A, got %v", got)
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Load(ctx, ""); err == nil {
		t.Fatalf("Load empty key: want error")
	}
	if err := s.Save(ctx, "", connectors.State{}); err == nil {
		t.Fatalf("Save empty key: want error")
	}
	if err := s.Delete(ctx, ""); err == nil {
		t.Fatalf("Delete empty key: want error")
	}
}

func TestDelete(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	if err := s.Save(ctx, "k", connectors.State{"v": 1.0}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err := s.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load after delete: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Load after delete: want empty, got %v", got)
	}
	// Delete is idempotent.
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete missing: want no error, got %v", err)
	}
}

func TestKeysSorted(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	for _, k := range []string{"c", "a", "b"} {
		if err := s.Save(ctx, k, connectors.State{"v": 1.0}); err != nil {
			t.Fatalf("Save %s: %v", k, err)
		}
	}
	keys, err := s.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	want := []string{"a", "b", "c"}
	if len(keys) != len(want) {
		t.Fatalf("Keys: got %v", keys)
	}
	for i, k := range want {
		if keys[i] != k {
			t.Fatalf("Keys[%d]: want %s got %s", i, k, keys[i])
		}
	}
}

func TestConcurrentWrites(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.Save(ctx, "k", connectors.State{"i": float64(i)}); err != nil {
				t.Errorf("concurrent Save %d: %v", i, err)
			}
			if _, err := s.Load(ctx, "k"); err != nil {
				t.Errorf("concurrent Load %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestList_ReturnsEntriesSortedWithUpdatedAt(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	want := map[string]connectors.State{
		"myapp/gsc":  {"cursor": "2026-04-20"},
		"myapp/hn":   {"created_at_i": float64(1776758411)},
		"blog/umami": nil,
	}
	for _, k := range []string{"myapp/hn", "myapp/gsc", "blog/umami"} {
		if err := s.Save(ctx, k, want[k]); err != nil {
			t.Fatalf("Save %s: %v", k, err)
		}
	}

	entries, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	gotKeys := make([]string, 0, len(entries))
	for _, e := range entries {
		gotKeys = append(gotKeys, e.Key)
	}
	wantKeys := []string{"blog/umami", "myapp/gsc", "myapp/hn"}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("List keys: got %v want %v", gotKeys, wantKeys)
	}
	for i, k := range wantKeys {
		if gotKeys[i] != k {
			t.Fatalf("List keys[%d]: got %s want %s", i, gotKeys[i], k)
		}
	}
	for _, e := range entries {
		if e.UpdatedAt == "" {
			t.Errorf("UpdatedAt empty for %s", e.Key)
		}
		if e.State == nil {
			t.Errorf("State nil for %s (want empty map at minimum)", e.Key)
		}
	}
	// Cursor value round-trips.
	for _, e := range entries {
		if e.Key == "myapp/hn" && e.State["created_at_i"] != float64(1776758411) {
			t.Fatalf("List hn cursor: got %v", e.State["created_at_i"])
		}
	}
}

func TestList_EmptyStoreReturnsEmpty(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	got, err := s.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List empty: got %v", got)
	}
}

func TestOpen_NotADBNoRawErrno(t *testing.T) {
	// Opening a non-SQLite file should return a friendly message,
	// not the raw sqlite errno (26).
	_, err := sqlite.Open("/etc/hostname")
	if err == nil {
		t.Fatalf("expected error opening non-sqlite file")
	}
	msg := err.Error()
	if strings.Contains(msg, "(26)") {
		t.Errorf("error leaks raw errno (26): %s", msg)
	}
	if !strings.Contains(msg, "not a valid SQLite database") {
		t.Errorf("error missing friendly message: %s", msg)
	}
}

func TestOpen_CantOpenNoRawErrno(t *testing.T) {
	// Opening a path inside a non-existent directory should return a
	// friendly message, not the raw sqlite errno (14).
	nodir := filepath.Join(t.TempDir(), "no", "such", "subdir", "db.sqlite")
	// Remove the tempdir to ensure MkdirAll cannot help us -- but
	// actually MkdirAll in Open creates the parent dir. So use a path
	// where the parent is a FILE, not a directory.
	f, err := os.CreateTemp(t.TempDir(), "notadir")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	nodir = filepath.Join(f.Name(), "db.sqlite")

	_, err = sqlite.Open(nodir)
	if err == nil {
		t.Fatalf("expected error for path inside a file")
	}
	msg := err.Error()
	if strings.Contains(msg, "(14)") {
		t.Errorf("error leaks raw errno (14): %s", msg)
	}
}

func TestOpen_MigrationIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ridgeline.db")
	for i := 0; i < 3; i++ {
		s, err := sqlite.Open(path)
		if err != nil {
			t.Fatalf("Open iteration %d: %v", i, err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close iteration %d: %v", i, err)
		}
	}
}
