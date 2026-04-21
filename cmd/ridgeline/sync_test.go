package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xydac/ridgeline/manifest"
)

func TestRunSync_DryRun(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := runSync(context.Background(), []string{"--dry-run", "--out", dir, "--records", "3"}); err != nil {
		t.Fatalf("runSync: %v", err)
	}
	// Manifest exists and records 2 partitions (pages + events).
	store := manifest.NewStore(filepath.Join(dir, "manifest.json"))
	m, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(m.Partitions) != 2 {
		t.Fatalf("Partitions = %d, want 2", len(m.Partitions))
	}
	var total int64
	for _, p := range m.Partitions {
		if p.Rows != 3 {
			t.Errorf("partition %s rows = %d, want 3", p.Stream, p.Rows)
		}
		total += p.Rows
		// Data file exists.
		if _, err := os.Stat(filepath.Join(dir, p.Path)); err != nil {
			t.Errorf("data file missing for %s: %v", p.Stream, err)
		}
	}
	if total != 6 {
		t.Errorf("total rows = %d, want 6", total)
	}
}

func TestRunSync_RequiresDryRunToday(t *testing.T) {
	t.Parallel()
	err := runSync(context.Background(), []string{})
	if err == nil {
		t.Fatal("expected error when --dry-run is absent")
	}
}

func TestRunSync_UnknownFlag(t *testing.T) {
	t.Parallel()
	err := runSync(context.Background(), []string{"--nope"})
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
}
