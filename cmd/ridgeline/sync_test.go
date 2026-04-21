package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/manifest"
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
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

func TestRunSync_RequiresModeFlag(t *testing.T) {
	t.Parallel()
	err := runSync(context.Background(), []string{})
	if err == nil {
		t.Fatal("expected error when neither --dry-run nor --config is set")
	}
}

func TestRunSync_UnknownFlag(t *testing.T) {
	t.Parallel()
	err := runSync(context.Background(), []string{"--nope"})
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
}

func TestRunSync_DryRun_RejectsEmptyOut(t *testing.T) {
	t.Parallel()
	err := runSync(context.Background(), []string{"--dry-run", "--out", ""})
	if err == nil {
		t.Fatal("expected error for explicit empty --out")
	}
	if !strings.Contains(err.Error(), "--out must not be empty") {
		t.Errorf("got %q, want substring '--out must not be empty'", err.Error())
	}
}

func TestRunSync_ConfigAndDryRunMutuallyExclusive(t *testing.T) {
	t.Parallel()
	err := runSync(context.Background(), []string{"--dry-run", "--config", "/tmp/x.yaml"})
	if err == nil {
		t.Fatal("expected mutual-exclusion error")
	}
}

func TestRunSync_Config_PersistsStateAcrossRuns(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	outDir := filepath.Join(dir, "out")
	dbPath := filepath.Join(dir, "ridgeline.db")
	keyPath := filepath.Join(dir, "key")
	cfgPath := filepath.Join(dir, "ridgeline.yaml")

	cfg := `
version: 1
state_path: ` + dbPath + `
key_path: ` + keyPath + `
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        config:
          records: 2
        streams: [pages, events]
        sink:
          type: jsonl
          options:
            dir: ` + outDir + `
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// First run creates the DB, writes state.
	if err := runSync(context.Background(), []string{"--config", cfgPath}); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("state db not created: %v", err)
	}

	store, err := sqlitestate.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	keys, err := store.Keys(context.Background())
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	want := config.StateKey("myapp", "demo")
	if len(keys) != 1 || keys[0] != want {
		t.Fatalf("keys after run 1: got %v, want [%s]", keys, want)
	}
	store.Close()

	// Second run reuses the same state file without error.
	if err := runSync(context.Background(), []string{"--config", cfgPath}); err != nil {
		t.Fatalf("second sync: %v", err)
	}

	// Manifest captures partitions from both runs.
	m, err := manifest.NewStore(filepath.Join(outDir, "manifest.json")).Load()
	if err != nil {
		t.Fatalf("manifest load: %v", err)
	}
	if len(m.Partitions) != 4 {
		t.Fatalf("partitions = %d, want 4 (2 streams x 2 runs)", len(m.Partitions))
	}
}

func TestRunSync_Config_MissingFile(t *testing.T) {
	t.Parallel()
	err := runSync(context.Background(), []string{"--config", "/tmp/definitely-not-a-config.yaml"})
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
	// Regression for QA F-008: the path must appear at most once and
	// the os.* error verb (open/read) must not be wrapped by another
	// matching verb.
	msg := err.Error()
	if strings.Count(msg, "/tmp/definitely-not-a-config.yaml") != 1 {
		t.Errorf("path should appear exactly once in %q", msg)
	}
	if strings.Count(msg, "open ") > 1 || strings.Contains(msg, "read ") && strings.Contains(msg, "open ") {
		t.Errorf("doubled os-verb prefix in %q", msg)
	}
}

func TestRunSync_Config_InvalidConnectorConfigFailsBeforeAnyIO(t *testing.T) {
	t.Parallel()
	// A healthy connector ("ok") paired with a misconfigured external
	// connector ("bad") that omits the required command field. The run
	// must refuse at load time, before "ok" writes anything to its sink.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	okOut := filepath.Join(dir, "ok-out")
	badOut := filepath.Join(dir, "bad-out")
	cfg := `
version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: aok
        type: testsrc
        config: { records: 1 }
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + okOut + ` } }
      - name: bbad
        type: external
        config: { command: "" }
        streams: [events]
        sink: { type: jsonl, options: { dir: ` + badOut + ` } }
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	err := runSync(context.Background(), []string{"--config", cfgPath})
	if err == nil {
		t.Fatal("expected load-time validation error")
	}
	if !strings.Contains(err.Error(), "bbad") {
		t.Errorf("err = %v, want to mention the bbad connector", err)
	}
	// The healthy connector must not have produced any output, since
	// validation failed before the extract loop ran.
	if _, statErr := os.Stat(okOut); statErr == nil {
		t.Error("aok wrote output; load-time validation should have aborted before any IO")
	}
}

func TestRunSync_Config_UnknownConnectorType(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	cfg := `
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: demo
        type: not-a-real-connector
        sink: { type: jsonl, options: { dir: ` + filepath.Join(dir, "out") + ` } }
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := runSync(context.Background(), []string{"--config", cfgPath}); err == nil {
		t.Fatal("expected error for unknown connector type")
	}
}

func TestRunSync_Config_UnknownSinkType(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	cfg := `
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        sink: { type: parquet }
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := runSync(context.Background(), []string{"--config", cfgPath}); err == nil {
		t.Fatal("expected error for unknown sink type")
	}
}
