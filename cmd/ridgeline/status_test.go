package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// configFixture writes a minimal ridgeline.yaml pointing at dbPath,
// with a single testsrc connector under myapp/demo. It returns the
// config path.
func configFixture(t *testing.T, dir, dbPath, outDir string) string {
	t.Helper()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	cfg := `
version: 1
state_path: ` + dbPath + `
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
	return cfgPath
}

func TestRunStatus_RequiresConfig(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runStatus(context.Background(), nil, &buf)
	if err == nil {
		t.Fatal("expected error when --config is missing")
	}
	if !strings.Contains(err.Error(), "--config") {
		t.Errorf("got %q, want substring '--config'", err.Error())
	}
}

func TestRunStatus_MissingConfigFile(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runStatus(context.Background(), []string{"--config", "/tmp/no-such-ridgeline.yaml"}, &buf)
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
}

func TestRunStatus_NoStateYet_PrintsNeverSynced(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ridgeline.db")
	outDir := filepath.Join(dir, "out")
	cfgPath := configFixture(t, dir, dbPath, outDir)

	var buf bytes.Buffer
	if err := runStatus(context.Background(), []string{"--config", cfgPath}, &buf); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "(not created yet)") {
		t.Errorf("missing 'not created yet' banner:\n%s", out)
	}
	if !strings.Contains(out, "myapp/demo (testsrc)") {
		t.Errorf("missing connector line:\n%s", out)
	}
	if !strings.Contains(out, "never synced") {
		t.Errorf("missing 'never synced' for unseen connector:\n%s", out)
	}
	// No side effect: the state DB must not have been created.
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Errorf("status must not create the state DB; got stat err=%v", err)
	}
}

func TestRunStatus_AfterSync_ShowsCursorAndTimestamp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ridgeline.db")
	outDir := filepath.Join(dir, "out")
	cfgPath := configFixture(t, dir, dbPath, outDir)

	if err := runSync(context.Background(), []string{"--config", cfgPath}); err != nil {
		t.Fatalf("runSync: %v", err)
	}

	var buf bytes.Buffer
	if err := runStatus(context.Background(), []string{"--config", cfgPath}, &buf); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "(not created yet)") {
		t.Errorf("should not print 'not created yet' after a sync:\n%s", out)
	}
	if !strings.Contains(out, "myapp/demo (testsrc)") {
		t.Errorf("missing connector line:\n%s", out)
	}
	if !strings.Contains(out, "last sync:") {
		t.Errorf("missing 'last sync' line:\n%s", out)
	}
	if !strings.Contains(out, "cursor:") {
		t.Errorf("missing 'cursor' line:\n%s", out)
	}
	if strings.Contains(out, "never synced") {
		t.Errorf("should not say 'never synced' after sync:\n%s", out)
	}
}

func TestRunStatus_ReportsOrphanStateEntries(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ridgeline.db")
	outDir := filepath.Join(dir, "out")
	cfgPath := configFixture(t, dir, dbPath, outDir)

	// Run sync to create the state DB and a row for myapp/demo.
	if err := runSync(context.Background(), []string{"--config", cfgPath}); err != nil {
		t.Fatalf("runSync: %v", err)
	}

	// Rewrite config to rename the connector, leaving the prior key
	// as an orphan.
	renamedCfg := `
version: 1
state_path: ` + dbPath + `
products:
  myapp:
    connectors:
      - name: demo2
        type: testsrc
        config:
          records: 2
        streams: [pages, events]
        sink:
          type: jsonl
          options:
            dir: ` + outDir + `
`
	if err := os.WriteFile(cfgPath, []byte(renamedCfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	var buf bytes.Buffer
	if err := runStatus(context.Background(), []string{"--config", cfgPath}, &buf); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "orphan state entries") {
		t.Errorf("missing orphan section:\n%s", out)
	}
	if !strings.Contains(out, "myapp/demo ") && !strings.HasSuffix(strings.TrimSpace(out), "myapp/demo") {
		// The previous key should appear in the orphan list.
		if !strings.Contains(out, "myapp/demo") {
			t.Errorf("orphan list missing myapp/demo:\n%s", out)
		}
	}
	if !strings.Contains(out, "myapp/demo2 (testsrc)") {
		t.Errorf("renamed connector not shown:\n%s", out)
	}
}
