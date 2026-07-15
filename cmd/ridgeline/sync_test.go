package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/creds"
	"github.com/xydac/ridgeline/manifest"
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

// captureStdout swaps os.Stdout for a pipe, runs fn, and returns what
// was written. It is not safe for t.Parallel callers.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan struct{})
	var buf bytes.Buffer
	go func() {
		io.Copy(&buf, r)
		close(done)
	}()
	fn()
	w.Close()
	<-done
	os.Stdout = old
	return buf.String()
}

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

	// Manifest captures only the first run's partitions. The second
	// run emits the same deterministic records, so every timestamp is
	// already covered by a partition on disk and the sink drops the
	// whole batch without appending a new manifest entry.
	m, err := manifest.NewStore(filepath.Join(outDir, "manifest.json")).Load()
	if err != nil {
		t.Fatalf("manifest load: %v", err)
	}
	if len(m.Partitions) != 2 {
		t.Fatalf("partitions = %d, want 2 (2 streams, second run pruned)", len(m.Partitions))
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
version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: demo
        type: not-a-real-connector
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + filepath.Join(dir, "out") + ` } }
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	err := runSync(context.Background(), []string{"--config", cfgPath})
	if err == nil {
		t.Fatal("expected error for unknown connector type")
	}
	if !strings.Contains(err.Error(), "not-a-real-connector") {
		t.Errorf("error should name the bad type: %v", err)
	}
	if !strings.Contains(err.Error(), "known:") {
		t.Errorf("error should list known types: %v", err)
	}
}

func TestRunSync_Config_UnknownEnricherType(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	cfg := `
version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        config: { records: 1 }
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + filepath.Join(dir, "out") + ` } }
        enrichers:
          - type: not-a-real-enricher
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	err := runSync(context.Background(), []string{"--config", cfgPath})
	if err == nil {
		t.Fatal("expected error for unknown enricher type")
	}
	if !strings.Contains(err.Error(), "not-a-real-enricher") {
		t.Errorf("error should name the bad type: %v", err)
	}
	if !strings.Contains(err.Error(), "known:") {
		t.Errorf("error should list known types: %v", err)
	}
}

func TestRunSync_Config_RunsConnectorsInDeclaredYAMLOrder(t *testing.T) {
	// Regression for QA F-020: the pipeline sorted connectors by name
	// before running them, so a config listing connectors in order
	// "zulu, alpha, mike" would run alpha first, then mike, then zulu,
	// and a rename could silently re-order the pipeline. The fix is to
	// iterate product.Connectors in its declared slice order.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	dbPath := filepath.Join(dir, "state.db")
	cfg := `
version: 1
state_path: ` + dbPath + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: zulu
        type: testsrc
        config: { records: 1 }
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + filepath.Join(dir, "z") + ` } }
      - name: alpha
        type: testsrc
        config: { records: 1 }
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + filepath.Join(dir, "a") + ` } }
      - name: mike
        type: testsrc
        config: { records: 1 }
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + filepath.Join(dir, "m") + ` } }
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	var err error
	out := captureStdout(t, func() {
		err = runSync(context.Background(), []string{"--config", cfgPath})
	})
	if err != nil {
		t.Fatalf("runSync: %v", err)
	}
	want := []string{"myapp/zulu:", "myapp/alpha:", "myapp/mike:"}
	var idx int
	for _, w := range want {
		pos := strings.Index(out[idx:], w)
		if pos < 0 {
			t.Fatalf("output missing %q in declared order; full output:\n%s", w, out)
		}
		idx += pos + len(w)
	}
}

func TestRunSync_Config_TypoInSinkOptionsFailsFast(t *testing.T) {
	t.Parallel()
	// Regression for QA F-018: typo'd sink options keys were silently
	// dropped by the YAML decoder, so the user saw a misleading
	// "dir is required" instead of a hint pointing at the typo.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	cfg := `
version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        config: { records: 1 }
        streams: [pages]
        sink:
          type: jsonl
          options:
            dirr: ` + filepath.Join(dir, "out") + `
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	err := runSync(context.Background(), []string{"--config", cfgPath})
	if err == nil {
		t.Fatal("expected error for typo'd sink option")
	}
	msg := err.Error()
	if !strings.Contains(msg, `"dirr"`) || !strings.Contains(msg, `"dir"`) {
		t.Errorf("got %q, want did-you-mean for dirr -> dir", msg)
	}
}

func TestResolveConfigRefs_ReplacesRefs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	keyPath := filepath.Join(dir, "key")

	key, err := creds.NewRandomKey()
	if err != nil {
		t.Fatalf("NewRandomKey: %v", err)
	}
	if err := creds.WriteKeyFile(keyPath, key); err != nil {
		t.Fatalf("WriteKeyFile: %v", err)
	}
	store, err := sqlitestate.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer store.Close()
	cs, err := creds.New(store.DB(), key)
	if err != nil {
		t.Fatalf("creds.New: %v", err)
	}
	if err := cs.Put(context.Background(), "umami_main", []byte("stored-plaintext")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	cfg := &config.File{
		Version:   1,
		StatePath: dbPath,
		KeyPath:   keyPath,
		Products: map[string]config.Product{
			"myapp": {Connectors: []config.ConnectorInstance{{
				Name:    "umami",
				Type:    "umami",
				Config:  map[string]any{"api_key_ref": "umami_main", "website_id": "abc"},
				Streams: []string{"events"},
				Sink:    config.SinkRef{Type: "jsonl", Options: map[string]any{"dir": filepath.Join(dir, "o")}},
			}}},
		},
	}
	var stderr bytes.Buffer
	if _, err := resolveConfigRefs(context.Background(), cfg, store, &stderr); err != nil {
		t.Fatalf("resolveConfigRefs: %v", err)
	}
	got := cfg.Products["myapp"].Connectors[0].Config
	if got["api_key"] != "stored-plaintext" {
		t.Fatalf("api_key = %v, want stored-plaintext", got["api_key"])
	}
	if _, still := got["api_key_ref"]; still {
		t.Fatalf("api_key_ref should have been removed: %v", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr output: %q", stderr.String())
	}
}

func TestResolveConfigRefs_NoRefsSkipsCredsOpen(t *testing.T) {
	// When no connector declares a *_ref, the helper must NOT try to
	// open the key file. The key_path in this cfg points at a file that
	// does not exist, and resolveConfigRefs should still succeed.
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	store, err := sqlitestate.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer store.Close()

	cfg := &config.File{
		Version:   1,
		StatePath: dbPath,
		KeyPath:   filepath.Join(dir, "does-not-exist"),
		Products: map[string]config.Product{
			"myapp": {Connectors: []config.ConnectorInstance{{
				Name: "demo", Type: "testsrc",
				Config:  map[string]any{"records": 1},
				Streams: []string{"pages"},
				Sink:    config.SinkRef{Type: "jsonl", Options: map[string]any{"dir": filepath.Join(dir, "o")}},
			}}},
		},
	}
	var stderr bytes.Buffer
	if _, err := resolveConfigRefs(context.Background(), cfg, store, &stderr); err != nil {
		t.Fatalf("unexpected error without refs: %v", err)
	}
}

func TestRunSync_Config_RefResolutionFailsForMissingCred(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	dbPath := filepath.Join(dir, "state.db")
	keyPath := filepath.Join(dir, "key")

	// Write a key file but DO NOT store the credential the config refers to.
	key, err := creds.NewRandomKey()
	if err != nil {
		t.Fatalf("NewRandomKey: %v", err)
	}
	if err := creds.WriteKeyFile(keyPath, key); err != nil {
		t.Fatalf("WriteKeyFile: %v", err)
	}

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
          records: 1
          api_key_ref: not_in_store
        streams: [pages]
        sink:
          type: jsonl
          options:
            dir: ` + filepath.Join(dir, "out") + `
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	err = runSync(context.Background(), []string{"--config", cfgPath})
	if err == nil {
		t.Fatal("expected error for missing credential")
	}
	if !strings.Contains(err.Error(), "not_in_store") {
		t.Errorf("err = %v, want to name missing credential", err)
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

// failCfg returns a YAML snippet for an external connector that exits 1,
// passing validation (non-empty command) but failing at extract time.
func failConnectorYAML(name, outDir string) string {
	return `
      - name: ` + name + `
        type: external
        config: { command: "/bin/false" }
        streams: [events]
        sink: { type: jsonl, options: { dir: ` + outDir + ` } }`
}

func TestRunSync_ContinueOnError_OneConnectorFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	firstOut := filepath.Join(dir, "first")
	failOut := filepath.Join(dir, "fail")
	thirdOut := filepath.Join(dir, "third")

	cfg := `
version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: first
        type: testsrc
        config: { records: 2 }
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + firstOut + ` } }` +
		failConnectorYAML("mid", failOut) + `
      - name: third
        type: testsrc
        config: { records: 2 }
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + thirdOut + ` } }
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	err := runSync(context.Background(), []string{"--config", cfgPath, "--continue-on-error"})
	if err == nil {
		t.Fatal("expected PartialSyncError")
	}
	var pse *PartialSyncError
	if !errors.As(err, &pse) {
		t.Fatalf("want *PartialSyncError, got %T: %v", err, err)
	}
	if len(pse.failures) != 1 {
		t.Errorf("failures = %d, want 1", len(pse.failures))
	}
	if pse.failures[0].connector != "mid" {
		t.Errorf("failed connector = %q, want 'mid'", pse.failures[0].connector)
	}
	if pse.succeeded != 2 {
		t.Errorf("succeeded = %d, want 2", pse.succeeded)
	}
	if pse.IsTotal() {
		t.Error("IsTotal() = true; want false with 2 succeeded")
	}
	// successful connectors must have written output
	if _, err := os.Stat(firstOut); err != nil {
		t.Errorf("first connector output missing: %v", err)
	}
	if _, err := os.Stat(thirdOut); err != nil {
		t.Errorf("third connector output missing: %v", err)
	}
	// error message must name the failed connector
	if !strings.Contains(err.Error(), "mid") {
		t.Errorf("error %q does not name failed connector", err.Error())
	}
}

func TestRunSync_ContinueOnError_AllFail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")

	cfg := `
version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:` +
		failConnectorYAML("a", filepath.Join(dir, "a")) +
		failConnectorYAML("b", filepath.Join(dir, "b")) + `
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	err := runSync(context.Background(), []string{"--config", cfgPath, "--continue-on-error"})
	if err == nil {
		t.Fatal("expected PartialSyncError")
	}
	var pse *PartialSyncError
	if !errors.As(err, &pse) {
		t.Fatalf("want *PartialSyncError, got %T: %v", err, err)
	}
	if len(pse.failures) != 2 {
		t.Errorf("failures = %d, want 2", len(pse.failures))
	}
	if pse.succeeded != 0 {
		t.Errorf("succeeded = %d, want 0", pse.succeeded)
	}
	if !pse.IsTotal() {
		t.Error("IsTotal() = false; want true with 0 succeeded")
	}
}

func TestRunSync_ContinueOnError_AllPass(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	firstOut := filepath.Join(dir, "first")
	secondOut := filepath.Join(dir, "second")

	cfg := `
version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: first
        type: testsrc
        config: { records: 1 }
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + firstOut + ` } }
      - name: second
        type: testsrc
        config: { records: 1 }
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + secondOut + ` } }
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := runSync(context.Background(), []string{"--config", cfgPath, "--continue-on-error"}); err != nil {
		t.Fatalf("expected nil on all-pass with --continue-on-error: %v", err)
	}
}

func TestRunSync_Config_TimedOutConnector_ContinueOnError(t *testing.T) {
	// F-072: a hanging external connector must time out (via the per-connector
	// timeout config field) and, with --continue-on-error, let the remaining
	// connectors finish and commit their data.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	goodOut := filepath.Join(dir, "good")
	stuckOut := filepath.Join(dir, "stuck")

	// Build the stuck connector config using a shell loop that blocks until killed.
	// Using /bin/sh avoids test-binary re-exec complications (coverage env vars, etc.).
	stuckConnector := `
      - name: stuck
        type: external
        config:
          command: /bin/sh
          args: ["-c", "while :; do sleep 1; done"]
          timeout: "500ms"
        streams: [events]
        sink: { type: jsonl, options: { dir: ` + stuckOut + ` } }`

	cfg := `
version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: good
        type: testsrc
        config: { records: 2 }
        streams: [pages]
        sink: { type: jsonl, options: { dir: ` + goodOut + ` } }` +
		stuckConnector + `
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	err := runSync(context.Background(), []string{"--config", cfgPath, "--continue-on-error"})
	if err == nil {
		t.Fatal("expected PartialSyncError from the timed-out connector")
	}
	var pse *PartialSyncError
	if !errors.As(err, &pse) {
		t.Fatalf("want *PartialSyncError, got %T: %v", err, err)
	}
	if len(pse.failures) != 1 {
		t.Errorf("failures = %d, want 1", len(pse.failures))
	}
	if pse.failures[0].connector != "stuck" {
		t.Errorf("failed connector = %q, want stuck", pse.failures[0].connector)
	}
	if !strings.Contains(pse.failures[0].err.Error(), "timed out") {
		t.Errorf("failure error %q does not mention timed out", pse.failures[0].err)
	}
	if pse.succeeded != 1 {
		t.Errorf("succeeded = %d, want 1", pse.succeeded)
	}
	// The good connector must have written output.
	if _, err := os.Stat(goodOut); err != nil {
		t.Errorf("good connector output missing: %v", err)
	}
}

// TestRunSync_OutDirRoot verifies that --out-dir-root writes stream
// files directly under the output directory (no per-run subdirectory).
func TestRunSync_OutDirRoot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := runSync(context.Background(), []string{
		"--dry-run", "--out", dir, "--records", "2", "--out-dir-root",
	}); err != nil {
		t.Fatalf("runSync: %v", err)
	}
	// With --out-dir-root, stream files land directly in dir.
	for _, stream := range []string{"pages", "events"} {
		path := filepath.Join(dir, stream+".jsonl")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected flat file %s: %v", path, err)
		}
	}
	// No per-run numeric subdirectory should exist.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			t.Errorf("unexpected subdirectory under flat output dir: %s", e.Name())
		}
	}
}

// TestRunSync_DefaultNestedLayout verifies that without --out-dir-root
// the default layout nests files under a per-run directory.
func TestRunSync_DefaultNestedLayout(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := runSync(context.Background(), []string{
		"--dry-run", "--out", dir, "--records", "2",
	}); err != nil {
		t.Fatalf("runSync: %v", err)
	}
	// Stream files must NOT be at the root.
	for _, stream := range []string{"pages", "events"} {
		path := filepath.Join(dir, stream+".jsonl")
		if _, err := os.Stat(path); err == nil {
			t.Errorf("flat file %s exists but should be nested under a run-id dir", path)
		}
	}
	// At least one subdirectory (the run-id dir) must exist.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	foundSubdir := false
	for _, e := range entries {
		if e.IsDir() {
			foundSubdir = true
			break
		}
	}
	if !foundSubdir {
		t.Error("no per-run subdirectory found in default output layout")
	}
}
