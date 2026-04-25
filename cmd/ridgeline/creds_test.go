package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeMinimalConfig drops a ridgeline.yaml in dir that points state
// and key paths at dir. Returns the config path.
func writeMinimalConfig(t *testing.T, dir string) string {
	t.Helper()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	body := `
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
            dir: ` + filepath.Join(dir, "out") + `
`
	if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return cfgPath
}

func TestRunCreds_PutListGetRm(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()

	// put
	var out, errOut bytes.Buffer
	in := bytes.NewBufferString("super-secret\n")
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "umami_main"}, in, &out, &errOut); err != nil {
		t.Fatalf("put: %v", err)
	}
	if !strings.Contains(errOut.String(), "stored credential") {
		t.Errorf("put stderr = %q, want confirmation", errOut.String())
	}

	// list
	out.Reset()
	errOut.Reset()
	if err := runCreds(ctx, []string{"list", "--config", cfgPath}, bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "umami_main" {
		t.Errorf("list stdout = %q, want umami_main", got)
	}

	// get
	out.Reset()
	errOut.Reset()
	if err := runCreds(ctx, []string{"get", "--config", cfgPath, "umami_main"}, bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := strings.TrimRight(out.String(), "\n"); got != "super-secret" {
		t.Errorf("get stdout = %q, want super-secret", got)
	}

	// rm
	out.Reset()
	errOut.Reset()
	if err := runCreds(ctx, []string{"rm", "--config", cfgPath, "umami_main"}, bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("rm: %v", err)
	}
	// List is empty after rm.
	out.Reset()
	if err := runCreds(ctx, []string{"list", "--config", cfgPath}, bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("list after rm: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "" {
		t.Errorf("list after rm stdout = %q, want empty", got)
	}
}

func TestRunCreds_GetMissingFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	var out, errOut bytes.Buffer
	err := runCreds(context.Background(), []string{"get", "--config", cfgPath, "does_not_exist"},
		bytes.NewReader(nil), &out, &errOut)
	if err == nil {
		t.Fatal("want error for missing credential")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("err = %v, want 'not found'", err)
	}
}

func TestRunCreds_UnknownVerb(t *testing.T) {
	t.Parallel()
	err := runCreds(context.Background(), []string{"wat"}, bytes.NewReader(nil), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("want error for unknown verb")
	}
}

func TestRunCreds_NoArgsPrintsHelp(t *testing.T) {
	t.Parallel()
	var out, errOut bytes.Buffer
	if err := runCreds(context.Background(), nil, bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("help: %v", err)
	}
	if !strings.Contains(out.String(), "creds list") {
		t.Errorf("help output missing usage: %q", out.String())
	}
}

func TestRunCreds_PutRejectsEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	var out, errOut bytes.Buffer
	err := runCreds(context.Background(), []string{"put", "--config", cfgPath, "empty"},
		bytes.NewReader([]byte("")), &out, &errOut)
	if err == nil {
		t.Fatal("want error for empty secret")
	}
}

func TestRunCreds_PutRequiresName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	var out, errOut bytes.Buffer
	err := runCreds(context.Background(), []string{"put", "--config", cfgPath},
		bytes.NewBufferString("x"), &out, &errOut)
	if err == nil {
		t.Fatal("want error when NAME is missing")
	}
}

func TestRunCreds_PutTrimsTrailingNewline(t *testing.T) {
	// Verifies the round-trip preserves the secret byte-for-byte
	// without the trailing newline a shell here-doc or echo adds.
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	var out, errOut bytes.Buffer
	ctx := context.Background()
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "k"},
		bytes.NewBufferString("secret-value\n"), &out, &errOut); err != nil {
		t.Fatalf("put: %v", err)
	}
	out.Reset()
	if err := runCreds(ctx, []string{"get", "--config", cfgPath, "k"},
		bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := strings.TrimRight(out.String(), "\n"); got != "secret-value" {
		t.Errorf("round trip: got %q", got)
	}
}

func TestRunCreds_PutRawPreservesTrailingNewline(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	var out, errOut bytes.Buffer
	ctx := context.Background()
	if err := runCreds(ctx, []string{"put", "--raw", "--config", cfgPath, "k"},
		bytes.NewBufferString("secret-value\n"), &out, &errOut); err != nil {
		t.Fatalf("put --raw: %v", err)
	}
	out.Reset()
	if err := runCreds(ctx, []string{"get", "--config", cfgPath, "k"},
		bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("get: %v", err)
	}
	// --raw keeps the trailing newline; get adds one more when absent,
	// so the stored bytes ("secret-value\n") come back as "secret-value\n".
	if got := out.String(); got != "secret-value\n" {
		t.Errorf("raw round trip: got %q, want %q", got, "secret-value\n")
	}
}

func TestRunCreds_PutPrintsReplaced(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()

	var errOut bytes.Buffer
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "mykey"},
		bytes.NewBufferString("first\n"), &bytes.Buffer{}, &errOut); err != nil {
		t.Fatalf("first put: %v", err)
	}
	if !strings.Contains(errOut.String(), "stored") {
		t.Errorf("first put stderr = %q, want 'stored'", errOut.String())
	}
	if strings.Contains(errOut.String(), "replaced") {
		t.Errorf("first put stderr = %q, must not say 'replaced'", errOut.String())
	}

	errOut.Reset()
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "mykey"},
		bytes.NewBufferString("second\n"), &bytes.Buffer{}, &errOut); err != nil {
		t.Fatalf("second put: %v", err)
	}
	if !strings.Contains(errOut.String(), "replaced") {
		t.Errorf("second put stderr = %q, want 'replaced'", errOut.String())
	}
}
