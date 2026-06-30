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
)

// writeCredsOnlyConfig drops a ridgeline.yaml with no products: block.
// It is valid for creds commands but not for sync/status.
func writeCredsOnlyConfig(t *testing.T, dir string) string {
	t.Helper()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	body := `version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
`
	if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return cfgPath
}

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
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("err = %v, want 'does not exist'", err)
	}
}

func TestRunCreds_UnknownVerb(t *testing.T) {
	t.Parallel()
	err := runCreds(context.Background(), []string{"wat"}, bytes.NewReader(nil), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("want error for unknown verb")
	}
}

func TestRunCreds_NoArgsPrintsUsageToStderr(t *testing.T) {
	t.Parallel()
	var out, errOut bytes.Buffer
	err := runCreds(context.Background(), nil, bytes.NewReader(nil), &out, &errOut)
	if err == nil {
		t.Fatal("want usage error when no verb given, got nil")
	}
	var ue *usageError
	if !errors.As(err, &ue) {
		t.Fatalf("want usageError, got %T: %v", err, err)
	}
	// Usage goes to stderr so the caller can exit 2 without stdout noise.
	if !strings.Contains(errOut.String(), "creds list") {
		t.Errorf("stderr missing usage; got: %q", errOut.String())
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

// F-033: creds commands must work against a config with no products:.
func TestRunCreds_NoProductsConfig(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeCredsOnlyConfig(t, dir)
	ctx := context.Background()

	var out, errOut bytes.Buffer
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "mykey"},
		bytes.NewBufferString("secret\n"), &out, &errOut); err != nil {
		t.Fatalf("put against no-products config: %v", err)
	}
	out.Reset()
	if err := runCreds(ctx, []string{"list", "--config", cfgPath},
		bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("list against no-products config: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "mykey" {
		t.Errorf("list = %q, want mykey", got)
	}
}

// F-034: creds put/get/rm must reject names with path traversal or whitespace.
func TestRunCreds_RejectsInvalidNames(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeCredsOnlyConfig(t, dir)
	ctx := context.Background()

	cases := []struct {
		verb string
		name string
	}{
		{"put", "../../etc/x"},
		{"put", "../secret"},
		{"put", "foo/bar"},
		{"put", "foo bar"},
		{"put", "foo\tbar"},
		{"get", "../../etc/x"},
		{"rm", "../../etc/x"},
	}
	for _, tc := range cases {
		args := []string{tc.verb, "--config", cfgPath, tc.name}
		var stdin *bytes.Buffer
		if tc.verb == "put" {
			stdin = bytes.NewBufferString("x\n")
		} else {
			stdin = bytes.NewBufferString("")
		}
		err := runCreds(ctx, args, stdin, io.Discard, io.Discard)
		if err == nil {
			t.Errorf("creds %s %q: want error, got nil", tc.verb, tc.name)
		}
	}
}

// F-032: creds error messages must not double the "creds:" prefix.
func TestRunCreds_ErrorPrefixNotDoubled(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeCredsOnlyConfig(t, dir)
	ctx := context.Background()

	// get on a missing key should give exactly one "creds:" prefix when
	// main.go wraps it; at the runCreds level the error should NOT start
	// with "creds:" so that the main.go wrap adds the first and only one.
	err := runCreds(ctx, []string{"get", "--config", cfgPath, "missing"},
		bytes.NewReader(nil), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("want error for missing credential")
	}
	msg := err.Error()
	if strings.HasPrefix(msg, "creds:") {
		t.Errorf("error %q starts with 'creds:'; main.go will double it", msg)
	}
	if !strings.Contains(msg, "does not exist") {
		t.Errorf("error %q should say 'does not exist'", msg)
	}
}
