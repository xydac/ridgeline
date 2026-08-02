package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
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

// F-077: creds get --raw must suppress the trailing newline so the byte count
// equals the byte count of the stored secret exactly.
func TestRunCreds_GetRawSuppressesNewline(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()
	var out, errOut bytes.Buffer

	// Store exactly 3 bytes ("tok", no newline) with put --raw.
	if err := runCreds(ctx, []string{"put", "--raw", "--config", cfgPath, "tok"},
		bytes.NewBufferString("tok"), &out, &errOut); err != nil {
		t.Fatalf("put --raw: %v", err)
	}

	out.Reset()
	if err := runCreds(ctx, []string{"get", "--raw", "--config", cfgPath, "tok"},
		bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("get --raw: %v", err)
	}
	got := out.Bytes()
	if string(got) != "tok" {
		t.Errorf("get --raw: got %q (%d bytes), want %q (3 bytes)", got, len(got), "tok")
	}
}

// F-077: without --raw, get still appends a newline for shell friendliness.
func TestRunCreds_GetWithoutRawAppendsNewline(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()
	var out, errOut bytes.Buffer

	if err := runCreds(ctx, []string{"put", "--raw", "--config", cfgPath, "notrail"},
		bytes.NewBufferString("val"), &out, &errOut); err != nil {
		t.Fatalf("put --raw: %v", err)
	}

	out.Reset()
	if err := runCreds(ctx, []string{"get", "--config", cfgPath, "notrail"},
		bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := out.String(); got != "val\n" {
		t.Errorf("get without --raw: got %q, want %q", got, "val\n")
	}
}

// F-064: --config is honored when placed after the NAME positional.
func TestRunCreds_ConfigFlagAfterName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()
	var out, errOut bytes.Buffer

	// put with --config after NAME
	if err := runCreds(ctx, []string{"put", "cfgafter", "--config", cfgPath},
		bytes.NewBufferString("afterval\n"), &out, &errOut); err != nil {
		t.Fatalf("put with --config after NAME: %v", err)
	}

	// get with --config after NAME
	out.Reset()
	if err := runCreds(ctx, []string{"get", "cfgafter", "--config", cfgPath},
		bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("get with --config after NAME: %v", err)
	}
	if got := strings.TrimRight(out.String(), "\n"); got != "afterval" {
		t.Errorf("get after-name: got %q, want %q", got, "afterval")
	}

	// rm with --config after NAME
	out.Reset()
	if err := runCreds(ctx, []string{"rm", "cfgafter", "--config", cfgPath},
		bytes.NewReader(nil), &out, &errOut); err != nil {
		t.Fatalf("rm with --config after NAME: %v", err)
	}
}

// F-116: absent key file over a populated store must error, not silently orphan secrets.
func TestRunCreds_AbsentKeyOverPopulatedStoreErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()

	// Store a credential so the database is not empty.
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "tok"},
		bytes.NewBufferString("secret\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Remove the key file to simulate a lost key.
	keyPath := filepath.Join(dir, "key")
	if err := os.Remove(keyPath); err != nil {
		t.Fatalf("remove key: %v", err)
	}

	// Any creds verb must now error, not silently mint a new key.
	for _, verb := range []string{"list", "get", "rm"} {
		args := []string{verb, "--config", cfgPath}
		if verb != "list" {
			args = append(args, "tok")
		}
		err := runCreds(ctx, args, bytes.NewReader(nil), io.Discard, io.Discard)
		if err == nil {
			t.Errorf("creds %s with absent key: want error, got nil", verb)
			continue
		}
		if !strings.Contains(err.Error(), "key file missing") {
			t.Errorf("creds %s: error %q should mention 'key file missing'", verb, err.Error())
		}
	}
}

// F-116: absent key file over an EMPTY store must still auto-init (fresh-machine flow).
func TestRunCreds_AbsentKeyOverEmptyStoreAutoMints(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()
	var out, errOut bytes.Buffer

	// First put on a brand new store (no key file yet) must succeed.
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "newkey"},
		bytes.NewBufferString("val\n"), &out, &errOut); err != nil {
		t.Fatalf("put on empty store: %v", err)
	}
	if !strings.Contains(errOut.String(), "stored") {
		t.Errorf("put stderr = %q, want 'stored'", errOut.String())
	}
}

// F-116: creds init --force-new-key wipes the store and replaces the key file.
func TestRunCreds_InitForceNewKeyWipesStore(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()

	// Seed the store.
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "secret1"},
		bytes.NewBufferString("val\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Remove the key to simulate a lost key, then use --force-new-key.
	if err := os.Remove(filepath.Join(dir, "key")); err != nil {
		t.Fatalf("remove key: %v", err)
	}
	if err := runCreds(ctx, []string{"init", "--config", cfgPath, "--force-new-key", "--yes"},
		bytes.NewReader(nil), io.Discard, io.Discard); err != nil {
		t.Fatalf("init --force-new-key --yes: %v", err)
	}

	// Store should now be empty and accessible with the new key.
	var out bytes.Buffer
	if err := runCreds(ctx, []string{"list", "--config", cfgPath},
		bytes.NewReader(nil), &out, io.Discard); err != nil {
		t.Fatalf("list after init --force-new-key: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "" {
		t.Errorf("list after force-new-key: got %q, want empty", got)
	}
}

// F-127: --force-new-key without --yes must print a warning and refuse.
func TestRunCreds_ForceNewKeyRequiresYes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()

	// Seed the store so there is something to lose.
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "mykey"},
		bytes.NewBufferString("myval\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("put: %v", err)
	}

	var errOut bytes.Buffer
	err := runCreds(ctx, []string{"init", "--config", cfgPath, "--force-new-key"},
		bytes.NewReader(nil), io.Discard, &errOut)
	if err == nil {
		t.Fatal("--force-new-key without --yes: want error, got nil")
	}
	if !strings.Contains(errOut.String(), "--yes") {
		t.Errorf("stderr should mention --yes; got %q", errOut.String())
	}
	if !strings.Contains(errOut.String(), "WARNING") {
		t.Errorf("stderr should include WARNING; got %q", errOut.String())
	}

	// The original credential must still be readable (key was not replaced).
	var out bytes.Buffer
	if err := runCreds(ctx, []string{"get", "--config", cfgPath, "mykey"},
		bytes.NewReader(nil), &out, io.Discard); err != nil {
		t.Fatalf("get after refused force-new-key: %v", err)
	}
	if !strings.Contains(out.String(), "myval") {
		t.Errorf("credential should still be readable; got %q", out.String())
	}
}

// F-116: creds init without --force-new-key errors when key file already exists.
func TestRunCreds_InitRejectsExistingKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()

	// Seed so the key file is created.
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "k"},
		bytes.NewBufferString("v\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("put: %v", err)
	}

	// init without --force-new-key must error when key already exists.
	err := runCreds(ctx, []string{"init", "--config", cfgPath},
		bytes.NewReader(nil), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("want error when key file already exists, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error %q should mention 'already exists'", err.Error())
	}
}

// F-117: put over an undecryptable record must print a warning, not silently say "stored".
func TestRunCreds_PutOverUndecryptableWarns(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)
	ctx := context.Background()

	// Put a credential so a record exists.
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "tok"},
		bytes.NewBufferString("original\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("first put: %v", err)
	}

	// Replace the key file with a different random key so the stored record
	// is now undecryptable with the current key file.
	keyPath := filepath.Join(dir, "key")
	newKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, newKey); err != nil {
		t.Fatalf("rand: %v", err)
	}
	hexKey := make([]byte, 65)
	hex.Encode(hexKey[:64], newKey)
	hexKey[64] = '\n'
	if err := os.WriteFile(keyPath, hexKey, 0o600); err != nil {
		t.Fatalf("write new key: %v", err)
	}

	// Put the same name again. The existing record is undecryptable with the
	// new key; put should warn and report "replaced", not "stored".
	var errOut bytes.Buffer
	if err := runCreds(ctx, []string{"put", "--config", cfgPath, "tok"},
		bytes.NewBufferString("recovered\n"), io.Discard, &errOut); err != nil {
		t.Fatalf("second put: %v", err)
	}
	got := errOut.String()
	if !strings.Contains(got, "replaced") {
		t.Errorf("stderr = %q, want 'replaced'", got)
	}
	if !strings.Contains(got, "undecryptable") {
		t.Errorf("stderr = %q, want 'undecryptable' warning", got)
	}
	if strings.Contains(got, "stored") {
		t.Errorf("stderr = %q, must not say 'stored' when replacing undecryptable record", got)
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
