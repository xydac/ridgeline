package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// buildRidgeline compiles the binary into t.TempDir and returns the path.
// Building once per subtest keeps each test hermetic and parallelisable.
func buildRidgeline(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "ridgeline")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Stderr = &bytes.Buffer{}
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build: %v\n%s", err, cmd.Stderr.(*bytes.Buffer).String())
	}
	return bin
}

// TestResolveVersion covers the ldflags-vs-module-version fallback logic.
func TestResolveVersion(t *testing.T) {
	cases := []struct {
		ldflags string
		module  string
		want    string
	}{
		// ldflags already injected -- wins regardless of module version
		{"v0.1.5", "(devel)", "v0.1.5"},
		{"v0.1.5", "v0.1.5", "v0.1.5"},
		{"v0.1.5", "", "v0.1.5"},
		// dev placeholder + workspace build -- stays placeholder
		{"0.0.0-dev", "(devel)", "0.0.0-dev"},
		{"0.0.0-dev", "", "0.0.0-dev"},
		// dev placeholder + go install path -- falls back to module version
		{"0.0.0-dev", "v0.1.5", "v0.1.5"},
		{"0.0.0-dev", "v0.2.0-rc1", "v0.2.0-rc1"},
	}
	for _, tc := range cases {
		got := resolveVersion(tc.ldflags, tc.module)
		if got != tc.want {
			t.Errorf("resolveVersion(%q, %q) = %q, want %q", tc.ldflags, tc.module, got, tc.want)
		}
	}
}

func TestCLI_HelpFlags(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	for _, arg := range []string{"help", "--help", "-h"} {
		arg := arg
		t.Run(arg, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(bin, arg)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%s: exit %v\n%s", arg, err, out)
			}
			if !strings.Contains(string(out), "Usage:") {
				t.Errorf("%s: usage missing from output:\n%s", arg, out)
			}
			if !strings.Contains(string(out), "ridgeline sync") {
				t.Errorf("%s: sync command missing from output:\n%s", arg, out)
			}
		})
	}
}

func TestCLI_NoArgs_ExitsTwoWithUsage(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cmd := exec.Command(bin)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 {
		t.Fatalf("expected exit 2, got: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "Usage:") {
		t.Errorf("usage missing from output: %s", out)
	}
}

func TestCLI_Version_RejectsExtraArgs(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cmd := exec.Command(bin, "version", "--extra-flag", "foo")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit, got: %s", out)
	}
	if !strings.Contains(string(out), "unexpected argument") {
		t.Errorf("output missing 'unexpected argument': %s", out)
	}
}

func TestCLI_Version_BareSucceeds(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	out, err := exec.Command(bin, "version").CombinedOutput()
	if err != nil {
		t.Fatalf("version: %v\n%s", err, out)
	}
	if strings.TrimSpace(string(out)) == "" {
		t.Error("version printed empty output")
	}
}

// TestCLI_Version_LdflagsInjectionWins verifies that a binary built with an
// explicit -ldflags version reports that version, not the module fallback.
func TestCLI_Version_LdflagsInjectionWins(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bin := filepath.Join(dir, "ridgeline-ldflag")
	cmd := exec.Command("go", "build", "-ldflags", "-X main.Version=v9.8.7-test", "-o", bin, ".")
	cmd.Stderr = &bytes.Buffer{}
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build: %v\n%s", err, cmd.Stderr.(*bytes.Buffer).String())
	}
	out, err := exec.Command(bin, "version").CombinedOutput()
	if err != nil {
		t.Fatalf("version: %v\n%s", err, out)
	}
	if strings.TrimSpace(string(out)) != "v9.8.7-test" {
		t.Errorf("got %q, want %q", strings.TrimSpace(string(out)), "v9.8.7-test")
	}
}

func TestCLI_Version_HelpFlags(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	for _, arg := range []string{"--help", "-h", "help"} {
		arg := arg
		t.Run(arg, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(bin, "version", arg)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("version %s: exit %v\n%s", arg, err, out)
			}
			if !strings.Contains(string(out), "Usage: ridgeline version") {
				t.Errorf("version %s: usage missing: %s", arg, out)
			}
		})
	}
}

func TestCLI_SubcommandHelpFlags_ExitZero(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cases := [][]string{
		{"sync", "--help"},
		{"sync", "-h"},
		{"status", "--help"},
		{"query", "--help"},
		{"creds", "list", "--help"},
		{"creds", "put", "--help"},
		{"tui", "--help"},
		{"version", "--help"},
	}
	for _, args := range cases {
		args := args
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Parallel()
			var stdout, stderr strings.Builder
			cmd := exec.Command(bin, args...)
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			err := cmd.Run()
			if err != nil {
				t.Fatalf("%v: expected exit 0, got %v\nstdout: %s\nstderr: %s", args, err, stdout.String(), stderr.String())
			}
			got := stdout.String()
			if strings.Contains(stderr.String(), "flag: help requested") || strings.Contains(got, "flag: help requested") {
				t.Errorf("%v: leaked 'flag: help requested': stdout=%s stderr=%s", args, got, stderr.String())
			}
			if !strings.Contains(got, "Usage: ridgeline") {
				t.Errorf("%v: stdout missing 'Usage: ridgeline': %s", args, got)
			}
			if stderr.String() != "" {
				t.Errorf("%v: --help wrote to stderr: %s", args, stderr.String())
			}
		})
	}
}

func TestCLI_SyncExtras_Rejected(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cmd := exec.Command(bin, "sync", "--dry-run", "surprise")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit, got: %s", out)
	}
	if !strings.Contains(string(out), "unexpected argument") {
		t.Errorf("output missing 'unexpected argument': %s", out)
	}
}

func TestCLI_StatusExtras_Rejected(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cmd := exec.Command(bin, "status", "--config", "nonexistent.yaml", "extra")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit, got: %s", out)
	}
	if !strings.Contains(string(out), "unexpected argument") {
		t.Errorf("output missing 'unexpected argument': %s", out)
	}
}

func TestCLI_UnknownSubcommand_PointsAtHelp(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cmd := exec.Command(bin, "frobnicate")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected non-zero exit for unknown subcommand")
	}
	got := string(out)
	if !strings.Contains(got, "unknown subcommand") {
		t.Errorf("output missing 'unknown subcommand': %s", got)
	}
	if !strings.Contains(got, "--help") {
		t.Errorf("output should mention --help: %s", got)
	}
}

// TestCLI_MisinvocationExitCodes asserts that every form of "you invoked
// me wrong" exits 2, not 0 or 1. Covers F-079 and F-087.
func TestCLI_MisinvocationExitCodes(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cases := []struct {
		desc string
		args []string
	}{
		// verb/dispatch errors
		{"bare ridgeline", []string{}},
		{"creds with no verb", []string{"creds"}},
		{"unknown creds verb", []string{"creds", "bogus"}},
		{"unknown top-level command", []string{"frobnicate"}},
		// flag-parse errors (previously exited 1 - F-087)
		{"sync unknown flag", []string{"sync", "--nope"}},
		{"status unknown flag", []string{"status", "--nope"}},
		{"creds put unknown flag", []string{"creds", "put", "--nope", "NAME"}},
		{"serve unknown flag", []string{"serve", "--nope"}},
		{"tui unknown flag", []string{"tui", "--nope"}},
		// required-arg errors (previously exited 1 - F-087)
		{"status missing required config", []string{"status"}},
		// query arg-count errors (previously exited 1 - F-087)
		{"query no args", []string{"query"}},
		{"query multiple positional args", []string{"query", "SELECT", "1"}},
		// query unknown flag (F-082)
		{"query unknown flag", []string{"query", "--write-mode", "SELECT 1"}},
		{"query typo of --write", []string{"query", "--writ", "SELECT 1"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(bin, tc.args...)
			err := cmd.Run()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("%s: expected non-zero exit, got nil", tc.desc)
			}
			if exitErr.ExitCode() != 2 {
				t.Errorf("%s: got exit %d, want 2", tc.desc, exitErr.ExitCode())
			}
		})
	}
}

// TestCLI_SyncPartialFailureExitsThree asserts that --continue-on-error with
// at least one success and at least one runtime failure exits 3 (not 2 - F-087).
func TestCLI_SyncPartialFailureExitsThree(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	dir := t.TempDir()
	// Place a regular file where the second connector's sink expects a directory.
	// The first connector succeeds; the second fails at sink init (runtime error).
	blockPath := filepath.Join(dir, "blocked")
	if err := os.WriteFile(blockPath, []byte("not a dir"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := `version: 1
state_path: ` + dir + `/state.db
products:
  myapp:
    connectors:
      - name: good
        type: testsrc
        config: {records: 1}
        streams: [pages]
        sink: {type: jsonl, options: {dir: ` + dir + `/out}}
      - name: bad
        type: testsrc
        config: {records: 1}
        streams: [pages]
        sink: {type: jsonl, options: {dir: ` + blockPath + `}}
`
	cfgFile := dir + "/test.yaml"
	if err := os.WriteFile(cfgFile, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "sync", "--config", cfgFile, "--continue-on-error")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected non-zero exit, got nil; output: %s", out)
	}
	if exitErr.ExitCode() != 3 {
		t.Errorf("got exit %d, want 3 (partial failure); output: %s", exitErr.ExitCode(), out)
	}
}

// TestCLI_DuplicateConnectorOrdinal1Based asserts that the error message
// for a duplicate connector name uses 1-based list indices. Covers F-074.
func TestCLI_DuplicateConnectorOrdinal1Based(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	// Three connectors; the 3rd (index 2 in Go, but #3 to the user) duplicates
	// the name of the 1st. The error should say "#3", not "#2".
	cfg := `version: 1
state_path: /tmp/dup3test.db
products:
  myapp:
    connectors:
      - {name: a, type: testsrc, config: {records: 1}, streams: [pages], sink: {type: jsonl, options: {dir: /tmp/o1}}}
      - {name: b, type: testsrc, config: {records: 1}, streams: [pages], sink: {type: jsonl, options: {dir: /tmp/o2}}}
      - {name: a, type: testsrc, config: {records: 1}, streams: [pages], sink: {type: jsonl, options: {dir: /tmp/o3}}}
`
	tmpFile := filepath.Join(t.TempDir(), "dup3.yaml")
	if err := os.WriteFile(tmpFile, []byte(cfg), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "sync", "--config", tmpFile)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected non-zero exit for duplicate connector name")
	}
	if !strings.Contains(string(out), "#3") {
		t.Errorf("expected 1-based ordinal (#3) in error output; got: %s", out)
	}
	if strings.Contains(string(out), "#2") {
		t.Errorf("got 0-based ordinal (#2) in error output; want #3: %s", out)
	}
}
