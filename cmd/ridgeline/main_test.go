package main

import (
	"bytes"
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

func TestCLI_NoArgs_PrintsUsage(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cmd := exec.Command(bin)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("exit %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "Usage:") {
		t.Errorf("usage missing: %s", out)
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
	cmd := exec.Command(bin, "version")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("version: %v\n%s", err, out)
	}
	if strings.TrimSpace(string(out)) != Version {
		t.Errorf("got %q, want %q", string(out), Version)
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
