package main

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"testing"
)

func TestRunQueryExecutesSimpleSelect(t *testing.T) {
	var buf bytes.Buffer
	err := runQuery(context.Background(), []string{"SELECT 1 AS one"}, &buf)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if !strings.Contains(buf.String(), "one") || !strings.Contains(buf.String(), "(1 row)") {
		t.Errorf("unexpected output:\n%s", buf.String())
	}
}

func TestRunQueryJoinsMultiplePositionalArgs(t *testing.T) {
	var buf bytes.Buffer
	// Simulate an unquoted shell invocation: "ridgeline query SELECT 1".
	err := runQuery(context.Background(), []string{"SELECT", "1", "AS", "n"}, &buf)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if !strings.Contains(buf.String(), "n") {
		t.Errorf("expected column n in output:\n%s", buf.String())
	}
}

func TestRunQueryRejectsNoArgs(t *testing.T) {
	var buf bytes.Buffer
	err := runQuery(context.Background(), nil, &buf)
	if err == nil {
		t.Fatal("expected error for empty args")
	}
	if !strings.Contains(err.Error(), "usage") {
		t.Errorf("error should include usage hint, got %q", err.Error())
	}
}

func TestCLI_Query_SimpleSelect(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cmd := exec.Command(bin, "query", "SELECT 7 AS lucky")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("query: %v\n%s", err, out)
	}
	got := string(out)
	for _, want := range []string{"lucky", "7", "(1 row)"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
}

func TestCLI_Query_MissingArgsExits1(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cmd := exec.Command(bin, "query")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit, got:\n%s", out)
	}
	if !strings.Contains(string(out), "usage") {
		t.Errorf("output should include usage hint, got:\n%s", out)
	}
}
