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

func TestRunQueryRejectsMultiplePositionalArgs(t *testing.T) {
	var buf bytes.Buffer
	// Multiple positional args are rejected to prevent ambiguous SQL joins.
	// Users must quote the whole statement: ridgeline query "SELECT 1 AS n".
	err := runQuery(context.Background(), []string{"SELECT", "1", "AS", "n"}, &buf)
	if err == nil {
		t.Fatal("expected error for multiple positional args, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "single quoted") {
		t.Errorf("error should mention single-quoted argument, got %q", err.Error())
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

// TestRunQuery_LineCommentSQL verifies that SQL beginning with a -- line
// comment is accepted and executed (F-055). Previously the flag parser
// misinterpreted the leading -- as a flag name.
func TestRunQuery_LineCommentSQL(t *testing.T) {
	var buf bytes.Buffer
	err := runQuery(context.Background(), []string{"-- find the answer\nSELECT 42 AS answer"}, &buf)
	if err != nil {
		t.Fatalf("runQuery with line-comment SQL: %v", err)
	}
	if !strings.Contains(buf.String(), "42") {
		t.Errorf("expected 42 in output, got:\n%s", buf.String())
	}
}

// TestRunQuery_EndOfFlagsSentinel verifies that -- stops flag parsing so
// that subsequent args (including SQL beginning with --) are treated as
// positional args.
func TestRunQuery_EndOfFlagsSentinel(t *testing.T) {
	var buf bytes.Buffer
	err := runQuery(context.Background(), []string{"--", "SELECT 7 AS n"}, &buf)
	if err != nil {
		t.Fatalf("runQuery after -- sentinel: %v", err)
	}
	if !strings.Contains(buf.String(), "7") {
		t.Errorf("expected 7 in output, got:\n%s", buf.String())
	}
}

// TestCLI_Query_LineCommentSQL verifies the binary accepts -- leading SQL.
func TestCLI_Query_LineCommentSQL(t *testing.T) {
	t.Parallel()
	bin := buildRidgeline(t)
	cmd := exec.Command(bin, "query", "-- the answer\nSELECT 42 AS answer")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("query with line-comment SQL failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "42") {
		t.Errorf("expected 42 in output, got:\n%s", out)
	}
}
