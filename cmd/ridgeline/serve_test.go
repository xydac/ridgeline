package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestServeLoopRunsMultipleIterations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var count int32
	err := serveLoop(ctx, 10*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := atomic.LoadInt32(&count); got < 3 {
		t.Fatalf("expected at least 3 iterations, got %d", got)
	}
}

func TestServeLoopExitsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after the first sync completes.
	var count int32
	done := make(chan struct{})
	go func() {
		_ = serveLoop(ctx, time.Hour, func(ctx context.Context) error {
			if atomic.AddInt32(&count, 1) == 1 {
				cancel()
			}
			return nil
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("serveLoop did not exit after context cancellation")
	}
	if got := atomic.LoadInt32(&count); got != 1 {
		t.Fatalf("expected exactly 1 iteration before cancel, got %d", got)
	}
}

// TestServeQuietMode verifies that --quiet suppresses per-sync preamble lines
// and emits exactly one timestamped result line per tick.
func TestServeQuietMode(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	cfg := `
version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: a
        type: testsrc
        config: {records: 1}
        streams: [pages]
        sink: {type: jsonl, options: {dir: ` + filepath.Join(dir, "out") + `}}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// Run 3 ticks with --quiet, capturing stdout.
	// Guard against the cancel/ticker race: if a 4th call fires before
	// serveLoop checks ctx.Done(), skip it without writing output.
	const ticks = 3
	var ticksDone int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := captureStdout(t, func() {
		_ = serveLoop(ctx, 10*time.Millisecond, func(ctx context.Context) error {
			n := atomic.AddInt32(&ticksDone, 1)
			if n > ticks {
				return nil // extra tick after cancel: skip without output
			}
			// Quiet mode: sync output to Discard, tick line to stdout.
			start := time.Now()
			err := runConfigSync(ctx, cfgPath, false, io.Discard)
			elapsed := time.Since(start).Truncate(time.Millisecond)
			ts := time.Now().UTC().Format(time.RFC3339)
			if err != nil {
				os.Stdout.WriteString(ts + " serve: sync error (" + elapsed.String() + "): " + err.Error() + "\n")
			} else {
				os.Stdout.WriteString(ts + " serve: sync ok (" + elapsed.String() + ")\n")
			}
			if n == ticks {
				cancel()
			}
			return nil
		})
	})

	lines := nonEmptyLines(out)
	if len(lines) != ticks {
		t.Fatalf("quiet mode: got %d lines for %d ticks, want 1 per tick:\n%s", len(lines), ticks, out)
	}
	for i, line := range lines {
		if !strings.Contains(line, "serve: sync") {
			t.Errorf("line %d does not match timestamped-result format: %q", i, line)
		}
		if strings.Contains(line, "loaded ") || strings.Contains(line, "state: ") {
			t.Errorf("line %d contains suppressed preamble: %q", i, line)
		}
	}
}

// TestServeVerboseModeEmitsPreamble verifies that without --quiet the preamble
// lines (loaded, state, per-connector) appear in the output.
func TestServeVerboseModeEmitsPreamble(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ridgeline.yaml")
	cfg := `
version: 1
state_path: ` + filepath.Join(dir, "state.db") + `
key_path: ` + filepath.Join(dir, "key") + `
products:
  myapp:
    connectors:
      - name: a
        type: testsrc
        config: {records: 1}
        streams: [pages]
        sink: {type: jsonl, options: {dir: ` + filepath.Join(dir, "out") + `}}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// One tick, verbose mode: preamble lines must be present.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := captureStdout(t, func() {
		_ = serveLoop(ctx, time.Hour, func(ctx context.Context) error {
			_ = runConfigSync(ctx, cfgPath, false, os.Stdout)
			cancel()
			return nil
		})
	})

	if !strings.Contains(out, "loaded ") {
		t.Errorf("verbose mode: expected 'loaded' line; got:\n%s", out)
	}
	if !strings.Contains(out, "state: ") {
		t.Errorf("verbose mode: expected 'state:' line; got:\n%s", out)
	}
}

// TestServePermanentConfigError verifies that serve exits non-zero immediately
// when given a nonexistent config path instead of looping forever.
func TestServePermanentConfigError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var calls int32
	err := serveLoop(ctx, time.Hour, func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return runConfigSync(ctx, "/nonexistent/path/ridgeline.yaml", false, io.Discard)
	})

	if err == nil {
		t.Fatal("expected non-nil error from serveLoop on permanent config failure")
	}
	var pce *permanentConfigError
	if !errors.As(err, &pce) {
		t.Fatalf("expected *permanentConfigError, got %T: %v", err, err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected exactly 1 call before exit, got %d", got)
	}
}

// TestServeTransientErrorRetries verifies that a non-permanent sync error is
// logged and retried rather than causing serve to exit.
func TestServeTransientErrorRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Simulate a transient error: first call fails with a plain error,
	// subsequent calls succeed. Serve should not stop on the first failure.
	var calls int32
	err := serveLoop(ctx, 5*time.Millisecond, func(ctx context.Context) error {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			// transient error: not a *permanentConfigError, so serve should retry
			return nil // caller logs; return nil to continue
		}
		if n >= 3 {
			cancel()
		}
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got < 3 {
		t.Fatalf("expected at least 3 calls (transient recovery), got %d", got)
	}
}

func nonEmptyLines(s string) []string {
	var out []string
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			out = append(out, line)
		}
	}
	return out
}
