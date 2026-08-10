package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestRunMemory_NoSubcommand(t *testing.T) {
	t.Parallel()
	err := runMemory(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error for missing subcommand")
	}
}

func TestRunMemory_UnknownSubcommand(t *testing.T) {
	t.Parallel()
	err := runMemory(context.Background(), []string{"bogus"}, nil)
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestRunMemoryStreams_RequiresConfig(t *testing.T) {
	t.Parallel()
	err := runMemoryStreams(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error when --config is missing")
	}
	if !strings.Contains(err.Error(), "--config") {
		t.Errorf("error should mention --config, got %q", err.Error())
	}
}

func TestRunMemoryMetrics_RequiresConfig(t *testing.T) {
	t.Parallel()
	err := runMemoryMetrics(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error when --config is missing")
	}
}

func TestRunMemoryStreams_AfterSync(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/state.db"
	outDir := dir + "/out"
	cfgPath := configFixture(t, dir, dbPath, outDir)

	// Run sync to populate the catalog.
	if err := runSync(context.Background(), []string{"--config", cfgPath}); err != nil {
		t.Fatalf("sync: %v", err)
	}

	out := captureStdout(t, func() {
		if err := runMemoryStreams(context.Background(), []string{"--config", cfgPath}, os.Stdout); err != nil {
			t.Errorf("memory streams: %v", err)
		}
	})

	// testsrc has two streams: pages and events.
	if !strings.Contains(out, "pages") {
		t.Errorf("output should mention 'pages', got:\n%s", out)
	}
	if !strings.Contains(out, "events") {
		t.Errorf("output should mention 'events', got:\n%s", out)
	}
	if !strings.Contains(out, "testsrc") {
		t.Errorf("output should mention 'testsrc' connector, got:\n%s", out)
	}
	// first_seen_at and last_seen_at should appear (RFC3339 timestamps).
	if !strings.Contains(out, "FIRST SEEN") {
		t.Errorf("output should have FIRST SEEN header, got:\n%s", out)
	}
}

func TestRunMemoryStreams_TwoSyncs_LifetimeAccumulates(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/state.db"
	outDir := dir + "/out"
	cfgPath := configFixture(t, dir, dbPath, outDir)

	// Run sync twice; each run emits 2 records per stream.
	for range 2 {
		if err := runSync(context.Background(), []string{"--config", cfgPath}); err != nil {
			t.Fatalf("sync: %v", err)
		}
	}

	out := captureStdout(t, func() {
		if err := runMemoryStreams(context.Background(), []string{"--config", cfgPath}, os.Stdout); err != nil {
			t.Errorf("memory streams: %v", err)
		}
	})

	// testsrc emits 2 records per stream; two runs = 4 lifetime.
	if !strings.Contains(out, "4") {
		t.Errorf("output should show lifetime row count of 4, got:\n%s", out)
	}
}

func TestRunMemoryMetrics_EmptyAfterTestSrc(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/state.db"
	outDir := dir + "/out"
	cfgPath := configFixture(t, dir, dbPath, outDir)

	if err := runSync(context.Background(), []string{"--config", cfgPath}); err != nil {
		t.Fatalf("sync: %v", err)
	}

	out := captureStdout(t, func() {
		if err := runMemoryMetrics(context.Background(), []string{"--config", cfgPath}, os.Stdout); err != nil {
			t.Errorf("memory metrics: %v", err)
		}
	})

	// testsrc has no semantic columns, so the catalog should be empty.
	if !strings.Contains(out, "No metrics") {
		t.Errorf("expected 'No metrics' message for testsrc, got:\n%s", out)
	}
}

func TestRunMemoryBaselines_RequiresConfig(t *testing.T) {
	t.Parallel()
	err := runMemoryBaselines(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error when --config missing")
	}
}

func TestRunMemoryBaselines_RequiresMetricArg(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := dir + "/state.db"
	outDir := dir + "/out"
	cfgPath := configFixture(t, dir, dbPath, outDir)
	err := runMemoryBaselines(context.Background(), []string{"--config", cfgPath}, os.Stdout)
	if err == nil {
		t.Fatal("expected error when metric name missing")
	}
}

func TestRunMemoryBaselines_NoData(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/state.db"
	outDir := dir + "/out"
	cfgPath := configFixture(t, dir, dbPath, outDir)

	out := captureStdout(t, func() {
		if err := runMemoryBaselines(context.Background(), []string{"--config", cfgPath, "app.unknown.metric"}, os.Stdout); err != nil {
			t.Errorf("memory baselines: %v", err)
		}
	})
	if !strings.Contains(out, "No baselines") {
		t.Errorf("expected 'No baselines' message, got:\n%s", out)
	}
}

func TestRunMemoryRecompute_RequiresConfig(t *testing.T) {
	t.Parallel()
	err := runMemoryRecompute(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error when --config missing")
	}
}

func TestRunMemoryRecompute_EmptyDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/state.db"
	outDir := dir + "/out"
	cfgPath := configFixture(t, dir, dbPath, outDir)

	out := captureStdout(t, func() {
		if err := runMemoryRecompute(context.Background(), []string{"--config", cfgPath}, os.Stdout); err != nil {
			t.Errorf("memory recompute: %v", err)
		}
	})
	if !strings.Contains(out, "recomputed") {
		t.Errorf("expected 'recomputed' in output, got:\n%s", out)
	}
}

func TestRunMemoryEvents_RequiresConfig(t *testing.T) {
	t.Parallel()
	err := runMemoryEvents(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error when --config missing")
	}
}

func TestRunMemoryEvents_NoData(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/state.db"
	outDir := dir + "/out"
	cfgPath := configFixture(t, dir, dbPath, outDir)

	out := captureStdout(t, func() {
		if err := runMemoryEvents(context.Background(), []string{"--config", cfgPath, "--since", "7d"}, os.Stdout); err != nil {
			t.Errorf("memory events: %v", err)
		}
	})
	if !strings.Contains(out, "No anomaly events") {
		t.Errorf("expected 'No anomaly events' message, got:\n%s", out)
	}
}
