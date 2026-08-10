package memory

import (
	"context"
	"testing"
	"time"
)

func seedBaseline(t *testing.T, cat *Catalog, fqName string, windowDays int, mean, stddev float64, sampleCount int) {
	t.Helper()
	ctx := context.Background()
	_, err := cat.db.ExecContext(ctx, `
INSERT INTO bm_baselines (fq_name, window_days, mean, stddev, min, max, sample_count, last_computed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fq_name, window_days) DO UPDATE SET
    mean=excluded.mean, stddev=excluded.stddev, min=excluded.min,
    max=excluded.max, sample_count=excluded.sample_count, last_computed_at=excluded.last_computed_at`,
		fqName, windowDays, mean, stddev, mean-stddev, mean+stddev, sampleCount, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
}

func seedMetric(t *testing.T, cat *Catalog, fqName, direction string) {
	t.Helper()
	ctx := context.Background()
	_, err := cat.db.ExecContext(ctx, `
INSERT OR IGNORE INTO bm_metrics (fq_name, unit, direction, aggregation, updated_at)
VALUES (?, '', ?, 'last', ?)`,
		fqName, direction, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("seed metric: %v", err)
	}
}

func TestDetectAnomalies_Triggers(t *testing.T) {
	cat := openTestCatalog(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seedMetric(t, cat, "myapp.daily.revenue", "higher_is_better")
	// baseline: mean=100, stddev=10, 20 samples
	seedBaseline(t, cat, "myapp.daily.revenue", 7, 100, 10, 20)

	// value = 70: deviation = (70-100)/10 = -3.0 (exceeds k=2.5)
	if err := cat.DetectAndRecordAnomalies(ctx, "myapp.daily.revenue", 70, now, DefaultAnomalyK, DefaultMinSamples); err != nil {
		t.Fatalf("detect: %v", err)
	}

	events, err := cat.ListEvents(ctx, 0)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	e := events[0]
	if e.MetricFQ != "myapp.daily.revenue" {
		t.Errorf("want metric myapp.daily.revenue, got %q", e.MetricFQ)
	}
	if e.Direction != "surprise-bad" {
		t.Errorf("want surprise-bad (revenue dropped), got %q", e.Direction)
	}
	if e.WindowDays != 7 {
		t.Errorf("want window 7, got %d", e.WindowDays)
	}
	if e.Kind != "anomaly" {
		t.Errorf("want kind anomaly, got %q", e.Kind)
	}
}

func TestDetectAnomalies_SurpriseGood(t *testing.T) {
	cat := openTestCatalog(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seedMetric(t, cat, "myapp.daily.signups", "higher_is_better")
	seedBaseline(t, cat, "myapp.daily.signups", 30, 50, 5, 30)

	// value = 75: deviation = (75-50)/5 = 5.0 -- spike
	if err := cat.DetectAndRecordAnomalies(ctx, "myapp.daily.signups", 75, now, DefaultAnomalyK, DefaultMinSamples); err != nil {
		t.Fatalf("detect: %v", err)
	}

	events, err := cat.ListEvents(ctx, 0)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected event for large spike")
	}
	if events[0].Direction != "surprise-good" {
		t.Errorf("want surprise-good (signups spiked), got %q", events[0].Direction)
	}
}

func TestDetectAnomalies_BelowThreshold(t *testing.T) {
	cat := openTestCatalog(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seedMetric(t, cat, "myapp.daily.visitors", "higher_is_better")
	seedBaseline(t, cat, "myapp.daily.visitors", 7, 100, 10, 20)

	// value = 95: deviation = (95-100)/10 = -0.5 -- within threshold
	if err := cat.DetectAndRecordAnomalies(ctx, "myapp.daily.visitors", 95, now, DefaultAnomalyK, DefaultMinSamples); err != nil {
		t.Fatalf("detect: %v", err)
	}

	events, err := cat.ListEvents(ctx, 0)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("want 0 events for in-range value, got %d", len(events))
	}
}

func TestDetectAnomalies_MinSamplesGate(t *testing.T) {
	cat := openTestCatalog(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seedMetric(t, cat, "myapp.daily.errors", "lower_is_better")
	// only 5 samples -- below minSamples=14
	seedBaseline(t, cat, "myapp.daily.errors", 7, 10, 2, 5)

	// large deviation but should be suppressed
	if err := cat.DetectAndRecordAnomalies(ctx, "myapp.daily.errors", 50, now, DefaultAnomalyK, DefaultMinSamples); err != nil {
		t.Fatalf("detect: %v", err)
	}

	events, err := cat.ListEvents(ctx, 0)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("want 0 events (insufficient samples), got %d", len(events))
	}
}

func TestDetectAnomalies_ZeroStddevSkipped(t *testing.T) {
	cat := openTestCatalog(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seedMetric(t, cat, "myapp.daily.constant", "neutral")
	// stddev=0 (all values identical)
	seedBaseline(t, cat, "myapp.daily.constant", 7, 42, 0, 20)

	if err := cat.DetectAndRecordAnomalies(ctx, "myapp.daily.constant", 100, now, DefaultAnomalyK, DefaultMinSamples); err != nil {
		t.Fatalf("detect: %v", err)
	}

	events, err := cat.ListEvents(ctx, 0)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("want 0 events (stddev=0 skipped), got %d", len(events))
	}
}

func TestDetectAnomalies_Idempotent(t *testing.T) {
	cat := openTestCatalog(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seedMetric(t, cat, "myapp.daily.revenue", "higher_is_better")
	seedBaseline(t, cat, "myapp.daily.revenue", 7, 100, 10, 20)

	for range 3 {
		if err := cat.DetectAndRecordAnomalies(ctx, "myapp.daily.revenue", 60, now, DefaultAnomalyK, DefaultMinSamples); err != nil {
			t.Fatalf("detect: %v", err)
		}
	}

	events, err := cat.ListEvents(ctx, 0)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event (idempotent), got %d", len(events))
	}
}

func TestDetectAnomalies_LowerIsBetter(t *testing.T) {
	cat := openTestCatalog(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seedMetric(t, cat, "myapp.daily.latency_ms", "lower_is_better")
	seedBaseline(t, cat, "myapp.daily.latency_ms", 7, 50, 5, 20)

	// latency drop: surprise-good
	if err := cat.DetectAndRecordAnomalies(ctx, "myapp.daily.latency_ms", 20, now, DefaultAnomalyK, DefaultMinSamples); err != nil {
		t.Fatalf("detect: %v", err)
	}

	events, err := cat.ListEvents(ctx, 0)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[0].Direction != "surprise-good" {
		t.Errorf("want surprise-good (latency dropped), got %q", events[0].Direction)
	}
}

func TestListEvents_SinceFilter(t *testing.T) {
	cat := openTestCatalog(t)
	ctx := context.Background()

	seedMetric(t, cat, "myapp.daily.revenue", "higher_is_better")
	seedBaseline(t, cat, "myapp.daily.revenue", 7, 100, 10, 20)

	oldTime := time.Now().UTC().Add(-48 * time.Hour)
	recentTime := time.Now().UTC()

	// insert old event
	if err := cat.DetectAndRecordAnomalies(ctx, "myapp.daily.revenue", 50, oldTime, DefaultAnomalyK, DefaultMinSamples); err != nil {
		t.Fatalf("detect old: %v", err)
	}
	// insert recent event with a different timestamp
	if err := cat.DetectAndRecordAnomalies(ctx, "myapp.daily.revenue", 50, recentTime, DefaultAnomalyK, DefaultMinSamples); err != nil {
		t.Fatalf("detect recent: %v", err)
	}

	// filter to last 24h -- should get only the recent event
	events, err := cat.ListEvents(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event in last 24h, got %d", len(events))
	}
}
