package memory

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func setupMetricWithHistory(t *testing.T, cat *Catalog, fq, unit, direction string, values []float64, windowDays []int) {
	t.Helper()
	ctx := context.Background()
	lastVal := values[len(values)-1]
	if err := cat.UpsertMetric(ctx, fq, unit, direction, "sum", &lastVal); err != nil {
		t.Fatalf("upsert metric %s: %v", fq, err)
	}
	now := time.Now().UTC()
	for i, v := range values {
		at := now.Add(-time.Duration(len(values)-i) * 24 * time.Hour)
		insertValueAt(t, cat, fq, v, at)
	}
	if len(windowDays) > 0 {
		if err := cat.ComputeBaselines(ctx, fq, windowDays); err != nil {
			t.Fatalf("compute baselines %s: %v", fq, err)
		}
	}
}

func TestCompareMetrics_pairwise(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fqA := "plausible.daily.visitors"
	fqB := "plausible.daily.pageviews"

	// A: trending up (higher_is_better) -- should be "improved"
	setupMetricWithHistory(t, cat, fqA, "visitors", "higher_is_better",
		[]float64{100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230},
		[]int{7, 14})

	// B: trending down (higher_is_better) -- should be "regressed"
	setupMetricWithHistory(t, cat, fqB, "pageviews", "higher_is_better",
		[]float64{500, 490, 480, 470, 460, 450, 440, 430, 420, 410, 400, 390, 380, 370},
		[]int{7, 14})

	d, err := cat.CompareMetrics(ctx, fqA, fqB, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("CompareMetrics: %v", err)
	}

	if d.A.MetricFQ != fqA {
		t.Errorf("A.MetricFQ: got %s, want %s", d.A.MetricFQ, fqA)
	}
	if d.B.MetricFQ != fqB {
		t.Errorf("B.MetricFQ: got %s, want %s", d.B.MetricFQ, fqB)
	}
	if !d.Diverged {
		t.Errorf("want Diverged=true, got false (verdict: %s)", d.Verdict)
	}
	if !strings.Contains(d.Verdict, "diverged") {
		t.Errorf("verdict should contain 'diverged', got: %s", d.Verdict)
	}
}

func TestCompareMetrics_bothImproved(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	for _, fq := range []string{"app.web.visitors", "app.web.signups"} {
		setupMetricWithHistory(t, cat, fq, "count", "higher_is_better",
			[]float64{50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180},
			[]int{7, 14})
	}

	d, err := cat.CompareMetrics(ctx, "app.web.visitors", "app.web.signups", 7*24*time.Hour)
	if err != nil {
		t.Fatalf("CompareMetrics: %v", err)
	}
	if d.Diverged {
		t.Errorf("want Diverged=false (both should improve), got true (verdict: %s)", d.Verdict)
	}
	if d.Verdict != "both improved" {
		t.Errorf("verdict: got %q, want %q", d.Verdict, "both improved")
	}
}

func TestCompareMetrics_notFound(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	_, err := cat.CompareMetrics(ctx, "never.seen.a", "never.seen.b", 7*24*time.Hour)
	if err == nil {
		t.Fatal("want error for unknown metrics, got nil")
	}
}

func TestCompareMetrics_sharedEvents(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fqA := "app.daily.visitors"
	fqB := "app.daily.signups"
	setupMetricWithHistory(t, cat, fqA, "visitors", "higher_is_better",
		[]float64{100, 110, 120, 130, 140, 150, 160}, []int{7})
	setupMetricWithHistory(t, cat, fqB, "signups", "higher_is_better",
		[]float64{10, 12, 14, 16, 18, 20, 22}, []int{7})

	// Insert a non-anomaly event (e.g. deploy note) into bm_events.
	// This should appear in both metrics' eventsInWindow results.
	now := time.Now().UTC()
	_, err := cat.db.Exec(
		`INSERT INTO bm_events (kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, description, at)
		 VALUES ('note', '', 0, 0, 0, 'neutral', 0, 'deployed v1.2', ?)`,
		now.Add(-2*24*time.Hour).UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insert note event: %v", err)
	}

	d, err := cat.CompareMetrics(ctx, fqA, fqB, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("CompareMetrics: %v", err)
	}
	if len(d.SharedEvents) != 1 {
		t.Errorf("want 1 shared event, got %d", len(d.SharedEvents))
	}
}

func TestComposePairwiseNarrative(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fqA := "plausible.daily.visitors"
	fqB := "plausible.daily.pageviews"
	setupMetricWithHistory(t, cat, fqA, "visitors", "higher_is_better",
		[]float64{100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230}, []int{7, 14})
	setupMetricWithHistory(t, cat, fqB, "pageviews", "higher_is_better",
		[]float64{100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230}, []int{7, 14})

	d, err := cat.CompareMetrics(ctx, fqA, fqB, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("CompareMetrics: %v", err)
	}

	text := ComposePairwiseNarrative(d)
	if !strings.Contains(text, "visitors") {
		t.Error("narrative should mention 'visitors'")
	}
	if !strings.Contains(text, "pageviews") {
		t.Error("narrative should mention 'pageviews'")
	}
	if !strings.Contains(text, "Verdict:") {
		t.Error("narrative should contain 'Verdict:'")
	}
	if !strings.Contains(text, "Summary:") {
		t.Error("narrative should contain 'Summary:'")
	}
}

func TestToCompareJSON(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fqA := "app.visitors"
	fqB := "app.signups"
	setupMetricWithHistory(t, cat, fqA, "visitors", "higher_is_better",
		[]float64{100, 110, 120, 130, 140, 150, 160}, []int{7})
	setupMetricWithHistory(t, cat, fqB, "signups", "higher_is_better",
		[]float64{10, 12, 14, 16, 18, 20, 22}, []int{7})

	d, err := cat.CompareMetrics(ctx, fqA, fqB, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("CompareMetrics: %v", err)
	}

	j := ToCompareJSON(d)
	if j.MetricA == nil || j.MetricB == nil {
		t.Fatal("want non-nil MetricA and MetricB")
	}
	if j.Since == "" {
		t.Error("want non-empty Since")
	}
	if j.Verdict == "" {
		t.Error("want non-empty Verdict")
	}
	if j.Summary == "" {
		t.Error("want non-empty Summary")
	}

	// round-trip through JSON
	b, err := json.Marshal(j)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	var out CompareJSON
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if out.Verdict != j.Verdict {
		t.Errorf("json round-trip verdict: got %s, want %s", out.Verdict, j.Verdict)
	}
}

func TestCompareMetricPeriods(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fq := "plausible.daily.visitors"
	// Insert 14 values in the "prior" window and 7 values in the "recent" window.
	// prior (14d ago to 7d ago): mean ~100. recent (7d ago to now): mean ~200.
	now := time.Now().UTC()
	for i := 14; i >= 8; i-- {
		insertValueAt(t, cat, fq, 100, now.Add(-time.Duration(i)*24*time.Hour))
	}
	for i := 7; i >= 1; i-- {
		insertValueAt(t, cat, fq, 200, now.Add(-time.Duration(i)*24*time.Hour))
	}
	if err := cat.UpsertMetric(ctx, fq, "visitors", "higher_is_better", "sum", ptr(200)); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	d, err := cat.CompareMetricPeriods(ctx, fq, 7*24*time.Hour, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("CompareMetricPeriods: %v", err)
	}

	if d.MetricFQ != fq {
		t.Errorf("MetricFQ: got %s, want %s", d.MetricFQ, fq)
	}
	if d.RecentSamples == 0 {
		t.Error("want non-zero RecentSamples")
	}
	if d.PriorSamples == 0 {
		t.Error("want non-zero PriorSamples")
	}
	if d.PctChange <= 0 {
		t.Errorf("want positive PctChange (recent > prior), got %.2f", d.PctChange)
	}
	if d.Verdict != "improved" {
		t.Errorf("verdict: got %q, want %q", d.Verdict, "improved")
	}
}

func TestCompareMetricPeriods_notFound(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	_, err := cat.CompareMetricPeriods(ctx, "never.seen", 7*24*time.Hour, 7*24*time.Hour)
	if err == nil {
		t.Fatal("want error for unknown metric, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("want 'not found' in error, got: %v", err)
	}
}

func TestComposePeriodOverPeriodNarrative(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fq := "app.daily.visitors"
	now := time.Now().UTC()
	for i := 14; i >= 8; i-- {
		insertValueAt(t, cat, fq, 100, now.Add(-time.Duration(i)*24*time.Hour))
	}
	for i := 7; i >= 1; i-- {
		insertValueAt(t, cat, fq, 200, now.Add(-time.Duration(i)*24*time.Hour))
	}
	if err := cat.UpsertMetric(ctx, fq, "visitors", "higher_is_better", "sum", ptr(200)); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	d, err := cat.CompareMetricPeriods(ctx, fq, 7*24*time.Hour, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("CompareMetricPeriods: %v", err)
	}

	text := ComposePeriodOverPeriodNarrative(d)
	if !strings.Contains(text, "visitors") {
		t.Error("narrative should mention 'visitors'")
	}
	if !strings.Contains(text, "Verdict:") {
		t.Error("narrative should contain 'Verdict:'")
	}
	if !strings.Contains(text, "vs prior") {
		t.Error("narrative should contain 'vs prior'")
	}
	if !strings.Contains(text, "Summary:") {
		t.Error("narrative should contain 'Summary:'")
	}
}

func TestToPeriodOverPeriodJSON(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fq := "app.daily.signups"
	now := time.Now().UTC()
	for i := 14; i >= 8; i-- {
		insertValueAt(t, cat, fq, 50, now.Add(-time.Duration(i)*24*time.Hour))
	}
	for i := 7; i >= 1; i-- {
		insertValueAt(t, cat, fq, 30, now.Add(-time.Duration(i)*24*time.Hour))
	}
	if err := cat.UpsertMetric(ctx, fq, "signups", "higher_is_better", "sum", ptr(30)); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	d, err := cat.CompareMetricPeriods(ctx, fq, 7*24*time.Hour, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("CompareMetricPeriods: %v", err)
	}

	j := ToPeriodOverPeriodJSON(d)
	if j.MetricFQ != fq {
		t.Errorf("MetricFQ: got %s, want %s", j.MetricFQ, fq)
	}
	if j.Verdict != "regressed" {
		t.Errorf("verdict: got %q, want 'regressed'", j.Verdict)
	}
	if j.PctChange >= 0 {
		t.Errorf("want negative PctChange, got %.2f", j.PctChange)
	}

	b, err := json.Marshal(j)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	var out PeriodOverPeriodJSON
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if out.Verdict != j.Verdict {
		t.Errorf("json round-trip verdict: got %s, want %s", out.Verdict, j.Verdict)
	}
}
