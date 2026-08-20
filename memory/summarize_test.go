package memory

import (
	"context"
	"testing"
	"time"
)

// TestSummarizeAll_ranking verifies that directionality-adjusted ranking places
// surprise-bad events above surprise-good events of smaller magnitude.
//
// Setup:
//
//	"app.revenue"  (higher_is_better): current = baseline_mean - 3*stddev  => score +3 (bad)
//	"app.errors"   (lower_is_better):  current = baseline_mean + 2*stddev   => score +2 (bad)
//	"app.uptime"   (higher_is_better): current = baseline_mean + 4*stddev   => score -4 (good)
//
// Expected ranking: revenue (score=3), errors (score=2), uptime (score=-4).
func TestSummarizeAll_ranking(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	now := time.Now().UTC()
	window := 7 * 24 * time.Hour

	// "app.revenue": higher_is_better; we want current value = mean - 3*stddev.
	// Insert 30 values with mean=1000, stddev~10 to build a stable baseline.
	if err := cat.UpsertMetric(ctx, "app.revenue", "usd", "higher_is_better", "sum", nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		v := 1000.0 + float64(i%3-1)*5 // values: 995, 1000, 1005
		insertValueAt(t, cat, "app.revenue", v, now.Add(-time.Duration(30-i)*24*time.Hour))
	}
	if err := cat.ComputeBaselines(ctx, "app.revenue", []int{30}); err != nil {
		t.Fatal(err)
	}
	// Set current value well below mean to simulate a bad drop.
	revenueBaseline := cat.pickBaseline(ctx, "app.revenue", 7)
	if revenueBaseline == nil {
		t.Fatal("no baseline for app.revenue")
	}
	revenueBadVal := revenueBaseline.Mean - 3*revenueBaseline.Stddev - 1
	if err := cat.UpsertMetric(ctx, "app.revenue", "usd", "higher_is_better", "sum", &revenueBadVal); err != nil {
		t.Fatal(err)
	}
	insertValueAt(t, cat, "app.revenue", revenueBadVal, now.Add(-time.Hour))

	// "app.errors": lower_is_better; current = mean + 2*stddev (bad spike).
	if err := cat.UpsertMetric(ctx, "app.errors", "count", "lower_is_better", "sum", nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		v := 10.0 + float64(i%3-1)*1 // values: 9, 10, 11
		insertValueAt(t, cat, "app.errors", v, now.Add(-time.Duration(30-i)*24*time.Hour))
	}
	if err := cat.ComputeBaselines(ctx, "app.errors", []int{30}); err != nil {
		t.Fatal(err)
	}
	errorsBaseline := cat.pickBaseline(ctx, "app.errors", 7)
	if errorsBaseline == nil {
		t.Fatal("no baseline for app.errors")
	}
	errorsBadVal := errorsBaseline.Mean + 2*errorsBaseline.Stddev + 1
	if err := cat.UpsertMetric(ctx, "app.errors", "count", "lower_is_better", "sum", &errorsBadVal); err != nil {
		t.Fatal(err)
	}
	insertValueAt(t, cat, "app.errors", errorsBadVal, now.Add(-time.Hour))

	// "app.uptime": higher_is_better; current = mean + 4*stddev (surprise-good).
	if err := cat.UpsertMetric(ctx, "app.uptime", "pct", "higher_is_better", "avg", nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		v := 99.0 + float64(i%3-1)*0.5 // values: 98.5, 99.0, 99.5
		insertValueAt(t, cat, "app.uptime", v, now.Add(-time.Duration(30-i)*24*time.Hour))
	}
	if err := cat.ComputeBaselines(ctx, "app.uptime", []int{30}); err != nil {
		t.Fatal(err)
	}
	uptimeBaseline := cat.pickBaseline(ctx, "app.uptime", 7)
	if uptimeBaseline == nil {
		t.Fatal("no baseline for app.uptime")
	}
	uptimeGoodVal := uptimeBaseline.Mean + 4*uptimeBaseline.Stddev + 1
	if err := cat.UpsertMetric(ctx, "app.uptime", "pct", "higher_is_better", "avg", &uptimeGoodVal); err != nil {
		t.Fatal(err)
	}
	insertValueAt(t, cat, "app.uptime", uptimeGoodVal, now.Add(-time.Hour))

	// Run summarize.
	data, err := cat.SummarizeAll(ctx, window, 10)
	if err != nil {
		t.Fatalf("SummarizeAll: %v", err)
	}
	if data.TotalMetrics != 3 {
		t.Errorf("want 3 total metrics, got %d", data.TotalMetrics)
	}
	if data.TotalConnectors != 1 {
		t.Errorf("want 1 connector (app), got %d", data.TotalConnectors)
	}
	if len(data.TopMetrics) != 3 {
		t.Fatalf("want 3 top metrics, got %d", len(data.TopMetrics))
	}

	// Ranking: revenue (score ~3+) > errors (score ~2+) > uptime (score negative).
	if data.TopMetrics[0].FQName != "app.revenue" {
		t.Errorf("want app.revenue ranked #1, got %s (score=%.2f)", data.TopMetrics[0].FQName, data.TopMetrics[0].Score)
	}
	if data.TopMetrics[1].FQName != "app.errors" {
		t.Errorf("want app.errors ranked #2, got %s (score=%.2f)", data.TopMetrics[1].FQName, data.TopMetrics[1].Score)
	}
	if data.TopMetrics[2].FQName != "app.uptime" {
		t.Errorf("want app.uptime ranked #3 (surprise-good), got %s (score=%.2f)", data.TopMetrics[2].FQName, data.TopMetrics[2].Score)
	}

	// Revenue and errors must have positive scores (surprise-bad).
	if data.TopMetrics[0].Score <= 0 {
		t.Errorf("app.revenue score should be positive (bad), got %.2f", data.TopMetrics[0].Score)
	}
	if data.TopMetrics[1].Score <= 0 {
		t.Errorf("app.errors score should be positive (bad), got %.2f", data.TopMetrics[1].Score)
	}
	// Uptime is surprise-good so score must be negative.
	if data.TopMetrics[2].Score >= 0 {
		t.Errorf("app.uptime score should be negative (surprise-good), got %.2f", data.TopMetrics[2].Score)
	}

	// TopK cap: requesting topK=2 returns exactly 2.
	data2, err := cat.SummarizeAll(ctx, window, 2)
	if err != nil {
		t.Fatalf("SummarizeAll topK=2: %v", err)
	}
	if len(data2.TopMetrics) != 2 {
		t.Errorf("topK=2: want 2 entries, got %d", len(data2.TopMetrics))
	}
}

func TestSummarizeAll_empty(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	data, err := cat.SummarizeAll(ctx, 7*24*time.Hour, 5)
	if err != nil {
		t.Fatalf("SummarizeAll on empty catalog: %v", err)
	}
	if data.TotalMetrics != 0 {
		t.Errorf("want 0 metrics, got %d", data.TotalMetrics)
	}
	narrative := ComposeSummaryNarrative(data)
	if narrative == "" {
		t.Error("narrative should not be empty even for empty catalog")
	}
}

func TestSummarizeAll_connectorGrouping(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	now := time.Now().UTC()

	for _, fq := range []string{"plausible.daily.visitors", "plausible.daily.pageviews", "github.commits.total"} {
		if err := cat.UpsertMetric(ctx, fq, "count", "higher_is_better", "sum", ptr(42.0)); err != nil {
			t.Fatal(err)
		}
		insertValueAt(t, cat, fq, 42.0, now.Add(-time.Hour))
	}
	data, err := cat.SummarizeAll(ctx, 7*24*time.Hour, 10)
	if err != nil {
		t.Fatalf("SummarizeAll: %v", err)
	}
	if data.TotalConnectors != 2 {
		t.Errorf("want 2 connectors (plausible, github), got %d", data.TotalConnectors)
	}
	// ComposeSummaryNarrative should not panic and should mention both connectors.
	narrative := ComposeSummaryNarrative(data)
	for _, want := range []string{"plausible", "github"} {
		if !containsStr(narrative, want) {
			t.Errorf("narrative missing connector %q: %s", want, narrative)
		}
	}
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && (s[:len(sub)] == sub || containsStr(s[1:], sub)))
}

func TestToSummaryJSON(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	data, err := cat.SummarizeAll(ctx, 7*24*time.Hour, 5)
	if err != nil {
		t.Fatal(err)
	}
	j := ToSummaryJSON(data)
	if j.TotalMetrics != 0 {
		t.Errorf("want 0 metrics in JSON, got %d", j.TotalMetrics)
	}
	if j.TopMetrics == nil {
		t.Error("TopMetrics should not be nil")
	}
}
