package memory

import (
	"context"
	"testing"
	"time"
)

func TestRecommendForecastBoost(t *testing.T) {
	tests := []struct {
		direction     string
		forecastLabel string
		want          float64
	}{
		{"higher_is_better", "likely-decline", 1.0},
		{"lower_is_better", "likely-improvement", 1.0},
		{"higher_is_better", "likely-improvement", -0.5},
		{"lower_is_better", "likely-decline", -0.5},
		{"higher_is_better", "stable", 0},
		{"neutral", "likely-decline", 0},
		{"neutral", "stable", 0},
	}
	for _, tc := range tests {
		got := recommendForecastBoost(tc.direction, tc.forecastLabel)
		if got != tc.want {
			t.Errorf("recommendForecastBoost(%q, %q) = %v, want %v",
				tc.direction, tc.forecastLabel, got, tc.want)
		}
	}
}

func TestWorstAnomalyLabel(t *testing.T) {
	events := []EventRow{
		{Kind: "deploy", Direction: ""},
		{Kind: "anomaly", Direction: "surprise-good"},
		{Kind: "anomaly", Direction: "surprise-bad"},
		{Kind: "commit", Direction: ""},
	}
	got := worstAnomalyLabel(events)
	if got != "surprise-bad" {
		t.Errorf("worstAnomalyLabel = %q, want surprise-bad", got)
	}

	// No anomaly events.
	got = worstAnomalyLabel([]EventRow{{Kind: "deploy"}})
	if got != "" {
		t.Errorf("worstAnomalyLabel with no anomalies = %q, want empty", got)
	}
}

// TestRecommendAll_noSignal verifies that a metric sitting exactly at baseline
// with a stable forecast is excluded from recommendations.
func TestRecommendAll_noSignal(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	now := time.Now().UTC()

	if err := cat.UpsertMetric(ctx, "app.visits", "count", "higher_is_better", "sum", nil); err != nil {
		t.Fatal(err)
	}
	// 30 values tightly clustered around 500.
	for i := 0; i < 30; i++ {
		insertValueAt(t, cat, "app.visits", 500.0, now.Add(-time.Duration(30-i)*24*time.Hour))
	}
	if err := cat.ComputeBaselines(ctx, "app.visits", []int{30}); err != nil {
		t.Fatal(err)
	}
	// Set current value at exactly the mean.
	v := 500.0
	if err := cat.UpsertMetric(ctx, "app.visits", "count", "higher_is_better", "sum", &v); err != nil {
		t.Fatal(err)
	}
	insertValueAt(t, cat, "app.visits", v, now.Add(-time.Hour))

	d, err := cat.RecommendAll(ctx, 7*24*time.Hour, 5)
	if err != nil {
		t.Fatalf("RecommendAll: %v", err)
	}
	// At baseline with stable forecast => no recommendations.
	if len(d.Items) != 0 {
		t.Errorf("expected 0 recommendations for at-baseline metric, got %d", len(d.Items))
	}
}

// TestRecommendAll_anomalyRankedFirst verifies that a metric with a surprise-bad
// anomaly ranks above a metric with only a minor deviation.
func TestRecommendAll_anomalyRankedFirst(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	now := time.Now().UTC()

	// "app.revenue": higher_is_better, surprise-bad anomaly.
	if err := cat.UpsertMetric(ctx, "app.revenue", "usd", "higher_is_better", "sum", nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		v := 1000.0 + float64(i%3-1)*5
		insertValueAt(t, cat, "app.revenue", v, now.Add(-time.Duration(30-i)*24*time.Hour))
	}
	if err := cat.ComputeBaselines(ctx, "app.revenue", []int{30}); err != nil {
		t.Fatal(err)
	}
	bl := cat.pickBaseline(ctx, "app.revenue", 7)
	if bl == nil {
		t.Fatal("no baseline for app.revenue")
	}
	badVal := bl.Mean - 4*bl.Stddev - 1
	insertValueAt(t, cat, "app.revenue", badVal, now.Add(-time.Hour))
	if err := cat.UpsertMetric(ctx, "app.revenue", "usd", "higher_is_better", "sum", &badVal); err != nil {
		t.Fatal(err)
	}
	if err := cat.DetectAndRecordAnomalies(ctx, "app.revenue", badVal, now.Add(-time.Hour), 2.5, 5); err != nil {
		t.Fatal(err)
	}

	// "app.pageviews": higher_is_better, slight deviation.
	if err := cat.UpsertMetric(ctx, "app.pageviews", "count", "higher_is_better", "sum", nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		v := 5000.0 + float64(i%3-1)*20
		insertValueAt(t, cat, "app.pageviews", v, now.Add(-time.Duration(30-i)*24*time.Hour))
	}
	if err := cat.ComputeBaselines(ctx, "app.pageviews", []int{30}); err != nil {
		t.Fatal(err)
	}
	pageviewsV := 4950.0
	insertValueAt(t, cat, "app.pageviews", pageviewsV, now.Add(-time.Hour))
	if err := cat.UpsertMetric(ctx, "app.pageviews", "count", "higher_is_better", "sum", &pageviewsV); err != nil {
		t.Fatal(err)
	}

	d, err := cat.RecommendAll(ctx, 7*24*time.Hour, 5)
	if err != nil {
		t.Fatalf("RecommendAll: %v", err)
	}
	if len(d.Items) == 0 {
		t.Fatal("expected at least one recommendation")
	}
	if d.Items[0].MetricFQ != "app.revenue" {
		t.Errorf("expected app.revenue ranked first, got %s", d.Items[0].MetricFQ)
	}
	if d.Items[0].AnomalyLabel != "surprise-bad" {
		t.Errorf("expected anomaly_label=surprise-bad, got %q", d.Items[0].AnomalyLabel)
	}
	if d.Items[0].SuggestedCommand != "ridgeline investigate app.revenue" {
		t.Errorf("expected investigate command, got %q", d.Items[0].SuggestedCommand)
	}
}

// TestRecommendAll_topKRespected verifies that topK caps the output.
func TestRecommendAll_topKRespected(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	now := time.Now().UTC()

	for i := 0; i < 5; i++ {
		fq := "app.metric" + string(rune('a'+i))
		if err := cat.UpsertMetric(ctx, fq, "count", "higher_is_better", "sum", nil); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 30; j++ {
			insertValueAt(t, cat, fq, 100.0+float64(j%3-1), now.Add(-time.Duration(30-j)*24*time.Hour))
		}
		if err := cat.ComputeBaselines(ctx, fq, []int{30}); err != nil {
			t.Fatal(err)
		}
		// Set each metric below baseline to generate a score.
		v := 50.0 - float64(i)*5
		insertValueAt(t, cat, fq, v, now.Add(-time.Hour))
		if err := cat.UpsertMetric(ctx, fq, "count", "higher_is_better", "sum", &v); err != nil {
			t.Fatal(err)
		}
	}

	d, err := cat.RecommendAll(ctx, 7*24*time.Hour, 3)
	if err != nil {
		t.Fatalf("RecommendAll: %v", err)
	}
	if len(d.Items) > 3 {
		t.Errorf("expected at most 3 items with topK=3, got %d", len(d.Items))
	}
}

// TestComposeRecommendNarrative_empty verifies the empty-state message.
func TestComposeRecommendNarrative_empty(t *testing.T) {
	d := &RecommendData{Since: 7 * 24 * time.Hour, Items: nil}
	out := ComposeRecommendNarrative(d)
	if out == "" {
		t.Error("expected non-empty narrative for empty items")
	}
	if !recContains(out, "No notable signals") {
		t.Errorf("expected 'No notable signals' in output, got: %s", out)
	}
}

// TestToRecommendJSON verifies JSON serialization shape.
func TestToRecommendJSON(t *testing.T) {
	d := &RecommendData{
		Since: 7 * 24 * time.Hour,
		Items: []RecommendItem{
			{
				MetricFQ:         "plausible.daily.visitors",
				Connector:        "plausible",
				Score:            3.5,
				AnomalyLabel:     "surprise-bad",
				ForecastLabel:    "likely-decline",
				Reason:           "visitors dropped; forecast shows likely-decline.",
				SuggestedCommand: "ridgeline investigate plausible.daily.visitors",
				Confidence:       0.72,
			},
		},
	}
	j := ToRecommendJSON(d)
	if j.Since != "7d" {
		t.Errorf("Since = %q, want 7d", j.Since)
	}
	if len(j.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(j.Items))
	}
	item := j.Items[0]
	if item.MetricFQ != "plausible.daily.visitors" {
		t.Errorf("MetricFQ = %q", item.MetricFQ)
	}
	if item.SuggestedCommand != "ridgeline investigate plausible.daily.visitors" {
		t.Errorf("SuggestedCommand = %q", item.SuggestedCommand)
	}
}

func recContains(s, sub string) bool {
	if len(s) < len(sub) {
		return false
	}
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
