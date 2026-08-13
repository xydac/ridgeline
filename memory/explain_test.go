package memory

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// insertValueAt inserts a metric value with a specific timestamp directly into
// the DB, bypassing RecordMetricValue's use of time.Now(). Used to test
// window-bounded queries.
func insertValueAt(t *testing.T, cat *Catalog, fqName string, value float64, at time.Time) {
	t.Helper()
	_, err := cat.db.Exec(
		`INSERT INTO bm_metric_values (fq_name, value, observed_at) VALUES (?, ?, ?)`,
		fqName, value, at.UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insertValueAt %s: %v", fqName, err)
	}
}

// insertEventAt inserts an anomaly event with a specific timestamp.
func insertEventAt(t *testing.T, cat *Catalog, fqName string, obsVal, mean, dev float64, windowDays int, direction string, at time.Time) {
	t.Helper()
	_, err := cat.db.Exec(
		`INSERT INTO bm_events (kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, at)
		 VALUES ('anomaly', ?, ?, ?, ?, ?, ?, ?)`,
		fqName, obsVal, mean, dev, direction, windowDays, at.UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insertEventAt %s: %v", fqName, err)
	}
}

func TestExplainMetric_notFound(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	_, err := cat.ExplainMetric(ctx, "app.never.seen", 7*24*time.Hour)
	if err == nil {
		t.Fatal("want error for unknown metric, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("want 'not found' in error, got: %v", err)
	}
}

func TestExplainMetric_withBaseline(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fq := "plausible.daily.visitors"
	if err := cat.UpsertMetric(ctx, fq, "visitors", "higher_is_better", "sum", ptr(1234)); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}
	for i := 1; i <= 30; i++ {
		if err := cat.RecordMetricValue(ctx, fq, float64(i*10)); err != nil {
			t.Fatalf("record value: %v", err)
		}
	}
	if err := cat.ComputeBaselines(ctx, fq, []int{7, 30}); err != nil {
		t.Fatalf("compute baselines: %v", err)
	}

	data, err := cat.ExplainMetric(ctx, fq, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}

	if data.MetricFQ != fq {
		t.Errorf("metric_fq: got %s, want %s", data.MetricFQ, fq)
	}
	if data.Direction != "higher_is_better" {
		t.Errorf("direction: got %s, want higher_is_better", data.Direction)
	}
	if data.CurrentValue == nil || *data.CurrentValue != 1234 {
		t.Errorf("current_value: got %v, want 1234", data.CurrentValue)
	}
	if data.Baseline == nil {
		t.Fatal("want baseline, got nil")
	}
	if data.Baseline.WindowDays != 7 {
		t.Errorf("baseline window_days: got %d, want 7", data.Baseline.WindowDays)
	}
	if data.WindowSamples == 0 {
		t.Error("want non-zero window samples")
	}
	if len(data.Anomalies) != 0 {
		t.Errorf("want 0 anomalies, got %d", len(data.Anomalies))
	}
}

func TestExplainMetric_withAnomalies(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fq := "github.traffic.views.count"
	if err := cat.UpsertMetric(ctx, fq, "views", "higher_is_better", "sum", ptr(500)); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	now := time.Now().UTC()
	insertEventAt(t, cat, fq, 1500, 500, 4.2, 30, "surprise-good", now.Add(-2*24*time.Hour))

	data, err := cat.ExplainMetric(ctx, fq, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if len(data.Anomalies) != 1 {
		t.Fatalf("want 1 anomaly, got %d", len(data.Anomalies))
	}
	if data.Anomalies[0].Direction != "surprise-good" {
		t.Errorf("direction: got %s, want surprise-good", data.Anomalies[0].Direction)
	}
}

func TestExplainMetric_priorPeriod(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fq := "umami.pageviews.pageviews"
	if err := cat.UpsertMetric(ctx, fq, "pageviews", "higher_is_better", "sum", ptr(900)); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	now := time.Now().UTC()
	// current window [now-7d, now]: insert 7 values at mean=900
	for i := 0; i < 7; i++ {
		insertValueAt(t, cat, fq, 900, now.Add(-time.Duration(i+1)*time.Hour))
	}
	// prior window [now-14d, now-7d]: insert 7 values at mean=700
	for i := 0; i < 7; i++ {
		insertValueAt(t, cat, fq, 700, now.Add(-7*24*time.Hour-time.Duration(i+1)*time.Hour))
	}

	data, err := cat.ExplainMetric(ctx, fq, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if data.PriorMean == nil {
		t.Fatal("want prior mean, got nil")
	}
	if *data.PriorMean != 700 {
		t.Errorf("prior_mean: got %f, want 700", *data.PriorMean)
	}
	if data.PriorSamples != 7 {
		t.Errorf("prior_samples: got %d, want 7", data.PriorSamples)
	}
	if data.WindowMean != 900 {
		t.Errorf("window_mean: got %f, want 900", data.WindowMean)
	}
	if data.WindowSamples != 7 {
		t.Errorf("window_samples: got %d, want 7", data.WindowSamples)
	}
}

func TestComposeNarrative_containsExpectedPhrases(t *testing.T) {
	v := 1234.0
	d := &ExplainData{
		MetricFQ:     "plausible.daily.visitors",
		Direction:    "higher_is_better",
		Unit:         "visitors",
		Since:        7 * 24 * time.Hour,
		CurrentValue: &v,
		Baseline: &BaselineRow{
			FQName:      "plausible.daily.visitors",
			WindowDays:  30,
			Mean:        1000,
			Stddev:      100,
			SampleCount: 30,
		},
		WindowMean:    1180,
		WindowSamples: 7,
	}

	narr := ComposeNarrative(d)

	for _, want := range []string{
		"plausible.daily.visitors",
		"last 7d",
		"1234",
		"30d baseline",
		"No anomalies",
		"Summary:",
		"higher is better",
	} {
		if !strings.Contains(narr, want) {
			t.Errorf("narrative missing %q\nfull output:\n%s", want, narr)
		}
	}
}

func TestComposeNarrative_withAnomalyAndPrior(t *testing.T) {
	v := 800.0
	prior := 400.0
	d := &ExplainData{
		MetricFQ:     "github.traffic.views.count",
		Direction:    "higher_is_better",
		Unit:         "views",
		Since:        7 * 24 * time.Hour,
		CurrentValue: &v,
		Baseline: &BaselineRow{
			WindowDays:  30,
			Mean:        480,
			Stddev:      50,
			SampleCount: 30,
		},
		WindowMean:    600,
		WindowSamples: 7,
		PriorMean:     &prior,
		PriorSamples:  7,
		Anomalies: []EventRow{
			{
				Kind:           "anomaly",
				At:             time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC),
				MetricFQ:       "github.traffic.views.count",
				ObservedValue:  1500,
				BaselineMean:   480,
				StddevFromMean: 6.4,
				WindowDays:     30,
				Direction:      "surprise-good",
			},
		},
	}

	narr := ComposeNarrative(d)

	for _, want := range []string{
		"1 anomaly detected",
		"2026-08-09",
		"surprise-good",
		"+50.0%", // (600-400)/400*100 = +50%
		"Summary:",
	} {
		if !strings.Contains(narr, want) {
			t.Errorf("narrative missing %q\nfull output:\n%s", want, narr)
		}
	}
}

func TestToExplainJSON_structure(t *testing.T) {
	v := 500.0
	priorMean := 400.0
	d := &ExplainData{
		MetricFQ:      "github.traffic.views.count",
		Direction:     "higher_is_better",
		Unit:          "views",
		Since:         7 * 24 * time.Hour,
		CurrentValue:  &v,
		Baseline:      &BaselineRow{WindowDays: 30, Mean: 480, Stddev: 50, SampleCount: 30},
		WindowMean:    490,
		WindowSamples: 7,
		PriorMean:     &priorMean,
		PriorSamples:  7,
		Anomalies: []EventRow{
			{
				Kind:           "anomaly",
				At:             time.Now().Add(-24 * time.Hour),
				MetricFQ:       "github.traffic.views.count",
				ObservedValue:  800,
				BaselineMean:   480,
				StddevFromMean: 6.4,
				WindowDays:     30,
				Direction:      "surprise-good",
			},
		},
	}

	j := ToExplainJSON(d)

	if j.MetricFQ != d.MetricFQ {
		t.Errorf("metric_fq mismatch: got %s", j.MetricFQ)
	}
	if j.Since != "7d" {
		t.Errorf("since: got %s, want 7d", j.Since)
	}
	if j.Baseline == nil || j.Baseline.WindowDays != 30 {
		t.Error("baseline mismatch")
	}
	if len(j.Anomalies) != 1 {
		t.Errorf("anomalies: got %d, want 1", len(j.Anomalies))
	}
	if j.PriorMean == nil || *j.PriorMean != 400 {
		t.Error("prior_mean mismatch")
	}
	if j.Summary == "" {
		t.Error("want non-empty summary")
	}

	b, err := json.Marshal(j)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out["metric_fq"] != d.MetricFQ {
		t.Errorf("JSON metric_fq mismatch")
	}
	if _, ok := out["anomalies"]; !ok {
		t.Error("JSON missing anomalies key")
	}
}

func TestFormatSince(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{7 * 24 * time.Hour, "7d"},
		{30 * 24 * time.Hour, "30d"},
		{1 * 24 * time.Hour, "1d"},
		{12 * time.Hour, "12h0m0s"},
	}
	for _, tt := range tests {
		got := FormatSince(tt.d)
		if got != tt.want {
			t.Errorf("FormatSince(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestPickBaseline_smallestAtLeastSince(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	fq := "app.mrr"
	if err := cat.UpsertMetric(ctx, fq, "usd", "higher_is_better", "last", nil); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}
	for i := 1; i <= 90; i++ {
		if err := cat.RecordMetricValue(ctx, fq, float64(1000+i)); err != nil {
			t.Fatalf("record value: %v", err)
		}
	}
	if err := cat.ComputeBaselines(ctx, fq, []int{7, 30, 90}); err != nil {
		t.Fatalf("compute: %v", err)
	}

	// since=7d should pick the 7d window (smallest >= 7)
	b7 := cat.pickBaseline(ctx, fq, 7)
	if b7 == nil || b7.WindowDays != 7 {
		t.Errorf("since=7d: want 7d window, got %v", b7)
	}

	// since=14d should pick 30d (smallest >= 14)
	b14 := cat.pickBaseline(ctx, fq, 14)
	if b14 == nil || b14.WindowDays != 30 {
		t.Errorf("since=14d: want 30d window, got %v", b14)
	}

	// since=100d should pick 90d (largest available, none >= 100)
	b100 := cat.pickBaseline(ctx, fq, 100)
	if b100 == nil || b100.WindowDays != 90 {
		t.Errorf("since=100d: want 90d window, got %v", b100)
	}
}
