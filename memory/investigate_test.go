package memory

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

// insertNonAnomalyEventAt inserts a non-anomaly event (deploy, commit, note) at a specific time.
func insertNonAnomalyEventAt(t *testing.T, cat *Catalog, kind, description string, at time.Time) {
	t.Helper()
	_, err := cat.db.Exec(
		`INSERT INTO bm_events (kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, description, at)
		 VALUES (?, '', 0, 0, 0, '', 0, ?, ?)`,
		kind, description, at.Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insertNonAnomalyEventAt: %v", err)
	}
}

func TestInvestigateMetric_notFound(t *testing.T) {
	cat := openTestCatalog(t)
	_, err := cat.InvestigateMetric(context.Background(), "does.not.exist", 7*24*time.Hour)
	if err == nil {
		t.Fatal("expected error for unknown metric")
	}
}

func TestInvestigateMetric_noAnomalies(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	fq := "app.daily.revenue"
	setupMetricWithHistory(t, cat, fq, "usd", "higher_is_better",
		[]float64{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100},
		[]int{7, 14})

	d, err := cat.InvestigateMetric(ctx, fq, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("InvestigateMetric: %v", err)
	}
	if d.Explain.MetricFQ != fq {
		t.Errorf("MetricFQ: got %s, want %s", d.Explain.MetricFQ, fq)
	}
	if len(d.Causal) != 0 {
		t.Errorf("expected no causal candidates without anomalies, got %d", len(d.Causal))
	}
}

func TestInvestigateMetric_causalCandidates(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	fq := "plausible.daily.visitors"
	now := time.Now().UTC()

	// Set up metric with a clear anomaly 2 days ago.
	setupMetricWithHistory(t, cat, fq, "visitors", "higher_is_better",
		[]float64{1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 50, 50},
		[]int{7, 14, 30})

	anomalyAt := now.Add(-2 * 24 * time.Hour)
	insertEventAt(t, cat, fq, 50, 1000, 5.0, 14, "surprise-bad", anomalyAt)

	// Deploy event 6h before anomaly -- should be a causal candidate.
	insertNonAnomalyEventAt(t, cat, "deploy", "shipped v0.2.0-rc1", anomalyAt.Add(-6*time.Hour))
	// Commit 12h before anomaly -- should also be a causal candidate.
	insertNonAnomalyEventAt(t, cat, "commit", "Remove caching layer", anomalyAt.Add(-12*time.Hour))
	// Event 72h before anomaly -- outside the proximity window, should NOT appear.
	insertNonAnomalyEventAt(t, cat, "commit", "Too old commit", anomalyAt.Add(-72*time.Hour))

	d, err := cat.InvestigateMetric(ctx, fq, 14*24*time.Hour)
	if err != nil {
		t.Fatalf("InvestigateMetric: %v", err)
	}
	if len(d.Causal) < 2 {
		t.Errorf("expected at least 2 causal candidates, got %d", len(d.Causal))
	}
	// Verify the 72h-old event is NOT included.
	for _, c := range d.Causal {
		if strings.Contains(c.Event.Description, "Too old") {
			t.Error("event beyond 48h window should not be a causal candidate")
		}
	}
}

func TestInvestigateMetric_siblingCorrelation(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	now := time.Now().UTC()

	fqA := "app.daily.visitors"
	fqB := "app.daily.pageviews"
	fqC := "app.daily.bounce_rate" // will be inversely correlated

	// Insert 14 days of perfectly correlated values.
	vA := make([]float64, 14)
	vB := make([]float64, 14)
	vC := make([]float64, 14)
	for i := range vA {
		vA[i] = float64(100 + i*10)
		vB[i] = float64(200 + i*20) // perfect positive correlation with A
		vC[i] = float64(90 - i*5)   // perfect negative correlation with A
	}

	lastA := vA[len(vA)-1]
	lastB := vB[len(vB)-1]
	lastC := vC[len(vC)-1]
	if err := cat.UpsertMetric(ctx, fqA, "visitors", "higher_is_better", "sum", &lastA); err != nil {
		t.Fatal(err)
	}
	if err := cat.UpsertMetric(ctx, fqB, "pageviews", "higher_is_better", "sum", &lastB); err != nil {
		t.Fatal(err)
	}
	if err := cat.UpsertMetric(ctx, fqC, "pct", "lower_is_better", "mean", &lastC); err != nil {
		t.Fatal(err)
	}
	for i := range vA {
		at := now.Add(-time.Duration(14-i) * 24 * time.Hour)
		insertValueAt(t, cat, fqA, vA[i], at)
		insertValueAt(t, cat, fqB, vB[i], at)
		insertValueAt(t, cat, fqC, vC[i], at)
	}

	d, err := cat.InvestigateMetric(ctx, fqA, 14*24*time.Hour)
	if err != nil {
		t.Fatalf("InvestigateMetric: %v", err)
	}

	found := map[string]float64{}
	for _, s := range d.Siblings {
		found[s.MetricFQ] = s.R
	}
	if r, ok := found[fqB]; !ok || math.Abs(r-1.0) > 0.01 {
		t.Errorf("expected fqB with r~1.0, got r=%.4f, ok=%v", r, ok)
	}
	if r, ok := found[fqC]; !ok || math.Abs(r+1.0) > 0.01 {
		t.Errorf("expected fqC with r~-1.0, got r=%.4f, ok=%v", r, ok)
	}
}

func TestComposeCausalNarrative_noAnomalies(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	fq := "app.daily.signups"
	setupMetricWithHistory(t, cat, fq, "users", "higher_is_better",
		[]float64{50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50},
		[]int{7})

	d, err := cat.InvestigateMetric(ctx, fq, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("InvestigateMetric: %v", err)
	}
	narrative := ComposeCausalNarrative(d)
	if !strings.Contains(narrative, "No anomalies detected") {
		t.Errorf("expected no-anomaly message, got:\n%s", narrative)
	}
}

func TestComposeCausalNarrative_withCausal(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	fq := "plausible.daily.visitors"
	now := time.Now().UTC()

	setupMetricWithHistory(t, cat, fq, "visitors", "higher_is_better",
		[]float64{1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 50, 50},
		[]int{14, 30})
	anomalyAt := now.Add(-2 * 24 * time.Hour)
	insertEventAt(t, cat, fq, 50, 1000, 5.0, 14, "surprise-bad", anomalyAt)
	insertNonAnomalyEventAt(t, cat, "deploy", "shipped hotfix", anomalyAt.Add(-3*time.Hour))

	d, err := cat.InvestigateMetric(ctx, fq, 14*24*time.Hour)
	if err != nil {
		t.Fatalf("InvestigateMetric: %v", err)
	}
	narrative := ComposeCausalNarrative(d)
	if !strings.Contains(narrative, "Correlated events") {
		t.Errorf("expected causal section, got:\n%s", narrative)
	}
	if !strings.Contains(narrative, "hotfix") {
		t.Errorf("expected deploy description in narrative, got:\n%s", narrative)
	}
}

func TestToInvestigateJSON_roundtrip(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	fq := "myapp.daily.users"
	now := time.Now().UTC()

	setupMetricWithHistory(t, cat, fq, "users", "higher_is_better",
		[]float64{500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 100, 100},
		[]int{7, 14})
	anomalyAt := now.Add(-2 * 24 * time.Hour)
	insertEventAt(t, cat, fq, 100, 500, 4.5, 14, "surprise-bad", anomalyAt)
	insertNonAnomalyEventAt(t, cat, "deploy", "Deploy 1.3.0", anomalyAt.Add(-4*time.Hour))

	d, err := cat.InvestigateMetric(ctx, fq, 14*24*time.Hour)
	if err != nil {
		t.Fatalf("InvestigateMetric: %v", err)
	}
	j := ToInvestigateJSON(d)
	if j.MetricFQ != fq {
		t.Errorf("MetricFQ: got %s, want %s", j.MetricFQ, fq)
	}
	if j.Explain == nil {
		t.Fatal("Explain must be non-nil")
	}
	if len(j.Causal) == 0 {
		t.Error("expected at least one causal candidate in JSON")
	}
	// Verify round-trip through JSON encoding.
	b, err := json.Marshal(j)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var decoded InvestigateJSON
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if decoded.MetricFQ != fq {
		t.Errorf("decoded MetricFQ: got %s, want %s", decoded.MetricFQ, fq)
	}
}

func TestPearsonR(t *testing.T) {
	// Perfect positive correlation.
	a := map[string]float64{"d1": 1, "d2": 2, "d3": 3, "d4": 4}
	b := map[string]float64{"d1": 2, "d2": 4, "d3": 6, "d4": 8}
	r, n := pearsonR(a, b)
	if n != 4 {
		t.Errorf("n: got %d, want 4", n)
	}
	if math.Abs(r-1.0) > 0.0001 {
		t.Errorf("r: got %.6f, want 1.0", r)
	}

	// Perfect negative correlation.
	c := map[string]float64{"d1": 4, "d2": 3, "d3": 2, "d4": 1}
	r2, n2 := pearsonR(a, c)
	if n2 != 4 {
		t.Errorf("n: got %d, want 4", n2)
	}
	if math.Abs(r2+1.0) > 0.0001 {
		t.Errorf("r: got %.6f, want -1.0", r2)
	}

	// No shared days.
	d := map[string]float64{"d5": 10, "d6": 20}
	r3, n3 := pearsonR(a, d)
	if n3 != 0 || r3 != 0 {
		t.Errorf("no overlap: got r=%v, n=%d; want r=0, n=0", r3, n3)
	}

	// Constant values (zero variance) -- should not panic.
	flat := map[string]float64{"d1": 5, "d2": 5, "d3": 5, "d4": 5}
	r4, _ := pearsonR(a, flat)
	if !math.IsNaN(r4) && r4 != 0 {
		// Either NaN or 0 is acceptable for zero-variance denominator.
		t.Errorf("constant series: expected 0 or NaN, got %v", r4)
	}
}
