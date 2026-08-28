package memory

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestLinearRegression(t *testing.T) {
	// Perfect upward line: y = 2x + 10
	xs := []float64{0, 1, 2, 3, 4}
	ys := []float64{10, 12, 14, 16, 18}
	slope, intercept := linearRegression(xs, ys)
	if math.Abs(slope-2.0) > 1e-9 {
		t.Errorf("slope = %v, want 2.0", slope)
	}
	if math.Abs(intercept-10.0) > 1e-9 {
		t.Errorf("intercept = %v, want 10.0", intercept)
	}

	r2 := rSquared(xs, ys, slope, intercept)
	if math.Abs(r2-1.0) > 1e-9 {
		t.Errorf("R² = %v, want 1.0 for perfect fit", r2)
	}
}

func TestRSquaredConstant(t *testing.T) {
	// Constant series: slope=0, R²=1 (no variance to explain)
	xs := []float64{0, 1, 2, 3}
	ys := []float64{5, 5, 5, 5}
	slope, intercept := linearRegression(xs, ys)
	r2 := rSquared(xs, ys, slope, intercept)
	if r2 != 1.0 {
		t.Errorf("R² = %v, want 1.0 for constant series", r2)
	}
}

func TestForecastLabel(t *testing.T) {
	tests := []struct {
		name      string
		slope     float64
		direction string
		refMean   float64
		horizon   float64
		wantLabel string
	}{
		{"rising higher-is-better", 5.0, "higher_is_better", 100.0, 7, "likely-improvement"},
		{"falling higher-is-better", -5.0, "higher_is_better", 100.0, 7, "likely-decline"},
		{"rising lower-is-better", 5.0, "lower_is_better", 100.0, 7, "likely-decline"},
		{"falling lower-is-better", -5.0, "lower_is_better", 100.0, 7, "likely-improvement"},
		{"tiny slope stable", 0.01, "higher_is_better", 100.0, 7, "stable"},
		{"neutral direction decline", -5.0, "neutral", 100.0, 7, "likely-decline"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := &BaselineRow{Mean: tc.refMean}
			got := forecastLabel(tc.slope, tc.direction, b, tc.horizon)
			if got != tc.wantLabel {
				t.Errorf("forecastLabel = %q, want %q", got, tc.wantLabel)
			}
		})
	}
}

func TestForecastMetricNotFound(t *testing.T) {
	cat := openTestCatalog(t)
	_, err := cat.ForecastMetric(context.Background(), "notexist.metric", 7*24*time.Hour)
	if err == nil {
		t.Fatal("expected error for unknown metric")
	}
}

func TestForecastMetricInsufficientData(t *testing.T) {
	cat := openTestCatalog(t)
	insertTestMetric(t, cat, "app.daily.visits", "higher_is_better", "visits")
	// Only one observation -- insufficient for regression.
	insertMetricValue(t, cat, "app.daily.visits", 100.0, time.Now().UTC().Add(-1*24*time.Hour))

	_, err := cat.ForecastMetric(context.Background(), "app.daily.visits", 7*24*time.Hour)
	if err == nil {
		t.Fatal("expected error for single observation")
	}
}

func TestForecastMetricTrend(t *testing.T) {
	cat := openTestCatalog(t)
	insertTestMetric(t, cat, "app.daily.visits", "higher_is_better", "visits")

	// Insert 30 days of linearly increasing data.
	base := time.Now().UTC().Add(-30 * 24 * time.Hour)
	for i := 0; i < 30; i++ {
		v := float64(1000 + i*10) // +10/day: clear upward trend
		insertMetricValue(t, cat, "app.daily.visits", v, base.Add(time.Duration(i)*24*time.Hour))
	}
	if err := cat.ComputeBaselines(context.Background(), "app.daily.visits", []int{7, 30, 90}); err != nil {
		t.Fatal(err)
	}

	d, err := cat.ForecastMetric(context.Background(), "app.daily.visits", 7*24*time.Hour)
	if err != nil {
		t.Fatalf("ForecastMetric: %v", err)
	}
	if d.Directional != "likely-improvement" {
		t.Errorf("directional = %q, want likely-improvement", d.Directional)
	}
	if d.Slope < 5 || d.Slope > 15 {
		t.Errorf("slope = %v, expected ~10/day", d.Slope)
	}
	if d.RSquared < 0.95 {
		t.Errorf("R² = %v, expected high fit for linear data", d.RSquared)
	}
	if d.Confidence <= 0 {
		t.Errorf("confidence = %v, expected > 0", d.Confidence)
	}

	j := ToForecastJSON(d)
	if j.MetricFQ != "app.daily.visits" {
		t.Errorf("JSON metric_fq = %q", j.MetricFQ)
	}
	if j.Directional != "likely-improvement" {
		t.Errorf("JSON directional = %q", j.Directional)
	}
	if j.Summary == "" {
		t.Error("JSON summary is empty")
	}
}

func TestForecastMetricDecline(t *testing.T) {
	cat := openTestCatalog(t)
	insertTestMetric(t, cat, "app.daily.errors", "lower_is_better", "errors")

	// Insert 20 days of increasing errors (bad for lower-is-better).
	base := time.Now().UTC().Add(-20 * 24 * time.Hour)
	for i := 0; i < 20; i++ {
		v := float64(10 + i*2) // errors increasing
		insertMetricValue(t, cat, "app.daily.errors", v, base.Add(time.Duration(i)*24*time.Hour))
	}

	d, err := cat.ForecastMetric(context.Background(), "app.daily.errors", 7*24*time.Hour)
	if err != nil {
		t.Fatalf("ForecastMetric: %v", err)
	}
	if d.Directional != "likely-decline" {
		t.Errorf("directional = %q, want likely-decline (errors rising is bad)", d.Directional)
	}
}

func TestComposeForecastNarrative(t *testing.T) {
	d := &ForecastData{
		MetricFQ:      "app.daily.visits",
		Direction:     "higher_is_better",
		Unit:          "visits",
		Horizon:       7 * 24 * time.Hour,
		SampleCount:   30,
		Slope:         10.0,
		Intercept:     1000.0,
		RSquared:      0.98,
		ProjectedMean: 1270.0,
		BandWidth:     45.0,
		Directional:   "likely-improvement",
		Confidence:    0.82,
		Baseline:      &BaselineRow{WindowDays: 30, Mean: 1150.0, Stddev: 80.0, SampleCount: 30},
	}
	narrative := ComposeForecastNarrative(d)
	if narrative == "" {
		t.Fatal("narrative is empty")
	}
	for _, want := range []string{"visits", "7d", "1270", "trending toward improvement"} {
		if !forecastContains(narrative, want) {
			t.Errorf("narrative missing %q:\n%s", want, narrative)
		}
	}
}

// insertMetricValue inserts one timestamped observation into bm_metric_values.
func insertMetricValue(t *testing.T, cat *Catalog, fqName string, value float64, at time.Time) {
	t.Helper()
	_, err := cat.db.Exec(
		`INSERT INTO bm_metric_values (fq_name, value, observed_at) VALUES (?, ?, ?)`,
		fqName, value, at.Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insertMetricValue: %v", err)
	}
}

// insertTestMetric inserts a minimal row into bm_metrics.
func insertTestMetric(t *testing.T, cat *Catalog, fqName, direction, unit string) {
	t.Helper()
	_, err := cat.db.Exec(
		`INSERT OR IGNORE INTO bm_metrics (fq_name, direction, unit, aggregation, updated_at) VALUES (?, ?, ?, 'sum', '2000-01-01T00:00:00Z')`,
		fqName, direction, unit)
	if err != nil {
		t.Fatalf("insertTestMetric: %v", err)
	}
}

func forecastContains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return len(sub) == 0
}
