package memory

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestRecordMetricValue_andListBaselines(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	// 10 distinct hourly observations within the last 7 days.
	now := time.Now().UTC()
	for i := 1; i <= 10; i++ {
		at := now.Add(-time.Duration(10-i) * time.Hour)
		if err := cat.RecordMetricValueAt(ctx, "app.daily.signups", float64(i), at); err != nil {
			t.Fatalf("record value %d: %v", i, err)
		}
	}

	if err := cat.ComputeBaselines(ctx, "app.daily.signups", []int{7, 30}); err != nil {
		t.Fatalf("compute baselines: %v", err)
	}

	rows, err := cat.ListBaselines(ctx, "app.daily.signups")
	if err != nil {
		t.Fatalf("list baselines: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 baseline rows (7d, 30d), got %d", len(rows))
	}

	r7 := rows[0]
	if r7.WindowDays != 7 {
		t.Errorf("want window_days=7, got %d", r7.WindowDays)
	}
	if r7.SampleCount != 10 {
		t.Errorf("want sample_count=10, got %d", r7.SampleCount)
	}
	wantMean := 5.5
	if math.Abs(r7.Mean-wantMean) > 1e-9 {
		t.Errorf("want mean=5.5, got %f", r7.Mean)
	}
	if r7.Min != 1 {
		t.Errorf("want min=1, got %f", r7.Min)
	}
	if r7.Max != 10 {
		t.Errorf("want max=10, got %f", r7.Max)
	}
	// population stddev of 1..10 = sqrt(8.25) ~ 2.872
	wantStddev := math.Sqrt(8.25)
	if math.Abs(r7.Stddev-wantStddev) > 1e-6 {
		t.Errorf("want stddev~2.872, got %f", r7.Stddev)
	}
}

func TestComputeBaselines_idempotent(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	now := time.Now().UTC()
	for i := 1; i <= 5; i++ {
		at := now.Add(-time.Duration(5-i) * 24 * time.Hour)
		if err := cat.RecordMetricValueAt(ctx, "app.mrr", float64(i*100), at); err != nil {
			t.Fatalf("record: %v", err)
		}
	}

	for range 3 {
		if err := cat.ComputeBaselines(ctx, "app.mrr", []int{30}); err != nil {
			t.Fatalf("compute: %v", err)
		}
	}

	rows, err := cat.ListBaselines(ctx, "app.mrr")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0].SampleCount != 5 {
		t.Errorf("want sample_count=5, got %d", rows[0].SampleCount)
	}
}

func TestComputeBaselines_singleSample_stddevZero(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	if err := cat.RecordMetricValue(ctx, "app.cac", 42); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := cat.ComputeBaselines(ctx, "app.cac", []int{7}); err != nil {
		t.Fatalf("compute: %v", err)
	}
	rows, err := cat.ListBaselines(ctx, "app.cac")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0].Stddev != 0 {
		t.Errorf("single sample stddev must be 0, got %f", rows[0].Stddev)
	}
	if math.IsNaN(rows[0].Stddev) {
		t.Error("stddev must not be NaN")
	}
}

func TestComputeBaselines_noData_skips(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	// no data recorded
	if err := cat.ComputeBaselines(ctx, "app.visitors", []int{7, 30}); err != nil {
		t.Fatalf("compute with no data: %v", err)
	}
	rows, err := cat.ListBaselines(ctx, "app.visitors")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("want 0 rows for metric with no data, got %d", len(rows))
	}
}

func TestRecompute(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	now := time.Now().UTC()
	metrics := []string{"app.a", "app.b"}
	for _, m := range metrics {
		for i := 1; i <= 3; i++ {
			at := now.Add(-time.Duration(3-i) * 24 * time.Hour)
			if err := cat.RecordMetricValueAt(ctx, m, float64(i), at); err != nil {
				t.Fatalf("record %s: %v", m, err)
			}
		}
	}

	if err := cat.Recompute(ctx, 0, []int{7}); err != nil {
		t.Fatalf("recompute: %v", err)
	}

	for _, m := range metrics {
		rows, err := cat.ListBaselines(ctx, m)
		if err != nil {
			t.Fatalf("list %s: %v", m, err)
		}
		if len(rows) != 1 {
			t.Errorf("metric %s: want 1 baseline row, got %d", m, len(rows))
		}
	}
}

func TestSparkline_empty(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	s, err := cat.Sparkline(ctx, "app.visitors", 30, 20)
	if err != nil {
		t.Fatalf("sparkline: %v", err)
	}
	if s != "" {
		t.Errorf("want empty sparkline for no data, got %q", s)
	}
}

func TestSparkline_renders(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	now := time.Now().UTC()
	vals := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	for j, v := range vals {
		at := now.Add(-time.Duration(len(vals)-j) * 24 * time.Hour)
		if err := cat.RecordMetricValueAt(ctx, "app.revenue", v, at); err != nil {
			t.Fatalf("record: %v", err)
		}
	}

	s, err := cat.Sparkline(ctx, "app.revenue", 30, 20)
	if err != nil {
		t.Fatalf("sparkline: %v", err)
	}
	if len([]rune(s)) == 0 {
		t.Error("want non-empty sparkline")
	}
	// first char should be lower than last (ascending values)
	runes := []rune(s)
	if runes[0] >= runes[len(runes)-1] {
		t.Errorf("ascending values should produce ascending sparkline, got %s", s)
	}
}

func TestWindowStats_knownValues(t *testing.T) {
	mean, stddev, min, max := windowStats([]float64{2, 4, 4, 4, 5, 5, 7, 9})
	if math.Abs(mean-5.0) > 1e-9 {
		t.Errorf("mean: want 5, got %f", mean)
	}
	if math.Abs(stddev-2.0) > 1e-9 {
		t.Errorf("stddev: want 2, got %f", stddev)
	}
	if min != 2 {
		t.Errorf("min: want 2, got %f", min)
	}
	if max != 9 {
		t.Errorf("max: want 9, got %f", max)
	}
}

func TestRenderSparkline_uniformValues(t *testing.T) {
	// all same value -> all mid-range chars
	s := renderSparkline([]float64{5, 5, 5, 5}, 4)
	runes := []rune(s)
	if len(runes) != 4 {
		t.Fatalf("want 4 chars, got %d", len(runes))
	}
	for _, r := range runes {
		if r != sparkChars[0] {
			t.Errorf("uniform values should all map to sparkChars[0], got %c", r)
		}
	}
}

func TestSubsample(t *testing.T) {
	// 10 values downsampled to 5 => averages of pairs
	vals := []float64{1, 3, 5, 7, 9, 11, 13, 15, 17, 19}
	got := subsample(vals, 5)
	want := []float64{2, 6, 10, 14, 18}
	if len(got) != len(want) {
		t.Fatalf("want len %d, got %d", len(want), len(got))
	}
	for i := range want {
		if math.Abs(got[i]-want[i]) > 1e-9 {
			t.Errorf("[%d]: want %f, got %f", i, want[i], got[i])
		}
	}
}
