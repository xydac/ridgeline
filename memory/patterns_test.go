package memory

import (
	"context"
	"math"
	"testing"
	"time"
)

// insertSample directly inserts a timestamped metric value into bm_metric_values.
func insertSample(t *testing.T, cat *Catalog, fqName string, at time.Time, value float64) {
	t.Helper()
	_, err := cat.db.Exec(
		`INSERT INTO bm_metric_values (fq_name, value, observed_at) VALUES (?, ?, ?)`,
		fqName, value, at.UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insert sample %s at %s: %v", fqName, at.Format("2006-01-02"), err)
	}
}

// recentMonday returns the most recent Monday at or before now, at noon UTC.
func recentMonday() time.Time {
	now := time.Now().UTC()
	for now.Weekday() != time.Monday {
		now = now.AddDate(0, 0, -1)
	}
	return time.Date(now.Year(), now.Month(), now.Day(), 12, 0, 0, 0, time.UTC)
}

// TestDetectWeekendDip checks that the weekend-dip pattern fires when Sat/Sun
// are consistently lower than Mon-Fri across >= 4 weeks.
func TestDetectWeekendDip(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	// 6 weeks ending this Monday. Start 6 weeks ago.
	monday := recentMonday().AddDate(0, 0, -6*7)
	metric := "app.daily.visits"
	for week := 0; week < 6; week++ {
		for day := 0; day < 7; day++ {
			at := monday.AddDate(0, 0, week*7+day)
			v := 100.0
			if at.Weekday() == time.Saturday || at.Weekday() == time.Sunday {
				v = 50.0 // 50% lower on weekends
			}
			insertSample(t, cat, metric, at, v)
		}
	}

	patterns, err := cat.DetectPatterns(ctx, metric)
	if err != nil {
		t.Fatalf("detect patterns: %v", err)
	}

	found := false
	for _, p := range patterns {
		if p.Pattern == PatternWeekendDip {
			found = true
			if p.Confidence <= 0 || p.Confidence > 1 {
				t.Errorf("weekend-dip confidence out of range: %f", p.Confidence)
			}
		}
	}
	if !found {
		t.Error("expected weekend-dip pattern, not detected")
	}
}

// TestDetectWeekendDip_NoPattern verifies that flat data does not fire.
func TestDetectWeekendDip_NoPattern(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	monday := recentMonday().AddDate(0, 0, -6*7)
	metric := "app.daily.flat"
	for week := 0; week < 6; week++ {
		for day := 0; day < 7; day++ {
			at := monday.AddDate(0, 0, week*7+day)
			insertSample(t, cat, metric, at, 100.0)
		}
	}

	patterns, err := cat.DetectPatterns(ctx, metric)
	if err != nil {
		t.Fatalf("detect patterns: %v", err)
	}
	for _, p := range patterns {
		if p.Pattern == PatternWeekendDip {
			t.Error("weekend-dip should not fire on flat data")
		}
	}
}

// TestDetectMonthEndSpike checks that month-end-spike fires when last 2 days
// of each month are consistently higher across >= 3 months.
// Uses the last 85 days so data stays within the 90-day loadSamples window.
func TestDetectMonthEndSpike(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	metric := "app.daily.revenue"
	// Insert 85 days of data going back from 5 days ago.
	base := time.Now().UTC().AddDate(0, 0, -85)
	base = time.Date(base.Year(), base.Month(), base.Day(), 12, 0, 0, 0, time.UTC)
	for i := 0; i < 85; i++ {
		at := base.AddDate(0, 0, i)
		// find last day of this month
		nextMonth := time.Date(at.Year(), at.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		lastDay := nextMonth.AddDate(0, 0, -1).Day()
		v := 100.0
		if at.Day() >= lastDay-1 {
			v = 500.0 // spike last 2 days of month
		}
		insertSample(t, cat, metric, at, v)
	}

	patterns, err := cat.DetectPatterns(ctx, metric)
	if err != nil {
		t.Fatalf("detect patterns: %v", err)
	}

	found := false
	for _, p := range patterns {
		if p.Pattern == PatternMonthEndSpike {
			found = true
		}
	}
	if !found {
		t.Error("expected month-end-spike pattern, not detected")
	}
}

// TestDetectSteadyGrowth checks that steady-growth fires on a clear upward trend.
func TestDetectSteadyGrowth(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	metric := "app.daily.mrr"
	// 60 days of linearly increasing values: 100 + 5*day, starting 60 days ago.
	start := time.Now().UTC().AddDate(0, 0, -60)
	start = time.Date(start.Year(), start.Month(), start.Day(), 12, 0, 0, 0, time.UTC)
	for day := 0; day < 60; day++ {
		at := start.AddDate(0, 0, day)
		insertSample(t, cat, metric, at, 100.0+5.0*float64(day))
	}

	patterns, err := cat.DetectPatterns(ctx, metric)
	if err != nil {
		t.Fatalf("detect patterns: %v", err)
	}

	found := false
	for _, p := range patterns {
		if p.Pattern == PatternSteadyGrowth {
			found = true
			if p.Confidence <= 0 || p.Confidence > 1 {
				t.Errorf("steady-growth confidence out of range: %f", p.Confidence)
			}
		}
		if p.Pattern == PatternSteadyDecline {
			t.Error("steady-decline should not fire on upward trend")
		}
	}
	if !found {
		t.Error("expected steady-growth pattern, not detected")
	}
}

// TestDetectSteadyDecline checks that steady-decline fires on a downward trend.
func TestDetectSteadyDecline(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	metric := "app.daily.churn"
	start := time.Now().UTC().AddDate(0, 0, -60)
	start = time.Date(start.Year(), start.Month(), start.Day(), 12, 0, 0, 0, time.UTC)
	for day := 0; day < 60; day++ {
		at := start.AddDate(0, 0, day)
		insertSample(t, cat, metric, at, 500.0-5.0*float64(day))
	}

	patterns, err := cat.DetectPatterns(ctx, metric)
	if err != nil {
		t.Fatalf("detect patterns: %v", err)
	}

	found := false
	for _, p := range patterns {
		if p.Pattern == PatternSteadyDecline {
			found = true
		}
		if p.Pattern == PatternSteadyGrowth {
			t.Error("steady-growth should not fire on downward trend")
		}
	}
	if !found {
		t.Error("expected steady-decline pattern, not detected")
	}
}

// TestDetectHighVolatility checks that high-volatility fires when stddev > 2x mean.
func TestDetectHighVolatility(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	metric := "app.daily.errors"
	// 28 days: 27 near-zero values + 1 large spike.
	// mean ~3.7, stddev ~17.5, ratio ~4.7 -- well above 2x.
	start := time.Now().UTC().AddDate(0, 0, -28)
	start = time.Date(start.Year(), start.Month(), start.Day(), 12, 0, 0, 0, time.UTC)
	for day := 0; day < 28; day++ {
		at := start.AddDate(0, 0, day)
		v := 0.1
		if day == 14 {
			v = 100.0
		}
		insertSample(t, cat, metric, at, v)
	}

	patterns, err := cat.DetectPatterns(ctx, metric)
	if err != nil {
		t.Fatalf("detect patterns: %v", err)
	}

	found := false
	for _, p := range patterns {
		if p.Pattern == PatternHighVolatility {
			found = true
		}
	}
	if !found {
		t.Error("expected high-volatility pattern, not detected")
	}
}

// TestDetectPatterns_TooFewSamples verifies that fewer than minPatternSamples
// returns an empty result without error.
func TestDetectPatterns_TooFewSamples(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	metric := "app.daily.sparse"
	start := time.Now().UTC().AddDate(0, 0, -10)
	start = time.Date(start.Year(), start.Month(), start.Day(), 12, 0, 0, 0, time.UTC)
	for day := 0; day < 10; day++ {
		insertSample(t, cat, metric, start.AddDate(0, 0, day), 100.0)
	}

	patterns, err := cat.DetectPatterns(ctx, metric)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(patterns) != 0 {
		t.Errorf("expected 0 patterns for sparse metric, got %d", len(patterns))
	}
}

// TestListPatterns verifies that DetectPatterns persists and ListPatterns reads back.
func TestListPatterns(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	monday := recentMonday().AddDate(0, 0, -6*7)
	metric := "app.daily.visits"
	for week := 0; week < 6; week++ {
		for day := 0; day < 7; day++ {
			at := monday.AddDate(0, 0, week*7+day)
			v := 100.0
			if at.Weekday() == time.Saturday || at.Weekday() == time.Sunday {
				v = 50.0
			}
			insertSample(t, cat, metric, at, v)
		}
	}

	if _, err := cat.DetectPatterns(ctx, metric); err != nil {
		t.Fatalf("detect: %v", err)
	}

	all, err := cat.ListPatterns(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) == 0 {
		t.Fatal("expected at least one pattern in list")
	}
	for _, p := range all {
		if p.FQNAME != metric {
			t.Errorf("unexpected metric in list: %s", p.FQNAME)
		}
		if math.IsNaN(p.Confidence) || p.Confidence <= 0 || p.Confidence > 1 {
			t.Errorf("confidence out of range for %s: %f", p.Pattern, p.Confidence)
		}
		if p.SampleCount == 0 {
			t.Errorf("sample_count should not be zero for %s", p.Pattern)
		}
	}
}
