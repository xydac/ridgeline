package memory

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParseCondition_valid(t *testing.T) {
	tests := []struct {
		expr     string
		wantOp   string
		wantVal  float64
		wantUnit string
	}{
		{"above 1000", "above", 1000, ""},
		{"below 500", "below", 500, ""},
		{"deviates-by 2.5sigma", "deviates-by", 2.5, "sigma"},
		{"deviates-by 3", "deviates-by", 3, ""},
		{"above 0", "above", 0, ""},
	}
	for _, tc := range tests {
		op, val, unit, err := ParseCondition(tc.expr)
		if err != nil {
			t.Errorf("ParseCondition(%q) unexpected error: %v", tc.expr, err)
			continue
		}
		if op != tc.wantOp || val != tc.wantVal || unit != tc.wantUnit {
			t.Errorf("ParseCondition(%q) = (%q, %v, %q), want (%q, %v, %q)",
				tc.expr, op, val, unit, tc.wantOp, tc.wantVal, tc.wantUnit)
		}
	}
}

func TestParseCondition_invalid(t *testing.T) {
	bad := []string{
		"",
		"above",
		"above 1000 extra",
		"sideways 100",
		"above -1",
		"above NaN",
		"below 500sigma",
	}
	for _, expr := range bad {
		_, _, _, err := ParseCondition(expr)
		if err == nil {
			t.Errorf("ParseCondition(%q) expected error, got nil", expr)
		}
	}
}

func TestAddWatch_and_ListWatches(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	if err := cat.AddWatch(ctx, "visitors-low", "plausible.daily.visitors", "below 500"); err != nil {
		t.Fatalf("AddWatch: %v", err)
	}
	if err := cat.AddWatch(ctx, "bounce-spike", "plausible.daily.bounce_rate", "deviates-by 3sigma"); err != nil {
		t.Fatalf("AddWatch: %v", err)
	}

	watches, err := cat.ListWatches(ctx)
	if err != nil {
		t.Fatalf("ListWatches: %v", err)
	}
	if len(watches) != 2 {
		t.Fatalf("want 2 watches, got %d", len(watches))
	}
	// Ordered by name.
	if watches[0].Name != "bounce-spike" || watches[1].Name != "visitors-low" {
		t.Errorf("unexpected order: %v, %v", watches[0].Name, watches[1].Name)
	}
	if watches[1].Op != "below" || watches[1].Threshold != 500 {
		t.Errorf("unexpected watch: op=%q threshold=%v", watches[1].Op, watches[1].Threshold)
	}
}

func TestAddWatch_duplicate_name(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	if err := cat.AddWatch(ctx, "dup", "m.s.v", "above 100"); err != nil {
		t.Fatalf("first AddWatch: %v", err)
	}
	if err := cat.AddWatch(ctx, "dup", "m.s.v", "above 200"); err == nil {
		t.Fatal("expected error for duplicate name, got nil")
	}
}

func TestRemoveWatch(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	if err := cat.AddWatch(ctx, "w1", "m.s.v", "above 10"); err != nil {
		t.Fatalf("AddWatch: %v", err)
	}
	if err := cat.RemoveWatch(ctx, "w1"); err != nil {
		t.Fatalf("RemoveWatch: %v", err)
	}
	watches, _ := cat.ListWatches(ctx)
	if len(watches) != 0 {
		t.Errorf("want 0 watches after rm, got %d", len(watches))
	}
	// Remove non-existent.
	if err := cat.RemoveWatch(ctx, "w1"); err == nil {
		t.Fatal("expected error removing non-existent watch")
	}
}

func TestRunWatches_above_triggers(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	// Seed metric with last_value.
	if err := cat.UpsertMetric(ctx, "app.daily.revenue", "", "higher_is_better", "sum", ptr(2500.0)); err != nil {
		t.Fatalf("UpsertMetric: %v", err)
	}
	if err := cat.AddWatch(ctx, "revenue-high", "app.daily.revenue", "above 2000"); err != nil {
		t.Fatalf("AddWatch: %v", err)
	}

	res, err := cat.RunWatches(ctx)
	if err != nil {
		t.Fatalf("RunWatches: %v", err)
	}
	if res.Evaluated != 1 {
		t.Errorf("want Evaluated=1, got %d", res.Evaluated)
	}
	if len(res.Triggered) != 1 {
		t.Fatalf("want 1 triggered, got %d", len(res.Triggered))
	}
	trig := res.Triggered[0]
	if trig.WatchName != "revenue-high" || trig.Value != 2500 {
		t.Errorf("unexpected trigger: %+v", trig)
	}
}

func TestRunWatches_below_no_trigger(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	if err := cat.UpsertMetric(ctx, "app.daily.errors", "", "lower_is_better", "sum", ptr(5.0)); err != nil {
		t.Fatalf("UpsertMetric: %v", err)
	}
	if err := cat.AddWatch(ctx, "errors-low", "app.daily.errors", "below 3"); err != nil {
		t.Fatalf("AddWatch: %v", err)
	}

	res, err := cat.RunWatches(ctx)
	if err != nil {
		t.Fatalf("RunWatches: %v", err)
	}
	if len(res.Triggered) != 0 {
		t.Errorf("want 0 triggered (value 5 is not below 3), got %d", len(res.Triggered))
	}
}

func TestRunWatches_deviates_by_sigma(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	// Insert baseline: mean=1000, stddev=100.
	_, err := cat.db.ExecContext(ctx, `
INSERT INTO bm_baselines (fq_name, window_days, mean, stddev, min, max, sample_count, last_computed_at)
VALUES ('app.daily.visitors', 30, 1000.0, 100.0, 800.0, 1200.0, 30, ?)`, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insert baseline: %v", err)
	}

	// last_value = 1350 -> deviation = (1350-1000)/100 = 3.5 sigma.
	if err := cat.UpsertMetric(ctx, "app.daily.visitors", "", "higher_is_better", "sum", ptr(1350.0)); err != nil {
		t.Fatalf("UpsertMetric: %v", err)
	}
	if err := cat.AddWatch(ctx, "big-deviation", "app.daily.visitors", "deviates-by 3sigma"); err != nil {
		t.Fatalf("AddWatch: %v", err)
	}

	res, err := cat.RunWatches(ctx)
	if err != nil {
		t.Fatalf("RunWatches: %v", err)
	}
	if len(res.Triggered) != 1 {
		t.Fatalf("want 1 triggered, got %d", len(res.Triggered))
	}
	if res.Triggered[0].Deviation < 3.0 {
		t.Errorf("want deviation >= 3, got %v", res.Triggered[0].Deviation)
	}
}

func TestRunWatches_records_event(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	if err := cat.UpsertMetric(ctx, "app.daily.sessions", "", "higher_is_better", "sum", ptr(50.0)); err != nil {
		t.Fatalf("UpsertMetric: %v", err)
	}
	if err := cat.AddWatch(ctx, "sessions-above", "app.daily.sessions", "above 10"); err != nil {
		t.Fatalf("AddWatch: %v", err)
	}

	_, err := cat.RunWatches(ctx)
	if err != nil {
		t.Fatalf("RunWatches: %v", err)
	}

	// Verify event was recorded in bm_events.
	events, err := cat.ListEvents(ctx, 0)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	var found bool
	for _, e := range events {
		if e.Kind == "monitor" && strings.Contains(e.Description, "sessions-above") {
			found = true
		}
	}
	if !found {
		t.Error("expected monitor event in bm_events, not found")
	}
}

func TestRunWatches_no_data_skips(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	// Watch on a metric that has no last_value -- should not error.
	if err := cat.AddWatch(ctx, "ghost", "nonexistent.metric.visitors", "above 100"); err != nil {
		t.Fatalf("AddWatch: %v", err)
	}

	res, err := cat.RunWatches(ctx)
	if err != nil {
		t.Fatalf("RunWatches: %v", err)
	}
	if len(res.Triggered) != 0 {
		t.Errorf("want 0 triggered for metric with no data, got %d", len(res.Triggered))
	}
}

func TestToMonitorRunJSON(t *testing.T) {
	now := time.Now().UTC()
	r := &WatchRunResult{
		Evaluated: 3,
		Triggered: []WatchTrigger{
			{WatchName: "w1", MetricFQ: "m.s.v", Condition: "above 100", Value: 150, At: now},
		},
	}
	j := ToMonitorRunJSON(r)
	if j.Evaluated != 3 || len(j.Triggered) != 1 {
		t.Errorf("unexpected JSON: %+v", j)
	}
	if j.Triggered[0].WatchName != "w1" || j.Triggered[0].Value != 150 {
		t.Errorf("unexpected trigger JSON: %+v", j.Triggered[0])
	}
}
