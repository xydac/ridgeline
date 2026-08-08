package memory

import (
	"context"
	"testing"

	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

func openTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	store, err := sqlitestate.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return New(store.DB())
}

func ptr(f float64) *float64 { return &f }

func TestUpsertStream_idempotent(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	if err := cat.UpsertStream(ctx, "plausible", "daily", "metric", 10); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if err := cat.UpsertStream(ctx, "plausible", "daily", "metric", 15); err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if err := cat.UpsertStream(ctx, "plausible", "daily", "metric", 20); err != nil {
		t.Fatalf("third upsert: %v", err)
	}

	rows, err := cat.ListStreams(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	r := rows[0]
	if r.RowCountLifetime != 45 {
		t.Errorf("want row_count_lifetime=45, got %d", r.RowCountLifetime)
	}
	if r.Kind != "metric" {
		t.Errorf("want kind=metric, got %s", r.Kind)
	}
}

func TestUpsertStream_firstSeenPreserved(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	if err := cat.UpsertStream(ctx, "hn", "items", "event", 100); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	rows, err := cat.ListStreams(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	firstSeen := rows[0].FirstSeenAt

	if err := cat.UpsertStream(ctx, "hn", "items", "event", 50); err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	rows, err = cat.ListStreams(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if !rows[0].FirstSeenAt.Equal(firstSeen) {
		t.Errorf("first_seen_at changed: was %v, now %v", firstSeen, rows[0].FirstSeenAt)
	}
	if rows[0].LastSeenAt.Before(firstSeen) {
		t.Errorf("last_seen_at should not precede first_seen_at")
	}
}

func TestUpsertStream_multipleConnectors(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	connectors := [][3]string{
		{"plausible", "daily", "metric"},
		{"plausible", "events", "event"},
		{"hn", "items", "event"},
	}
	for _, c := range connectors {
		if err := cat.UpsertStream(ctx, c[0], c[1], c[2], 1); err != nil {
			t.Fatalf("upsert %s/%s: %v", c[0], c[1], err)
		}
	}

	rows, err := cat.ListStreams(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

func TestUpsertMetric_idempotent(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	for range 3 {
		if err := cat.UpsertMetric(ctx, "plausible.daily.visitors", "users", "higher_is_better", "sum", ptr(1234)); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	rows, err := cat.ListMetrics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	r := rows[0]
	if r.FQName != "plausible.daily.visitors" {
		t.Errorf("unexpected fq_name: %s", r.FQName)
	}
	if r.Unit != "users" {
		t.Errorf("unexpected unit: %s", r.Unit)
	}
	if r.LastValue == nil || *r.LastValue != 1234 {
		t.Errorf("unexpected last_value: %v", r.LastValue)
	}
}

func TestUpsertMetric_lastValuePreservedWhenNil(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	// First upsert sets a value.
	if err := cat.UpsertMetric(ctx, "plausible.daily.visitors", "users", "higher_is_better", "sum", ptr(999)); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	// Second upsert passes nil last_value (metadata-only update).
	if err := cat.UpsertMetric(ctx, "plausible.daily.visitors", "users", "higher_is_better", "sum", nil); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	rows, err := cat.ListMetrics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0].LastValue == nil || *rows[0].LastValue != 999 {
		t.Errorf("last_value should be preserved as 999, got %v", rows[0].LastValue)
	}
}

func TestListMetrics_order(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	names := []string{"z.metric", "a.metric", "m.metric"}
	for _, n := range names {
		if err := cat.UpsertMetric(ctx, n, "", "neutral", "none", nil); err != nil {
			t.Fatalf("upsert %s: %v", n, err)
		}
	}

	rows, err := cat.ListMetrics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []string{"a.metric", "m.metric", "z.metric"}
	for i, r := range rows {
		if r.FQName != want[i] {
			t.Errorf("row[%d]: want %s, got %s", i, want[i], r.FQName)
		}
	}
}
