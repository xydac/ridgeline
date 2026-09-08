package memory

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAlertChannel_AddList(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	id, err := cat.AddAlertChannel(ctx, "my-webhook", "webhook", "https://example.com/hook")
	if err != nil {
		t.Fatalf("AddAlertChannel: %v", err)
	}
	if id <= 0 {
		t.Errorf("expected positive id, got %d", id)
	}

	channels, err := cat.ListAlertChannels(ctx)
	if err != nil {
		t.Fatalf("ListAlertChannels: %v", err)
	}
	if len(channels) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(channels))
	}
	ch := channels[0]
	if ch.Name != "my-webhook" {
		t.Errorf("name: got %q, want %q", ch.Name, "my-webhook")
	}
	if ch.Kind != "webhook" {
		t.Errorf("kind: got %q, want %q", ch.Kind, "webhook")
	}
	if ch.LastDeliveredAt != nil {
		t.Error("expected LastDeliveredAt to be nil on new channel")
	}
}

func TestAlertChannel_AddDuplicateName(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	_, _ = cat.AddAlertChannel(ctx, "dup", "stderr", "")
	_, err := cat.AddAlertChannel(ctx, "dup", "file", "/tmp/x")
	if err == nil {
		t.Error("expected error on duplicate name, got nil")
	}
}

func TestAlertChannel_UnknownKind(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	_, err := cat.AddAlertChannel(ctx, "bad", "email", "user@example.com")
	if err == nil {
		t.Error("expected error for unknown kind, got nil")
	}
}

func TestAlertChannel_Delete(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	_, _ = cat.AddAlertChannel(ctx, "temp", "stderr", "")

	if err := cat.DeleteAlertChannel(ctx, "temp"); err != nil {
		t.Fatalf("DeleteAlertChannel: %v", err)
	}
	channels, _ := cat.ListAlertChannels(ctx)
	if len(channels) != 0 {
		t.Errorf("expected 0 channels after delete, got %d", len(channels))
	}
}

func TestAlertChannel_DeleteNotFound(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)
	if err := cat.DeleteAlertChannel(ctx, "ghost"); err == nil {
		t.Error("expected error deleting non-existent channel")
	}
}

// seedEvent inserts a bm_events row directly and returns its id.
func seedEvent(t *testing.T, cat *Catalog, kind, metricFQ, desc string) int64 {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := cat.db.ExecContext(ctx, `
INSERT INTO bm_events (kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, description, at)
VALUES (?, ?, 0, 0, 0, 'none', 0, ?, ?)`, kind, metricFQ, desc, now)
	if err != nil {
		t.Fatalf("seedEvent: %v", err)
	}
	id, _ := res.LastInsertId()
	return id
}

func TestUndeliveredEvents_FiltersDelivered(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	id1 := seedEvent(t, cat, "anomaly", "myapp.demo.pv.visitors", "event1")
	id2 := seedEvent(t, cat, "monitor", "myapp.demo.pv.visitors", "event2")

	chID, _ := cat.AddAlertChannel(ctx, "ch", "stderr", "")

	// Before recording any deliveries, both events should be undelivered.
	events, err := cat.UndeliveredEvents(ctx, 24*time.Hour, chID)
	if err != nil {
		t.Fatalf("UndeliveredEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 undelivered, got %d", len(events))
	}

	// Record delivery of event1.
	if err := cat.RecordDelivery(ctx, id1, chID); err != nil {
		t.Fatalf("RecordDelivery: %v", err)
	}

	// Now only id2 should be undelivered.
	events, err = cat.UndeliveredEvents(ctx, 24*time.Hour, chID)
	if err != nil {
		t.Fatalf("UndeliveredEvents after one delivery: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 undelivered after recording id1, got %d", len(events))
	}
	if events[0].ID != id2 {
		t.Errorf("expected event id2=%d, got %d", id2, events[0].ID)
	}
}

func TestRecordDelivery_Idempotent(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	evID := seedEvent(t, cat, "test", "myapp.demo.pv.v", "idempotency test")
	chID, _ := cat.AddAlertChannel(ctx, "ch", "stderr", "")

	// Recording the same delivery twice must not error.
	if err := cat.RecordDelivery(ctx, evID, chID); err != nil {
		t.Fatalf("first RecordDelivery: %v", err)
	}
	if err := cat.RecordDelivery(ctx, evID, chID); err != nil {
		t.Fatalf("second RecordDelivery (idempotent): %v", err)
	}

	// Should still show as delivered (0 undelivered).
	events, _ := cat.UndeliveredEvents(ctx, 24*time.Hour, chID)
	if len(events) != 0 {
		t.Errorf("expected 0 undelivered, got %d", len(events))
	}
}

func TestRecordDelivery_UpdatesLastDeliveredAt(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	evID := seedEvent(t, cat, "test", "myapp.demo.m.v", "update last delivered")
	chID, _ := cat.AddAlertChannel(ctx, "ch2", "stderr", "")

	before, _ := cat.ListAlertChannels(ctx)
	if before[0].LastDeliveredAt != nil {
		t.Error("expected nil LastDeliveredAt before delivery")
	}

	if err := cat.RecordDelivery(ctx, evID, chID); err != nil {
		t.Fatalf("RecordDelivery: %v", err)
	}

	after, _ := cat.ListAlertChannels(ctx)
	if after[0].LastDeliveredAt == nil {
		t.Error("expected non-nil LastDeliveredAt after delivery")
	}
}

func TestDeliverToChannel_TwoChannelsSameEvent(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	var count1, count2 atomic.Int32
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count1.Add(1)
	}))
	defer srv1.Close()
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count2.Add(1)
	}))
	defer srv2.Close()

	ch1ID, _ := cat.AddAlertChannel(ctx, "hook1", "webhook", srv1.URL)
	ch2ID, _ := cat.AddAlertChannel(ctx, "hook2", "webhook", srv2.URL)

	evID := seedEvent(t, cat, "anomaly", "myapp.demo.m.v", "two channels")

	channels, _ := cat.ListAlertChannels(ctx)
	ev := EventRow{ID: evID, Kind: "anomaly", MetricFQ: "myapp.demo.m.v", Description: "two channels", At: time.Now()}

	for _, ch := range channels {
		if err := DeliverToChannel(ch, ev); err != nil {
			t.Errorf("DeliverToChannel %q: %v", ch.Name, err)
		}
		if err := cat.RecordDelivery(ctx, evID, ch.ID); err != nil {
			t.Errorf("RecordDelivery %q: %v", ch.Name, err)
		}
	}
	_ = ch1ID
	_ = ch2ID

	if count1.Load() != 1 {
		t.Errorf("hook1 received %d requests, want 1", count1.Load())
	}
	if count2.Load() != 1 {
		t.Errorf("hook2 received %d requests, want 1", count2.Load())
	}

	// Both channels should now show no undelivered events.
	for _, ch := range channels {
		events, _ := cat.UndeliveredEvents(ctx, 24*time.Hour, ch.ID)
		if len(events) != 0 {
			t.Errorf("channel %q: expected 0 undelivered, got %d", ch.Name, len(events))
		}
	}
}

func TestDeliverToChannel_WebhookPayload(t *testing.T) {
	var received AlertPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	ch := AlertChannel{ID: 1, Name: "hook", Kind: "webhook", Target: srv.URL}
	ev := EventRow{
		ID:            42,
		Kind:          "anomaly",
		MetricFQ:      "myapp.demo.pv.visitors",
		Description:   "spike detected",
		Direction:     "surprise-bad",
		ObservedValue: 9000,
		At:            time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
	}

	if err := DeliverToChannel(ch, ev); err != nil {
		t.Fatalf("DeliverToChannel: %v", err)
	}
	if received.EventID != 42 {
		t.Errorf("payload event_id: got %d, want 42", received.EventID)
	}
	if received.MetricFQ != "myapp.demo.pv.visitors" {
		t.Errorf("payload metric_fq: got %q", received.MetricFQ)
	}
}

func TestSyntheticEvent(t *testing.T) {
	ev := SyntheticEvent()
	if ev.Kind != "test" {
		t.Errorf("expected kind=test, got %q", ev.Kind)
	}
	if ev.MetricFQ == "" {
		t.Error("expected non-empty MetricFQ")
	}
}
