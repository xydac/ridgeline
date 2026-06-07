package posthog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/posthog"
)

// newPaginatedServer creates an httptest server that serves pages of events,
// injecting the server URL into "next" links for pagination tests.
func newPaginatedServer(t *testing.T, pages [][]map[string]any, wantToken string) *httptest.Server {
	t.Helper()
	// We need the server URL in the handler, but we don't have it until
	// after creating the server. Use an indirect approach: create a
	// closure over a pointer that we fill after creation.
	var srv *httptest.Server
	callCount := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/events") {
			http.NotFound(w, r)
			return
		}
		if wantToken != "" {
			auth := r.Header.Get("Authorization")
			if auth != "Bearer "+wantToken {
				http.Error(w, `{"detail":"bad token"}`, http.StatusUnauthorized)
				return
			}
		}
		idx := callCount
		callCount++
		if idx >= len(pages) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "next": nil})
			return
		}
		var nextPtr *string
		if idx+1 < len(pages) {
			next := srv.URL + fmt.Sprintf("/api/projects/123/events/?page=%d", idx+2)
			nextPtr = &next
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"results": pages[idx],
			"next":    nextPtr,
		})
	})
	srv = httptest.NewServer(handler)
	return srv
}

func collect(t *testing.T, ch <-chan connectors.Message) (records []connectors.Record, states []connectors.State, logs []string, fatal error) {
	t.Helper()
	for m := range ch {
		switch m.Type {
		case connectors.RecordMsg:
			records = append(records, *m.Record)
		case connectors.StateMsg:
			states = append(states, *m.State)
		case connectors.LogMsg:
			logs = append(logs, m.Log.Message)
		case connectors.ErrorMsg:
			fatal = m.Err
		}
	}
	return
}

func makeEvent(id, event, distinctID, timestamp string) map[string]any {
	return map[string]any{
		"id":          id,
		"event":       event,
		"distinct_id": distinctID,
		"timestamp":   timestamp,
	}
}

func TestSpec(t *testing.T) {
	c := posthog.New()
	spec := c.Spec()
	if spec.Name != posthog.Name {
		t.Fatalf("Name: got %q", spec.Name)
	}
	if len(spec.Streams) == 0 {
		t.Fatal("Spec: no streams")
	}
	s := spec.Streams[0]
	if s.Name != posthog.StreamEvents {
		t.Fatalf("stream name: got %q", s.Name)
	}
	cols := map[string]connectors.ColumnType{}
	for _, col := range s.Schema.Columns {
		cols[col.Name] = col.Type
	}
	for _, name := range []string{"timestamp", "event", "distinct_id"} {
		if _, ok := cols[name]; !ok {
			t.Errorf("schema missing column %q", name)
		}
	}
	if cols["timestamp"] != connectors.Timestamp {
		t.Errorf("timestamp type: got %v", cols["timestamp"])
	}
	if cols["event"] != connectors.String {
		t.Errorf("event type: got %v", cols["event"])
	}
}

func TestValidate_MissingProjectID(t *testing.T) {
	c := posthog.New()
	err := c.Validate(context.Background(), connectors.ConnectorConfig{
		"api_key": "phk_test",
	})
	if err == nil || !strings.Contains(err.Error(), "project_id") {
		t.Fatalf("expected project_id error, got %v", err)
	}
}

func TestValidate_MissingAPIKey(t *testing.T) {
	c := posthog.New()
	err := c.Validate(context.Background(), connectors.ConnectorConfig{
		"project_id": "123",
	})
	if err == nil || !strings.Contains(err.Error(), "api_key") {
		t.Fatalf("expected api_key error, got %v", err)
	}
}

func TestValidate_UnknownKey(t *testing.T) {
	c := posthog.New()
	err := c.Validate(context.Background(), connectors.ConnectorConfig{
		"project_id": "123",
		"api_key":    "phk_test",
		"typo_key":   "val",
	})
	if err == nil || !strings.Contains(err.Error(), "typo_key") {
		t.Fatalf("expected unknown-key error, got %v", err)
	}
}

func TestExtract_BasicEvents(t *testing.T) {
	pages := [][]map[string]any{
		{
			makeEvent("e2", "$pageview", "user2", "2026-06-06T10:00:00Z"),
			makeEvent("e1", "$identify", "user1", "2026-06-06T09:00:00Z"),
		},
	}
	srv := newPaginatedServer(t, pages, "phk_token")
	defer srv.Close()

	c := &posthog.Connector{Client: srv.Client()}
	cfg := connectors.ConnectorConfig{
		"base_url":   srv.URL,
		"project_id": "123",
		"api_key":    "phk_token",
	}
	streams := []connectors.Stream{{Name: posthog.StreamEvents, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, states, logs, fatal := collect(t, ch)
	if fatal != nil {
		t.Fatalf("fatal error: %v", fatal)
	}
	if len(logs) > 0 {
		t.Errorf("unexpected logs: %v", logs)
	}
	if len(records) != 2 {
		t.Fatalf("records: want 2, got %d", len(records))
	}

	// Records emitted in chronological order (oldest first).
	r0 := records[0]
	if r0.Stream != posthog.StreamEvents {
		t.Errorf("stream: got %q", r0.Stream)
	}
	want0 := time.Date(2026, 6, 6, 9, 0, 0, 0, time.UTC)
	if !r0.Timestamp.Equal(want0) {
		t.Errorf("r0 timestamp: want %v, got %v", want0, r0.Timestamp)
	}
	if r0.Data["event"] != "$identify" {
		t.Errorf("r0 event: got %v", r0.Data["event"])
	}

	r1 := records[1]
	if r1.Data["event"] != "$pageview" {
		t.Errorf("r1 event: got %v", r1.Data["event"])
	}

	if len(states) == 0 {
		t.Fatal("no state emitted")
	}
	last := states[len(states)-1]
	if _, ok := last[posthog.CursorKey]; !ok {
		t.Errorf("state missing cursor key, got %v", last)
	}
}

func TestExtract_Pagination(t *testing.T) {
	pages := [][]map[string]any{
		{
			makeEvent("e3", "$pageview", "u3", "2026-06-06T12:00:00Z"),
			makeEvent("e2", "$pageview", "u2", "2026-06-06T11:00:00Z"),
		},
		{
			makeEvent("e1", "$pageview", "u1", "2026-06-06T10:00:00Z"),
		},
	}
	srv := newPaginatedServer(t, pages, "")
	defer srv.Close()

	c := &posthog.Connector{Client: srv.Client()}
	cfg := connectors.ConnectorConfig{
		"base_url":   srv.URL,
		"project_id": "123",
		"api_key":    "any",
	}
	streams := []connectors.Stream{{Name: posthog.StreamEvents, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, states, _, fatal := collect(t, ch)
	if fatal != nil {
		t.Fatalf("fatal: %v", fatal)
	}
	if len(records) != 3 {
		t.Fatalf("records: want 3, got %d", len(records))
	}
	// Emitted oldest-first: e1, e2, e3.
	wantEvents := []string{"$pageview", "$pageview", "$pageview"}
	for i, r := range records {
		if r.Data["event"] != wantEvents[i] {
			t.Errorf("records[%d].event: got %v", i, r.Data["event"])
		}
	}
	// High-water is e3 (2026-06-06T12:00:00Z).
	last := states[len(states)-1]
	cursor, _ := last[posthog.CursorKey].(string)
	if !strings.Contains(cursor, "2026-06-06T12:00:00") {
		t.Errorf("cursor: want 2026-06-06T12:..., got %q", cursor)
	}
}

func TestExtract_AlreadyUpToDate(t *testing.T) {
	pages := [][]map[string]any{{}} // empty first page
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "next": nil})
	}))
	defer srv.Close()
	_ = pages

	c := &posthog.Connector{Client: srv.Client()}
	cfg := connectors.ConnectorConfig{
		"base_url":   srv.URL,
		"project_id": "123",
		"api_key":    "tok",
	}
	state := connectors.State{posthog.CursorKey: "2026-06-06T12:00:00Z"}
	streams := []connectors.Stream{{Name: posthog.StreamEvents, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, states, _, fatal := collect(t, ch)
	if fatal != nil {
		t.Fatalf("fatal: %v", fatal)
	}
	if !called {
		t.Error("expected HTTP call for empty-results path")
	}
	if len(records) != 0 {
		t.Errorf("want 0 records, got %d", len(records))
	}
	// State re-emitted unchanged.
	if len(states) == 0 {
		t.Fatal("expected state emit on up-to-date path")
	}
}

func TestExtract_Unauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"detail":"Invalid API key"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := &posthog.Connector{Client: srv.Client()}
	cfg := connectors.ConnectorConfig{
		"base_url":   srv.URL,
		"project_id": "123",
		"api_key":    "bad",
	}
	streams := []connectors.Stream{{Name: posthog.StreamEvents, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	_, _, _, fatal := collect(t, ch)
	if fatal == nil {
		t.Fatal("expected error for 401, got nil")
	}
	if !strings.Contains(fatal.Error(), "401") && !strings.Contains(fatal.Error(), "Unauthorized") {
		t.Errorf("unexpected error: %v", fatal)
	}
}

func TestExtract_UnknownStream(t *testing.T) {
	c := posthog.New()
	cfg := connectors.ConnectorConfig{
		"base_url":   "https://app.posthog.com",
		"project_id": "123",
		"api_key":    "tok",
	}
	streams := []connectors.Stream{{Name: "nosuchstream"}}
	ch, err := c.Extract(context.Background(), cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	_, _, logs, _ := collect(t, ch)
	if len(logs) == 0 || !strings.Contains(logs[0], "nosuchstream") {
		t.Errorf("expected unknown-stream log, got %v", logs)
	}
}

func TestExtract_ContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				makeEvent("e1", "$pageview", "u1", "2026-06-06T10:00:00Z"),
			},
			"next": nil,
		})
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Extract

	c := &posthog.Connector{Client: srv.Client()}
	cfg := connectors.ConnectorConfig{
		"base_url":   srv.URL,
		"project_id": "123",
		"api_key":    "tok",
	}
	streams := []connectors.Stream{{Name: posthog.StreamEvents, Mode: connectors.Incremental}}
	ch, err := c.Extract(ctx, cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	// Drain; should not deadlock.
	for range ch {
	}
}
