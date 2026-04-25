package plausible_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/plausible"
)

// fakeTimeseries returns a handler that serves a canned timeseries response.
func fakeTimeseries(t *testing.T, rows []map[string]any, wantToken string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/v1/stats/timeseries") {
			http.NotFound(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		if wantToken != "" && auth != "Bearer "+wantToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"results": rows})
	}
}

func collect(t *testing.T, ch <-chan connectors.Message) (records []connectors.Record, states []connectors.State, logs []string) {
	t.Helper()
	for m := range ch {
		switch m.Type {
		case connectors.RecordMsg:
			records = append(records, *m.Record)
		case connectors.StateMsg:
			states = append(states, *m.State)
		case connectors.LogMsg:
			logs = append(logs, m.Log.Message)
		}
	}
	return
}

func TestSpec(t *testing.T) {
	c := plausible.New()
	spec := c.Spec()
	if spec.Name != plausible.Name {
		t.Fatalf("Name: got %q", spec.Name)
	}
	if len(spec.Streams) == 0 {
		t.Fatal("Spec: no streams")
	}
	ts := spec.Streams[0]
	if ts.Name != plausible.StreamTimeseries {
		t.Fatalf("stream name: got %q", ts.Name)
	}
	cols := map[string]connectors.ColumnType{}
	for _, col := range ts.Schema.Columns {
		cols[col.Name] = col.Type
	}
	for _, name := range []string{"date", "visitors", "pageviews", "bounce_rate", "visit_duration"} {
		if _, ok := cols[name]; !ok {
			t.Errorf("schema missing column %q", name)
		}
	}
	if cols["visitors"] != connectors.Int {
		t.Errorf("visitors type: got %v", cols["visitors"])
	}
	if cols["bounce_rate"] != connectors.Float {
		t.Errorf("bounce_rate type: got %v", cols["bounce_rate"])
	}
	if cols["date"] != connectors.Timestamp {
		t.Errorf("date type: got %v", cols["date"])
	}
}

func TestValidate_MissingSiteID(t *testing.T) {
	c := plausible.New()
	err := c.Validate(context.Background(), connectors.ConnectorConfig{
		"api_token": "tok",
	})
	if err == nil || !strings.Contains(err.Error(), "site_id") {
		t.Fatalf("expected site_id error, got %v", err)
	}
}

func TestValidate_MissingToken(t *testing.T) {
	c := plausible.New()
	err := c.Validate(context.Background(), connectors.ConnectorConfig{
		"site_id": "example.com",
	})
	if err == nil || !strings.Contains(err.Error(), "api_token") {
		t.Fatalf("expected api_token error, got %v", err)
	}
}

func TestValidate_UnknownKey(t *testing.T) {
	c := plausible.New()
	err := c.Validate(context.Background(), connectors.ConnectorConfig{
		"site_id":   "example.com",
		"api_token": "tok",
		"typo_key":  "val",
	})
	if err == nil || !strings.Contains(err.Error(), "typo_key") {
		t.Fatalf("expected unknown-key error, got %v", err)
	}
}

func TestExtract_BasicTimeseries(t *testing.T) {
	rows := []map[string]any{
		{"date": "2026-04-20", "visitors": 100, "pageviews": 150, "bounce_rate": 45.2, "visit_duration": 120.5},
		{"date": "2026-04-21", "visitors": 80, "pageviews": 110, "bounce_rate": 50.0, "visit_duration": 90.0},
	}
	srv := httptest.NewServer(fakeTimeseries(t, rows, "mytoken"))
	defer srv.Close()

	pinned := time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC)
	c := &plausible.Connector{
		Client: srv.Client(),
		Now:    func() time.Time { return pinned },
	}

	cfg := connectors.ConnectorConfig{
		"base_url":  srv.URL,
		"site_id":   "example.com",
		"api_token": "mytoken",
	}
	streams := []connectors.Stream{{Name: plausible.StreamTimeseries, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, states, logs := collect(t, ch)
	if len(logs) > 0 {
		t.Errorf("unexpected log messages: %v", logs)
	}
	if len(records) != 2 {
		t.Fatalf("records: want 2, got %d", len(records))
	}
	if len(states) == 0 {
		t.Fatal("no state emitted")
	}

	r0 := records[0]
	if r0.Stream != plausible.StreamTimeseries {
		t.Errorf("stream: got %q", r0.Stream)
	}
	wantTS := time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC)
	if !r0.Timestamp.Equal(wantTS) {
		t.Errorf("timestamp: want %v, got %v", wantTS, r0.Timestamp)
	}
	if v, _ := r0.Data["visitors"].(int64); v != 100 {
		t.Errorf("visitors: want 100, got %v", r0.Data["visitors"])
	}
	if v, _ := r0.Data["bounce_rate"].(float64); v != 45.2 {
		t.Errorf("bounce_rate: want 45.2, got %v", v)
	}

	// State should carry the high-water date.
	last := states[len(states)-1]
	if last["last_date"] != "2026-04-21" {
		t.Errorf("state last_date: got %v", last["last_date"])
	}
}

func TestExtract_IncrementalCursor(t *testing.T) {
	var capturedQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{"date": "2026-04-22", "visitors": 42, "pageviews": 60, "bounce_rate": 38.0, "visit_duration": 200.0},
			},
		})
	}))
	defer srv.Close()

	pinned := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)
	c := &plausible.Connector{
		Client: srv.Client(),
		Now:    func() time.Time { return pinned },
	}
	cfg := connectors.ConnectorConfig{
		"base_url":  srv.URL,
		"site_id":   "example.com",
		"api_token": "tok",
	}
	state := connectors.State{"last_date": "2026-04-21"}
	streams := []connectors.Stream{{Name: plausible.StreamTimeseries, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, states, _ := collect(t, ch)
	if len(records) != 1 {
		t.Fatalf("records: want 1, got %d", len(records))
	}
	if len(states) == 0 || states[len(states)-1]["last_date"] != "2026-04-22" {
		t.Errorf("state: got %v", states)
	}
	// The request date range should start one day after the cursor.
	if !strings.Contains(capturedQuery, "2026-04-22%2C2026-04-23") &&
		!strings.Contains(capturedQuery, "2026-04-22,2026-04-23") {
		t.Errorf("unexpected date range in query: %s", capturedQuery)
	}
}

func TestExtract_AlreadyUpToDate(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "should not be called", http.StatusInternalServerError)
	}))
	defer srv.Close()

	// Cursor is yesterday -> nothing new to fetch.
	pinned := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)
	c := &plausible.Connector{
		Client: srv.Client(),
		Now:    func() time.Time { return pinned },
	}
	cfg := connectors.ConnectorConfig{
		"base_url":  srv.URL,
		"site_id":   "example.com",
		"api_token": "tok",
	}
	state := connectors.State{"last_date": "2026-04-23"} // yesterday
	streams := []connectors.Stream{{Name: plausible.StreamTimeseries, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, _, _ := collect(t, ch)
	if called {
		t.Error("HTTP server was called but cursor was already at yesterday")
	}
	if len(records) != 0 {
		t.Errorf("expected no records, got %d", len(records))
	}
}

func TestExtract_Unauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid token", http.StatusUnauthorized)
	}))
	defer srv.Close()

	pinned := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)
	c := &plausible.Connector{
		Client: srv.Client(),
		Now:    func() time.Time { return pinned },
	}
	cfg := connectors.ConnectorConfig{
		"base_url":  srv.URL,
		"site_id":   "example.com",
		"api_token": "badtoken",
	}
	streams := []connectors.Stream{{Name: plausible.StreamTimeseries, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract returned error: %v", err)
	}
	_, _, logs := collect(t, ch)
	if len(logs) == 0 {
		t.Fatal("expected error log for 401")
	}
	if !strings.Contains(logs[0], "401") && !strings.Contains(logs[0], "Unauthorized") {
		t.Errorf("unexpected error log: %s", logs[0])
	}
}

func TestExtract_UnknownStream(t *testing.T) {
	c := plausible.New()
	cfg := connectors.ConnectorConfig{
		"base_url":  "https://plausible.io",
		"site_id":   "example.com",
		"api_token": "tok",
	}
	streams := []connectors.Stream{{Name: "nosuchstream"}}
	ch, err := c.Extract(context.Background(), cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	_, _, logs := collect(t, ch)
	if len(logs) == 0 || !strings.Contains(logs[0], "nosuchstream") {
		t.Errorf("expected unknown-stream log, got %v", logs)
	}
}
