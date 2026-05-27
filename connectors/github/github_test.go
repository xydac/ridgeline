package github_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/github"
)

// fakeTraffic returns an httptest handler that serves canned views and clones responses.
func fakeTraffic(t *testing.T, viewsEntries, clonesEntries []map[string]any, wantToken string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if wantToken != "" && auth != "Bearer "+wantToken {
			http.Error(w, `{"message":"Bad credentials"}`, http.StatusUnauthorized)
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/traffic/views"):
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"count":   100,
				"uniques": 80,
				"views":   viewsEntries,
			})
		case strings.HasSuffix(r.URL.Path, "/traffic/clones"):
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"count":   20,
				"uniques": 15,
				"clones":  clonesEntries,
			})
		default:
			http.NotFound(w, r)
		}
	}
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

func TestSpec(t *testing.T) {
	c := github.New()
	spec := c.Spec()
	if spec.Name != github.Name {
		t.Fatalf("Name: got %q", spec.Name)
	}
	if len(spec.Streams) != 2 {
		t.Fatalf("want 2 streams, got %d", len(spec.Streams))
	}
	streamNames := map[string]bool{}
	for _, s := range spec.Streams {
		streamNames[s.Name] = true
		cols := map[string]connectors.ColumnType{}
		for _, col := range s.Schema.Columns {
			cols[col.Name] = col.Type
		}
		for _, name := range []string{"date", "count", "uniques"} {
			if _, ok := cols[name]; !ok {
				t.Errorf("stream %q: schema missing column %q", s.Name, name)
			}
		}
		if cols["count"] != connectors.Int {
			t.Errorf("stream %q: count type: got %v", s.Name, cols["count"])
		}
		if cols["uniques"] != connectors.Int {
			t.Errorf("stream %q: uniques type: got %v", s.Name, cols["uniques"])
		}
		if cols["date"] != connectors.Timestamp {
			t.Errorf("stream %q: date type: got %v", s.Name, cols["date"])
		}
	}
	if !streamNames[github.StreamViews] {
		t.Errorf("missing stream %q", github.StreamViews)
	}
	if !streamNames[github.StreamClones] {
		t.Errorf("missing stream %q", github.StreamClones)
	}
}

func TestValidate_MissingOwner(t *testing.T) {
	c := github.New()
	err := c.Validate(context.Background(), connectors.ConnectorConfig{
		"repo": "widgets", "api_token": "tok",
	})
	if err == nil || !strings.Contains(err.Error(), "owner") {
		t.Fatalf("expected owner error, got %v", err)
	}
}

func TestValidate_MissingRepo(t *testing.T) {
	c := github.New()
	err := c.Validate(context.Background(), connectors.ConnectorConfig{
		"owner": "acme", "api_token": "tok",
	})
	if err == nil || !strings.Contains(err.Error(), "repo") {
		t.Fatalf("expected repo error, got %v", err)
	}
}

func TestValidate_MissingToken(t *testing.T) {
	c := github.New()
	err := c.Validate(context.Background(), connectors.ConnectorConfig{
		"owner": "acme", "repo": "widgets",
	})
	if err == nil || !strings.Contains(err.Error(), "api_token") {
		t.Fatalf("expected api_token error, got %v", err)
	}
}

func TestValidate_UnknownKey(t *testing.T) {
	c := github.New()
	err := c.Validate(context.Background(), connectors.ConnectorConfig{
		"owner": "acme", "repo": "widgets", "api_token": "tok", "typo": "x",
	})
	if err == nil || !strings.Contains(err.Error(), "typo") {
		t.Fatalf("expected unknown-key error, got %v", err)
	}
}

func TestExtract_ViewsAndClones(t *testing.T) {
	viewsData := []map[string]any{
		{"timestamp": "2026-05-20T00:00:00Z", "count": 42, "uniques": 30},
		{"timestamp": "2026-05-21T00:00:00Z", "count": 55, "uniques": 40},
	}
	clonesData := []map[string]any{
		{"timestamp": "2026-05-20T00:00:00Z", "count": 5, "uniques": 4},
	}

	srv := httptest.NewServer(fakeTraffic(t, viewsData, clonesData, "mytoken"))
	defer srv.Close()

	c := &github.Connector{Client: srv.Client()}
	cfg := connectors.ConnectorConfig{
		"base_url":  srv.URL,
		"owner":     "acme",
		"repo":      "widgets",
		"api_token": "mytoken",
	}
	streams := []connectors.Stream{
		{Name: github.StreamViews, Mode: connectors.Incremental},
		{Name: github.StreamClones, Mode: connectors.Incremental},
	}
	ch, err := c.Extract(context.Background(), cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, states, logs, _ := collect(t, ch)
	if len(logs) > 0 {
		t.Errorf("unexpected log messages: %v", logs)
	}
	// 2 views + 1 clone
	if len(records) != 3 {
		t.Fatalf("records: want 3, got %d", len(records))
	}
	// One state per stream.
	if len(states) != 2 {
		t.Fatalf("states: want 2, got %d", len(states))
	}

	// First two records are views.
	if records[0].Stream != github.StreamViews {
		t.Errorf("record[0] stream: got %q", records[0].Stream)
	}
	wantTS := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	if !records[0].Timestamp.Equal(wantTS) {
		t.Errorf("record[0] timestamp: want %v, got %v", wantTS, records[0].Timestamp)
	}
	if v, _ := records[0].Data["count"].(int64); v != 42 {
		t.Errorf("record[0] count: want 42, got %v", records[0].Data["count"])
	}
	if v, _ := records[0].Data["uniques"].(int64); v != 30 {
		t.Errorf("record[0] uniques: want 30, got %v", records[0].Data["uniques"])
	}

	// State for views stream should carry the high-water cursor.
	viewsState := states[0]
	if viewsState["views_cursor"] != "2026-05-21T00:00:00Z" {
		t.Errorf("views_cursor: got %v", viewsState["views_cursor"])
	}
}

func TestExtract_IncrementalCursor(t *testing.T) {
	viewsData := []map[string]any{
		{"timestamp": "2026-05-19T00:00:00Z", "count": 10, "uniques": 8},
		{"timestamp": "2026-05-20T00:00:00Z", "count": 20, "uniques": 15},
	}

	srv := httptest.NewServer(fakeTraffic(t, viewsData, nil, ""))
	defer srv.Close()

	c := &github.Connector{Client: srv.Client()}
	cfg := connectors.ConnectorConfig{
		"base_url":  srv.URL,
		"owner":     "acme",
		"repo":      "widgets",
		"api_token": "tok",
	}
	// Cursor already at 2026-05-19; only the 20th should be emitted.
	state := connectors.State{"views_cursor": "2026-05-19T00:00:00Z"}
	streams := []connectors.Stream{{Name: github.StreamViews, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, states, _, _ := collect(t, ch)
	if len(records) != 1 {
		t.Fatalf("records: want 1, got %d", len(records))
	}
	wantTS := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	if !records[0].Timestamp.Equal(wantTS) {
		t.Errorf("timestamp: want %v, got %v", wantTS, records[0].Timestamp)
	}
	if len(states) == 0 || states[len(states)-1]["views_cursor"] != "2026-05-20T00:00:00Z" {
		t.Errorf("state: got %v", states)
	}
}

func TestExtract_AlreadyUpToDate(t *testing.T) {
	viewsData := []map[string]any{
		{"timestamp": "2026-05-19T00:00:00Z", "count": 10, "uniques": 8},
	}

	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		fakeTraffic(t, viewsData, nil, "")(w, r)
	}))
	defer srv.Close()

	c := &github.Connector{Client: srv.Client()}
	cfg := connectors.ConnectorConfig{
		"base_url":  srv.URL,
		"owner":     "acme",
		"repo":      "widgets",
		"api_token": "tok",
	}
	// Cursor is at the only available date - nothing new to emit.
	state := connectors.State{"views_cursor": "2026-05-19T00:00:00Z"}
	streams := []connectors.Stream{{Name: github.StreamViews, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, _, _, _ := collect(t, ch)
	// HTTP was still called (GitHub does not support date filtering) but no
	// records should be emitted because all are at or before the cursor.
	_ = called
	if len(records) != 0 {
		t.Errorf("expected no records, got %d", len(records))
	}
}

func TestExtract_Unauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"message":"Bad credentials"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := &github.Connector{Client: srv.Client()}
	cfg := connectors.ConnectorConfig{
		"base_url":  srv.URL,
		"owner":     "acme",
		"repo":      "widgets",
		"api_token": "badtoken",
	}
	streams := []connectors.Stream{{Name: github.StreamViews, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract returned error: %v", err)
	}
	_, _, _, fatal := collect(t, ch)
	if fatal == nil {
		t.Fatal("expected ErrorMsg for 401, got nil")
	}
	if !strings.Contains(fatal.Error(), "401") && !strings.Contains(fatal.Error(), "Unauthorized") {
		t.Errorf("unexpected error: %v", fatal)
	}
}

func TestExtract_Forbidden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"message":"Must have push access to repository"}`, http.StatusForbidden)
	}))
	defer srv.Close()

	c := &github.Connector{Client: srv.Client()}
	cfg := connectors.ConnectorConfig{
		"base_url":  srv.URL,
		"owner":     "acme",
		"repo":      "widgets",
		"api_token": "tok",
	}
	streams := []connectors.Stream{{Name: github.StreamViews, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract returned error: %v", err)
	}
	_, _, _, fatal := collect(t, ch)
	if fatal == nil {
		t.Fatal("expected ErrorMsg for 403, got nil")
	}
	if !strings.Contains(fatal.Error(), "403") && !strings.Contains(fatal.Error(), "Forbidden") {
		t.Errorf("unexpected error: %v", fatal)
	}
}

func TestExtract_UnknownStream(t *testing.T) {
	c := github.New()
	cfg := connectors.ConnectorConfig{
		"owner":     "acme",
		"repo":      "widgets",
		"api_token": "tok",
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
