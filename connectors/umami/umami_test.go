package umami_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/umami"
)

func TestRegistered(t *testing.T) {
	t.Parallel()
	if _, ok := connectors.Get(umami.Name); !ok {
		t.Fatalf("umami connector not registered")
	}
}

func TestSpec(t *testing.T) {
	t.Parallel()
	spec := umami.New().Spec()
	if spec.Name != umami.Name {
		t.Errorf("Name = %q, want %q", spec.Name, umami.Name)
	}
	if spec.AuthType != connectors.AuthAPIKey {
		t.Errorf("AuthType = %v, want AuthAPIKey", spec.AuthType)
	}
	if len(spec.Streams) != 1 || spec.Streams[0].Name != umami.StreamEvents {
		t.Errorf("streams = %v, want one %q stream", spec.Streams, umami.StreamEvents)
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	c := umami.New()
	ctx := context.Background()

	must := func(cfg connectors.ConnectorConfig, wantSubstr string) {
		t.Helper()
		err := c.Validate(ctx, cfg)
		if err == nil {
			t.Fatalf("cfg %v: want error", cfg)
		}
		if !strings.Contains(err.Error(), wantSubstr) {
			t.Errorf("cfg %v: err = %v, want %q", cfg, err, wantSubstr)
		}
	}
	must(connectors.ConnectorConfig{}, "base_url")
	must(connectors.ConnectorConfig{"base_url": "x"}, "website_id")
	must(connectors.ConnectorConfig{"base_url": "x", "website_id": "y"}, "api_key")
	must(connectors.ConnectorConfig{"base_url": "x", "website_id": "y", "api_key": "k", "page_size": 0}, "page_size")
	must(connectors.ConnectorConfig{"base_url": "x", "website_id": "y", "api_key": "k", "page_size": 5000}, "page_size")
	must(connectors.ConnectorConfig{"base_url": "x", "website_id": "y", "api_key": "k", "max_pages": 0}, "max_pages")
	must(connectors.ConnectorConfig{"base_url": "x", "website_id": "y", "api_key": "k", "secret": "uhoh"}, "unknown config key")

	good := connectors.ConnectorConfig{"base_url": "x", "website_id": "y", "api_key": "k"}
	if err := c.Validate(ctx, good); err != nil {
		t.Errorf("valid cfg rejected: %v", err)
	}
}

// Extract against a fake server that returns two pages of synthetic
// events, verifying header auth, pagination, cursor advance, and
// record shape in one go.
func TestExtract_HappyPath(t *testing.T) {
	t.Parallel()

	const websiteID = "abc-123"
	var pagesServed int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(umami.APIKeyHeader); got != "the-key" {
			t.Errorf("missing or wrong API key header: %q", got)
		}
		if want := "/api/websites/" + websiteID + "/events"; r.URL.Path != want {
			t.Errorf("path = %q, want %q", r.URL.Path, want)
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		atomic.AddInt32(&pagesServed, 1)
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case 1:
			json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": "e1", "createdAt": "2026-04-20T12:00:00Z", "urlPath": "/a"},
					{"id": "e2", "createdAt": "2026-04-20T11:00:00Z", "urlPath": "/b"},
				},
				"pageSize": 2,
			})
		case 2:
			// Short page, signals end.
			json.NewEncoder(w).Encode(map[string]any{
				"data":     []map[string]any{{"id": "e3", "createdAt": "2026-04-20T10:00:00Z", "urlPath": "/c"}},
				"pageSize": 2,
			})
		default:
			t.Errorf("unexpected page %d requested", page)
		}
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{
		"base_url":   srv.URL,
		"website_id": websiteID,
		"api_key":    "the-key",
		"page_size":  2,
		"max_pages":  5,
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var records int
	var lastCursor string
	for m := range ch {
		switch m.Type {
		case connectors.RecordMsg:
			records++
		case connectors.StateMsg:
			if s := *m.State; s != nil {
				lastCursor, _ = s["last_created_at"].(string)
			}
		case connectors.LogMsg:
			if m.Log.Level == connectors.LevelError {
				t.Errorf("unexpected error log: %v", m.Log.Message)
			}
		}
	}
	if records != 3 {
		t.Errorf("records = %d, want 3", records)
	}
	if atomic.LoadInt32(&pagesServed) != 2 {
		t.Errorf("pages fetched = %d, want 2", pagesServed)
	}
	// The cursor must be the newest createdAt seen.
	if !strings.HasPrefix(lastCursor, "2026-04-20T12:00:00") {
		t.Errorf("cursor = %q, want newest createdAt", lastCursor)
	}
}

// A subsequent sync must not re-emit records older than or equal to
// the stored cursor.
func TestExtract_IncrementalSkipsSeen(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Every record here is at-or-before the stored cursor.
		json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"id": "e1", "createdAt": "2026-04-19T12:00:00Z"},
				{"id": "e2", "createdAt": "2026-04-19T11:00:00Z"},
			},
			"pageSize": 100,
		})
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{"base_url": srv.URL, "website_id": "w", "api_key": "k"}
	state := connectors.State{"last_created_at": "2026-04-20T00:00:00Z"}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var records int
	for m := range ch {
		if m.Type == connectors.RecordMsg {
			records++
		}
	}
	if records != 0 {
		t.Errorf("records = %d, want 0 (all older than cursor)", records)
	}
}

func TestExtract_Non200SurfacesBody(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintln(w, `{"error":"bad key"}`)
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{"base_url": srv.URL, "website_id": "w", "api_key": "k"}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var logged string
	for m := range ch {
		if m.Type == connectors.LogMsg && m.Log.Level == connectors.LevelError {
			logged = m.Log.Message
		}
	}
	if !strings.Contains(logged, "401") || !strings.Contains(logged, "bad key") {
		t.Errorf("error log = %q, want 401 + body", logged)
	}
}

func TestExtract_UnknownStreamLogsAndContinues(t *testing.T) {
	t.Parallel()
	c := umami.New()
	cfg := connectors.ConnectorConfig{"base_url": "http://example", "website_id": "w", "api_key": "k"}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: "not_a_stream"}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var warn string
	for m := range ch {
		if m.Type == connectors.LogMsg && m.Log.Level == connectors.LevelWarn {
			warn = m.Log.Message
		}
	}
	if !strings.Contains(warn, "unknown stream") {
		t.Errorf("warn = %q, want 'unknown stream'", warn)
	}
}

func TestExtract_RespectsMaxPages(t *testing.T) {
	// A pathologically chatty server keeps returning full pages of
	// brand-new events. The connector must still stop at max_pages.
	t.Parallel()

	ts := time.Date(2026, 4, 21, 0, 0, 0, 0, time.UTC)
	var served int32
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		n := atomic.AddInt32(&served, 1)
		// Each page holds two records with ever-newer timestamps so
		// progressed=true and pagination does not short-circuit.
		t1 := ts.Add(time.Duration(n*2) * time.Second)
		t0 := ts.Add(time.Duration(n*2-1) * time.Second)
		json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"id": fmt.Sprintf("e%d", n*2), "createdAt": t1.Format(time.RFC3339Nano)},
				{"id": fmt.Sprintf("e%d", n*2-1), "createdAt": t0.Format(time.RFC3339Nano)},
			},
			"pageSize": 2,
		})
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{
		"base_url": srv.URL, "website_id": "w", "api_key": "k",
		"page_size": 2, "max_pages": 3,
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	for range ch {
	}
	if got := atomic.LoadInt32(&served); got != 3 {
		t.Errorf("pages served = %d, want exactly max_pages (3)", got)
	}
}

func TestExtract_MalformedCreatedAtSkips(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"id": "e1", "createdAt": "not-a-timestamp"},
				{"id": "e2", "createdAt": "2026-04-20T12:00:00Z"},
			},
			"pageSize": 100,
		})
	}))
	defer srv.Close()
	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{"base_url": srv.URL, "website_id": "w", "api_key": "k"}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var records int
	var warn string
	for m := range ch {
		switch m.Type {
		case connectors.RecordMsg:
			records++
		case connectors.LogMsg:
			if m.Log.Level == connectors.LevelWarn {
				warn = m.Log.Message
			}
		}
	}
	if records != 1 {
		t.Errorf("records = %d, want 1 (the malformed one is skipped)", records)
	}
	if warn == "" {
		t.Error("expected warn log for malformed createdAt")
	}
}

func TestExtract_EmitsFinalStateEvenWhenNoRecords(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pageSize": 100})
	}))
	defer srv.Close()
	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{"base_url": srv.URL, "website_id": "w", "api_key": "k"}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var states int
	for m := range ch {
		if m.Type == connectors.StateMsg {
			states++
		}
	}
	if states != 1 {
		t.Errorf("state messages = %d, want 1", states)
	}
}

func TestExtract_CancelledContextStops(t *testing.T) {
	t.Parallel()
	// Server that blocks until the request is cancelled.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{"base_url": srv.URL, "website_id": "w", "api_key": "k"}
	ch, err := c.Extract(ctx, cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	cancel()
	// The channel must close within a reasonable time.
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Extract did not stop after context cancel")
	}
}
