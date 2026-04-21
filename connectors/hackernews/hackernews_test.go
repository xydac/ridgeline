package hackernews_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/hackernews"
)

func TestRegistered(t *testing.T) {
	t.Parallel()
	if _, ok := connectors.Get(hackernews.Name); !ok {
		t.Fatalf("hackernews connector not registered")
	}
}

func TestSpec(t *testing.T) {
	t.Parallel()
	spec := hackernews.New().Spec()
	if spec.Name != hackernews.Name {
		t.Errorf("Name = %q, want %q", spec.Name, hackernews.Name)
	}
	if spec.AuthType != connectors.AuthNone {
		t.Errorf("AuthType = %v, want AuthNone", spec.AuthType)
	}
	gotStreams := map[string]bool{}
	for _, s := range spec.Streams {
		gotStreams[s.Name] = true
	}
	if !gotStreams[hackernews.StreamStories] || !gotStreams[hackernews.StreamComments] {
		t.Errorf("missing streams, got %v", gotStreams)
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	c := hackernews.New()
	if err := c.Validate(context.Background(), connectors.ConnectorConfig{}); err == nil {
		t.Error("expected error when query missing")
	}
	if err := c.Validate(context.Background(), connectors.ConnectorConfig{"query": "   "}); err == nil {
		t.Error("expected error when query whitespace")
	}
	if err := c.Validate(context.Background(), connectors.ConnectorConfig{"query": "golang"}); err != nil {
		t.Errorf("valid query rejected: %v", err)
	}
}

func TestDiscover(t *testing.T) {
	t.Parallel()
	c := hackernews.New()
	cat, err := c.Discover(context.Background(), connectors.ConnectorConfig{"query": "go"})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(cat.Streams) != 2 {
		t.Fatalf("streams = %d, want 2", len(cat.Streams))
	}
	for _, s := range cat.Streams {
		if !s.Available {
			t.Errorf("stream %q should be available", s.Name)
		}
	}
}

// fakeAlgolia serves canned Algolia responses. Pages are returned in
// the order supplied; each call consumes one. When exhausted the
// server returns an empty page.
func fakeAlgolia(t *testing.T, pages [][]map[string]any) (*httptest.Server, *int32) {
	t.Helper()
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/search_by_date", func(w http.ResponseWriter, r *http.Request) {
		idx := int(atomic.AddInt32(&calls, 1)) - 1
		var hits []map[string]any
		if idx < len(pages) {
			hits = pages[idx]
		} else {
			hits = []map[string]any{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"hits": hits})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &calls
}

func hit(id string, createdAt int64, extras map[string]any) map[string]any {
	h := map[string]any{
		"objectID":     id,
		"created_at_i": createdAt,
		"created_at":   time.Unix(createdAt, 0).UTC().Format(time.RFC3339),
	}
	for k, v := range extras {
		h[k] = v
	}
	return h
}

func collect(ch <-chan connectors.Message) (records []connectors.Record, states []connectors.State, logs []connectors.LogEntry) {
	for m := range ch {
		switch m.Type {
		case connectors.RecordMsg:
			if m.Record != nil {
				records = append(records, *m.Record)
			}
		case connectors.StateMsg:
			if m.State != nil {
				states = append(states, *m.State)
			}
		case connectors.LogMsg:
			if m.Log != nil {
				logs = append(logs, *m.Log)
			}
		}
	}
	return
}

func TestExtract_SinglePageStories(t *testing.T) {
	t.Parallel()
	srv, calls := fakeAlgolia(t, [][]map[string]any{{
		hit("s1", 1700000300, map[string]any{"title": "one", "_tags": []any{"story"}}),
		hit("s2", 1700000200, map[string]any{"title": "two", "_tags": []any{"story"}}),
		hit("s3", 1700000100, map[string]any{"title": "three", "_tags": []any{"story"}}),
	}})

	c := hackernews.New()
	cfg := connectors.ConnectorConfig{
		"query":         "golang",
		"base_url":      srv.URL,
		"hits_per_page": 10, // larger than len(hits) so we stop after one page
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: hackernews.StreamStories}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	recs, states, logs := collect(ch)
	if got, want := len(recs), 3; got != want {
		t.Fatalf("records = %d, want %d", got, want)
	}
	if got, want := len(states), 1; got != want {
		t.Fatalf("states = %d, want %d (%+v)", got, want, states)
	}
	if got := atomic.LoadInt32(calls); got != 1 {
		t.Errorf("API calls = %d, want 1", got)
	}
	// Highest created_at_i on page is 1700000300, so that's the cursor
	// we must persist.
	if got := states[0]["since_stories"]; got != int64(1700000300) {
		t.Errorf("cursor = %v (%T), want 1700000300", got, got)
	}
	if len(logs) != 0 {
		t.Errorf("unexpected logs: %+v", logs)
	}
	if recs[0].Stream != hackernews.StreamStories {
		t.Errorf("stream on record = %q, want %q", recs[0].Stream, hackernews.StreamStories)
	}
}

func TestExtract_AppliesIncrementalCursor(t *testing.T) {
	t.Parallel()
	var seenFilter string
	mux := http.NewServeMux()
	mux.HandleFunc("/search_by_date", func(w http.ResponseWriter, r *http.Request) {
		seenFilter = r.URL.Query().Get("numericFilters")
		_ = json.NewEncoder(w).Encode(map[string]any{"hits": []map[string]any{
			hit("s1", 1700000400, map[string]any{"title": "newer"}),
		}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := hackernews.New()
	cfg := connectors.ConnectorConfig{
		"query":         "golang",
		"base_url":      srv.URL,
		"hits_per_page": 10,
	}
	state := connectors.State{"since_stories": int64(1700000300)}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: hackernews.StreamStories}}, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	recs, states, _ := collect(ch)
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	// On the initial call we *do* want the upper-exclusive filter to be
	// empty: the first page should pull the freshest items; the since
	// cursor is used to decide when pagination can stop early, not to
	// cap the first request. So seenFilter should be "".
	if seenFilter != "" {
		t.Errorf("first-call numericFilters = %q, want empty", seenFilter)
	}
	if got := states[0]["since_stories"]; got != int64(1700000400) {
		t.Errorf("cursor = %v, want 1700000400", got)
	}
}

func TestExtract_PaginatesUntilOlderThanCursor(t *testing.T) {
	t.Parallel()
	// Two pages, newest-first. Page 1 is all newer than the cursor,
	// page 2 straddles the cursor so pagination stops.
	page1 := []map[string]any{
		hit("s1", 1700000500, nil),
		hit("s2", 1700000450, nil),
	}
	page2 := []map[string]any{
		hit("s3", 1700000400, nil),
		hit("s4", 1700000350, nil),
	}
	srv, calls := fakeAlgolia(t, [][]map[string]any{page1, page2})

	c := hackernews.New()
	cfg := connectors.ConnectorConfig{
		"query":         "golang",
		"base_url":      srv.URL,
		"hits_per_page": 2, // matches page size so we need to fetch more
		"max_pages":     5,
	}
	state := connectors.State{"since_stories": int64(1700000400)}
	ch, _ := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: hackernews.StreamStories}}, state)
	recs, states, _ := collect(ch)
	// All 4 hits are emitted (we don't filter client-side; we just stop
	// paginating past the cursor). The caller's upsert logic in the
	// sink is responsible for dedup; the cursor advances monotonically.
	if len(recs) != 4 {
		t.Fatalf("records = %d, want 4", len(recs))
	}
	if got := atomic.LoadInt32(calls); got != 2 {
		t.Errorf("API calls = %d, want 2 (page1, page2)", got)
	}
	if got := states[0]["since_stories"]; got != int64(1700000500) {
		t.Errorf("cursor = %v, want 1700000500", got)
	}
}

func TestExtract_RespectsMaxPages(t *testing.T) {
	t.Parallel()
	// Three full pages, but max_pages = 2 so we only fetch two.
	fullPage := func(base int64) []map[string]any {
		return []map[string]any{
			hit(fmt.Sprintf("s%d", base), base+1, nil),
			hit(fmt.Sprintf("s%d", base+2), base, nil),
		}
	}
	srv, calls := fakeAlgolia(t, [][]map[string]any{
		fullPage(1700000500),
		fullPage(1700000300),
		fullPage(1700000100),
	})
	c := hackernews.New()
	cfg := connectors.ConnectorConfig{
		"query":         "golang",
		"base_url":      srv.URL,
		"hits_per_page": 2,
		"max_pages":     2,
	}
	ch, _ := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: hackernews.StreamStories}}, nil)
	recs, _, _ := collect(ch)
	if len(recs) != 4 {
		t.Errorf("records = %d, want 4 (2 pages * 2)", len(recs))
	}
	if got := atomic.LoadInt32(calls); got != 2 {
		t.Errorf("API calls = %d, want 2 (capped)", got)
	}
}

func TestExtract_EmptyResultsStillEmitState(t *testing.T) {
	t.Parallel()
	srv, _ := fakeAlgolia(t, [][]map[string]any{{}})
	c := hackernews.New()
	cfg := connectors.ConnectorConfig{"query": "zzzznopehits", "base_url": srv.URL}
	ch, _ := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: hackernews.StreamStories}}, connectors.State{"since_stories": int64(1700)})
	recs, states, _ := collect(ch)
	if len(recs) != 0 {
		t.Errorf("records = %d, want 0", len(recs))
	}
	if len(states) != 1 {
		t.Fatalf("states = %d, want 1 (high-water stays at previous value)", len(states))
	}
	if got := states[0]["since_stories"]; got != int64(1700) {
		t.Errorf("cursor = %v, want 1700 (unchanged)", got)
	}
}

func TestExtract_ServerErrorLogsAndMovesOn(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/search_by_date", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := hackernews.New()
	cfg := connectors.ConnectorConfig{"query": "x", "base_url": srv.URL}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: hackernews.StreamStories}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	recs, states, logs := collect(ch)
	if len(recs) != 0 {
		t.Errorf("records = %d, want 0", len(recs))
	}
	if len(states) != 1 {
		t.Errorf("want one state even after error, got %d", len(states))
	}
	if len(logs) == 0 {
		t.Fatal("expected error log")
	}
	if got := logs[0].Level; got != connectors.LevelError {
		t.Errorf("log level = %v, want error", got)
	}
}

func TestExtract_UnknownStreamWarns(t *testing.T) {
	t.Parallel()
	srv, _ := fakeAlgolia(t, nil)
	c := hackernews.New()
	cfg := connectors.ConnectorConfig{"query": "x", "base_url": srv.URL}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: "nonsense"}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	_, _, logs := collect(ch)
	if len(logs) == 0 {
		t.Fatal("expected warn log for unknown stream")
	}
	if got := logs[0].Level; got != connectors.LevelWarn {
		t.Errorf("log level = %v, want warn", got)
	}
}

func TestExtract_RejectsBadConfig(t *testing.T) {
	t.Parallel()
	c := hackernews.New()
	tests := []struct {
		name string
		cfg  connectors.ConnectorConfig
	}{
		{"empty query", connectors.ConnectorConfig{}},
		{"zero hits_per_page", connectors.ConnectorConfig{"query": "x", "hits_per_page": 0}},
		{"negative hits_per_page", connectors.ConnectorConfig{"query": "x", "hits_per_page": -1}},
		{"above max hits_per_page", connectors.ConnectorConfig{"query": "x", "hits_per_page": hackernews.MaxHitsPerPage + 1}},
		{"zero max_pages", connectors.ConnectorConfig{"query": "x", "max_pages": 0}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := c.Extract(context.Background(), tc.cfg, nil, nil); err == nil {
				t.Errorf("%s: expected error, got nil", tc.name)
			}
		})
	}
}

func TestExtract_ContextCancelStopsFetch(t *testing.T) {
	t.Parallel()
	// Serve indefinitely many pages; cancel after first page.
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/search_by_date", func(w http.ResponseWriter, r *http.Request) {
		_ = atomic.AddInt32(&calls, 1)
		_ = json.NewEncoder(w).Encode(map[string]any{"hits": []map[string]any{
			hit(strconv.Itoa(int(atomic.LoadInt32(&calls))), 1700000000+int64(atomic.LoadInt32(&calls)), nil),
			hit(strconv.Itoa(int(atomic.LoadInt32(&calls))+1000), 1700000000, nil),
		}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := hackernews.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := connectors.ConnectorConfig{
		"query":         "x",
		"base_url":      srv.URL,
		"hits_per_page": 2,
		"max_pages":     1000,
	}
	ch, _ := c.Extract(ctx, cfg, []connectors.Stream{{Name: hackernews.StreamStories}}, nil)

	// Consume the first record, then cancel.
	first := false
	for m := range ch {
		if m.Type == connectors.RecordMsg && !first {
			first = true
			cancel()
		}
	}
	// If ctx cancellation were ignored, we'd have hit 1000 pages. Allow
	// some slack for in-flight pages, but we must not hit the max.
	if got := atomic.LoadInt32(&calls); got >= 1000 {
		t.Errorf("API calls = %d, want < 1000 after cancel", got)
	}
}

func TestExtract_BothStreamsIndependentCursors(t *testing.T) {
	t.Parallel()
	// Two calls total: one per stream. Each returns a single hit.
	mux := http.NewServeMux()
	var tags []string
	var mu errGuard
	mux.HandleFunc("/search_by_date", func(w http.ResponseWriter, r *http.Request) {
		tag := r.URL.Query().Get("tags")
		mu.append(&tags, tag)
		var ts int64
		switch tag {
		case "story":
			ts = 1700000100
		case "comment":
			ts = 1700000200
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"hits": []map[string]any{
			hit("x", ts, nil),
		}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := hackernews.New()
	cfg := connectors.ConnectorConfig{"query": "x", "base_url": srv.URL, "hits_per_page": 10}
	ch, _ := c.Extract(context.Background(), cfg, []connectors.Stream{
		{Name: hackernews.StreamStories},
		{Name: hackernews.StreamComments},
	}, nil)
	_, states, _ := collect(ch)
	if len(states) != 2 {
		t.Fatalf("states = %d, want 2", len(states))
	}
	if got := states[0]["since_stories"]; got != int64(1700000100) {
		t.Errorf("stories cursor = %v, want 1700000100", got)
	}
	if got := states[1]["since_comments"]; got != int64(1700000200) {
		t.Errorf("comments cursor = %v, want 1700000200", got)
	}
	if want := []string{"story", "comment"}; !equal(tags, want) {
		t.Errorf("hit tags in order = %v, want %v", tags, want)
	}
}

// --- helpers ---

// errGuard is a tiny mutex wrapper so tests can mutate a slice from
// the HTTP handler without hitting the race detector.
type errGuard struct {
	m sync.Mutex
}

func (g *errGuard) append(dst *[]string, v string) {
	g.m.Lock()
	defer g.m.Unlock()
	*dst = append(*dst, v)
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
