package gsc_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/gsc"
)

func TestRegistered(t *testing.T) {
	t.Parallel()
	if _, ok := connectors.Get(gsc.Name); !ok {
		t.Fatalf("gsc connector not registered")
	}
}

func TestSpec(t *testing.T) {
	t.Parallel()
	spec := gsc.New().Spec()
	if spec.Name != gsc.Name {
		t.Errorf("Name = %q, want %q", spec.Name, gsc.Name)
	}
	if spec.AuthType != connectors.AuthOAuth2 {
		t.Errorf("AuthType = %v, want AuthOAuth2", spec.AuthType)
	}
	if len(spec.Streams) != 1 || spec.Streams[0].Name != gsc.StreamSearchAnalytics {
		t.Errorf("streams = %v, want one %q stream", spec.Streams, gsc.StreamSearchAnalytics)
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	c := gsc.New()
	ctx := context.Background()

	must := func(cfg connectors.ConnectorConfig, wantSubstr string) {
		t.Helper()
		err := c.Validate(ctx, cfg)
		if err == nil {
			t.Fatalf("cfg %v: want error containing %q", cfg, wantSubstr)
		}
		if !strings.Contains(err.Error(), wantSubstr) {
			t.Errorf("cfg %v: err = %v, want %q", cfg, err, wantSubstr)
		}
	}

	base := connectors.ConnectorConfig{
		"site_url":      "sc-domain:example.com",
		"client_id":     "cid",
		"client_secret": "csec",
		"refresh_token": "rtok",
	}

	must(connectors.ConnectorConfig{}, "site_url")
	must(connectors.ConnectorConfig{"site_url": "x"}, "client_id")
	must(connectors.ConnectorConfig{"site_url": "x", "client_id": "c"}, "client_secret")
	must(connectors.ConnectorConfig{"site_url": "x", "client_id": "c", "client_secret": "s"}, "refresh_token")

	if err := c.Validate(ctx, base); err != nil {
		t.Errorf("valid base rejected: %v", err)
	}

	bad := clone(base)
	bad["dimensions"] = []string{"date", "nope"}
	must(bad, "unknown dimension")

	bad = clone(base)
	bad["row_limit"] = 0
	must(bad, "row_limit")

	bad = clone(base)
	bad["row_limit"] = 99999
	must(bad, "row_limit")

	bad = clone(base)
	bad["max_pages"] = 0
	must(bad, "max_pages")

	bad = clone(base)
	bad["lookback_days"] = 0
	must(bad, "lookback_days")

	bad = clone(base)
	bad["end_offset_days"] = -1
	must(bad, "end_offset_days")

	bad = clone(base)
	bad["extra"] = "oops"
	must(bad, "unknown config key")
}

// TestExtractGoldenPath walks a fresh sync: no cached token, one
// token exchange, one API page with two rows, one StateMsg carrying
// the cached access token plus the YYYY-MM-DD cursor.
func TestExtractGoldenPath(t *testing.T) {
	t.Parallel()

	var tokenCalls, apiCalls atomic.Int32
	fake := newFakeServer(&fakeConfig{
		tokenCalls: &tokenCalls,
		apiCalls:   &apiCalls,
		token:      "access-001",
		rows: []apiRow{
			{Keys: []string{"2026-04-20", "ridgeline", "/docs"}, Clicks: 3, Impressions: 50, CTR: 0.06, Position: 4.5},
			{Keys: []string{"2026-04-21", "ridgeline", "/docs"}, Clicks: 5, Impressions: 80, CTR: 0.0625, Position: 3.8},
		},
	})
	defer fake.Close()

	c := gsc.New()
	c.APIBase = fake.URL
	c.TokenURL = fake.URL + "/token"
	c.Now = func() time.Time { return time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC) }

	cfg := connectors.ConnectorConfig{
		"site_url":      "sc-domain:example.com",
		"client_id":     "cid",
		"client_secret": "csec",
		"refresh_token": "rtok",
		"lookback_days": 7,
	}
	state := connectors.State{}
	streams := []connectors.Stream{{Name: gsc.StreamSearchAnalytics}}

	records, states, logs := collect(t, c, cfg, streams, state)

	if got := tokenCalls.Load(); got != 1 {
		t.Errorf("token calls = %d, want 1", got)
	}
	if got := apiCalls.Load(); got != 1 {
		t.Errorf("api calls = %d, want 1 (rows=2 < rowLimit, so one page)", got)
	}
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2: %v", len(records), records)
	}
	if len(logs) != 0 {
		t.Errorf("unexpected logs: %v", logs)
	}
	if records[0].Data["query"] != "ridgeline" {
		t.Errorf("record[0].query = %v, want ridgeline", records[0].Data["query"])
	}
	if records[1].Data["clicks"].(float64) != 5 {
		t.Errorf("record[1].clicks = %v, want 5", records[1].Data["clicks"])
	}

	// The final StateMsg carries the high-water date and the cached
	// access token so the next sync skips the token exchange.
	final := states[len(states)-1]
	if final["last_date"] != "2026-04-21" {
		t.Errorf("final last_date = %v, want 2026-04-21", final["last_date"])
	}
	if final["access_token"] != "access-001" {
		t.Errorf("final access_token = %v, want access-001", final["access_token"])
	}
	if _, ok := final["access_token_expires_at"].(string); !ok {
		t.Errorf("final state missing access_token_expires_at: %v", final)
	}
}

// TestExtractReusesCachedToken skips the token exchange when state
// carries an unexpired access token.
func TestExtractReusesCachedToken(t *testing.T) {
	t.Parallel()

	var tokenCalls, apiCalls atomic.Int32
	fake := newFakeServer(&fakeConfig{
		tokenCalls: &tokenCalls,
		apiCalls:   &apiCalls,
		token:      "access-002",
		rows:       nil,
	})
	defer fake.Close()

	c := gsc.New()
	c.APIBase = fake.URL
	c.TokenURL = fake.URL + "/token"
	c.Now = func() time.Time { return time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC) }

	cfg := connectors.ConnectorConfig{
		"site_url":      "sc-domain:example.com",
		"client_id":     "cid",
		"client_secret": "csec",
		"refresh_token": "rtok",
	}
	// Cached token with 30 minutes of headroom. The connector accepts
	// anything whose expiry is more than refreshSkew (60s) in the future.
	state := connectors.State{
		"access_token":            "cached-tok",
		"access_token_expires_at": time.Date(2026, 4, 23, 12, 30, 0, 0, time.UTC).Format(time.RFC3339Nano),
		"last_date":               "2026-04-20",
	}
	streams := []connectors.Stream{{Name: gsc.StreamSearchAnalytics}}

	fake.wantBearer = "cached-tok"
	collect(t, c, cfg, streams, state)

	if got := tokenCalls.Load(); got != 0 {
		t.Errorf("token calls = %d, want 0 (cache should be reused)", got)
	}
	if got := apiCalls.Load(); got == 0 {
		t.Errorf("api calls = 0, want >= 1 (cached token should have been used)")
	}
}

// TestExtractReauthsOn401 exercises the revoked-token path: the data
// endpoint rejects the cached token once, the connector forces a
// refresh, and the retry succeeds.
func TestExtractReauthsOn401(t *testing.T) {
	t.Parallel()

	var tokenCalls, apiCalls atomic.Int32
	fake := newFakeServer(&fakeConfig{
		tokenCalls: &tokenCalls,
		apiCalls:   &apiCalls,
		token:      "access-fresh",
		rows: []apiRow{
			{Keys: []string{"2026-04-22", "q", "/p"}, Clicks: 1, Impressions: 2, CTR: 0.5, Position: 1.0},
		},
		reject401Once: true,
	})
	defer fake.Close()

	c := gsc.New()
	c.APIBase = fake.URL
	c.TokenURL = fake.URL + "/token"
	c.Now = func() time.Time { return time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC) }

	cfg := connectors.ConnectorConfig{
		"site_url":      "sc-domain:example.com",
		"client_id":     "cid",
		"client_secret": "csec",
		"refresh_token": "rtok",
	}
	state := connectors.State{
		"access_token":            "stale-tok",
		"access_token_expires_at": time.Date(2026, 4, 23, 13, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}
	streams := []connectors.Stream{{Name: gsc.StreamSearchAnalytics}}

	records, states, logs := collect(t, c, cfg, streams, state)

	if got := tokenCalls.Load(); got != 1 {
		t.Errorf("token calls = %d, want 1 (forced by 401)", got)
	}
	if got := apiCalls.Load(); got != 2 {
		t.Errorf("api calls = %d, want 2 (stale 401 + retry)", got)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1: %v", len(records), records)
	}
	if len(logs) != 0 {
		t.Errorf("unexpected logs on successful reauth: %v", logs)
	}
	// A safety checkpoint must fire after the fresh exchange, then
	// end-of-stream emits the final one: at least 2 state messages.
	if len(states) < 2 {
		t.Errorf("states = %d, want >= 2 (safety + end-of-stream)", len(states))
	}
	final := states[len(states)-1]
	if final["access_token"] != "access-fresh" {
		t.Errorf("final access_token = %v, want access-fresh", final["access_token"])
	}
}

// TestExtractTokenFailureSurfaces confirms that a token-endpoint 400
// ends the stream with an error log rather than hanging.
func TestExtractTokenFailureSurfaces(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/token") {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"error":"invalid_grant"}`)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := gsc.New()
	c.APIBase = srv.URL
	c.TokenURL = srv.URL + "/token"
	c.Now = func() time.Time { return time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC) }

	cfg := connectors.ConnectorConfig{
		"site_url":      "sc-domain:example.com",
		"client_id":     "cid",
		"client_secret": "csec",
		"refresh_token": "rtok",
	}
	streams := []connectors.Stream{{Name: gsc.StreamSearchAnalytics}}

	records, _, logs := collect(t, c, cfg, streams, connectors.State{})
	if len(records) != 0 {
		t.Errorf("records = %d, want 0 on token failure", len(records))
	}
	if len(logs) == 0 {
		t.Fatalf("expected an error log, got none")
	}
	if !strings.Contains(logs[0].Message, "invalid_grant") {
		t.Errorf("log = %q, want server body surfaced", logs[0].Message)
	}
}

// TestExtractUnknownStreamWarns mirrors the other connectors: an
// unrecognized stream name is a warn log, not an error.
func TestExtractUnknownStreamWarns(t *testing.T) {
	t.Parallel()

	c := gsc.New()
	c.Now = func() time.Time { return time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC) }
	// No HTTP traffic expected; APIBase intentionally left at a
	// non-routable value to catch any accidental calls.
	c.APIBase = "http://127.0.0.1:1"
	c.TokenURL = "http://127.0.0.1:1/token"

	cfg := connectors.ConnectorConfig{
		"site_url":      "sc-domain:example.com",
		"client_id":     "cid",
		"client_secret": "csec",
		"refresh_token": "rtok",
	}
	streams := []connectors.Stream{{Name: "not_a_stream"}}

	records, _, logs := collect(t, c, cfg, streams, connectors.State{})
	if len(records) != 0 {
		t.Errorf("records = %d, want 0 for unknown stream", len(records))
	}
	if len(logs) != 1 || logs[0].Level != connectors.LevelWarn {
		t.Fatalf("logs = %v, want one warn", logs)
	}
	if !strings.Contains(logs[0].Message, "not_a_stream") {
		t.Errorf("log = %q, want it to name the stream", logs[0].Message)
	}
}

// --- helpers ---

type apiRow struct {
	Keys        []string `json:"keys"`
	Clicks      float64  `json:"clicks"`
	Impressions float64  `json:"impressions"`
	CTR         float64  `json:"ctr"`
	Position    float64  `json:"position"`
}

type fakeConfig struct {
	tokenCalls    *atomic.Int32
	apiCalls      *atomic.Int32
	token         string
	rows          []apiRow
	reject401Once bool
}

type fakeServer struct {
	*httptest.Server
	cfg        *fakeConfig
	wantBearer string
	rejected   atomic.Bool
}

func newFakeServer(cfg *fakeConfig) *fakeServer {
	f := &fakeServer{cfg: cfg}
	f.Server = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

func (f *fakeServer) handle(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/token") {
		f.cfg.tokenCalls.Add(1)
		_ = r.ParseForm()
		if r.PostForm.Get("grant_type") != "refresh_token" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": f.cfg.token,
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
		return
	}
	if strings.Contains(r.URL.Path, "/searchAnalytics/query") {
		f.cfg.apiCalls.Add(1)
		auth := r.Header.Get("Authorization")
		if f.cfg.reject401Once && !f.rejected.Load() {
			f.rejected.Store(true)
			w.WriteHeader(http.StatusUnauthorized)
			io.WriteString(w, `{"error":"token expired"}`)
			return
		}
		if f.wantBearer != "" && auth != "Bearer "+f.wantBearer {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if !strings.HasPrefix(auth, "Bearer ") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"rows": f.cfg.rows})
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func clone(c connectors.ConnectorConfig) connectors.ConnectorConfig {
	out := make(connectors.ConnectorConfig, len(c))
	for k, v := range c {
		out[k] = v
	}
	return out
}

type logEntry struct {
	Level   connectors.LogLevel
	Message string
}

func collect(t *testing.T, c *gsc.Connector, cfg connectors.ConnectorConfig, streams []connectors.Stream, state connectors.State) ([]connectors.Record, []connectors.State, []logEntry) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := c.Extract(ctx, cfg, streams, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var records []connectors.Record
	var states []connectors.State
	var logs []logEntry
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
				logs = append(logs, logEntry{Level: m.Log.Level, Message: m.Log.Message})
			}
		}
	}
	return records, states, logs
}
