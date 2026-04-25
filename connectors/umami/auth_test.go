package umami_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/umami"
)

// memTokenStore is an in-memory TokenStore used in tests.
type memTokenStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemTokenStore() *memTokenStore { return &memTokenStore{data: map[string][]byte{}} }

func (s *memTokenStore) Put(_ context.Context, name string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	s.data[name] = cp
	return nil
}

func (s *memTokenStore) Get(_ context.Context, name string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[name]
	if !ok {
		return nil, connectors.ErrTokenNotFound
	}
	return v, nil
}

func (s *memTokenStore) has(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[name]
	return ok
}

// TestExtract_LoginHappyPath verifies a fresh auth=login run: the
// connector logs in once, caches the returned JWT in state, and pulls
// one page with a Bearer header.
func TestExtract_LoginHappyPath(t *testing.T) {
	t.Parallel()

	var logins int32
	var dataHits int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/login":
			atomic.AddInt32(&logins, 1)
			if r.Method != http.MethodPost {
				t.Errorf("login method = %s, want POST", r.Method)
			}
			var body struct {
				Username, Password string
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Username != "alice" || body.Password != "hunter2" {
				t.Errorf("login body = %+v, want alice/hunter2", body)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "jwt-aaa"})
		case "/api/websites/w/events":
			atomic.AddInt32(&dataHits, 1)
			if got := r.Header.Get("Authorization"); got != "Bearer jwt-aaa" {
				t.Errorf("Authorization header = %q, want Bearer jwt-aaa", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": "e1", "createdAt": "2026-04-20T12:00:00Z"},
				},
			})
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{
		"base_url":   srv.URL,
		"website_id": "w",
		"auth":       "login",
		"username":   "alice",
		"password":   "hunter2",
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var records int
	var lastState connectors.State
	for m := range ch {
		switch m.Type {
		case connectors.RecordMsg:
			records++
		case connectors.StateMsg:
			if m.State != nil {
				lastState = *m.State
			}
		case connectors.LogMsg:
			if m.Log.Level == connectors.LevelError {
				t.Errorf("unexpected error: %s", m.Log.Message)
			}
		}
	}
	if records != 1 {
		t.Errorf("records = %d, want 1", records)
	}
	if got := atomic.LoadInt32(&logins); got != 1 {
		t.Errorf("logins = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&dataHits); got != 1 {
		t.Errorf("data hits = %d, want 1", got)
	}
	tok, _ := lastState["auth_token"].(string)
	if tok != "jwt-aaa" {
		t.Errorf("state.auth_token = %q, want jwt-aaa", tok)
	}
	if _, ok := lastState["auth_token_issued_at"].(string); !ok {
		t.Errorf("state missing auth_token_issued_at, got %v", lastState)
	}
}

// TestExtract_LoginReusesCachedToken verifies that a state map
// carrying auth_token from a prior sync skips the login round-trip.
func TestExtract_LoginReusesCachedToken(t *testing.T) {
	t.Parallel()

	var logins int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/login" {
			atomic.AddInt32(&logins, 1)
			t.Errorf("unexpected login call")
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer cached-jwt" {
			t.Errorf("Authorization = %q, want Bearer cached-jwt", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}})
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{
		"base_url": srv.URL, "website_id": "w",
		"auth": "login", "username": "u", "password": "p",
	}
	state := connectors.State{
		"auth_token":           "cached-jwt",
		"auth_token_issued_at": "2026-04-20T00:00:00Z",
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	for range ch {
	}
	if got := atomic.LoadInt32(&logins); got != 0 {
		t.Errorf("logins = %d, want 0 (token already cached)", got)
	}
}

// TestExtract_LoginRetriesOn401 verifies the 401-then-retry path: a
// stale cached token triggers one re-login, then the retried request
// succeeds.
func TestExtract_LoginRetriesOn401(t *testing.T) {
	t.Parallel()

	var logins, dataHits int32
	var stale atomic.Bool
	stale.Store(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/login":
			atomic.AddInt32(&logins, 1)
			stale.Store(false)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "fresh-jwt"})
		case "/api/websites/w/events":
			hit := atomic.AddInt32(&dataHits, 1)
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if stale.Load() || token == "stale-jwt" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"expired"}`))
				return
			}
			if token != "fresh-jwt" {
				t.Errorf("hit %d: token = %q, want fresh-jwt", hit, token)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{"id": "e1", "createdAt": "2026-04-20T12:00:00Z"}},
			})
		}
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{
		"base_url": srv.URL, "website_id": "w",
		"auth": "login", "username": "u", "password": "p",
	}
	state := connectors.State{"auth_token": "stale-jwt"}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var records int
	var errLog string
	var lastState connectors.State
	for m := range ch {
		switch m.Type {
		case connectors.RecordMsg:
			records++
		case connectors.StateMsg:
			if m.State != nil {
				lastState = *m.State
			}
		case connectors.LogMsg:
			if m.Log.Level == connectors.LevelError {
				errLog = m.Log.Message
			}
		}
	}
	if errLog != "" {
		t.Errorf("unexpected error log: %q", errLog)
	}
	if records != 1 {
		t.Errorf("records = %d, want 1", records)
	}
	if got := atomic.LoadInt32(&logins); got != 1 {
		t.Errorf("logins = %d, want exactly 1", got)
	}
	tok, _ := lastState["auth_token"].(string)
	if tok != "fresh-jwt" {
		t.Errorf("state.auth_token = %q, want fresh-jwt", tok)
	}
}

// TestExtract_LoginRetryStillFails verifies that when re-login
// succeeds but the retried request is also 401, the original 401
// body is surfaced rather than the second one.
func TestExtract_LoginRetryStillFails(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/login" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "new-but-useless"})
			return
		}
		// Every data request is 401, both before and after a login.
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"first body"}`))
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{
		"base_url": srv.URL, "website_id": "w",
		"auth": "login", "username": "u", "password": "p",
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var errLog string
	for m := range ch {
		if m.Type == connectors.LogMsg && m.Log.Level == connectors.LevelError {
			errLog = m.Log.Message
		}
	}
	if !strings.Contains(errLog, "401") || !strings.Contains(errLog, "first body") {
		t.Errorf("error log = %q, want 401 + original body", errLog)
	}
}

// TestExtract_LoginFailureSurfaces verifies that a login rejection on
// first try (no cached token) surfaces as an error log message; no
// records, no StateMsg.
func TestExtract_LoginFailureSurfaces(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/login" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"bad password"}`))
			return
		}
		t.Errorf("unexpected data request path %q", r.URL.Path)
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{
		"base_url": srv.URL, "website_id": "w",
		"auth": "login", "username": "u", "password": "wrong",
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var errLog string
	for m := range ch {
		if m.Type == connectors.LogMsg && m.Log.Level == connectors.LevelError {
			errLog = m.Log.Message
		}
	}
	if !strings.Contains(errLog, "login") || !strings.Contains(errLog, "bad password") {
		t.Errorf("error log = %q, want 'login' + 'bad password'", errLog)
	}
}

// TestExtract_LoginMissingTokenField verifies that a 200 response
// without a token field is treated as an error.
func TestExtract_LoginMissingTokenField(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/login" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"user":{"id":1}}`))
			return
		}
		t.Errorf("unexpected data request path %q", r.URL.Path)
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{
		"base_url": srv.URL, "website_id": "w",
		"auth": "login", "username": "u", "password": "p",
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var errLog string
	for m := range ch {
		if m.Type == connectors.LogMsg && m.Log.Level == connectors.LevelError {
			errLog = m.Log.Message
		}
	}
	if !strings.Contains(errLog, "missing token") {
		t.Errorf("error log = %q, want 'missing token'", errLog)
	}
}

// TestExtract_LoginWithTokenStore_JWTInStoreNotState verifies that when a
// TokenStore is injected, a freshly acquired JWT is written to the store
// and NOT written into the state map.
func TestExtract_LoginWithTokenStore_JWTInStoreNotState(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/login" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "sealed-jwt"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"id": "e1", "createdAt": "2026-04-20T12:00:00Z"}},
		})
	}))
	defer srv.Close()

	ts := newMemTokenStore()
	c := umami.New()
	c.Client = srv.Client()
	c.SetTokenStore(ts)
	cfg := connectors.ConnectorConfig{
		"base_url": srv.URL, "website_id": "w",
		"auth": "login", "username": "u", "password": "p",
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var lastState connectors.State
	for m := range ch {
		if m.Type == connectors.StateMsg && m.State != nil {
			lastState = *m.State
		}
	}
	// JWT must be in the token store.
	credKey := "umami.token." + srv.URL
	if !ts.has(credKey) {
		t.Errorf("token store missing key %q", credKey)
	}
	// JWT must NOT be in state.
	if _, ok := lastState["auth_token"]; ok {
		t.Errorf("state must not contain auth_token when tokenStore is set; got %v", lastState)
	}
	if _, ok := lastState["auth_token_issued_at"]; ok {
		t.Errorf("state must not contain auth_token_issued_at when tokenStore is set; got %v", lastState)
	}
}

// TestExtract_LoginWithTokenStore_ReusesStoredToken verifies that a JWT
// pre-loaded into the token store is used on the first request and no
// login round-trip is made.
func TestExtract_LoginWithTokenStore_ReusesStoredToken(t *testing.T) {
	t.Parallel()

	var logins int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/login" {
			atomic.AddInt32(&logins, 1)
			t.Errorf("unexpected login call")
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer stored-sealed-jwt" {
			t.Errorf("Authorization = %q, want stored-sealed-jwt", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}})
	}))
	defer srv.Close()

	ts := newMemTokenStore()
	credKey := "umami.token." + srv.URL
	_ = ts.Put(context.Background(), credKey, []byte("stored-sealed-jwt"))

	c := umami.New()
	c.Client = srv.Client()
	c.SetTokenStore(ts)
	cfg := connectors.ConnectorConfig{
		"base_url": srv.URL, "website_id": "w",
		"auth": "login", "username": "u", "password": "p",
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	for range ch {
	}
	if got := atomic.LoadInt32(&logins); got != 0 {
		t.Errorf("logins = %d, want 0 (token already in store)", got)
	}
}

// TestExtract_LoginEmitsCheckpointAfterFreshLogin verifies that a
// fresh login produces a StateMsg carrying the token BEFORE the
// end-of-stream checkpoint, so a crash mid-sync still persists the
// new credential.
func TestExtract_LoginEmitsCheckpointAfterFreshLogin(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/login" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "tok-1"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}})
	}))
	defer srv.Close()

	c := umami.New()
	c.Client = srv.Client()
	cfg := connectors.ConnectorConfig{
		"base_url": srv.URL, "website_id": "w",
		"auth": "login", "username": "u", "password": "p",
	}
	ch, err := c.Extract(context.Background(), cfg, []connectors.Stream{{Name: umami.StreamEvents}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var states []connectors.State
	for m := range ch {
		if m.Type == connectors.StateMsg && m.State != nil {
			states = append(states, *m.State)
		}
	}
	if len(states) < 2 {
		t.Fatalf("states emitted = %d, want >= 2 (checkpoint + end)", len(states))
	}
	tok, _ := states[0]["auth_token"].(string)
	if tok != "tok-1" {
		t.Errorf("first StateMsg auth_token = %q, want tok-1", tok)
	}
}
