package umami

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/xydac/ridgeline/connectors"
)

// authorizer decorates an *http.Request with the credentials Umami
// requires for the configured auth mode. Implementations are scoped to
// a single Extract call; state lives in the enclosing closure.
//
// decorate is called before every request. reauth is called once when
// the server rejects a request with 401; implementations that cannot
// re-authenticate (for example a static API key) return an error and
// let the caller surface the 401. applyTo merges any auth-managed
// fields into the state map immediately before the connector emits a
// StateMsg so a fresh token survives across sync invocations.
// consumeDirty reports whether a new credential was produced since the
// last check; callers emit an extra checkpoint StateMsg when it fires
// so a sync that crashes right after a successful login still persists
// the new token.
type authorizer interface {
	decorate(req *http.Request) error
	reauth(ctx context.Context) error
	applyTo(state connectors.State)
	consumeDirty() bool
}

// apiKeyAuth signs every request with the static x-umami-api-key
// header. It has no long-lived state, so reauth fails, applyTo is a
// no-op, and consumeDirty always reports false.
type apiKeyAuth struct {
	key string
}

func (a *apiKeyAuth) decorate(req *http.Request) error {
	req.Header.Set(APIKeyHeader, a.key)
	return nil
}

func (a *apiKeyAuth) reauth(_ context.Context) error {
	return fmt.Errorf("umami: api-key auth cannot refresh; check the stored key")
}

func (a *apiKeyAuth) applyTo(_ connectors.State) {}
func (a *apiKeyAuth) consumeDirty() bool         { return false }

// stateKeyAuthToken and stateKeyAuthTokenAt persist the JWT acquired
// via auth=login across sync invocations. The values live in the
// per-connector state map alongside the high-water cursor.
const (
	stateKeyAuthToken   = "auth_token"
	stateKeyAuthTokenAt = "auth_token_issued_at"
)

// loginPath is the Umami endpoint that trades a username/password for
// a JWT. The response shape is documented at
// https://umami.is/docs/api/authentication.
const loginPath = "/api/auth/login"

// loginAuth authenticates every request with a JWT acquired by POSTing
// to /api/auth/login. The token is cached in the connector's state
// map across syncs so a typical run makes one bearer request and
// skips the login round-trip.
//
// A loginAuth must not be shared across concurrent Extract calls: the
// dirty flag is a one-shot signal consumed by Extract, and a second
// reader would swallow the checkpoint the first needed.
type loginAuth struct {
	baseURL  string
	username string
	password string
	client   *http.Client
	now      func() time.Time

	mu       sync.Mutex
	token    string
	issuedAt time.Time
	dirty    bool
}

func newLoginAuth(cfg map[string]any, state connectors.State, client *http.Client, now func() time.Time) (*loginAuth, error) {
	base := strings.TrimRight(strings.TrimSpace(stringOf(cfg["base_url"])), "/")
	username := strings.TrimSpace(stringOf(cfg["username"]))
	password := strings.TrimSpace(stringOf(cfg["password"]))
	if base == "" {
		return nil, fmt.Errorf("umami: auth=login requires base_url")
	}
	if username == "" || password == "" {
		return nil, fmt.Errorf("umami: auth=login requires username and password (set username_ref and password_ref)")
	}
	a := &loginAuth{
		baseURL:  base,
		username: username,
		password: password,
		client:   client,
		now:      now,
	}
	if state != nil {
		if t, ok := state[stateKeyAuthToken].(string); ok {
			a.token = strings.TrimSpace(t)
		}
		if raw, ok := state[stateKeyAuthTokenAt].(string); ok {
			if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
				a.issuedAt = ts
			}
		}
	}
	return a, nil
}

func (a *loginAuth) decorate(req *http.Request) error {
	a.mu.Lock()
	token := a.token
	a.mu.Unlock()
	if token == "" {
		if err := a.reauth(req.Context()); err != nil {
			return err
		}
		a.mu.Lock()
		token = a.token
		a.mu.Unlock()
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

// reauth POSTs the login endpoint and caches the returned token. On
// any failure the cached token is cleared so the next decorate call
// surfaces the login error instead of silently sending a stale token.
func (a *loginAuth) reauth(ctx context.Context) error {
	body, err := json.Marshal(map[string]string{
		"username": a.username,
		"password": a.password,
	})
	if err != nil {
		return fmt.Errorf("umami login: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+loginPath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("umami login: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "ridgeline/0.0.0-dev (+https://github.com/xydac/ridgeline)")
	client := a.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		a.clearToken()
		return fmt.Errorf("umami login: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		a.clearToken()
		return fmt.Errorf("umami login: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var payload struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		a.clearToken()
		return fmt.Errorf("umami login: decode: %w", err)
	}
	tok := strings.TrimSpace(payload.Token)
	if tok == "" {
		a.clearToken()
		return fmt.Errorf("umami login: response missing token field")
	}
	now := a.now
	if now == nil {
		now = time.Now
	}
	a.mu.Lock()
	a.token = tok
	a.issuedAt = now().UTC()
	a.dirty = true
	a.mu.Unlock()
	return nil
}

func (a *loginAuth) clearToken() {
	a.mu.Lock()
	a.token = ""
	a.issuedAt = time.Time{}
	a.mu.Unlock()
}

func (a *loginAuth) applyTo(state connectors.State) {
	if state == nil {
		return
	}
	a.mu.Lock()
	token := a.token
	issued := a.issuedAt
	a.mu.Unlock()
	if token == "" {
		// Nothing earned yet; don't overwrite any pre-existing value
		// in state with an empty string.
		return
	}
	state[stateKeyAuthToken] = token
	if !issued.IsZero() {
		state[stateKeyAuthTokenAt] = issued.UTC().Format(time.RFC3339Nano)
	}
}

func (a *loginAuth) consumeDirty() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.dirty {
		a.dirty = false
		return true
	}
	return false
}

// newAuthorizer builds the authorizer for cfg. The caller has already
// validated cfg, so any unreachable-branch errors here indicate a
// Validate/Extract drift.
func newAuthorizer(cfg map[string]any, state connectors.State, client *http.Client, now func() time.Time) (authorizer, error) {
	switch mode := authMode(cfg); mode {
	case authAPIKey:
		key := strings.TrimSpace(stringOf(cfg["api_key"]))
		if key == "" {
			return nil, fmt.Errorf("umami: api_key is required for auth=%s", mode)
		}
		return &apiKeyAuth{key: key}, nil
	case authLogin:
		return newLoginAuth(cfg, state, client, now)
	default:
		return nil, fmt.Errorf("umami: unsupported auth mode %q", mode)
	}
}

// authMode returns the normalized auth mode. Missing or empty
// defaults to api_key so pre-existing configs keep working.
func authMode(cfg map[string]any) string {
	m := strings.TrimSpace(strings.ToLower(stringOf(cfg["auth"])))
	if m == "" {
		return authAPIKey
	}
	return m
}

// stringOf coerces a map value to a string. Any non-string shape (for
// example an accidental YAML integer) yields "".
func stringOf(v any) string {
	s, _ := v.(string)
	return s
}

// authAPIKey and authLogin name the supported auth modes.
const (
	authAPIKey = "api_key"
	authLogin  = "login"
)
