package gsc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/xydac/ridgeline/connectors"
)

// Default Google OAuth 2.0 token endpoint. Exposed as a package
// variable so tests can redirect it at an httptest server.
var defaultTokenURL = "https://oauth2.googleapis.com/token"

// State keys under which the cached access token and its absolute
// expiry are persisted alongside the stream cursor.
const (
	stateKeyAccessToken   = "access_token"
	stateKeyAccessExpires = "access_token_expires_at"
)

// refreshSkew is the headroom treated as "already expired" so a token
// whose real expiry is a heartbeat away does not slip through and
// surface as a 401 at the API.
const refreshSkew = 60 * time.Second

// tokenAuth trades a long-lived refresh token for short-lived access
// tokens and signs every API request with `Authorization: Bearer ...`.
// The current access token and its absolute expiry are cached in the
// per-connector state map so a typical hourly sync makes one token
// call per token lifetime and skips the exchange on subsequent runs
// within the same hour.
//
// A tokenAuth must not be shared across concurrent Extract calls: the
// dirty flag is a one-shot consumed by Extract to emit a safety
// checkpoint right after a fresh exchange.
type tokenAuth struct {
	tokenURL     string
	clientID     string
	clientSecret string
	refreshToken string
	client       *http.Client
	now          func() time.Time

	mu        sync.Mutex
	token     string
	expiresAt time.Time
	dirty     bool
}

func newTokenAuth(cfg connectors.ConnectorConfig, state connectors.State, client *http.Client, now func() time.Time) (*tokenAuth, error) {
	cid := strings.TrimSpace(cfg.String("client_id"))
	csec := strings.TrimSpace(cfg.String("client_secret"))
	rtok := strings.TrimSpace(cfg.String("refresh_token"))
	if cid == "" || csec == "" || rtok == "" {
		return nil, fmt.Errorf("gsc: client_id, client_secret, and refresh_token are required (set *_ref keys)")
	}
	a := &tokenAuth{
		tokenURL:     defaultTokenURL,
		clientID:     cid,
		clientSecret: csec,
		refreshToken: rtok,
		client:       client,
		now:          now,
	}
	if state != nil {
		if t, ok := state[stateKeyAccessToken].(string); ok {
			a.token = strings.TrimSpace(t)
		}
		if raw, ok := state[stateKeyAccessExpires].(string); ok {
			if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
				a.expiresAt = ts
			}
		}
	}
	return a, nil
}

// decorate ensures a valid access token is loaded and attaches it as
// a Bearer credential. A missing or near-expiry token triggers one
// refresh exchange before the request is signed.
func (a *tokenAuth) decorate(req *http.Request) error {
	a.mu.Lock()
	tok := a.token
	exp := a.expiresAt
	a.mu.Unlock()
	now := a.clock()
	if tok == "" || !exp.IsZero() && now.Add(refreshSkew).After(exp) {
		if err := a.refresh(req.Context()); err != nil {
			return err
		}
		a.mu.Lock()
		tok = a.token
		a.mu.Unlock()
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	return nil
}

// reauth forces a refresh regardless of the cached token's apparent
// validity. It is called by the caller on an HTTP 401 so a token the
// issuer revoked mid-lifetime (for example, because the user rotated
// their OAuth client) gets replaced without a full connector restart.
func (a *tokenAuth) reauth(ctx context.Context) error { return a.refresh(ctx) }

// refresh POSTs to the OAuth2 token endpoint and caches the returned
// access_token + absolute expiry. On any failure the cached token is
// cleared so the next decorate call surfaces the refresh error rather
// than silently sending a known-bad credential.
func (a *tokenAuth) refresh(ctx context.Context) error {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("client_id", a.clientID)
	form.Set("client_secret", a.clientSecret)
	form.Set("refresh_token", a.refreshToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		a.clear()
		return fmt.Errorf("gsc token: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "ridgeline/0.0.0-dev (+https://github.com/xydac/ridgeline)")

	client := a.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		a.clear()
		return fmt.Errorf("gsc token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		a.clear()
		return fmt.Errorf("gsc token: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		a.clear()
		return fmt.Errorf("gsc token: decode: %w", err)
	}
	tok := strings.TrimSpace(payload.AccessToken)
	if tok == "" {
		a.clear()
		return fmt.Errorf("gsc token: response missing access_token")
	}
	// Google omits expires_in only when the token is non-expiring, which
	// real refresh-token grants never return; default to one hour rather
	// than caching forever and letting a 401 be the first signal.
	ttl := time.Duration(payload.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = time.Hour
	}
	a.mu.Lock()
	a.token = tok
	a.expiresAt = a.clock().UTC().Add(ttl)
	a.dirty = true
	a.mu.Unlock()
	return nil
}

func (a *tokenAuth) clear() {
	a.mu.Lock()
	a.token = ""
	a.expiresAt = time.Time{}
	a.mu.Unlock()
}

func (a *tokenAuth) clock() time.Time {
	if a.now != nil {
		return a.now()
	}
	return time.Now()
}

// applyTo merges the cached access token and its absolute expiry into
// the state map the connector is about to emit.
func (a *tokenAuth) applyTo(state connectors.State) {
	if state == nil {
		return
	}
	a.mu.Lock()
	tok := a.token
	exp := a.expiresAt
	a.mu.Unlock()
	if tok == "" {
		return
	}
	state[stateKeyAccessToken] = tok
	if !exp.IsZero() {
		state[stateKeyAccessExpires] = exp.UTC().Format(time.RFC3339Nano)
	}
}

// consumeDirty reports whether a fresh exchange has happened since the
// last call. Extract emits an extra safety StateMsg when it fires so a
// crash mid-sync still persists the new access token.
func (a *tokenAuth) consumeDirty() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.dirty {
		a.dirty = false
		return true
	}
	return false
}
