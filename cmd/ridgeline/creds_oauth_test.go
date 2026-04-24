package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// urlCapturingWriter tees writes to an underlying buffer while
// watching for the first http:// URL that crosses the wire. When
// seen, it fires onURL in a goroutine. This lets the oauth CLI test
// drive the printed auth URL without introducing a test-only hook
// into the production code path.
type urlCapturingWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	onURL func(string)
	fired atomic.Bool
}

var urlRegexp = regexp.MustCompile(`https?://[^\s]+`)

func (w *urlCapturingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, err := w.buf.Write(p)
	w.mu.Unlock()
	if !w.fired.Load() {
		if m := urlRegexp.FindString(w.buf.String()); m != "" {
			if w.fired.CompareAndSwap(false, true) {
				go w.onURL(m)
			}
		}
	}
	return n, err
}

// newFakeOAuthIssuer spins up a fake authorization + token endpoint
// suitable for driving `ridgeline creds oauth gsc` end to end without
// any real Google client.
func newFakeOAuthIssuer(t *testing.T, refreshToken string) *httptest.Server {
	t.Helper()
	var (
		mu        sync.Mutex
		challenge string
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/auth", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		mu.Lock()
		challenge = q.Get("code_challenge")
		mu.Unlock()
		redirect := q.Get("redirect_uri")
		u, err := url.Parse(redirect)
		if err != nil {
			http.Error(w, "bad redirect", http.StatusBadRequest)
			return
		}
		rq := u.Query()
		rq.Set("code", "test-code")
		rq.Set("state", q.Get("state"))
		u.RawQuery = rq.Encode()
		hc := &http.Client{Timeout: 5 * time.Second}
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
		resp, err := hc.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		resp.Body.Close()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("code") != "test-code" {
			http.Error(w, "bad code", http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		got := base64.RawURLEncoding.EncodeToString(sum[:])
		mu.Lock()
		want := challenge
		mu.Unlock()
		if got != want {
			http.Error(w, "verifier mismatch", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "access-xyz",
			"refresh_token": refreshToken,
			"expires_in":    3600,
			"token_type":    "Bearer",
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestRunCreds_OAuthGSCStoresThreeKeys(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := writeMinimalConfig(t, dir)

	issuer := newFakeOAuthIssuer(t, "refresh-from-flow")

	// The CLI prints the auth URL to stderr; our capturing writer
	// notices and follows it as a fake end user would.
	stderr := &urlCapturingWriter{onURL: func(u string) {
		hc := &http.Client{Timeout: 5 * time.Second}
		req, _ := http.NewRequest(http.MethodGet, u, nil)
		if resp, err := hc.Do(req); err == nil {
			resp.Body.Close()
		}
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	err := runCreds(ctx, []string{
		"oauth", "gsc",
		"--config", cfgPath,
		"--client-id", "cid",
		"--client-secret", "csec",
		"--name", "gscprod",
		"--auth-url", issuer.URL + "/auth",
		"--token-url", issuer.URL + "/token",
	}, bytes.NewReader(nil), &out, stderr)
	if err != nil {
		t.Fatalf("oauth gsc: %v (stderr=%s)", err, stderr.buf.String())
	}

	stdout := out.String()
	for _, line := range []string{
		"client_id_ref: gscprod_client_id",
		"client_secret_ref: gscprod_client_secret",
		"refresh_token_ref: gscprod_refresh_token",
	} {
		if !strings.Contains(stdout, line) {
			t.Errorf("stdout missing %q; got:\n%s", line, stdout)
		}
	}

	// Confirm the three credentials landed in the store and that the
	// refresh token is the one the fake issued.
	for _, name := range []string{"gscprod_client_id", "gscprod_client_secret", "gscprod_refresh_token"} {
		var got bytes.Buffer
		if err := runCreds(context.Background(), []string{"get", "--config", cfgPath, name}, bytes.NewReader(nil), &got, io.Discard.(io.Writer)); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		if strings.TrimSpace(got.String()) == "" {
			t.Errorf("%s: empty", name)
		}
	}
	var refresh bytes.Buffer
	if err := runCreds(context.Background(), []string{"get", "--config", cfgPath, "gscprod_refresh_token"}, bytes.NewReader(nil), &refresh, io.Discard.(io.Writer)); err != nil {
		t.Fatalf("get refresh: %v", err)
	}
	if strings.TrimSpace(refresh.String()) != "refresh-from-flow" {
		t.Errorf("refresh token = %q, want refresh-from-flow", strings.TrimSpace(refresh.String()))
	}
}

func TestRunCreds_OAuthRequiresProvider(t *testing.T) {
	t.Parallel()
	err := runCreds(context.Background(), []string{"oauth"}, bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "provider required") {
		t.Errorf("expected provider-required error, got %v", err)
	}
}

func TestRunCreds_OAuthUnknownProvider(t *testing.T) {
	t.Parallel()
	err := runCreds(context.Background(), []string{"oauth", "nope"}, bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "unknown provider") {
		t.Errorf("expected unknown-provider error, got %v", err)
	}
}
