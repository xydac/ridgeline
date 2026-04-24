package gsc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// fakeGoogle stands in for Google's authorization + token endpoints
// so the PKCE flow can be driven end to end without a real user.
type fakeGoogle struct {
	srv               *httptest.Server
	receivedChallenge string
	receivedClientID  string
	receivedState     string
}

func newFakeGoogle(t *testing.T, issueCode string, refreshToken string, accessToken string, httpClient *http.Client) *fakeGoogle {
	t.Helper()
	f := &fakeGoogle{}
	mux := http.NewServeMux()
	// Authorization endpoint: capture PKCE challenge and immediately
	// redirect to the registered redirect_uri with the fake code.
	mux.HandleFunc("/auth", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		f.receivedChallenge = q.Get("code_challenge")
		f.receivedClientID = q.Get("client_id")
		f.receivedState = q.Get("state")
		if q.Get("code_challenge_method") != "S256" {
			http.Error(w, "challenge method must be S256", http.StatusBadRequest)
			return
		}
		redirect := q.Get("redirect_uri")
		u, err := url.Parse(redirect)
		if err != nil {
			http.Error(w, "bad redirect", http.StatusBadRequest)
			return
		}
		rq := u.Query()
		rq.Set("code", issueCode)
		rq.Set("state", q.Get("state"))
		u.RawQuery = rq.Encode()
		// Emulate the issuer by having the test's own HTTP client hit
		// the redirect_uri directly.
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, "callback failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		resp.Body.Close()
		w.WriteHeader(http.StatusNoContent)
	})
	// Token endpoint: verify the code_verifier matches the stored
	// challenge, then issue the canned token.
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		if r.Form.Get("grant_type") != "authorization_code" {
			http.Error(w, "bad grant_type", http.StatusBadRequest)
			return
		}
		if r.Form.Get("code") != issueCode {
			http.Error(w, "bad code", http.StatusBadRequest)
			return
		}
		verifier := r.Form.Get("code_verifier")
		sum := sha256.Sum256([]byte(verifier))
		got := base64.RawURLEncoding.EncodeToString(sum[:])
		if got != f.receivedChallenge {
			http.Error(w, "verifier does not match challenge", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  accessToken,
			"refresh_token": refreshToken,
			"expires_in":    3600,
			"scope":         DefaultScope,
			"token_type":    "Bearer",
		})
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func TestRunPKCEFlow_Success(t *testing.T) {
	hc := &http.Client{Timeout: 5 * time.Second}
	fake := newFakeGoogle(t, "canned-code", "refresh-xyz", "access-abc", hc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := RunPKCEFlow(ctx, PKCEConfig{
		ClientID:     "client-id-123",
		ClientSecret: "client-secret-456",
		Listen:       "127.0.0.1:0",
		AuthURL:      fake.srv.URL + "/auth",
		TokenURL:     fake.srv.URL + "/token",
		HTTPClient:   hc,
		Timeout:      3 * time.Second,
		OnAuthURL: func(authURL string) {
			go func() {
				// Detached from ctx: once RunPKCEFlow returns, the
				// outer ctx is cancelled and the in-flight GET would
				// error out racily; all we care about is that the
				// callback fires before the flow's own timeout.
				req, _ := http.NewRequest(http.MethodGet, authURL, nil)
				if resp, err := hc.Do(req); err == nil {
					resp.Body.Close()
				}
			}()
		},
	})
	if err != nil {
		t.Fatalf("RunPKCEFlow: %v", err)
	}
	if res.RefreshToken != "refresh-xyz" {
		t.Errorf("refresh token: got %q want refresh-xyz", res.RefreshToken)
	}
	if res.AccessToken != "access-abc" {
		t.Errorf("access token: got %q want access-abc", res.AccessToken)
	}
	if fake.receivedClientID != "client-id-123" {
		t.Errorf("auth client_id: got %q", fake.receivedClientID)
	}
	if fake.receivedChallenge == "" {
		t.Error("auth code_challenge was not forwarded")
	}
	if fake.receivedState == "" {
		t.Error("auth state was empty")
	}
}

func TestRunPKCEFlow_StateMismatchFails(t *testing.T) {
	hc := &http.Client{Timeout: 2 * time.Second}
	// Custom fake that forges the state param on the redirect so we
	// can confirm the callback rejects it.
	mux := http.NewServeMux()
	mux.HandleFunc("/auth", func(w http.ResponseWriter, r *http.Request) {
		redirect := r.URL.Query().Get("redirect_uri")
		u, _ := url.Parse(redirect)
		rq := u.Query()
		rq.Set("code", "x")
		rq.Set("state", "wrong")
		u.RawQuery = rq.Encode()
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
		resp, err := hc.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		resp.Body.Close()
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := RunPKCEFlow(ctx, PKCEConfig{
		ClientID:     "id",
		ClientSecret: "sec",
		AuthURL:      srv.URL + "/auth",
		TokenURL:     srv.URL + "/token",
		HTTPClient:   hc,
		Timeout:      2 * time.Second,
		OnAuthURL: func(authURL string) {
			go func() {
				req, _ := http.NewRequest(http.MethodGet, authURL, nil)
				if resp, err := hc.Do(req); err == nil {
					resp.Body.Close()
				}
			}()
		},
	})
	if err == nil {
		t.Fatal("expected state mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "state mismatch") {
		t.Errorf("expected state mismatch error, got: %v", err)
	}
}

func TestRunPKCEFlow_MissingClientIDErrors(t *testing.T) {
	_, err := RunPKCEFlow(context.Background(), PKCEConfig{ClientSecret: "x"})
	if err == nil {
		t.Fatal("expected error for missing client_id")
	}
}

func TestS256Challenge_MatchesRFC7636Example(t *testing.T) {
	// RFC 7636 Appendix B example verifier/challenge pair.
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	want := "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
	got := s256Challenge(verifier)
	if got != want {
		t.Errorf("s256Challenge(%q) = %q, want %q", verifier, got, want)
	}
}
