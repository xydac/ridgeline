package gsc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultAuthURL is Google's installed-application OAuth 2.0
// authorization endpoint. Tests override via PKCEConfig.AuthURL.
var DefaultAuthURL = "https://accounts.google.com/o/oauth2/v2/auth"

// DefaultScope is the Search Console read-only scope the connector
// needs. Exposed so callers do not have to hard-code it again.
const DefaultScope = "https://www.googleapis.com/auth/webmasters.readonly"

// PKCEConfig drives an installed-application OAuth 2.0 flow with a
// PKCE S256 code challenge. ClientID and ClientSecret are required.
// Listen is a net.Listen address; the default "127.0.0.1:0" picks a
// free port, which Google's installed-app client accepts because its
// registered redirect is a bare http://127.0.0.1 with any port.
// AuthURL and TokenURL are test hooks.
type PKCEConfig struct {
	ClientID     string
	ClientSecret string
	Scope        string
	Listen       string
	AuthURL      string
	TokenURL     string
	HTTPClient   *http.Client
	Timeout      time.Duration
	// OnAuthURL, if non-nil, is called once the callback listener is
	// up, with the fully-formed authorization URL the user should
	// visit. Implementations typically print it to stderr; the CLI
	// may also auto-open it.
	OnAuthURL func(authURL string)
}

// PKCEResult is the outcome of a completed PKCE flow.
type PKCEResult struct {
	RefreshToken string
	AccessToken  string
	ExpiresIn    int
	Scope        string
}

// RunPKCEFlow runs the installed-application OAuth 2.0 PKCE flow: it
// binds a local HTTP listener, derives an authorization URL with a
// code challenge, hands that URL to OnAuthURL, waits for the OAuth
// issuer to redirect back with an authorization code, and exchanges
// that code for a refresh token. The caller is expected to print or
// open the URL when OnAuthURL fires.
//
// The returned error wraps any of: listener bind failure, state
// mismatch, missing code, token endpoint error, or ctx cancel.
func RunPKCEFlow(ctx context.Context, cfg PKCEConfig) (*PKCEResult, error) {
	if strings.TrimSpace(cfg.ClientID) == "" || strings.TrimSpace(cfg.ClientSecret) == "" {
		return nil, errors.New("oauth: client_id and client_secret are required")
	}
	scope := cfg.Scope
	if scope == "" {
		scope = DefaultScope
	}
	authURL := cfg.AuthURL
	if authURL == "" {
		authURL = DefaultAuthURL
	}
	tokenURL := cfg.TokenURL
	if tokenURL == "" {
		tokenURL = defaultTokenURL
	}
	listen := cfg.Listen
	if listen == "" {
		listen = "127.0.0.1:0"
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}

	verifier, err := randomURLSafe(64)
	if err != nil {
		return nil, fmt.Errorf("oauth: verifier: %w", err)
	}
	challenge := s256Challenge(verifier)
	state, err := randomURLSafe(32)
	if err != nil {
		return nil, fmt.Errorf("oauth: state: %w", err)
	}

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("oauth: listen: %w", err)
	}
	redirectURI := "http://" + ln.Addr().String() + "/callback"

	type cbResult struct {
		code string
		err  error
	}
	resCh := make(chan cbResult, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if e := q.Get("error"); e != "" {
			http.Error(w, "authorization failed: "+e, http.StatusBadRequest)
			select {
			case resCh <- cbResult{err: fmt.Errorf("oauth: authorization failed: %s", e)}:
			default:
			}
			return
		}
		if q.Get("state") != state {
			http.Error(w, "state mismatch", http.StatusBadRequest)
			select {
			case resCh <- cbResult{err: errors.New("oauth: state mismatch")}:
			default:
			}
			return
		}
		code := q.Get("code")
		if code == "" {
			http.Error(w, "missing code", http.StatusBadRequest)
			select {
			case resCh <- cbResult{err: errors.New("oauth: missing authorization code")}:
			default:
			}
			return
		}
		fmt.Fprintln(w, "Authorization received. You can close this tab and return to the terminal.")
		select {
		case resCh <- cbResult{code: code}:
		default:
		}
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = srv.Shutdown(sctx)
		cancel()
	}()

	u, err := url.Parse(authURL)
	if err != nil {
		return nil, fmt.Errorf("oauth: auth url: %w", err)
	}
	qs := url.Values{}
	qs.Set("response_type", "code")
	qs.Set("client_id", cfg.ClientID)
	qs.Set("redirect_uri", redirectURI)
	qs.Set("scope", scope)
	qs.Set("state", state)
	qs.Set("code_challenge", challenge)
	qs.Set("code_challenge_method", "S256")
	qs.Set("access_type", "offline")
	qs.Set("prompt", "consent")
	u.RawQuery = qs.Encode()
	if cfg.OnAuthURL != nil {
		cfg.OnAuthURL(u.String())
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var code string
	select {
	case <-waitCtx.Done():
		return nil, fmt.Errorf("oauth: %w", waitCtx.Err())
	case r := <-resCh:
		if r.err != nil {
			return nil, r.err
		}
		code = r.code
	}

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", cfg.ClientID)
	form.Set("client_secret", cfg.ClientSecret)
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("code_verifier", verifier)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("oauth: token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "ridgeline/0.0.0-dev (+https://github.com/xydac/ridgeline)")
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("oauth: token exchange: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("oauth: token exchange: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var payload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		Scope        string `json:"scope"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return nil, fmt.Errorf("oauth: token decode: %w", err)
	}
	if strings.TrimSpace(payload.RefreshToken) == "" {
		return nil, errors.New("oauth: response missing refresh_token (ensure access_type=offline and prompt=consent)")
	}
	return &PKCEResult{
		RefreshToken: payload.RefreshToken,
		AccessToken:  payload.AccessToken,
		ExpiresIn:    payload.ExpiresIn,
		Scope:        payload.Scope,
	}, nil
}

func randomURLSafe(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func s256Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
