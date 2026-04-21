package umami

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// authorizer decorates an *http.Request with the credentials Umami
// requires for the configured auth mode. Implementations are scoped to
// a single Extract call; state lives in the enclosing closure.
//
// reauth is called once when the server rejects a request with 401.
// Implementations that cannot re-authenticate (for example a static
// API key) return an error and let the caller surface the 401.
type authorizer interface {
	decorate(req *http.Request) error
	reauth(ctx context.Context) error
}

// apiKeyAuth signs every request with the static x-umami-api-key
// header. It has no long-lived state, so reauth is a no-op failure.
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

// newAuthorizer builds the authorizer for cfg. The caller has already
// validated cfg, so any unreachable-branch errors here indicate a
// Validate/Extract drift.
func newAuthorizer(cfg map[string]any) (authorizer, error) {
	switch mode := authMode(cfg); mode {
	case authAPIKey:
		key := strings.TrimSpace(stringOf(cfg["api_key"]))
		if key == "" {
			return nil, fmt.Errorf("umami: api_key is required for auth=%s", mode)
		}
		return &apiKeyAuth{key: key}, nil
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
