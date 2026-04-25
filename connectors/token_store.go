package connectors

import "context"

// TokenStore persists and retrieves short-lived derived credentials, such
// as cached JWTs acquired via username/password login, using sealed
// (encrypted) storage rather than the plain-text state database.
//
// Implementations must be safe for concurrent use.
type TokenStore interface {
	Put(ctx context.Context, name string, value []byte) error
	Get(ctx context.Context, name string) ([]byte, error)
}

// TokenStorer is an optional interface a Connector may implement to receive
// a TokenStore before Extract is called. The sync runner checks for this
// interface and calls SetTokenStore once after instantiation; connectors
// that do not implement it keep their existing state-based credential
// caching.
type TokenStorer interface {
	SetTokenStore(ts TokenStore)
}
