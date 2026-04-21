package creds

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// NewRandomKey returns a cryptographically random 32-byte key. The
// caller is responsible for persisting it somewhere safe; losing the
// key makes every stored credential unrecoverable.
func NewRandomKey() ([]byte, error) {
	k := make([]byte, KeySize)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		return nil, fmt.Errorf("creds: random key: %w", err)
	}
	return k, nil
}

// WriteKeyFile persists a 32-byte key to path as 64 hex characters
// followed by a newline. Parent directories are created with mode
// 0700 and the file itself is written with 0600 so other users on
// the host cannot read it.
func WriteKeyFile(path string, key []byte) error {
	if len(key) != KeySize {
		return fmt.Errorf("creds: WriteKeyFile: key must be %d bytes, got %d", KeySize, len(key))
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("creds: %w", err)
	}
	enc := make([]byte, hex.EncodedLen(KeySize)+1)
	hex.Encode(enc, key)
	enc[len(enc)-1] = '\n'
	if err := os.WriteFile(path, enc, 0o600); err != nil {
		return fmt.Errorf("creds: %w", err)
	}
	return nil
}

// KeyFromFile reads a hex-encoded key from path. Surrounding
// whitespace is tolerated so that keys edited by hand still load. The
// decoded key must be exactly KeySize bytes.
func KeyFromFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("creds: %w", err)
	}
	// Strip trailing whitespace (newline, CRLF, stray spaces) so a
	// hand-edited file still decodes.
	end := len(raw)
	for end > 0 {
		c := raw[end-1]
		if c == '\n' || c == '\r' || c == ' ' || c == '\t' {
			end--
			continue
		}
		break
	}
	trimmed := raw[:end]
	key, err := hex.DecodeString(string(trimmed))
	if err != nil {
		return nil, fmt.Errorf("creds: decode key %s: %w", path, err)
	}
	if len(key) != KeySize {
		return nil, fmt.Errorf("creds: key %s must decode to %d bytes, got %d", path, KeySize, len(key))
	}
	return key, nil
}
