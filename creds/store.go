package creds

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
)

// KeySize is the required AES-256 key size in bytes.
const KeySize = 32

// Store encrypts credentials with AES-256-GCM and persists them in the
// SQLite credentials table created by package state/sqlite.
//
// A Store is safe for concurrent use; the underlying *sql.DB serialises
// writes and the GCM cipher is stateless.
type Store struct {
	db   *sql.DB
	aead cipher.AEAD
}

// New returns a Store that encrypts with key. key must be exactly
// KeySize (32) bytes. The credentials table is expected to exist; use
// the state/sqlite.Open migrations to create it.
func New(db *sql.DB, key []byte) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("creds: nil *sql.DB")
	}
	if len(key) != KeySize {
		return nil, fmt.Errorf("creds: key must be %d bytes, got %d", KeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("creds: new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("creds: new gcm: %w", err)
	}
	return &Store{db: db, aead: aead}, nil
}

// Put seals plaintext under name. A fresh random nonce is generated
// each call, so repeated Puts of the same plaintext produce different
// ciphertext. Put overwrites any prior value for name.
func (s *Store) Put(ctx context.Context, name string, plaintext []byte) error {
	if name == "" {
		return fmt.Errorf("creds: name must not be empty")
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("creds: put %q: nonce: %w", name, err)
	}
	ct := s.aead.Seal(nil, nonce, plaintext, []byte(name))
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO credentials(name, nonce, ciphertext, updated_at)
		VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		ON CONFLICT(name) DO UPDATE SET
			nonce = excluded.nonce,
			ciphertext = excluded.ciphertext,
			updated_at = excluded.updated_at`, name, nonce, ct)
	if err != nil {
		return fmt.Errorf("creds: put %q: %w", name, err)
	}
	return nil
}

// Get loads and decrypts the credential named name. ErrNotFound is
// returned if no credential exists; any other error (including
// tampered ciphertext) wraps the underlying cause.
func (s *Store) Get(ctx context.Context, name string) ([]byte, error) {
	if name == "" {
		return nil, fmt.Errorf("creds: name must not be empty")
	}
	var nonce, ct []byte
	err := s.db.QueryRowContext(ctx, `SELECT nonce, ciphertext FROM credentials WHERE name = ?`, name).Scan(&nonce, &ct)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("creds: get %q: %w", name, err)
	}
	pt, err := s.aead.Open(nil, nonce, ct, []byte(name))
	if err != nil {
		return nil, fmt.Errorf("creds: get %q: decrypt: %w", name, err)
	}
	return pt, nil
}

// Delete removes the credential named name. Delete is a no-op if name
// does not exist.
func (s *Store) Delete(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("creds: name must not be empty")
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM credentials WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("creds: delete %q: %w", name, err)
	}
	return nil
}

// Names returns every credential name currently stored, sorted
// lexicographically. Names does not decrypt or touch the ciphertext
// and is safe to call without the key.
func (s *Store) Names(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name FROM credentials ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("creds: list names: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("creds: scan name: %w", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("creds: iterate names: %w", err)
	}
	return out, nil
}

// ErrNotFound is returned by Get when no credential exists for the
// requested name. Callers can use errors.Is to match.
var ErrNotFound = fmt.Errorf("creds: not found")
