package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xydac/ridgeline/connectors"

	_ "modernc.org/sqlite"
)

// Store is a SQLite-backed StateStore. It is safe for concurrent use.
//
// Store satisfies pipeline.StateStore via its Load and Save methods.
// It also exposes DB() so callers (for example the credential store)
// can reuse the same connection pool and transaction scope.
type Store struct {
	path string
	db   *sql.DB
}

// Open opens or creates a SQLite database at path, creates the parent
// directory if needed, and runs migrations. The returned Store must be
// closed by the caller.
//
// Passing ":memory:" as path opens an in-memory database, useful in
// tests. For disk paths, Open enforces secure file permissions (0600)
// so credentials stored in the same file are not world-readable.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("sqlite: path must not be empty")
	}

	if path != ":memory:" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("sqlite: %w", err)
		}
	}

	// busy_timeout lets concurrent writers wait instead of failing
	// immediately with SQLITE_BUSY. WAL is only meaningful for a
	// physical file; an in-memory database uses the default journal.
	var dsn string
	if path == ":memory:" {
		dsn = path + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)"
	} else {
		dsn = path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}
	// database/sql opens a fresh connection per request. For :memory:
	// each connection gets its own empty DB, so the pool must be
	// capped at 1 to keep all queries on the same in-memory handle.
	// For physical files, cap writers at 1 to serialize writes with
	// WAL, and allow a modest read pool.
	if path == ":memory:" {
		db.SetMaxOpenConns(1)
	} else {
		db.SetMaxOpenConns(8)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: ping %s: %w", path, err)
	}

	if path != ":memory:" {
		if err := os.Chmod(path, 0o600); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("sqlite: %w", err)
		}
	}

	s := &Store{path: path, db: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Path returns the filesystem path this Store was opened with.
// Returns ":memory:" for in-memory stores.
func (s *Store) Path() string { return s.path }

// DB returns the underlying *sql.DB. Callers that embed additional
// tables (for example the credential store) may use it directly but
// must not close it; call Store.Close instead.
func (s *Store) DB() *sql.DB { return s.db }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// Load returns the state for key. If no state exists, an empty State
// is returned with a nil error so callers can treat "never synced
// before" the same as "synced with empty cursor".
func (s *Store) Load(ctx context.Context, key string) (connectors.State, error) {
	if key == "" {
		return nil, fmt.Errorf("sqlite: Load: key must not be empty")
	}
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT data FROM state WHERE key = ?`, key).Scan(&raw)
	if err == sql.ErrNoRows {
		return connectors.State{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: Load %q: %w", key, err)
	}
	var state connectors.State
	if len(raw) == 0 {
		return connectors.State{}, nil
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("sqlite: Load %q: unmarshal: %w", key, err)
	}
	if state == nil {
		state = connectors.State{}
	}
	return state, nil
}

// Save persists state under key, overwriting any prior value. The
// state is serialized as JSON; any value the standard library's
// encoding/json can represent is accepted.
func (s *Store) Save(ctx context.Context, key string, state connectors.State) error {
	if key == "" {
		return fmt.Errorf("sqlite: Save: key must not be empty")
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("sqlite: Save %q: marshal: %w", key, err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO state(key, data, updated_at)
		VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		ON CONFLICT(key) DO UPDATE SET
			data = excluded.data,
			updated_at = excluded.updated_at`, key, raw)
	if err != nil {
		return fmt.Errorf("sqlite: Save %q: %w", key, err)
	}
	return nil
}

// Delete removes the state entry for key. Delete is a no-op if key
// does not exist.
func (s *Store) Delete(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("sqlite: Delete: key must not be empty")
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM state WHERE key = ?`, key)
	if err != nil {
		return fmt.Errorf("sqlite: Delete %q: %w", key, err)
	}
	return nil
}

// Keys returns every state key currently stored, sorted
// lexicographically. Useful for diagnostics.
func (s *Store) Keys(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key FROM state ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: Keys: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("sqlite: Keys scan: %w", err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: Keys rows: %w", err)
	}
	return out, nil
}

// Entry is a single row from the state table. UpdatedAt is the RFC
// 3339 millisecond UTC timestamp recorded by Save.
type Entry struct {
	Key       string
	State     connectors.State
	UpdatedAt string
}

// List returns every state entry with its deserialized value and the
// wall-clock timestamp of the last Save, sorted by key. Entries with
// malformed JSON payloads are surfaced with a nil State rather than
// aborting the whole listing; a diagnostic tool should still be able
// to show which keys exist when one of them is corrupt.
func (s *Store) List(ctx context.Context) ([]Entry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, data, updated_at FROM state ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: List: %w", err)
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var e Entry
		var raw []byte
		if err := rows.Scan(&e.Key, &raw, &e.UpdatedAt); err != nil {
			return nil, fmt.Errorf("sqlite: List scan: %w", err)
		}
		if len(raw) == 0 {
			e.State = connectors.State{}
		} else if err := json.Unmarshal(raw, &e.State); err != nil {
			// Malformed payload: show the key in the listing but
			// leave State nil so callers can signal "corrupt".
			e.State = nil
		}
		// A JSON "null" payload unmarshals into a nil map; treat
		// that as an empty state so callers can distinguish it
		// from the malformed case above.
		if e.State == nil && len(raw) > 0 && string(raw) == "null" {
			e.State = connectors.State{}
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: List rows: %w", err)
	}
	return out, nil
}
