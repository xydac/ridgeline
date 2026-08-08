package sqlite

import (
	"context"
	"fmt"
)

// schemaMigrations lists DDL statements that, applied in order, bring
// a fresh database up to the current schema. Each migration is
// idempotent via IF NOT EXISTS and is recorded in the schema_versions
// table so future additions can be appended without repeating prior
// work.
var schemaMigrations = []struct {
	version int
	stmt    string
}{
	{
		version: 1,
		stmt: `
CREATE TABLE IF NOT EXISTS state (
	key TEXT PRIMARY KEY,
	data BLOB NOT NULL,
	updated_at TEXT NOT NULL
) STRICT;`,
	},
	{
		version: 2,
		stmt: `
CREATE TABLE IF NOT EXISTS credentials (
	name TEXT PRIMARY KEY,
	nonce BLOB NOT NULL,
	ciphertext BLOB NOT NULL,
	updated_at TEXT NOT NULL
) STRICT;`,
	},
	{
		version: 3,
		stmt: `
CREATE TABLE IF NOT EXISTS bm_streams (
	connector TEXT NOT NULL,
	stream TEXT NOT NULL,
	kind TEXT NOT NULL DEFAULT 'unstructured',
	first_seen_at TEXT NOT NULL,
	last_seen_at TEXT NOT NULL,
	row_count_lifetime INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (connector, stream)
) STRICT;`,
	},
	{
		version: 4,
		stmt: `
CREATE TABLE IF NOT EXISTS bm_metrics (
	fq_name TEXT PRIMARY KEY,
	unit TEXT NOT NULL DEFAULT '',
	direction TEXT NOT NULL DEFAULT 'neutral',
	aggregation TEXT NOT NULL DEFAULT 'none',
	last_value REAL,
	last_value_at TEXT,
	updated_at TEXT NOT NULL
) STRICT;`,
	},
}

// migrate ensures every entry in schemaMigrations has been applied.
func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_versions (
	version INTEGER PRIMARY KEY,
	applied_at TEXT NOT NULL
) STRICT;`)
	if err != nil {
		return fmt.Errorf("sqlite: init schema_versions: %w", err)
	}
	for _, m := range schemaMigrations {
		var seen int
		err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM schema_versions WHERE version = ?`, m.version).Scan(&seen)
		if err != nil {
			return fmt.Errorf("sqlite: check migration v%d: %w", m.version, err)
		}
		if seen > 0 {
			continue
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("sqlite: begin migration v%d: %w", m.version, err)
		}
		if _, err := tx.ExecContext(ctx, m.stmt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("sqlite: apply migration v%d: %w", m.version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_versions(version, applied_at) VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))`, m.version); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("sqlite: record migration v%d: %w", m.version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("sqlite: commit migration v%d: %w", m.version, err)
		}
	}
	return nil
}
