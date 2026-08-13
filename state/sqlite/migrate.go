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
	{
		version: 5,
		stmt: `
CREATE TABLE IF NOT EXISTS bm_metric_values (
	fq_name TEXT NOT NULL,
	value REAL NOT NULL,
	observed_at TEXT NOT NULL
) STRICT;`,
	},
	{
		version: 6,
		stmt: `
CREATE INDEX IF NOT EXISTS idx_bm_metric_values_lookup
	ON bm_metric_values (fq_name, observed_at);`,
	},
	{
		version: 7,
		stmt: `
CREATE TABLE IF NOT EXISTS bm_baselines (
	fq_name TEXT NOT NULL,
	window_days INTEGER NOT NULL,
	mean REAL NOT NULL,
	stddev REAL NOT NULL,
	min REAL NOT NULL,
	max REAL NOT NULL,
	sample_count INTEGER NOT NULL,
	last_computed_at TEXT NOT NULL,
	PRIMARY KEY (fq_name, window_days)
) STRICT;`,
	},
	{
		version: 8,
		stmt: `
CREATE TABLE IF NOT EXISTS bm_events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	kind TEXT NOT NULL DEFAULT 'anomaly',
	metric_fq TEXT NOT NULL,
	observed_value REAL NOT NULL,
	baseline_mean REAL NOT NULL,
	stddev_from_mean REAL NOT NULL,
	direction TEXT NOT NULL DEFAULT 'surprise-neutral',
	window_days INTEGER NOT NULL,
	at TEXT NOT NULL,
	UNIQUE (metric_fq, window_days, at)
) STRICT;`,
	},
	{
		version: 9,
		stmt: `
CREATE INDEX IF NOT EXISTS idx_bm_events_at
	ON bm_events (at DESC);`,
	},
	{
		version: 10,
		stmt: `
CREATE TABLE bm_events_v10 (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	kind TEXT NOT NULL DEFAULT 'anomaly',
	metric_fq TEXT NOT NULL DEFAULT '',
	observed_value REAL NOT NULL DEFAULT 0.0,
	baseline_mean REAL NOT NULL DEFAULT 0.0,
	stddev_from_mean REAL NOT NULL DEFAULT 0.0,
	direction TEXT NOT NULL DEFAULT 'surprise-neutral',
	window_days INTEGER NOT NULL DEFAULT 0,
	description TEXT,
	at TEXT NOT NULL,
	UNIQUE (kind, metric_fq, window_days, at)
) STRICT;
INSERT INTO bm_events_v10
	SELECT id, kind, metric_fq, observed_value, baseline_mean, stddev_from_mean,
	       direction, window_days, NULL, at
	FROM bm_events;
DROP TABLE bm_events;
ALTER TABLE bm_events_v10 RENAME TO bm_events;
CREATE INDEX IF NOT EXISTS idx_bm_events_at_v10 ON bm_events (at DESC);`,
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
