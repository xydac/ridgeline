// Package memory owns the Business Memory catalog: persistent, queryable
// understanding of a business's data streams and metrics that accumulates
// across sync runs and survives sink wipes.
package memory

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Catalog wraps the Business Memory tables in the state store DB.
// All writes are best-effort: callers should log errors but not treat
// them as fatal to the sync pipeline.
type Catalog struct {
	db *sql.DB
}

// New returns a Catalog backed by db, which must already have the
// bm_streams and bm_metrics tables (created by the state store migrations).
func New(db *sql.DB) *Catalog {
	return &Catalog{db: db}
}

// StreamRow is one row from the bm_streams table.
type StreamRow struct {
	Connector        string
	Stream           string
	Kind             string
	FirstSeenAt      time.Time
	LastSeenAt       time.Time
	RowCountLifetime int64
}

// MetricRow is one row from the bm_metrics table.
type MetricRow struct {
	FQName      string
	Unit        string
	Direction   string
	Aggregation string
	LastValue   *float64
	LastValueAt *time.Time
}

// UpsertStream records (or updates) a stream observation. On first
// observation, first_seen_at is set to now. On subsequent calls,
// first_seen_at is preserved and last_seen_at is advanced.
// newRows is added to the lifetime row count.
func (c *Catalog) UpsertStream(ctx context.Context, connector, stream, kind string, newRows int64) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := c.db.ExecContext(ctx, `
INSERT INTO bm_streams (connector, stream, kind, first_seen_at, last_seen_at, row_count_lifetime)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(connector, stream) DO UPDATE SET
    kind = excluded.kind,
    last_seen_at = excluded.last_seen_at,
    row_count_lifetime = bm_streams.row_count_lifetime + excluded.row_count_lifetime
`, connector, stream, kind, now, now, newRows)
	if err != nil {
		return fmt.Errorf("memory: upsert stream %s/%s: %w", connector, stream, err)
	}
	return nil
}

// UpsertMetric records (or updates) a metric column declaration.
// fqName is the fully-qualified metric name (e.g. "plausible.daily.visitors").
// lastValue may be nil when the value is not yet known.
func (c *Catalog) UpsertMetric(ctx context.Context, fqName, unit, direction, aggregation string, lastValue *float64) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := c.db.ExecContext(ctx, `
INSERT INTO bm_metrics (fq_name, unit, direction, aggregation, last_value, last_value_at, updated_at)
VALUES (?, ?, ?, ?, ?, CASE WHEN ? IS NOT NULL THEN ? ELSE NULL END, ?)
ON CONFLICT(fq_name) DO UPDATE SET
    unit = excluded.unit,
    direction = excluded.direction,
    aggregation = excluded.aggregation,
    last_value = COALESCE(excluded.last_value, bm_metrics.last_value),
    last_value_at = CASE WHEN excluded.last_value IS NOT NULL THEN excluded.last_value_at ELSE bm_metrics.last_value_at END,
    updated_at = excluded.updated_at
`, fqName, unit, direction, aggregation, lastValue, lastValue, now, now)
	if err != nil {
		return fmt.Errorf("memory: upsert metric %s: %w", fqName, err)
	}
	return nil
}

// ListStreams returns all streams known to Business Memory, ordered by
// connector then stream name.
func (c *Catalog) ListStreams(ctx context.Context) ([]StreamRow, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT connector, stream, kind, first_seen_at, last_seen_at, row_count_lifetime
FROM bm_streams
ORDER BY connector, stream`)
	if err != nil {
		return nil, fmt.Errorf("memory: list streams: %w", err)
	}
	defer rows.Close()

	var out []StreamRow
	for rows.Next() {
		var r StreamRow
		var firstStr, lastStr string
		if err := rows.Scan(&r.Connector, &r.Stream, &r.Kind, &firstStr, &lastStr, &r.RowCountLifetime); err != nil {
			return nil, fmt.Errorf("memory: scan stream row: %w", err)
		}
		r.FirstSeenAt, _ = time.Parse(time.RFC3339, firstStr)
		r.LastSeenAt, _ = time.Parse(time.RFC3339, lastStr)
		out = append(out, r)
	}
	return out, rows.Err()
}

// MetricDeclared reports whether fqName is present in bm_metrics.
// It distinguishes "metric was never declared" from "declared but no
// baselines computed yet", enabling callers to surface a clear error
// for typo'd or impossible metric names.
func (c *Catalog) MetricDeclared(ctx context.Context, fqName string) (bool, error) {
	var n int
	err := c.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM bm_metrics WHERE fq_name = ?`, fqName).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("memory: metric declared check for %s: %w", fqName, err)
	}
	return n > 0, nil
}

// ListMetrics returns all metrics known to Business Memory, ordered by
// fully-qualified name.
func (c *Catalog) ListMetrics(ctx context.Context) ([]MetricRow, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT fq_name, unit, direction, aggregation, last_value, last_value_at
FROM bm_metrics
ORDER BY fq_name`)
	if err != nil {
		return nil, fmt.Errorf("memory: list metrics: %w", err)
	}
	defer rows.Close()

	var out []MetricRow
	for rows.Next() {
		var r MetricRow
		var lastValAtStr sql.NullString
		if err := rows.Scan(&r.FQName, &r.Unit, &r.Direction, &r.Aggregation, &r.LastValue, &lastValAtStr); err != nil {
			return nil, fmt.Errorf("memory: scan metric row: %w", err)
		}
		if lastValAtStr.Valid {
			t, _ := time.Parse(time.RFC3339, lastValAtStr.String)
			r.LastValueAt = &t
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
