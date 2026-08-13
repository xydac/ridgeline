package memory

import (
	"context"
	"fmt"
	"math"
	"time"
)

// EventRow is one row from bm_events.
type EventRow struct {
	ID             int64
	Kind           string
	MetricFQ       string
	ObservedValue  float64
	BaselineMean   float64
	StddevFromMean float64
	Direction      string
	WindowDays     int
	Description    string
	At             time.Time
}

// DefaultAnomalyK is the default stddev multiplier for anomaly detection.
const DefaultAnomalyK = 2.5

// DefaultMinSamples is the minimum baseline sample_count before a metric
// is eligible for anomaly detection.
const DefaultMinSamples = 14

// DetectAndRecordAnomalies checks whether value deviates from established
// baselines for fqName by more than k standard deviations. For each window
// where the deviation exceeds the threshold, an event is inserted into
// bm_events. k must be > 0; minSamples must be >= 1.
//
// The directionality of the metric (higher_is_better, lower_is_better,
// neutral) is read from bm_metrics to label each event as surprise-good,
// surprise-bad, or surprise-neutral. A stddev of 0 (uniform baseline) is
// skipped -- deviation is meaningless in that case.
//
// The unique constraint on (metric_fq, window_days, at) makes repeated
// calls with the same arguments idempotent.
func (c *Catalog) DetectAndRecordAnomalies(ctx context.Context, fqName string, value float64, observedAt time.Time, k float64, minSamples int) error {
	if k <= 0 {
		k = DefaultAnomalyK
	}
	if minSamples < 1 {
		minSamples = DefaultMinSamples
	}

	direction, err := c.metricDirection(ctx, fqName)
	if err != nil {
		return fmt.Errorf("memory: anomaly detect %s: %w", fqName, err)
	}

	rows, err := c.db.QueryContext(ctx,
		`SELECT window_days, mean, stddev FROM bm_baselines
		 WHERE fq_name = ? AND sample_count >= ?
		 ORDER BY window_days`,
		fqName, minSamples)
	if err != nil {
		return fmt.Errorf("memory: anomaly query baselines %s: %w", fqName, err)
	}
	defer rows.Close()

	atStr := observedAt.UTC().Format(time.RFC3339)

	type baselineCandidate struct {
		windowDays int
		mean       float64
		stddev     float64
	}
	var candidates []baselineCandidate
	for rows.Next() {
		var c baselineCandidate
		if err := rows.Scan(&c.windowDays, &c.mean, &c.stddev); err != nil {
			return fmt.Errorf("memory: anomaly scan baseline: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("memory: anomaly iterate baselines: %w", err)
	}

	for _, b := range candidates {
		if b.stddev == 0 {
			continue
		}
		deviation := (value - b.mean) / b.stddev
		if math.Abs(deviation) < k {
			continue
		}
		dir := anomalyDirection(direction, deviation)
		_, err := c.db.ExecContext(ctx, `
INSERT OR IGNORE INTO bm_events
	(kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, at)
VALUES ('anomaly', ?, ?, ?, ?, ?, ?, ?)`,
			fqName, value, b.mean, deviation, dir, b.windowDays, atStr)
		if err != nil {
			return fmt.Errorf("memory: insert event %s: %w", fqName, err)
		}
	}
	return nil
}

// InsertManualEvent inserts a user-created or connector-generated event
// that is not an anomaly detection result. kind is free-form (e.g. "deploy",
// "commit", "release"). Duplicate (kind, metric_fq=”, window_days=0, at)
// tuples are ignored.
func (c *Catalog) InsertManualEvent(ctx context.Context, kind, description string, at time.Time) error {
	atStr := at.UTC().Format(time.RFC3339)
	_, err := c.db.ExecContext(ctx, `
INSERT OR IGNORE INTO bm_events
	(kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, description, at)
VALUES (?, '', 0.0, 0.0, 0.0, 'none', 0, ?, ?)`,
		kind, description, atStr)
	if err != nil {
		return fmt.Errorf("memory: insert event %s: %w", kind, err)
	}
	return nil
}

// InsertCommitEvent inserts a git commit event, using the commit hash as the
// deduplication key via metric_fq so repeated syncs of the same repo are
// idempotent.
func (c *Catalog) InsertCommitEvent(ctx context.Context, hash, description string, at time.Time) error {
	atStr := at.UTC().Format(time.RFC3339)
	_, err := c.db.ExecContext(ctx, `
INSERT OR IGNORE INTO bm_events
	(kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, description, at)
VALUES ('commit', ?, 0.0, 0.0, 0.0, 'none', 0, ?, ?)`,
		hash, description, atStr)
	if err != nil {
		return fmt.Errorf("memory: insert commit event %s: %w", hash, err)
	}
	return nil
}

// ListEvents returns events newer than since (0 = all), newest-first.
func (c *Catalog) ListEvents(ctx context.Context, since time.Duration) ([]EventRow, error) {
	var q string
	var args []interface{}
	if since > 0 {
		cutoff := time.Now().UTC().Add(-since).Format(time.RFC3339)
		q = `SELECT id, kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, COALESCE(description,''), at
			 FROM bm_events WHERE at >= ? ORDER BY at DESC`
		args = []interface{}{cutoff}
	} else {
		q = `SELECT id, kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, COALESCE(description,''), at
			 FROM bm_events ORDER BY at DESC`
	}

	rows, err := c.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("memory: list events: %w", err)
	}
	defer rows.Close()

	var out []EventRow
	for rows.Next() {
		var r EventRow
		var atStr string
		if err := rows.Scan(&r.ID, &r.Kind, &r.MetricFQ, &r.ObservedValue, &r.BaselineMean, &r.StddevFromMean, &r.Direction, &r.WindowDays, &r.Description, &atStr); err != nil {
			return nil, fmt.Errorf("memory: scan event row: %w", err)
		}
		r.At, _ = time.Parse(time.RFC3339, atStr)
		out = append(out, r)
	}
	return out, rows.Err()
}

// metricDirection returns the direction string for fqName from bm_metrics.
// Returns "neutral" if the metric is not found.
func (c *Catalog) metricDirection(ctx context.Context, fqName string) (string, error) {
	var dir string
	err := c.db.QueryRowContext(ctx,
		`SELECT direction FROM bm_metrics WHERE fq_name = ?`, fqName).Scan(&dir)
	if err != nil {
		return "neutral", nil //nolint -- metric not yet cataloged, treat as neutral
	}
	return dir, nil
}

// anomalyDirection maps metric directionality + deviation sign to a label.
func anomalyDirection(metricDir string, deviation float64) string {
	switch metricDir {
	case "higher_is_better":
		if deviation > 0 {
			return "surprise-good"
		}
		return "surprise-bad"
	case "lower_is_better":
		if deviation < 0 {
			return "surprise-good"
		}
		return "surprise-bad"
	default:
		return "surprise-neutral"
	}
}
