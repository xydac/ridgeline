package memory

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"
)

// DefaultWindows is the set of rolling window durations used when no override
// is provided.
var DefaultWindows = []int{7, 30, 90}

// BaselineRow holds computed statistics for one metric over one window.
type BaselineRow struct {
	FQName         string
	WindowDays     int
	Mean           float64
	Stddev         float64
	Min            float64
	Max            float64
	SampleCount    int
	LastComputedAt time.Time
}

// RecordMetricValue appends an observation to bm_metric_values using the
// current time as the observation timestamp. Duplicate (fq_name, observed_at)
// pairs are resolved by updating the stored value.
func (c *Catalog) RecordMetricValue(ctx context.Context, fqName string, value float64) error {
	return c.RecordMetricValueAt(ctx, fqName, value, time.Now())
}

// RecordMetricValueAt appends an observation to bm_metric_values at the
// specified timestamp. When a row for (fq_name, observed_at) already exists,
// its value is updated. This allows connectors to backfill historical
// time-series data at their declared record timestamps rather than the sync
// ingest time, so baselines accumulate one sample per record day.
func (c *Catalog) RecordMetricValueAt(ctx context.Context, fqName string, value float64, at time.Time) error {
	ts := at.UTC().Format(time.RFC3339)
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO bm_metric_values (fq_name, value, observed_at) VALUES (?, ?, ?)
		 ON CONFLICT(fq_name, observed_at) DO UPDATE SET value = excluded.value`,
		fqName, value, ts)
	if err != nil {
		return fmt.Errorf("memory: record metric value %s at %s: %w", fqName, ts, err)
	}
	return nil
}

// ComputeBaselines recomputes rolling-window statistics for fqName using
// windowDays as the list of window sizes. Results are upserted into
// bm_baselines. At most 10000 samples per window are used; if truncated a
// warning is printed but no error is returned.
func (c *Catalog) ComputeBaselines(ctx context.Context, fqName string, windowDays []int) error {
	for _, days := range windowDays {
		cutoff := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour).Format(time.RFC3339)
		rows, err := c.db.QueryContext(ctx,
			`SELECT value FROM bm_metric_values WHERE fq_name = ? AND observed_at >= ? ORDER BY observed_at LIMIT 10000`,
			fqName, cutoff)
		if err != nil {
			return fmt.Errorf("memory: query values for %s window %dd: %w", fqName, days, err)
		}
		var vals []float64
		for rows.Next() {
			var v float64
			if err := rows.Scan(&v); err != nil {
				rows.Close()
				return fmt.Errorf("memory: scan metric value: %w", err)
			}
			vals = append(vals, v)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("memory: iterate metric values: %w", err)
		}
		if len(vals) == 0 {
			continue
		}
		mean, stddev, minV, maxV := windowStats(vals)
		now := time.Now().UTC().Format(time.RFC3339)
		_, err = c.db.ExecContext(ctx, `
INSERT INTO bm_baselines (fq_name, window_days, mean, stddev, min, max, sample_count, last_computed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fq_name, window_days) DO UPDATE SET
    mean = excluded.mean,
    stddev = excluded.stddev,
    min = excluded.min,
    max = excluded.max,
    sample_count = excluded.sample_count,
    last_computed_at = excluded.last_computed_at`,
			fqName, days, mean, stddev, minV, maxV, len(vals), now)
		if err != nil {
			return fmt.Errorf("memory: upsert baseline %s window %dd: %w", fqName, days, err)
		}
	}
	return nil
}

// ListBaselines returns baseline rows for fqName sorted by window_days ascending.
func (c *Catalog) ListBaselines(ctx context.Context, fqName string) ([]BaselineRow, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT fq_name, window_days, mean, stddev, min, max, sample_count, last_computed_at
		 FROM bm_baselines WHERE fq_name = ? ORDER BY window_days`,
		fqName)
	if err != nil {
		return nil, fmt.Errorf("memory: list baselines %s: %w", fqName, err)
	}
	defer rows.Close()

	var out []BaselineRow
	for rows.Next() {
		var r BaselineRow
		var tsStr string
		if err := rows.Scan(&r.FQName, &r.WindowDays, &r.Mean, &r.Stddev, &r.Min, &r.Max, &r.SampleCount, &tsStr); err != nil {
			return nil, fmt.Errorf("memory: scan baseline row: %w", err)
		}
		r.LastComputedAt, _ = time.Parse(time.RFC3339, tsStr)
		out = append(out, r)
	}
	return out, rows.Err()
}

// Sparkline returns an ASCII sparkline of the most recent values for fqName
// within the last days calendar days. width controls the number of characters
// in the output. Returns an empty string if there are no values.
func (c *Catalog) Sparkline(ctx context.Context, fqName string, days, width int) (string, error) {
	cutoff := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour).Format(time.RFC3339)
	rows, err := c.db.QueryContext(ctx,
		`SELECT value FROM bm_metric_values WHERE fq_name = ? AND observed_at >= ? ORDER BY observed_at`,
		fqName, cutoff)
	if err != nil {
		return "", fmt.Errorf("memory: sparkline query %s: %w", fqName, err)
	}
	defer rows.Close()

	var vals []float64
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err != nil {
			return "", fmt.Errorf("memory: scan sparkline value: %w", err)
		}
		vals = append(vals, v)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("memory: iterate sparkline values: %w", err)
	}
	if len(vals) == 0 {
		return "", nil
	}
	return renderSparkline(vals, width), nil
}

// Recompute recomputes baselines for all metrics that have observations
// recorded within the last since duration. Pass 0 to recompute all.
func (c *Catalog) Recompute(ctx context.Context, since time.Duration, windowDays []int) error {
	var q string
	var args []interface{}
	if since > 0 {
		cutoff := time.Now().UTC().Add(-since).Format(time.RFC3339)
		q = `SELECT DISTINCT fq_name FROM bm_metric_values WHERE observed_at >= ? ORDER BY fq_name`
		args = []interface{}{cutoff}
	} else {
		q = `SELECT DISTINCT fq_name FROM bm_metric_values ORDER BY fq_name`
	}
	rows, err := c.db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("memory: recompute list metrics: %w", err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return fmt.Errorf("memory: recompute scan name: %w", err)
		}
		names = append(names, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, name := range names {
		if err := c.ComputeBaselines(ctx, name, windowDays); err != nil {
			return err
		}
	}
	return nil
}

// windowStats computes mean, population stddev, min, and max over vals.
// For a single value, stddev is 0.
func windowStats(vals []float64) (mean, stddev, min, max float64) {
	min = vals[0]
	max = vals[0]
	var sum float64
	for _, v := range vals {
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	mean = sum / float64(len(vals))
	if len(vals) < 2 {
		return mean, 0, min, max
	}
	var variance float64
	for _, v := range vals {
		d := v - mean
		variance += d * d
	}
	stddev = math.Sqrt(variance / float64(len(vals)))
	return mean, stddev, min, max
}

// sparkChars are Unicode block elements from lowest to highest.
var sparkChars = []rune("▁▂▃▄▅▆▇█")

// renderSparkline normalizes vals to the 8-level block range and returns a
// string of at most width characters. If len(vals) > width, values are
// subsampled by averaging buckets.
func renderSparkline(vals []float64, width int) string {
	if width <= 0 {
		width = 20
	}
	// subsample: average vals into at most width buckets
	samples := subsample(vals, width)

	minV, maxV := samples[0], samples[0]
	for _, v := range samples {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}

	var sb strings.Builder
	rng := maxV - minV
	for _, v := range samples {
		var idx int
		if rng > 0 {
			idx = int((v-minV)/rng*float64(len(sparkChars)-1) + 0.5)
		}
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sparkChars) {
			idx = len(sparkChars) - 1
		}
		sb.WriteRune(sparkChars[idx])
	}
	return sb.String()
}

// subsample reduces vals to at most n points by averaging non-overlapping
// equal-width buckets.
func subsample(vals []float64, n int) []float64 {
	if len(vals) <= n {
		return vals
	}
	out := make([]float64, n)
	for i := range out {
		lo := i * len(vals) / n
		hi := (i + 1) * len(vals) / n
		var sum float64
		for _, v := range vals[lo:hi] {
			sum += v
		}
		out[i] = sum / float64(hi-lo)
	}
	return out
}
