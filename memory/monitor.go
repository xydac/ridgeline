package memory

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// WatchRow is one row from bm_watches.
type WatchRow struct {
	Name            string
	MetricFQ        string
	Op              string // "above", "below", "deviates-by"
	Threshold       float64
	Unit            string // "" for raw value; "sigma" for stddev
	Condition       string // original condition expression for display
	CreatedAt       time.Time
	LastTriggeredAt *time.Time
}

// WatchTrigger describes one watch rule that fired during RunWatches.
type WatchTrigger struct {
	WatchName string
	MetricFQ  string
	Condition string
	Value     float64 // the metric value that triggered the rule
	Deviation float64 // stddev deviation (deviates-by only; 0 otherwise)
	At        time.Time
}

// WatchRunResult holds the outcome of RunWatches.
type WatchRunResult struct {
	Evaluated int
	Triggered []WatchTrigger
}

// ParseCondition parses a condition expression into (op, threshold, unit).
// Accepted forms: "above N", "below N", "deviates-by N", "deviates-by Nsigma".
// N must be a non-negative finite number.
func ParseCondition(expr string) (op string, threshold float64, unit string, err error) {
	parts := strings.Fields(expr)
	if len(parts) != 2 {
		return "", 0, "", fmt.Errorf("condition must be two tokens (e.g. \"above 1000\" or \"deviates-by 2.5sigma\"): got %q", expr)
	}
	op = parts[0]
	if op != "above" && op != "below" && op != "deviates-by" {
		return "", 0, "", fmt.Errorf("unknown operator %q; valid operators: above, below, deviates-by", op)
	}
	rawVal := parts[1]
	if strings.HasSuffix(rawVal, "sigma") {
		unit = "sigma"
		rawVal = strings.TrimSuffix(rawVal, "sigma")
	}
	threshold, err = strconv.ParseFloat(rawVal, 64)
	if err != nil || math.IsNaN(threshold) || math.IsInf(threshold, 0) || threshold < 0 {
		return "", 0, "", fmt.Errorf("threshold must be a non-negative number: got %q", parts[1])
	}
	if op != "deviates-by" && unit == "sigma" {
		return "", 0, "", fmt.Errorf("sigma unit only valid with deviates-by operator")
	}
	return op, threshold, unit, nil
}

// AddWatch persists a watch rule to bm_watches.
// name must be unique. condition is the raw expression (e.g. "above 1000").
func (c *Catalog) AddWatch(ctx context.Context, name, metricFQ, condition string) error {
	op, threshold, unit, err := ParseCondition(condition)
	if err != nil {
		return fmt.Errorf("monitor add: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err = c.db.ExecContext(ctx, `
INSERT INTO bm_watches (name, metric_fq, op, threshold, unit, condition, extra, created_at)
VALUES (?, ?, ?, ?, ?, ?, '{}', ?)`,
		name, metricFQ, op, threshold, unit, condition, now)
	if err != nil {
		return fmt.Errorf("monitor add %q: %w", name, err)
	}
	return nil
}

// RemoveWatch deletes a watch rule by name. Returns an error if not found.
func (c *Catalog) RemoveWatch(ctx context.Context, name string) error {
	res, err := c.db.ExecContext(ctx, `DELETE FROM bm_watches WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("monitor rm %q: %w", name, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("monitor rm: watch %q not found", name)
	}
	return nil
}

// ListWatches returns all watch rules ordered by name.
func (c *Catalog) ListWatches(ctx context.Context) ([]WatchRow, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT name, metric_fq, op, threshold, unit, condition, created_at, last_triggered_at
FROM bm_watches ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("monitor list: %w", err)
	}
	defer rows.Close()

	var out []WatchRow
	for rows.Next() {
		var r WatchRow
		var createdStr string
		var lastStr *string
		if err := rows.Scan(&r.Name, &r.MetricFQ, &r.Op, &r.Threshold, &r.Unit, &r.Condition, &createdStr, &lastStr); err != nil {
			return nil, fmt.Errorf("monitor list: scan: %w", err)
		}
		r.CreatedAt, _ = time.Parse(time.RFC3339, createdStr)
		if lastStr != nil {
			t, _ := time.Parse(time.RFC3339, *lastStr)
			r.LastTriggeredAt = &t
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RunWatches evaluates all registered watch rules against current Business
// Memory state. For each rule that fires, a "monitor" event is appended to
// bm_events and last_triggered_at is updated on the watch row.
// Returns the list of triggered rules and any evaluation error.
func (c *Catalog) RunWatches(ctx context.Context) (*WatchRunResult, error) {
	watches, err := c.ListWatches(ctx)
	if err != nil {
		return nil, err
	}

	result := &WatchRunResult{Evaluated: len(watches)}

	for _, w := range watches {
		triggered, val, dev, evalErr := c.evaluateWatch(ctx, w)
		if evalErr != nil {
			// Non-fatal: metric may have no data yet. Skip.
			continue
		}
		if !triggered {
			continue
		}

		now := time.Now().UTC()
		nowStr := now.Format(time.RFC3339)
		desc := fmt.Sprintf("watch %q triggered: %s %s", w.Name, w.MetricFQ, w.Condition)

		// Insert monitor event (unique on kind+metric_fq+window_days+at; ignore dupes).
		_, _ = c.db.ExecContext(ctx, `
INSERT OR IGNORE INTO bm_events
	(kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, description, at)
VALUES ('monitor', ?, ?, 0.0, ?, 'none', 0, ?, ?)`,
			w.MetricFQ, val, dev, desc, nowStr)

		// Update last_triggered_at on the watch.
		_, _ = c.db.ExecContext(ctx, `UPDATE bm_watches SET last_triggered_at = ? WHERE name = ?`, nowStr, w.Name)

		result.Triggered = append(result.Triggered, WatchTrigger{
			WatchName: w.Name,
			MetricFQ:  w.MetricFQ,
			Condition: w.Condition,
			Value:     val,
			Deviation: dev,
			At:        now,
		})
	}

	return result, nil
}

// evaluateWatch checks whether a single watch rule is currently triggered.
// Returns (triggered, currentValue, deviationFromBaseline, error).
func (c *Catalog) evaluateWatch(ctx context.Context, w WatchRow) (bool, float64, float64, error) {
	var lastValue float64
	err := c.db.QueryRowContext(ctx,
		`SELECT last_value FROM bm_metrics WHERE fq_name = ? AND last_value IS NOT NULL`,
		w.MetricFQ).Scan(&lastValue)
	if err != nil {
		return false, 0, 0, fmt.Errorf("no current value for %s", w.MetricFQ)
	}

	switch w.Op {
	case "above":
		return lastValue > w.Threshold, lastValue, 0, nil
	case "below":
		return lastValue < w.Threshold, lastValue, 0, nil
	case "deviates-by":
		// Read the 30-day baseline; fall back to 7-day if absent.
		var mean, stddev float64
		err := c.db.QueryRowContext(ctx, `
SELECT mean, stddev FROM bm_baselines
WHERE fq_name = ? AND window_days IN (30, 7) ORDER BY window_days DESC LIMIT 1`, w.MetricFQ).Scan(&mean, &stddev)
		if err != nil || stddev == 0 {
			return false, lastValue, 0, fmt.Errorf("no baseline for %s", w.MetricFQ)
		}
		dev := math.Abs(lastValue-mean) / stddev
		return dev >= w.Threshold, lastValue, dev, nil
	}
	return false, 0, 0, fmt.Errorf("unknown op %q", w.Op)
}

// ComposeMonitorRunNarrative formats a WatchRunResult as human-readable text.
func ComposeMonitorRunNarrative(r *WatchRunResult) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Evaluated %d watch rule(s).\n", r.Evaluated)
	if len(r.Triggered) == 0 {
		fmt.Fprintln(&sb, "No rules triggered.")
		return sb.String()
	}
	fmt.Fprintf(&sb, "%d rule(s) triggered:\n\n", len(r.Triggered))
	for _, t := range r.Triggered {
		if t.Deviation > 0 {
			fmt.Fprintf(&sb, "  %s  %s  %s  (value: %.4g, deviation: %.2f sigma)\n",
				t.WatchName, t.MetricFQ, t.Condition, t.Value, t.Deviation)
		} else {
			fmt.Fprintf(&sb, "  %s  %s  %s  (value: %.4g)\n",
				t.WatchName, t.MetricFQ, t.Condition, t.Value)
		}
	}
	fmt.Fprintln(&sb, "\nTriggered events have been recorded in 'ridgeline memory events'.")
	return sb.String()
}

// MonitorRunJSON is the structured --json output for monitor run.
type MonitorRunJSON struct {
	Evaluated int                `json:"evaluated"`
	Triggered []WatchTriggerJSON `json:"triggered"`
}

// WatchTriggerJSON is one triggered entry in MonitorRunJSON.
type WatchTriggerJSON struct {
	WatchName string  `json:"watch_name"`
	MetricFQ  string  `json:"metric_fq"`
	Condition string  `json:"condition"`
	Value     float64 `json:"value"`
	Deviation float64 `json:"deviation,omitempty"`
	At        string  `json:"at"`
}

// ToMonitorRunJSON converts WatchRunResult to the structured JSON output type.
func ToMonitorRunJSON(r *WatchRunResult) *MonitorRunJSON {
	j := &MonitorRunJSON{
		Evaluated: r.Evaluated,
		Triggered: make([]WatchTriggerJSON, 0, len(r.Triggered)),
	}
	for _, t := range r.Triggered {
		j.Triggered = append(j.Triggered, WatchTriggerJSON{
			WatchName: t.WatchName,
			MetricFQ:  t.MetricFQ,
			Condition: t.Condition,
			Value:     t.Value,
			Deviation: t.Deviation,
			At:        t.At.UTC().Format(time.RFC3339),
		})
	}
	return j
}
