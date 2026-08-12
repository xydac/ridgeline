package memory

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"
)

// ExplainData holds the raw data assembled from the memory catalog for
// a metric explain report. All fields are populated by ExplainMetric.
type ExplainData struct {
	MetricFQ      string
	Direction     string
	Unit          string
	Since         time.Duration
	CurrentValue  *float64
	CurrentAt     *time.Time
	Baseline      *BaselineRow // best-matching baseline; nil if none exists
	WindowMean    float64      // mean of values in [now-since, now]
	WindowSamples int
	PriorMean     *float64 // mean in [now-2*since, now-since]; nil if no data
	PriorSamples  int
	Anomalies     []EventRow // anomaly events in the since window, newest-first
}

// ExplainJSON is the structured output format for --json.
type ExplainJSON struct {
	MetricFQ      string        `json:"metric_fq"`
	Since         string        `json:"since"`
	CurrentValue  *float64      `json:"current_value,omitempty"`
	CurrentAt     *string       `json:"current_at,omitempty"`
	Direction     string        `json:"direction"`
	Unit          string        `json:"unit"`
	Baseline      *BaselineJSON `json:"baseline,omitempty"`
	WindowMean    float64       `json:"window_mean"`
	WindowSamples int           `json:"window_samples"`
	PriorMean     *float64      `json:"prior_mean,omitempty"`
	PriorSamples  int           `json:"prior_samples,omitempty"`
	Anomalies     []AnomalyJSON `json:"anomalies"`
	Summary       string        `json:"summary"`
}

// BaselineJSON is the baseline sub-object in ExplainJSON.
type BaselineJSON struct {
	WindowDays  int     `json:"window_days"`
	Mean        float64 `json:"mean"`
	Stddev      float64 `json:"stddev"`
	SampleCount int     `json:"sample_count"`
}

// AnomalyJSON is one anomaly event in ExplainJSON.
type AnomalyJSON struct {
	At             string  `json:"at"`
	ObservedValue  float64 `json:"observed_value"`
	BaselineMean   float64 `json:"baseline_mean"`
	StddevFromMean float64 `json:"stddev_from_mean"`
	WindowDays     int     `json:"window_days"`
	Direction      string  `json:"direction"`
}

// ExplainMetric assembles an ExplainData for fqName over the since window.
// It reads the metric's current value, best-matching baseline, window and
// prior-period statistics, and any anomaly events in the window.
// Returns an error if the metric is not in the Business Memory catalog.
func (c *Catalog) ExplainMetric(ctx context.Context, fqName string, since time.Duration) (*ExplainData, error) {
	d := &ExplainData{MetricFQ: fqName, Since: since}

	var lastValAtStr sql.NullString
	err := c.db.QueryRowContext(ctx,
		`SELECT direction, unit, last_value, last_value_at FROM bm_metrics WHERE fq_name = ?`,
		fqName).Scan(&d.Direction, &d.Unit, &d.CurrentValue, &lastValAtStr)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("metric %q not found in Business Memory catalog; run 'ridgeline sync' first", fqName)
	}
	if err != nil {
		return nil, fmt.Errorf("memory: explain %s: %w", fqName, err)
	}
	if lastValAtStr.Valid {
		t, _ := time.Parse(time.RFC3339, lastValAtStr.String)
		d.CurrentAt = &t
	}

	sinceDays := int(math.Ceil(since.Hours() / 24))
	d.Baseline = c.pickBaseline(ctx, fqName, sinceDays)

	now := time.Now().UTC()
	windowStart := now.Add(-since)
	// Use now+1s as the upper bound so values inserted at this exact second
	// are included despite the strict-less-than boundary in periodMean.
	d.WindowMean, d.WindowSamples = c.periodMean(ctx, fqName, windowStart, now.Add(time.Second))
	priorMean, priorSamples := c.periodMean(ctx, fqName, windowStart.Add(-since), windowStart)
	if priorSamples > 0 {
		d.PriorMean = &priorMean
		d.PriorSamples = priorSamples
	}

	d.Anomalies, _ = c.eventsInWindow(ctx, fqName, since)

	return d, nil
}

// ToExplainJSON converts ExplainData to the structured JSON output type.
func ToExplainJSON(d *ExplainData) *ExplainJSON {
	j := &ExplainJSON{
		MetricFQ:      d.MetricFQ,
		Since:         FormatSince(d.Since),
		CurrentValue:  d.CurrentValue,
		Direction:     d.Direction,
		Unit:          d.Unit,
		WindowMean:    d.WindowMean,
		WindowSamples: d.WindowSamples,
		PriorMean:     d.PriorMean,
		PriorSamples:  d.PriorSamples,
		Anomalies:     []AnomalyJSON{},
		Summary:       composeSummary(d),
	}
	if d.CurrentAt != nil {
		s := d.CurrentAt.Format(time.RFC3339)
		j.CurrentAt = &s
	}
	if d.Baseline != nil {
		j.Baseline = &BaselineJSON{
			WindowDays:  d.Baseline.WindowDays,
			Mean:        d.Baseline.Mean,
			Stddev:      d.Baseline.Stddev,
			SampleCount: d.Baseline.SampleCount,
		}
	}
	for _, e := range d.Anomalies {
		j.Anomalies = append(j.Anomalies, AnomalyJSON{
			At:             e.At.Format(time.RFC3339),
			ObservedValue:  e.ObservedValue,
			BaselineMean:   e.BaselineMean,
			StddevFromMean: e.StddevFromMean,
			WindowDays:     e.WindowDays,
			Direction:      e.Direction,
		})
	}
	return j
}

// FormatSince formats a duration as a compact string: whole-day durations
// render as "Nd", others as Go's default duration string.
func FormatSince(d time.Duration) string {
	days := int(d.Hours() / 24)
	if days > 0 && d == time.Duration(days)*24*time.Hour {
		return fmt.Sprintf("%dd", days)
	}
	return d.String()
}

// ComposeNarrative returns a plain-text explanation stanza for d.
// Output is 3-6 sentences covering current value, baseline comparison,
// prior-period change, and any anomalies -- suitable for agent consumption.
func ComposeNarrative(d *ExplainData) string {
	var sb strings.Builder
	sinceStr := FormatSince(d.Since)

	fmt.Fprintf(&sb, "%s -- last %s\n\n", d.MetricFQ, sinceStr)

	if d.CurrentValue != nil {
		dateStr := ""
		if d.CurrentAt != nil {
			dateStr = " (as of " + d.CurrentAt.Format("2006-01-02") + ")"
		}
		fmt.Fprintf(&sb, "Current value: %.4g %s%s.\n", *d.CurrentValue, d.Unit, dateStr)
	} else if d.WindowSamples == 0 {
		fmt.Fprintln(&sb, "No observations recorded in this window.")
		return sb.String()
	}

	if d.Baseline != nil {
		b := d.Baseline
		dirLabel := directionLabel(d.Direction)
		if d.CurrentValue != nil && b.Stddev > 0 {
			dev := (*d.CurrentValue - b.Mean) / b.Stddev
			fmt.Fprintf(&sb, "The %dd baseline is %.4g +/- %.4g %s (%s); current is %+.1f sigma from the mean.\n",
				b.WindowDays, b.Mean, b.Stddev, d.Unit, dirLabel, dev)
		} else {
			fmt.Fprintf(&sb, "The %dd baseline mean is %.4g %s (%s).\n",
				b.WindowDays, b.Mean, d.Unit, dirLabel)
		}
	}

	if d.PriorMean != nil && d.WindowSamples > 0 {
		pct := 0.0
		if *d.PriorMean != 0 {
			pct = (d.WindowMean - *d.PriorMean) / math.Abs(*d.PriorMean) * 100
		}
		fmt.Fprintf(&sb, "Compared to the prior %s (mean %.4g, %d samples), this period is %+.1f%%.\n",
			sinceStr, *d.PriorMean, d.PriorSamples, pct)
	}

	if len(d.Anomalies) == 0 {
		fmt.Fprintf(&sb, "No anomalies detected in the last %s.\n", sinceStr)
	} else {
		word := "anomaly"
		if len(d.Anomalies) > 1 {
			word = "anomalies"
		}
		fmt.Fprintf(&sb, "%d %s detected in the last %s:\n", len(d.Anomalies), word, sinceStr)
		for _, e := range d.Anomalies {
			fmt.Fprintf(&sb, "  %s: %.4g observed (%+.1f sigma from %dd baseline) -- %s\n",
				e.At.Format("2006-01-02"), e.ObservedValue, e.StddevFromMean, e.WindowDays, e.Direction)
		}
	}

	fmt.Fprintf(&sb, "\nSummary: %s\n", composeSummary(d))
	return sb.String()
}

func directionLabel(dir string) string {
	switch dir {
	case "higher_is_better":
		return "higher is better"
	case "lower_is_better":
		return "lower is better"
	default:
		return "neutral"
	}
}

func composeSummary(d *ExplainData) string {
	short := metricShortName(d.MetricFQ)
	trend := "near baseline"
	if d.Baseline != nil && d.CurrentValue != nil && d.Baseline.Stddev > 0 {
		dev := (*d.CurrentValue - d.Baseline.Mean) / d.Baseline.Stddev
		switch {
		case dev > 1.0 && d.Direction == "higher_is_better":
			trend = "above baseline (positive)"
		case dev > 1.0 && d.Direction == "lower_is_better":
			trend = "above baseline (watch)"
		case dev > 1.0:
			trend = "above baseline"
		case dev < -1.0 && d.Direction == "lower_is_better":
			trend = "below baseline (positive)"
		case dev < -1.0 && d.Direction == "higher_is_better":
			trend = "below baseline (watch)"
		case dev < -1.0:
			trend = "below baseline"
		}
	} else if d.WindowSamples > 0 && d.Baseline != nil {
		if d.WindowMean > d.Baseline.Mean {
			trend = "above baseline"
		} else if d.WindowMean < d.Baseline.Mean {
			trend = "below baseline"
		}
	}

	anomalyPart := ""
	if len(d.Anomalies) > 0 {
		a := d.Anomalies[0]
		if len(d.Anomalies) == 1 {
			anomalyPart = fmt.Sprintf(", with one %s spike on %s",
				a.Direction, a.At.Format("2006-01-02"))
		} else {
			anomalyPart = fmt.Sprintf(", with %d anomalies including a %s spike on %s",
				len(d.Anomalies), a.Direction, a.At.Format("2006-01-02"))
		}
	}

	return fmt.Sprintf("%s is %s%s.", short, trend, anomalyPart)
}

func metricShortName(fq string) string {
	if i := strings.LastIndex(fq, "."); i >= 0 {
		return fq[i+1:]
	}
	return fq
}

// pickBaseline selects the best BaselineRow for the given metric and since
// window. Picks the smallest window_days >= sinceDays, or the largest
// available if none qualifies.
func (c *Catalog) pickBaseline(ctx context.Context, fqName string, sinceDays int) *BaselineRow {
	rows, err := c.db.QueryContext(ctx,
		`SELECT fq_name, window_days, mean, stddev, min, max, sample_count, last_computed_at
		 FROM bm_baselines WHERE fq_name = ? ORDER BY window_days`, fqName)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var all []BaselineRow
	for rows.Next() {
		var r BaselineRow
		var tsStr string
		if err := rows.Scan(&r.FQName, &r.WindowDays, &r.Mean, &r.Stddev, &r.Min, &r.Max, &r.SampleCount, &tsStr); err != nil {
			return nil
		}
		r.LastComputedAt, _ = time.Parse(time.RFC3339, tsStr)
		all = append(all, r)
	}
	if len(all) == 0 {
		return nil
	}
	for i := range all {
		if all[i].WindowDays >= sinceDays {
			return &all[i]
		}
	}
	return &all[len(all)-1]
}

// periodMean returns the mean and sample count of metric values in [start, end).
func (c *Catalog) periodMean(ctx context.Context, fqName string, start, end time.Time) (float64, int) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT value FROM bm_metric_values WHERE fq_name = ? AND observed_at >= ? AND observed_at < ?`,
		fqName, start.Format(time.RFC3339), end.Format(time.RFC3339))
	if err != nil {
		return 0, 0
	}
	defer rows.Close()
	var sum float64
	var n int
	for rows.Next() {
		var v float64
		if rows.Scan(&v) == nil {
			sum += v
			n++
		}
	}
	if n == 0 {
		return 0, 0
	}
	return sum / float64(n), n
}

// eventsInWindow returns anomaly events for fqName in the last since duration,
// newest-first.
func (c *Catalog) eventsInWindow(ctx context.Context, fqName string, since time.Duration) ([]EventRow, error) {
	cutoff := time.Now().UTC().Add(-since).Format(time.RFC3339)
	rows, err := c.db.QueryContext(ctx,
		`SELECT id, kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, at
		 FROM bm_events WHERE metric_fq = ? AND at >= ? ORDER BY at DESC`,
		fqName, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		var r EventRow
		var atStr string
		if err := rows.Scan(&r.ID, &r.Kind, &r.MetricFQ, &r.ObservedValue, &r.BaselineMean,
			&r.StddevFromMean, &r.Direction, &r.WindowDays, &atStr); err != nil {
			return nil, err
		}
		r.At, _ = time.Parse(time.RFC3339, atStr)
		out = append(out, r)
	}
	return out, rows.Err()
}
