package memory

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"
)

// PatternName is one of the five built-in recurring pattern labels.
const (
	PatternWeekendDip     = "weekend-dip"
	PatternMonthEndSpike  = "month-end-spike"
	PatternSteadyGrowth   = "steady-growth"
	PatternSteadyDecline  = "steady-decline"
	PatternHighVolatility = "high-volatility"
)

// minPatternSamples is the floor sample count required before any pattern is
// evaluated. Four weeks of daily observations gives enough evidence for the
// cyclic patterns.
const minPatternSamples = 28

// PatternRow is one detected recurring pattern for a metric.
type PatternRow struct {
	FQNAME        string
	Pattern       string
	Confidence    float64
	EvidenceStart time.Time
	EvidenceEnd   time.Time
	SampleCount   int
	DetectedAt    time.Time
}

// timestampedValue is one metric observation with a parsed timestamp.
type timestampedValue struct {
	At    time.Time
	Value float64
}

// DetectPatterns runs all five pattern detectors against the last 90 days of
// observations for fqName, upserts any detected patterns into bm_patterns, and
// returns the detected set. Metrics with fewer than minPatternSamples
// observations return an empty slice without error.
func (c *Catalog) DetectPatterns(ctx context.Context, fqName string) ([]PatternRow, error) {
	samples, err := c.loadSamples(ctx, fqName, 90)
	if err != nil {
		return nil, err
	}
	if len(samples) < minPatternSamples {
		return nil, nil
	}

	type detector struct {
		name string
		fn   func([]timestampedValue) (confidence float64, ok bool)
	}
	detectors := []detector{
		{PatternWeekendDip, detectWeekendDip},
		{PatternMonthEndSpike, detectMonthEndSpike},
		{PatternSteadyGrowth, detectSteadyGrowth},
		{PatternSteadyDecline, detectSteadyDecline},
		{PatternHighVolatility, detectHighVolatility},
	}

	evidenceStart := samples[0].At
	evidenceEnd := samples[len(samples)-1].At
	now := time.Now().UTC().Format(time.RFC3339)
	var out []PatternRow

	for _, d := range detectors {
		conf, ok := d.fn(samples)
		if !ok {
			continue
		}
		_, err := c.db.ExecContext(ctx, `
INSERT INTO bm_patterns (fq_name, pattern, confidence, evidence_start, evidence_end, sample_count, detected_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fq_name, pattern) DO UPDATE SET
    confidence = excluded.confidence,
    evidence_start = excluded.evidence_start,
    evidence_end = excluded.evidence_end,
    sample_count = excluded.sample_count,
    detected_at = excluded.detected_at`,
			fqName, d.name, conf,
			evidenceStart.Format(time.RFC3339),
			evidenceEnd.Format(time.RFC3339),
			len(samples), now)
		if err != nil {
			return nil, fmt.Errorf("memory: upsert pattern %s/%s: %w", fqName, d.name, err)
		}
		out = append(out, PatternRow{
			FQNAME:        fqName,
			Pattern:       d.name,
			Confidence:    conf,
			EvidenceStart: evidenceStart,
			EvidenceEnd:   evidenceEnd,
			SampleCount:   len(samples),
			DetectedAt:    time.Now().UTC(),
		})
	}
	return out, nil
}

// ListPatterns returns all pattern rows across all metrics, ordered by
// fq_name, pattern.
func (c *Catalog) ListPatterns(ctx context.Context) ([]PatternRow, error) {
	return c.queryPatterns(ctx,
		`SELECT fq_name, pattern, confidence, evidence_start, evidence_end, sample_count, detected_at
		 FROM bm_patterns ORDER BY fq_name, pattern`)
}

// ListPatternsForMetric returns detected patterns for a single metric.
func (c *Catalog) ListPatternsForMetric(ctx context.Context, fqName string) ([]PatternRow, error) {
	return c.queryPatterns(ctx,
		`SELECT fq_name, pattern, confidence, evidence_start, evidence_end, sample_count, detected_at
		 FROM bm_patterns WHERE fq_name = ? ORDER BY pattern`, fqName)
}

func (c *Catalog) queryPatterns(ctx context.Context, query string, args ...interface{}) ([]PatternRow, error) {
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("memory: list patterns: %w", err)
	}
	defer rows.Close()
	var out []PatternRow
	for rows.Next() {
		var r PatternRow
		var startStr, endStr, detStr string
		if err := rows.Scan(&r.FQNAME, &r.Pattern, &r.Confidence, &startStr, &endStr, &r.SampleCount, &detStr); err != nil {
			return nil, fmt.Errorf("memory: scan pattern: %w", err)
		}
		r.EvidenceStart, _ = time.Parse(time.RFC3339, startStr)
		r.EvidenceEnd, _ = time.Parse(time.RFC3339, endStr)
		r.DetectedAt, _ = time.Parse(time.RFC3339, detStr)
		out = append(out, r)
	}
	return out, rows.Err()
}

// RedetectAllPatterns runs DetectPatterns for every metric in the catalog.
func (c *Catalog) RedetectAllPatterns(ctx context.Context) error {
	rows, err := c.db.QueryContext(ctx, `SELECT fq_name FROM bm_metrics ORDER BY fq_name`)
	if err != nil {
		return fmt.Errorf("memory: list metrics for pattern detection: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if rows.Scan(&n) == nil {
			names = append(names, n)
		}
	}
	rows.Close()
	for _, n := range names {
		if _, err := c.DetectPatterns(ctx, n); err != nil {
			return err
		}
	}
	return nil
}

// FormatPatternNames returns a human-readable comma list of pattern names.
func FormatPatternNames(patterns []PatternRow) string {
	if len(patterns) == 0 {
		return ""
	}
	names := make([]string, len(patterns))
	for i, p := range patterns {
		names[i] = p.Pattern
	}
	return strings.Join(names, ", ")
}

// loadSamples fetches timestamped values for fqName from the last windowDays.
func (c *Catalog) loadSamples(ctx context.Context, fqName string, windowDays int) ([]timestampedValue, error) {
	cutoff := time.Now().UTC().Add(-time.Duration(windowDays) * 24 * time.Hour).Format(time.RFC3339)
	rows, err := c.db.QueryContext(ctx,
		`SELECT observed_at, value FROM bm_metric_values
		 WHERE fq_name = ? AND observed_at >= ? ORDER BY observed_at ASC`,
		fqName, cutoff)
	if err != nil {
		return nil, fmt.Errorf("memory: load samples %s: %w", fqName, err)
	}
	defer rows.Close()
	var out []timestampedValue
	for rows.Next() {
		var atStr string
		var v float64
		if err := rows.Scan(&atStr, &v); err != nil {
			return nil, fmt.Errorf("memory: scan sample: %w", err)
		}
		t, _ := time.Parse(time.RFC3339, atStr)
		out = append(out, timestampedValue{At: t, Value: v})
	}
	return out, rows.Err()
}

// detectWeekendDip fires when Sat+Sun mean is consistently lower than
// Mon-Fri mean across at least 4 separate weeks.
func detectWeekendDip(samples []timestampedValue) (float64, bool) {
	type weekBucket struct {
		weekdaySum float64
		weekdayN   int
		weekendSum float64
		weekendN   int
	}
	weeks := make(map[int]*weekBucket)
	for _, s := range samples {
		_, wk := s.At.ISOWeek()
		if _, ok := weeks[wk]; !ok {
			weeks[wk] = &weekBucket{}
		}
		b := weeks[wk]
		wd := s.At.Weekday()
		if wd == time.Saturday || wd == time.Sunday {
			b.weekendSum += s.Value
			b.weekendN++
		} else {
			b.weekdaySum += s.Value
			b.weekdayN++
		}
	}
	var dips int
	for _, b := range weeks {
		if b.weekdayN == 0 || b.weekendN == 0 {
			continue
		}
		wdMean := b.weekdaySum / float64(b.weekdayN)
		weMean := b.weekendSum / float64(b.weekendN)
		if wdMean > 0 && (wdMean-weMean)/wdMean > 0.10 {
			dips++
		}
	}
	if dips < 4 {
		return 0, false
	}
	conf := math.Min(1.0, float64(dips)/10.0)
	return conf, true
}

// detectMonthEndSpike fires when the last 2 days of each month are
// consistently higher than the month mean across at least 3 months.
func detectMonthEndSpike(samples []timestampedValue) (float64, bool) {
	type monthBucket struct {
		endSum float64
		endN   int
		allSum float64
		allN   int
		maxDay int
	}
	months := make(map[string]*monthBucket)
	for _, s := range samples {
		key := s.At.Format("2006-01")
		if _, ok := months[key]; !ok {
			months[key] = &monthBucket{}
		}
		b := months[key]
		day := s.At.Day()
		// determine last day of month
		nextMonth := time.Date(s.At.Year(), s.At.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		lastDay := nextMonth.AddDate(0, 0, -1).Day()
		b.allSum += s.Value
		b.allN++
		if day >= lastDay-1 {
			b.endSum += s.Value
			b.endN++
		}
		if day > b.maxDay {
			b.maxDay = day
		}
	}
	var spikes int
	for _, b := range months {
		if b.endN == 0 || b.allN == 0 {
			continue
		}
		monthMean := b.allSum / float64(b.allN)
		endMean := b.endSum / float64(b.endN)
		if monthMean > 0 && (endMean-monthMean)/monthMean > 0.15 {
			spikes++
		}
	}
	if spikes < 3 {
		return 0, false
	}
	conf := math.Min(1.0, float64(spikes)/6.0)
	return conf, true
}

// detectSteadyGrowth fires when linear regression slope over available data
// implies at least 5% growth over the window and R^2 >= 0.4.
func detectSteadyGrowth(samples []timestampedValue) (float64, bool) {
	xs, ys := samplesToRegression(samples)
	slope, intercept := linearRegression(xs, ys)
	r2 := rSquared(xs, ys, slope, intercept)
	if slope <= 0 || r2 < 0.4 {
		return 0, false
	}
	// require 5% growth over the full window
	span := xs[len(xs)-1] - xs[0]
	if span <= 0 {
		return 0, false
	}
	startY := intercept + slope*xs[0]
	endY := intercept + slope*xs[len(xs)-1]
	if startY <= 0 || (endY-startY)/startY < 0.05 {
		return 0, false
	}
	return math.Min(1.0, r2), true
}

// detectSteadyDecline fires when linear regression slope is negative and
// implies at least 5% decline with R^2 >= 0.4.
func detectSteadyDecline(samples []timestampedValue) (float64, bool) {
	xs, ys := samplesToRegression(samples)
	slope, intercept := linearRegression(xs, ys)
	r2 := rSquared(xs, ys, slope, intercept)
	if slope >= 0 || r2 < 0.4 {
		return 0, false
	}
	span := xs[len(xs)-1] - xs[0]
	if span <= 0 {
		return 0, false
	}
	startY := intercept + slope*xs[0]
	endY := intercept + slope*xs[len(xs)-1]
	if startY <= 0 || (startY-endY)/startY < 0.05 {
		return 0, false
	}
	return math.Min(1.0, r2), true
}

// detectHighVolatility fires when stddev exceeds twice the mean over the window.
func detectHighVolatility(samples []timestampedValue) (float64, bool) {
	vals := make([]float64, len(samples))
	for i, s := range samples {
		vals[i] = s.Value
	}
	mean, stddev, _, _ := windowStats(vals)
	if mean <= 0 || stddev < 2*mean {
		return 0, false
	}
	// confidence scales with how far above the 2x threshold stddev sits
	conf := math.Min(1.0, (stddev/mean-2.0)/2.0+0.5)
	return conf, true
}

// samplesToRegression converts timestamped samples to (x=day-offset, y=value)
// slices suitable for linearRegression.
func samplesToRegression(samples []timestampedValue) (xs, ys []float64) {
	if len(samples) == 0 {
		return nil, nil
	}
	origin := samples[0].At
	xs = make([]float64, len(samples))
	ys = make([]float64, len(samples))
	for i, s := range samples {
		xs[i] = s.At.Sub(origin).Hours() / 24
		ys[i] = s.Value
	}
	return xs, ys
}
