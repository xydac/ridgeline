package memory

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"
)

// CompareData holds assembled data for a pairwise metric comparison.
type CompareData struct {
	Since        time.Duration
	A            *ExplainData
	B            *ExplainData
	Verdict      string
	Diverged     bool
	SharedEvents []EventRow
}

// CompareJSON is the structured JSON output for pairwise compare.
type CompareJSON struct {
	Since        string           `json:"since"`
	MetricA      *ExplainJSON     `json:"metric_a"`
	MetricB      *ExplainJSON     `json:"metric_b"`
	Verdict      string           `json:"verdict"`
	Diverged     bool             `json:"diverged"`
	SharedEvents []CorrelatedJSON `json:"shared_events"`
	Confidence   float64          `json:"confidence"`
	Summary      string           `json:"summary"`
}

// PeriodOverPeriodData holds data for single-metric period-over-period compare.
type PeriodOverPeriodData struct {
	MetricFQ      string
	Direction     string
	Unit          string
	Since         time.Duration
	PriorSince    time.Duration
	RecentMean    float64
	RecentSamples int
	PriorMean     float64
	PriorSamples  int
	PctChange     float64
	Verdict       string
	Baseline      *BaselineRow
	Anomalies     []EventRow
}

// PeriodOverPeriodJSON is the structured JSON output for period-over-period compare.
type PeriodOverPeriodJSON struct {
	MetricFQ      string        `json:"metric_fq"`
	Since         string        `json:"since"`
	PriorSince    string        `json:"prior_since"`
	RecentMean    float64       `json:"recent_mean"`
	RecentSamples int           `json:"recent_samples"`
	PriorMean     float64       `json:"prior_mean"`
	PriorSamples  int           `json:"prior_samples"`
	PctChange     float64       `json:"pct_change"`
	Direction     string        `json:"direction"`
	Verdict       string        `json:"verdict"`
	Baseline      *BaselineJSON `json:"baseline,omitempty"`
	Anomalies     []AnomalyJSON `json:"anomalies"`
	Confidence    float64       `json:"confidence"`
	Summary       string        `json:"summary"`
}

// CompareMetrics runs ExplainMetric for both a and b over the same since window
// and assembles a pairwise comparative result.
func (c *Catalog) CompareMetrics(ctx context.Context, a, b string, since time.Duration) (*CompareData, error) {
	explainA, err := c.ExplainMetric(ctx, a, since)
	if err != nil {
		return nil, fmt.Errorf("metric %q: %w", a, err)
	}
	explainB, err := c.ExplainMetric(ctx, b, since)
	if err != nil {
		return nil, fmt.Errorf("metric %q: %w", b, err)
	}
	d := &CompareData{Since: since, A: explainA, B: explainB}
	d.Verdict, d.Diverged = pairwiseVerdict(explainA, explainB)
	d.SharedEvents = sharedNonAnomalyEvents(explainA.Anomalies, explainB.Anomalies)
	return d, nil
}

// CompareMetricPeriods computes a period-over-period comparison for a single metric.
// since is the recent window; priorSince is the comparison window immediately preceding it.
func (c *Catalog) CompareMetricPeriods(ctx context.Context, fqName string, since, priorSince time.Duration) (*PeriodOverPeriodData, error) {
	var direction, unit string
	err := c.db.QueryRowContext(ctx,
		`SELECT direction, unit FROM bm_metrics WHERE fq_name = ?`, fqName).
		Scan(&direction, &unit)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("metric %q not found in Business Memory catalog; run 'ridgeline sync' first", fqName)
	}
	if err != nil {
		return nil, fmt.Errorf("memory: compare periods %s: %w", fqName, err)
	}

	now := time.Now().UTC()
	recentEnd := now.Add(time.Second) // inclusive-end: same trick as ExplainMetric
	recentStart := now.Add(-since)
	priorStart := recentStart.Add(-priorSince)

	recentMean, recentSamples := c.periodMean(ctx, fqName, recentStart, recentEnd)
	priorMean, priorSamples := c.periodMean(ctx, fqName, priorStart, recentStart)

	d := &PeriodOverPeriodData{
		MetricFQ:      fqName,
		Direction:     direction,
		Unit:          unit,
		Since:         since,
		PriorSince:    priorSince,
		RecentMean:    recentMean,
		RecentSamples: recentSamples,
		PriorMean:     priorMean,
		PriorSamples:  priorSamples,
	}
	if priorSamples > 0 && priorMean != 0 {
		d.PctChange = (recentMean - priorMean) / math.Abs(priorMean) * 100
	}
	sinceDays := int(math.Ceil(since.Hours() / 24))
	d.Baseline = c.pickBaseline(ctx, fqName, sinceDays)
	d.Anomalies, _ = c.eventsInWindow(ctx, fqName, since)
	d.Verdict = popVerdict(d)
	return d, nil
}

// ComposePairwiseNarrative returns a plain-text pairwise comparison narrative.
func ComposePairwiseNarrative(d *CompareData) string {
	var sb strings.Builder
	sinceStr := FormatSince(d.Since)
	aName := metricShortName(d.A.MetricFQ)
	bName := metricShortName(d.B.MetricFQ)

	fmt.Fprintf(&sb, "Comparing %s vs %s -- last %s\n\n", aName, bName, sinceStr)

	writeSingleLineSummary(&sb, d.A)
	writeSingleLineSummary(&sb, d.B)
	fmt.Fprintln(&sb, "")

	fmt.Fprintf(&sb, "Verdict: %s.\n", d.Verdict)

	aAnomalies := filterKind(d.A.Anomalies, "anomaly")
	bAnomalies := filterKind(d.B.Anomalies, "anomaly")
	if len(aAnomalies) == 0 && len(bAnomalies) == 0 {
		fmt.Fprintln(&sb, "No anomalies detected for either metric in this window.")
	} else {
		if len(aAnomalies) > 0 {
			fmt.Fprintf(&sb, "%s: %d anomaly(s) -- %s\n", aName, len(aAnomalies), aAnomalies[0].Direction)
		}
		if len(bAnomalies) > 0 {
			fmt.Fprintf(&sb, "%s: %d anomaly(s) -- %s\n", bName, len(bAnomalies), bAnomalies[0].Direction)
		}
	}

	if len(d.SharedEvents) > 0 {
		fmt.Fprintf(&sb, "%d shared event(s) in window:\n", len(d.SharedEvents))
		for _, e := range d.SharedEvents {
			label := e.Description
			if label == "" {
				label = e.Kind
			}
			fmt.Fprintf(&sb, "  %s [%s]: %s\n", e.At.Format("2006-01-02"), e.Kind, label)
		}
	}

	pairConf := ConfidenceScore((d.A.Confidence.Float64() + d.B.Confidence.Float64()) / 2)
	fmt.Fprintf(&sb, "\nSummary: %s and %s %s %s.\n", aName, bName, d.Verdict, pairConf.Tag(baselineDetail(d.A.Baseline)))
	return sb.String()
}

// ComposePeriodOverPeriodNarrative returns a plain-text period-over-period narrative.
func ComposePeriodOverPeriodNarrative(d *PeriodOverPeriodData) string {
	var sb strings.Builder
	sinceStr := FormatSince(d.Since)
	priorStr := FormatSince(d.PriorSince)
	short := metricShortName(d.MetricFQ)

	fmt.Fprintf(&sb, "%s -- last %s vs prior %s\n\n", short, sinceStr, priorStr)

	if d.RecentSamples == 0 && d.PriorSamples == 0 {
		fmt.Fprintln(&sb, "No observations recorded in either window.")
		return sb.String()
	}

	fmt.Fprintf(&sb, "Recent %s: mean %.4g %s (%d sample(s)).\n",
		sinceStr, d.RecentMean, d.Unit, d.RecentSamples)
	if d.PriorSamples > 0 {
		fmt.Fprintf(&sb, "Prior %s: mean %.4g %s (%d sample(s)).\n",
			priorStr, d.PriorMean, d.Unit, d.PriorSamples)
		if d.PriorMean != 0 {
			fmt.Fprintf(&sb, "Change: %+.1f%% vs prior period.\n", d.PctChange)
		}
	} else {
		fmt.Fprintf(&sb, "No observations in prior %s window.\n", priorStr)
	}

	fmt.Fprintf(&sb, "Verdict: %s.\n", d.Verdict)

	if d.Baseline != nil {
		b := d.Baseline
		dl := directionLabel(d.Direction)
		if d.RecentSamples > 0 && b.Stddev > 0 {
			dev := (d.RecentMean - b.Mean) / b.Stddev
			fmt.Fprintf(&sb, "The %dd baseline is %.4g +/- %.4g %s (%s); recent mean is %+.1f sigma.\n",
				b.WindowDays, b.Mean, b.Stddev, d.Unit, dl, dev)
		} else {
			fmt.Fprintf(&sb, "The %dd baseline mean is %.4g %s (%s).\n",
				b.WindowDays, b.Mean, d.Unit, dl)
		}
	}

	anomalies := filterKind(d.Anomalies, "anomaly")
	if len(anomalies) > 0 {
		fmt.Fprintf(&sb, "%d anomaly(s) in recent window:\n", len(anomalies))
		for _, e := range anomalies {
			fmt.Fprintf(&sb, "  %s: %.4g observed (%+.1f sigma from %dd baseline) -- %s\n",
				e.At.Format("2006-01-02"), e.ObservedValue, e.StddevFromMean, e.WindowDays, e.Direction)
		}
	} else {
		fmt.Fprintf(&sb, "No anomalies detected in the recent %s.\n", sinceStr)
	}

	popConf := ScoreBaseline(0)
	if d.Baseline != nil {
		popConf = ScoreBaseline(d.Baseline.SampleCount)
	}
	fmt.Fprintf(&sb, "\nSummary: %s %s (%.1f%% vs prior %s) %s.\n",
		short, d.Verdict, d.PctChange, priorStr, popConf.Tag(baselineDetail(d.Baseline)))
	return sb.String()
}

// ToCompareJSON converts CompareData to the structured JSON output type.
func ToCompareJSON(d *CompareData) *CompareJSON {
	aName := metricShortName(d.A.MetricFQ)
	bName := metricShortName(d.B.MetricFQ)
	// Confidence for the pairwise verdict is the average of both metrics' confidence.
	pairConf := ConfidenceScore((d.A.Confidence.Float64() + d.B.Confidence.Float64()) / 2)
	j := &CompareJSON{
		Since:        FormatSince(d.Since),
		MetricA:      ToExplainJSON(d.A),
		MetricB:      ToExplainJSON(d.B),
		Verdict:      d.Verdict,
		Diverged:     d.Diverged,
		SharedEvents: []CorrelatedJSON{},
		Confidence:   pairConf.Float64(),
		Summary:      fmt.Sprintf("%s and %s %s %s.", aName, bName, d.Verdict, pairConf.Tag(baselineDetail(d.A.Baseline))),
	}
	for _, e := range d.SharedEvents {
		label := e.Description
		if label == "" {
			label = e.Kind
		}
		j.SharedEvents = append(j.SharedEvents, CorrelatedJSON{
			At:          e.At.Format(time.RFC3339),
			Kind:        e.Kind,
			Description: label,
		})
	}
	return j
}

// ToPeriodOverPeriodJSON converts PeriodOverPeriodData to the structured JSON output type.
func ToPeriodOverPeriodJSON(d *PeriodOverPeriodData) *PeriodOverPeriodJSON {
	short := metricShortName(d.MetricFQ)
	conf := ScoreBaseline(0)
	if d.Baseline != nil {
		conf = ScoreBaseline(d.Baseline.SampleCount)
	}
	j := &PeriodOverPeriodJSON{
		MetricFQ:      d.MetricFQ,
		Since:         FormatSince(d.Since),
		PriorSince:    FormatSince(d.PriorSince),
		RecentMean:    d.RecentMean,
		RecentSamples: d.RecentSamples,
		PriorMean:     d.PriorMean,
		PriorSamples:  d.PriorSamples,
		PctChange:     d.PctChange,
		Direction:     d.Direction,
		Verdict:       d.Verdict,
		Anomalies:     []AnomalyJSON{},
		Confidence:    conf.Float64(),
		Summary:       fmt.Sprintf("%s %s (%.1f%% vs prior %s) %s.", short, d.Verdict, d.PctChange, FormatSince(d.PriorSince), conf.Tag(baselineDetail(d.Baseline))),
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
		if e.Kind == "anomaly" {
			j.Anomalies = append(j.Anomalies, AnomalyJSON{
				At:             e.At.Format(time.RFC3339),
				ObservedValue:  e.ObservedValue,
				BaselineMean:   e.BaselineMean,
				StddevFromMean: e.StddevFromMean,
				WindowDays:     e.WindowDays,
				Direction:      e.Direction,
			})
		}
	}
	return j
}

// pairwiseVerdict returns a verdict string and diverged flag based on
// direction-adjusted improvement for each metric.
func pairwiseVerdict(a, b *ExplainData) (string, bool) {
	aImpr := metricImproved(a)
	bImpr := metricImproved(b)
	aName := metricShortName(a.MetricFQ)
	bName := metricShortName(b.MetricFQ)
	switch {
	case aImpr && bImpr:
		return "both improved", false
	case !aImpr && !bImpr:
		return "both regressed", false
	case aImpr:
		return fmt.Sprintf("diverged: %s improved, %s regressed", aName, bName), true
	default:
		return fmt.Sprintf("diverged: %s regressed, %s improved", aName, bName), true
	}
}

// metricImproved returns true when the metric's current value or window mean
// is directionally positive relative to the baseline or prior period.
func metricImproved(d *ExplainData) bool {
	if d.Baseline != nil && d.CurrentValue != nil && d.Baseline.Stddev > 0 {
		dev := (*d.CurrentValue - d.Baseline.Mean) / d.Baseline.Stddev
		return (dev > 0 && d.Direction == "higher_is_better") ||
			(dev < 0 && d.Direction == "lower_is_better")
	}
	if d.PriorMean != nil && d.WindowSamples > 0 {
		return (d.WindowMean > *d.PriorMean && d.Direction == "higher_is_better") ||
			(d.WindowMean < *d.PriorMean && d.Direction == "lower_is_better")
	}
	return false
}

// popVerdict returns a plain-English verdict for a period-over-period result.
func popVerdict(d *PeriodOverPeriodData) string {
	if d.PriorSamples == 0 {
		return "no prior data to compare"
	}
	if d.PriorMean == 0 {
		return "stable (prior mean zero)"
	}
	switch {
	case d.PctChange > 0 && d.Direction == "higher_is_better":
		return "improved"
	case d.PctChange < 0 && d.Direction == "lower_is_better":
		return "improved"
	case d.PctChange == 0:
		return "stable"
	default:
		return "regressed"
	}
}

// sharedNonAnomalyEvents returns non-anomaly events that appear in both slices,
// matched by event ID (bm_events global pulls produce the same row in both windows).
func sharedNonAnomalyEvents(eventsA, eventsB []EventRow) []EventRow {
	setA := make(map[int64]struct{}, len(eventsA))
	for _, e := range eventsA {
		if e.Kind != "anomaly" {
			setA[e.ID] = struct{}{}
		}
	}
	var shared []EventRow
	for _, e := range eventsB {
		if e.Kind != "anomaly" {
			if _, ok := setA[e.ID]; ok {
				shared = append(shared, e)
			}
		}
	}
	return shared
}

// filterKind returns events where Kind == kind.
func filterKind(events []EventRow, kind string) []EventRow {
	var out []EventRow
	for _, e := range events {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

// writeSingleLineSummary writes a one-line summary of one metric's explain data.
func writeSingleLineSummary(sb *strings.Builder, d *ExplainData) {
	short := metricShortName(d.MetricFQ)
	if d.WindowSamples == 0 {
		fmt.Fprintf(sb, "%s: no data in window.\n", short)
		return
	}
	if d.CurrentValue != nil {
		fmt.Fprintf(sb, "%s: current %.4g %s", short, *d.CurrentValue, d.Unit)
	} else {
		fmt.Fprintf(sb, "%s: mean %.4g %s", short, d.WindowMean, d.Unit)
	}
	if d.PriorMean != nil && *d.PriorMean != 0 {
		pct := (d.WindowMean - *d.PriorMean) / math.Abs(*d.PriorMean) * 100
		fmt.Fprintf(sb, " (%+.1f%% vs prior)", pct)
	}
	if d.Baseline != nil && d.CurrentValue != nil && d.Baseline.Stddev > 0 {
		dev := (*d.CurrentValue - d.Baseline.Mean) / d.Baseline.Stddev
		fmt.Fprintf(sb, ", %+.1f sigma from %dd baseline", dev, d.Baseline.WindowDays)
	}
	fmt.Fprintln(sb, ".")
}
