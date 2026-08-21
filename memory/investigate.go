package memory

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"
)

// InvestigateData holds the assembled result of InvestigateMetric.
type InvestigateData struct {
	Explain  *ExplainData
	Causal   []CausalCandidate
	Siblings []SiblingCorrelation
}

// CausalCandidate is a non-anomaly event temporally proximate to an anomaly.
type CausalCandidate struct {
	Event          EventRow
	AnomalyAt      time.Time
	ProximityHours float64
}

// SiblingCorrelation is the Pearson-r between the target metric and a sibling
// over the same window.
type SiblingCorrelation struct {
	MetricFQ string
	R        float64
	Samples  int
}

// InvestigateJSON is the structured JSON output for investigate.
type InvestigateJSON struct {
	MetricFQ   string                `json:"metric_fq"`
	Since      string                `json:"since"`
	Explain    *ExplainJSON          `json:"explain"`
	Causal     []CausalCandidateJSON `json:"causal_candidates"`
	Siblings   []SiblingCorrJSON     `json:"sibling_correlations"`
	Confidence float64               `json:"confidence"`
	Summary    string                `json:"summary"`
}

// CausalCandidateJSON is one causal candidate in InvestigateJSON.
type CausalCandidateJSON struct {
	EventAt        string  `json:"event_at"`
	Kind           string  `json:"kind"`
	Description    string  `json:"description"`
	AnomalyAt      string  `json:"anomaly_at"`
	ProximityHours float64 `json:"proximity_hours"`
	Confidence     float64 `json:"confidence"`
}

// SiblingCorrJSON is one sibling correlation in InvestigateJSON.
type SiblingCorrJSON struct {
	MetricFQ   string  `json:"metric_fq"`
	R          float64 `json:"r"`
	Samples    int     `json:"samples"`
	Confidence float64 `json:"confidence"`
}

// causalProximityWindow is how far before an anomaly we look for causal events.
const causalProximityWindow = 48 * time.Hour

// siblingMinR is the minimum |r| to include a sibling correlation in output.
const siblingMinR = 0.6

// InvestigateMetric assembles an InvestigateData for fqName over since.
// It wraps ExplainMetric, correlates non-anomaly events with any detected
// anomalies by temporal proximity, and computes Pearson-r for sibling metrics.
func (c *Catalog) InvestigateMetric(ctx context.Context, fqName string, since time.Duration) (*InvestigateData, error) {
	explain, err := c.ExplainMetric(ctx, fqName, since)
	if err != nil {
		return nil, err
	}

	d := &InvestigateData{Explain: explain}
	d.Causal = causalCandidates(explain.Anomalies)
	d.Siblings, _ = c.siblingCorrelations(ctx, fqName, since)
	return d, nil
}

// causalCandidates matches non-anomaly events against anomaly events by temporal
// proximity. For each anomaly, any non-anomaly event within causalProximityWindow
// before the anomaly is returned, sorted by proximity (closest first).
func causalCandidates(events []EventRow) []CausalCandidate {
	var anomalies, others []EventRow
	for _, e := range events {
		if e.Kind == "anomaly" {
			anomalies = append(anomalies, e)
		} else {
			others = append(others, e)
		}
	}
	if len(anomalies) == 0 || len(others) == 0 {
		return nil
	}

	seen := map[int64]bool{}
	var out []CausalCandidate
	for _, a := range anomalies {
		for _, o := range others {
			if seen[o.ID] {
				continue
			}
			delta := a.At.Sub(o.At)
			if delta >= 0 && delta <= causalProximityWindow {
				out = append(out, CausalCandidate{
					Event:          o,
					AnomalyAt:      a.At,
					ProximityHours: delta.Hours(),
				})
				seen[o.ID] = true
			}
		}
	}
	return out
}

// siblingCorrelations computes Pearson-r between fqName and every other metric
// in bm_metrics over the since window. Only siblings with |r| >= siblingMinR
// and at least 2 shared sample points are returned.
func (c *Catalog) siblingCorrelations(ctx context.Context, fqName string, since time.Duration) ([]SiblingCorrelation, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT fq_name FROM bm_metrics WHERE fq_name != ?`, fqName)
	if err != nil {
		return nil, err
	}
	var siblings []string
	for rows.Next() {
		var s string
		if rows.Scan(&s) == nil {
			siblings = append(siblings, s)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	windowStart := now.Add(-since)
	windowEnd := now.Add(time.Second)

	// Fetch target values indexed by day bucket.
	targetVals := c.metricValuesByDay(ctx, fqName, windowStart, windowEnd)
	if len(targetVals) < 2 {
		return nil, nil
	}

	var out []SiblingCorrelation
	for _, sib := range siblings {
		sibVals := c.metricValuesByDay(ctx, sib, windowStart, windowEnd)
		r, n := pearsonR(targetVals, sibVals)
		if n >= 2 && math.Abs(r) >= siblingMinR {
			out = append(out, SiblingCorrelation{MetricFQ: sib, R: r, Samples: n})
		}
	}
	return out, nil
}

// metricValuesByDay fetches all values for fqName in [start, end) and returns
// a map from day-bucket string (YYYY-MM-DD) to mean value for that day.
func (c *Catalog) metricValuesByDay(ctx context.Context, fqName string, start, end time.Time) map[string]float64 {
	rows, err := c.db.QueryContext(ctx,
		`SELECT observed_at, value FROM bm_metric_values
		 WHERE fq_name = ? AND observed_at >= ? AND observed_at < ?`,
		fqName, start.Format(time.RFC3339), end.Format(time.RFC3339))
	if err != nil {
		return nil
	}
	defer rows.Close()

	buckets := map[string][]float64{}
	for rows.Next() {
		var atStr string
		var v float64
		if rows.Scan(&atStr, &v) != nil {
			continue
		}
		t, err := time.Parse(time.RFC3339, atStr)
		if err != nil {
			continue
		}
		day := t.Format("2006-01-02")
		buckets[day] = append(buckets[day], v)
	}
	out := make(map[string]float64, len(buckets))
	for day, vals := range buckets {
		var sum float64
		for _, v := range vals {
			sum += v
		}
		out[day] = sum / float64(len(vals))
	}
	return out
}

// pearsonR computes the Pearson correlation coefficient between two day-bucketed
// value maps. Returns r and the number of shared days.
func pearsonR(a, b map[string]float64) (float64, int) {
	var xs, ys []float64
	for day, av := range a {
		if bv, ok := b[day]; ok {
			xs = append(xs, av)
			ys = append(ys, bv)
		}
	}
	n := len(xs)
	if n < 2 {
		return 0, n
	}
	fn := float64(n)
	var sumX, sumY, sumXX, sumYY, sumXY float64
	for i := 0; i < n; i++ {
		sumX += xs[i]
		sumY += ys[i]
		sumXX += xs[i] * xs[i]
		sumYY += ys[i] * ys[i]
		sumXY += xs[i] * ys[i]
	}
	num := sumXY - (sumX*sumY)/fn
	denX := math.Sqrt(sumXX - (sumX*sumX)/fn)
	denY := math.Sqrt(sumYY - (sumY*sumY)/fn)
	if denX == 0 || denY == 0 {
		return 0, n
	}
	return num / (denX * denY), n
}

// ComposeCausalNarrative returns a plain-text investigation narrative for d.
func ComposeCausalNarrative(d *InvestigateData) string {
	var sb strings.Builder
	sinceStr := FormatSince(d.Explain.Since)
	short := metricShortName(d.Explain.MetricFQ)

	fmt.Fprintf(&sb, "Investigating %s -- last %s\n\n", short, sinceStr)

	// Restate anomalies from explain.
	anomalies := filterKind(d.Explain.Anomalies, "anomaly")
	if len(anomalies) == 0 {
		fmt.Fprintln(&sb, "No anomalies detected in this window.")
	} else {
		fmt.Fprintf(&sb, "%d anomaly(s) detected:\n", len(anomalies))
		for _, a := range anomalies {
			pct := "0%"
			if a.BaselineMean != 0 {
				chg := (a.ObservedValue - a.BaselineMean) / math.Abs(a.BaselineMean) * 100
				pct = fmt.Sprintf("%+.0f%%", chg)
			}
			fmt.Fprintf(&sb, "  %s: %.4g (%s vs baseline %.4g, %.1f stddev, %s)\n",
				a.At.Format("2006-01-02"), a.ObservedValue, pct,
				a.BaselineMean, a.StddevFromMean, a.Direction)
		}
		fmt.Fprintln(&sb, "")
	}

	// Causal candidates.
	if len(d.Causal) > 0 {
		fmt.Fprintf(&sb, "Correlated events (within %dh before anomaly):\n", int(causalProximityWindow.Hours()))
		for _, c := range d.Causal {
			label := c.Event.Description
			if label == "" {
				label = c.Event.Kind
			}
			fmt.Fprintf(&sb, "  %s [%s]: %s (%.1fh before anomaly at %s)\n",
				c.Event.At.Format("2006-01-02 15:04"), c.Event.Kind, label,
				c.ProximityHours, c.AnomalyAt.Format("2006-01-02"))
		}
		fmt.Fprintln(&sb, "")
	} else if len(anomalies) > 0 {
		fmt.Fprintln(&sb, "No events found within 48h before any anomaly.")
	}

	// Sibling correlations.
	if len(d.Siblings) > 0 {
		fmt.Fprintln(&sb, "Sibling metric correlation:")
		for _, s := range d.Siblings {
			dir := "moved together"
			if s.R < 0 {
				dir = "moved inversely"
			}
			sibConf := ScoreCorrelation(math.Abs(s.R), s.Samples)
			fmt.Fprintf(&sb, "  %s: r=%.2f (%s, %d shared days) %s\n",
				s.MetricFQ, s.R, dir, s.Samples, sibConf.Tag(""))
		}
		fmt.Fprintln(&sb, "")
	} else {
		fmt.Fprintln(&sb, "No sibling metrics with notable correlation found.")
	}

	return sb.String()
}

// ToInvestigateJSON converts InvestigateData to the structured JSON output type.
func ToInvestigateJSON(d *InvestigateData) *InvestigateJSON {
	// Overall investigation confidence comes from the explain baseline.
	overallConf := d.Explain.Confidence
	j := &InvestigateJSON{
		MetricFQ:   d.Explain.MetricFQ,
		Since:      FormatSince(d.Explain.Since),
		Explain:    ToExplainJSON(d.Explain),
		Causal:     []CausalCandidateJSON{},
		Siblings:   []SiblingCorrJSON{},
		Confidence: overallConf.Float64(),
		Summary:    fmt.Sprintf("%s %s.", composeSummary(d.Explain), overallConf.Tag(baselineDetail(d.Explain.Baseline))),
	}
	for _, c := range d.Causal {
		label := c.Event.Description
		if label == "" {
			label = c.Event.Kind
		}
		causalConf := ScoreProximity(c.ProximityHours)
		j.Causal = append(j.Causal, CausalCandidateJSON{
			EventAt:        c.Event.At.Format(time.RFC3339),
			Kind:           c.Event.Kind,
			Description:    label,
			AnomalyAt:      c.AnomalyAt.Format(time.RFC3339),
			ProximityHours: c.ProximityHours,
			Confidence:     causalConf.Float64(),
		})
	}
	for _, s := range d.Siblings {
		sibConf := ScoreCorrelation(math.Abs(s.R), s.Samples)
		j.Siblings = append(j.Siblings, SiblingCorrJSON{
			MetricFQ:   s.MetricFQ,
			R:          s.R,
			Samples:    s.Samples,
			Confidence: sibConf.Float64(),
		})
	}
	return j
}
