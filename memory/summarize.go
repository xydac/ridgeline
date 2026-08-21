package memory

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// MetricSummary holds the explain data and ranking score for one metric in a
// summary pass.
type MetricSummary struct {
	FQName    string
	Connector string  // first dot-segment of FQName
	Score     float64 // directionality-adjusted deviation; higher = more notable
	Explain   *ExplainData
}

// SummarizeData is the output of SummarizeAll: a ranked set of notable metrics
// across the whole Business Memory catalog.
type SummarizeData struct {
	Since           time.Duration
	TotalMetrics    int
	TotalConnectors int
	TopMetrics      []MetricSummary // ordered by Score descending, capped at TopK
}

// SummaryJSON is the structured output type for --json.
type SummaryJSON struct {
	Since           string              `json:"since"`
	TotalMetrics    int                 `json:"total_metrics"`
	TotalConnectors int                 `json:"total_connectors"`
	TopMetrics      []MetricSummaryJSON `json:"top_metrics"`
}

// MetricSummaryJSON is one entry in SummaryJSON.TopMetrics.
type MetricSummaryJSON struct {
	MetricFQ   string       `json:"metric_fq"`
	Connector  string       `json:"connector"`
	Score      float64      `json:"score"`
	Confidence float64      `json:"confidence"`
	Explain    *ExplainJSON `json:"explain"`
}

// SummarizeAll walks all metrics in bm_metrics, calls ExplainMetric on each,
// ranks by directionality-adjusted deviation, and returns the top topK.
// Metrics that fail explain (e.g. no baseline, no samples) are included at
// score 0 so a user with a fresh catalog still gets output.
func (c *Catalog) SummarizeAll(ctx context.Context, since time.Duration, topK int) (*SummarizeData, error) {
	metrics, err := c.ListMetrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("summarize: list metrics: %w", err)
	}

	connectorSet := map[string]struct{}{}
	var summaries []MetricSummary

	for _, m := range metrics {
		connector := connectorFromFQ(m.FQName)
		connectorSet[connector] = struct{}{}

		exp, expErr := c.ExplainMetric(ctx, m.FQName, since)
		if expErr != nil {
			// metric declared but no data; include with zero score
			summaries = append(summaries, MetricSummary{
				FQName:    m.FQName,
				Connector: connector,
				Score:     0,
			})
			continue
		}
		score := deviationScore(exp)
		summaries = append(summaries, MetricSummary{
			FQName:    m.FQName,
			Connector: connector,
			Score:     score,
			Explain:   exp,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Score > summaries[j].Score
	})

	top := summaries
	if topK > 0 && len(top) > topK {
		top = top[:topK]
	}

	return &SummarizeData{
		Since:           since,
		TotalMetrics:    len(metrics),
		TotalConnectors: len(connectorSet),
		TopMetrics:      top,
	}, nil
}

// deviationScore returns a ranking score for a metric.
// "Surprise-bad" deviations rank higher than "surprise-good" deviations of
// the same magnitude. Metrics with no baseline score 0.
func deviationScore(d *ExplainData) float64 {
	if d.Baseline == nil || d.Baseline.Stddev == 0 || d.CurrentValue == nil {
		return 0
	}
	dev := (*d.CurrentValue - d.Baseline.Mean) / d.Baseline.Stddev
	switch d.Direction {
	case "higher_is_better":
		// negative deviation = bad; flip so bad events score positive (higher rank)
		return -dev
	case "lower_is_better":
		// positive deviation = bad; keep sign so bad events score positive
		return dev
	default:
		// no directionality preference: any extreme is notable
		return math.Abs(dev)
	}
}

// connectorFromFQ returns the first dot-segment of a fully-qualified metric
// name, which is the connector name (e.g. "plausible" from
// "plausible.daily.visitors").
func connectorFromFQ(fq string) string {
	if i := strings.IndexByte(fq, '.'); i >= 0 {
		return fq[:i]
	}
	return fq
}

// ComposeSummaryNarrative formats SummarizeData as a human-readable overview.
func ComposeSummaryNarrative(d *SummarizeData) string {
	var sb strings.Builder
	sinceStr := FormatSince(d.Since)
	fmt.Fprintf(&sb, "Business Memory: %d metric(s) across %d connector(s) -- last %s\n\n",
		d.TotalMetrics, d.TotalConnectors, sinceStr)

	if len(d.TopMetrics) == 0 {
		fmt.Fprintln(&sb, "No metrics recorded. Run 'ridgeline sync' to populate Business Memory.")
		return sb.String()
	}

	// Group by connector for readability.
	seen := map[string]bool{}
	order := []string{}
	byConnector := map[string][]MetricSummary{}
	for _, ms := range d.TopMetrics {
		if !seen[ms.Connector] {
			seen[ms.Connector] = true
			order = append(order, ms.Connector)
		}
		byConnector[ms.Connector] = append(byConnector[ms.Connector], ms)
	}

	for _, connector := range order {
		group := byConnector[connector]
		fmt.Fprintf(&sb, "[%s]\n", connector)
		for _, ms := range group {
			if ms.Explain == nil {
				fmt.Fprintf(&sb, "  %s: no data in window\n", ms.FQName)
				continue
			}
			fmt.Fprintf(&sb, "  %s: %s\n", ms.FQName, composeSummary(ms.Explain))
		}
		fmt.Fprintln(&sb)
	}

	return sb.String()
}

// ToSummaryJSON converts SummarizeData to the structured JSON output type.
func ToSummaryJSON(d *SummarizeData) *SummaryJSON {
	j := &SummaryJSON{
		Since:           FormatSince(d.Since),
		TotalMetrics:    d.TotalMetrics,
		TotalConnectors: d.TotalConnectors,
		TopMetrics:      make([]MetricSummaryJSON, 0, len(d.TopMetrics)),
	}
	for _, ms := range d.TopMetrics {
		conf := ConfidenceScore(0)
		if ms.Explain != nil {
			conf = ms.Explain.Confidence
		}
		entry := MetricSummaryJSON{
			MetricFQ:   ms.FQName,
			Connector:  ms.Connector,
			Score:      ms.Score,
			Confidence: conf.Float64(),
		}
		if ms.Explain != nil {
			entry.Explain = ToExplainJSON(ms.Explain)
		}
		j.TopMetrics = append(j.TopMetrics, entry)
	}
	return j
}
