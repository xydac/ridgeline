package memory

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// RecommendItem is one ranked focus-area recommendation.
type RecommendItem struct {
	MetricFQ         string
	Connector        string
	Score            float64 // combined deviation + forecast score; higher = more urgent
	AnomalyLabel     string  // "surprise-bad", "surprise-good", "surprise-neutral", or ""
	ForecastLabel    string  // "likely-decline", "likely-improvement", "stable", or ""
	Reason           string  // one-sentence explanation
	SuggestedCommand string  // ridgeline command to run next (no --config flag)
	Confidence       float64 // 0.0-1.0
}

// RecommendData is the output of RecommendAll.
type RecommendData struct {
	Since time.Duration
	Items []RecommendItem // ordered by Score descending, capped at TopK
}

// RecommendJSON is the structured output for --json.
type RecommendJSON struct {
	Since string              `json:"since"`
	Items []RecommendItemJSON `json:"items"`
}

// RecommendItemJSON is one entry in RecommendJSON.
type RecommendItemJSON struct {
	MetricFQ         string  `json:"metric_fq"`
	Connector        string  `json:"connector"`
	Score            float64 `json:"score"`
	AnomalyLabel     string  `json:"anomaly_label,omitempty"`
	ForecastLabel    string  `json:"forecast_label,omitempty"`
	Reason           string  `json:"reason"`
	SuggestedCommand string  `json:"suggested_command"`
	Confidence       float64 `json:"confidence"`
}

// RecommendAll walks all metrics, combines deviation score with forecast
// trajectory, and returns the top topK focus areas ranked by urgency.
// Metrics with no data or no notable signal are excluded.
// topK <= 0 returns all items with any signal.
func (c *Catalog) RecommendAll(ctx context.Context, since time.Duration, topK int) (*RecommendData, error) {
	metrics, err := c.ListMetrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("recommend: list metrics: %w", err)
	}

	var items []RecommendItem

	for _, m := range metrics {
		exp, err := c.ExplainMetric(ctx, m.FQName, since)
		if err != nil {
			// no data for this metric; skip
			continue
		}

		devScore := deviationScore(exp)

		// Try forecast -- errors mean insufficient data; treat as stable.
		var forecastLabel string
		var forecastBoost float64
		var forecastConf float64
		fc, fcErr := c.ForecastMetric(ctx, m.FQName, since)
		if fcErr == nil {
			forecastLabel = fc.Directional
			forecastBoost = recommendForecastBoost(exp.Direction, fc.Directional)
			forecastConf = fc.Confidence.Float64()
		}

		totalScore := devScore + forecastBoost

		// Exclude metrics with no signal: score at or below zero and no forecast signal.
		if totalScore <= 0 && forecastLabel == "" {
			continue
		}
		if totalScore <= 0 && forecastLabel == "stable" {
			continue
		}

		anomalyLabel := worstAnomalyLabel(exp.Anomalies)
		reason := composeRecommendReason(exp, forecastLabel)
		cmd := recommendedCommand(m.FQName, anomalyLabel, forecastLabel, exp.Direction)
		conf := exp.Confidence.Float64()
		if fcErr == nil && forecastConf < conf {
			conf = forecastConf
		}

		items = append(items, RecommendItem{
			MetricFQ:         m.FQName,
			Connector:        connectorFromFQ(m.FQName),
			Score:            totalScore,
			AnomalyLabel:     anomalyLabel,
			ForecastLabel:    forecastLabel,
			Reason:           reason,
			SuggestedCommand: cmd,
			Confidence:       conf,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score > items[j].Score
		}
		return items[i].MetricFQ < items[j].MetricFQ
	})

	if topK > 0 && len(items) > topK {
		items = items[:topK]
	}

	return &RecommendData{Since: since, Items: items}, nil
}

// recommendForecastBoost returns a score adjustment based on forecast
// trajectory relative to metric directionality. Declining metrics that
// should improve (higher_is_better) or improving metrics that should
// decline (lower_is_better) get a boost; improving good-direction metrics
// get a negative adjustment so they rank below urgent items.
func recommendForecastBoost(direction, forecastLabel string) float64 {
	switch {
	case forecastLabel == "likely-decline" && direction == "higher_is_better":
		return 1.0
	case forecastLabel == "likely-improvement" && direction == "lower_is_better":
		return 1.0
	case forecastLabel == "likely-improvement" && direction == "higher_is_better":
		return -0.5
	case forecastLabel == "likely-decline" && direction == "lower_is_better":
		return -0.5
	default:
		return 0
	}
}

// worstAnomalyLabel scans a list of events and returns the worst anomaly
// direction label: "surprise-bad" > "surprise-neutral" > "surprise-good" > "".
func worstAnomalyLabel(events []EventRow) string {
	worst := ""
	rank := map[string]int{
		"surprise-bad":     3,
		"surprise-neutral": 2,
		"surprise-good":    1,
	}
	for _, e := range events {
		if e.Kind != "anomaly" {
			continue
		}
		if rank[e.Direction] > rank[worst] {
			worst = e.Direction
		}
	}
	return worst
}

// composeRecommendReason builds a one-sentence explanation for why this
// metric is recommended as a focus area.
func composeRecommendReason(exp *ExplainData, forecastLabel string) string {
	short := metricShortName(exp.MetricFQ)
	var parts []string

	// Anomaly signal.
	anomaly := worstAnomalyLabel(exp.Anomalies)
	if anomaly == "surprise-bad" && exp.Baseline != nil && exp.CurrentValue != nil {
		dev := (*exp.CurrentValue - exp.Baseline.Mean) / math.Max(exp.Baseline.Stddev, 1e-9)
		pct := ((*exp.CurrentValue - exp.Baseline.Mean) / math.Max(math.Abs(exp.Baseline.Mean), 1e-9)) * 100
		if exp.Direction == "higher_is_better" {
			parts = append(parts, fmt.Sprintf("%s dropped %.0f%% (%.1f stddev below baseline)", short, -pct, -dev))
		} else {
			parts = append(parts, fmt.Sprintf("%s rose %.0f%% (%.1f stddev above baseline)", short, pct, dev))
		}
	} else if anomaly == "surprise-good" && exp.Baseline != nil && exp.CurrentValue != nil {
		parts = append(parts, fmt.Sprintf("%s improved beyond baseline", short))
	} else if exp.Baseline != nil && exp.CurrentValue != nil {
		dev := (*exp.CurrentValue - exp.Baseline.Mean) / math.Max(exp.Baseline.Stddev, 1e-9)
		if math.Abs(dev) >= 1.0 {
			parts = append(parts, fmt.Sprintf("%s %.1f stddev from baseline", short, dev))
		}
	}

	// Forecast signal.
	if forecastLabel == "likely-decline" {
		parts = append(parts, "forecast shows likely-decline")
	} else if forecastLabel == "likely-improvement" {
		parts = append(parts, "forecast shows likely-improvement")
	}

	if len(parts) == 0 {
		if exp.Baseline == nil {
			return fmt.Sprintf("%s has no baseline yet; monitor after more sync runs.", short)
		}
		return fmt.Sprintf("%s has minor deviation from baseline.", short)
	}
	return strings.Join(parts, "; ") + "."
}

// recommendedCommand returns the most useful ridgeline command to run next
// for the given metric given its signal type.
func recommendedCommand(fqName, anomalyLabel, forecastLabel, direction string) string {
	if anomalyLabel == "surprise-bad" {
		return fmt.Sprintf("ridgeline investigate %s", fqName)
	}
	// Declining trajectory without a current anomaly: forecast is the primary tool.
	if (forecastLabel == "likely-decline" && direction == "higher_is_better") ||
		(forecastLabel == "likely-improvement" && direction == "lower_is_better") {
		return fmt.Sprintf("ridgeline forecast %s", fqName)
	}
	return fmt.Sprintf("ridgeline explain %s", fqName)
}

// ComposeRecommendNarrative formats RecommendData as a human-readable
// prioritized focus list.
func ComposeRecommendNarrative(d *RecommendData) string {
	var sb strings.Builder
	sinceStr := FormatSince(d.Since)
	fmt.Fprintf(&sb, "Focus areas for the last %s:\n\n", sinceStr)

	if len(d.Items) == 0 {
		fmt.Fprintln(&sb, "No notable signals in Business Memory for this window.")
		fmt.Fprintln(&sb, "All tracked metrics are near baseline with stable forecasts.")
		fmt.Fprintln(&sb, "Run 'ridgeline sync' if you have not synced recently.")
		return sb.String()
	}

	for i, item := range d.Items {
		label := ""
		if item.AnomalyLabel == "surprise-bad" {
			label = " [anomaly]"
		} else if item.ForecastLabel == "likely-decline" {
			label = " [trending down]"
		} else if item.ForecastLabel == "likely-improvement" {
			label = " [trending up]"
		}
		fmt.Fprintf(&sb, "%d. %s%s (score: %.1f, confidence: %.0f%%)\n",
			i+1, item.MetricFQ, label, item.Score, item.Confidence*100)
		fmt.Fprintf(&sb, "   %s\n", item.Reason)
		fmt.Fprintf(&sb, "   -> %s\n", item.SuggestedCommand)
		if i < len(d.Items)-1 {
			fmt.Fprintln(&sb)
		}
	}

	return sb.String()
}

// ToRecommendJSON converts RecommendData to the structured JSON output type.
func ToRecommendJSON(d *RecommendData) *RecommendJSON {
	j := &RecommendJSON{
		Since: FormatSince(d.Since),
		Items: make([]RecommendItemJSON, 0, len(d.Items)),
	}
	for _, item := range d.Items {
		j.Items = append(j.Items, RecommendItemJSON{
			MetricFQ:         item.MetricFQ,
			Connector:        item.Connector,
			Score:            item.Score,
			AnomalyLabel:     item.AnomalyLabel,
			ForecastLabel:    item.ForecastLabel,
			Reason:           item.Reason,
			SuggestedCommand: item.SuggestedCommand,
			Confidence:       item.Confidence,
		})
	}
	return j
}
