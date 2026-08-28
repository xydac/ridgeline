package memory

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"
)

// ForecastData holds computed projection data for one metric over a horizon.
type ForecastData struct {
	MetricFQ      string
	Direction     string
	Unit          string
	Horizon       time.Duration
	SampleCount   int
	Slope         float64 // value change per day from linear regression
	Intercept     float64
	RSquared      float64
	ProjectedMean float64 // projected mean at end of horizon
	BandWidth     float64 // +/- uncertainty around ProjectedMean
	Directional   string  // "likely-improvement", "likely-decline", or "stable"
	Confidence    ConfidenceScore
	Baseline      *BaselineRow // 30d or 90d baseline, for display
}

// ForecastJSON is the structured output for --json.
type ForecastJSON struct {
	MetricFQ      string        `json:"metric_fq"`
	Horizon       string        `json:"horizon"`
	Direction     string        `json:"direction"`
	Unit          string        `json:"unit"`
	SampleCount   int           `json:"sample_count"`
	Slope         float64       `json:"slope_per_day"`
	RSquared      float64       `json:"r_squared"`
	ProjectedMean float64       `json:"projected_mean"`
	BandWidth     float64       `json:"band_width"`
	Directional   string        `json:"directional_label"`
	Confidence    float64       `json:"confidence"`
	Baseline      *BaselineJSON `json:"baseline,omitempty"`
	Summary       string        `json:"summary"`
}

// ForecastMetric computes a directional projection for fqName over horizon.
// It fits a linear regression to the available observations in bm_metric_values
// (up to 90 days of history) and projects the trend forward by horizonDays.
// Returns an error if the metric is not in the catalog or has fewer than 2
// observations.
func (c *Catalog) ForecastMetric(ctx context.Context, fqName string, horizon time.Duration) (*ForecastData, error) {
	d := &ForecastData{MetricFQ: fqName, Horizon: horizon}

	err := c.db.QueryRowContext(ctx,
		`SELECT direction, unit FROM bm_metrics WHERE fq_name = ?`,
		fqName).Scan(&d.Direction, &d.Unit)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("metric %q not found in Business Memory catalog; run 'ridgeline sync' first", fqName)
	}
	if err != nil {
		return nil, fmt.Errorf("memory: forecast %s: %w", fqName, err)
	}

	cutoff := time.Now().UTC().Add(-90 * 24 * time.Hour).Format(time.RFC3339)
	rows, err := c.db.QueryContext(ctx,
		`SELECT observed_at, value FROM bm_metric_values WHERE fq_name = ? AND observed_at >= ? ORDER BY observed_at ASC`,
		fqName, cutoff)
	if err != nil {
		return nil, fmt.Errorf("memory: forecast query %s: %w", fqName, err)
	}
	defer rows.Close()

	var ts []time.Time
	var vals []float64
	for rows.Next() {
		var atStr string
		var v float64
		if err := rows.Scan(&atStr, &v); err != nil {
			return nil, fmt.Errorf("memory: forecast scan: %w", err)
		}
		t, err := time.Parse(time.RFC3339, atStr)
		if err != nil {
			return nil, fmt.Errorf("memory: forecast parse time: %w", err)
		}
		ts = append(ts, t)
		vals = append(vals, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: forecast iterate: %w", err)
	}
	if len(vals) < 2 {
		return nil, fmt.Errorf("metric %q has fewer than 2 observations; cannot forecast (run 'ridgeline sync' to collect more data)", fqName)
	}

	d.SampleCount = len(vals)
	origin := ts[0]

	// x[i] = days since first observation
	xs := make([]float64, len(ts))
	for i, t := range ts {
		xs[i] = t.Sub(origin).Hours() / 24.0
	}

	d.Slope, d.Intercept = linearRegression(xs, vals)
	d.RSquared = rSquared(xs, vals, d.Slope, d.Intercept)

	horizonDays := horizon.Hours() / 24.0
	lastX := xs[len(xs)-1]
	d.ProjectedMean = d.Intercept + d.Slope*(lastX+horizonDays)

	// Residual-based uncertainty band: std dev of residuals, scaled by horizon.
	d.BandWidth = residualBand(xs, vals, d.Slope, d.Intercept, horizonDays)

	d.Directional = forecastLabel(d.Slope, d.Direction, d.Baseline, horizonDays)

	// Baseline for display (prefer 30d; fall back to whatever is available).
	horizonDaysInt := int(math.Ceil(horizonDays))
	d.Baseline = c.pickBaseline(ctx, fqName, horizonDaysInt)

	// Confidence: baseline evidence scaled by regression fit quality.
	baseConf := ScoreBaseline(d.SampleCount)
	// R² weight: R²=0 -> 0.5x, R²=1 -> 1.0x. Prevents 0*X collapse.
	rFactor := 0.5 + 0.5*math.Max(0, d.RSquared)
	d.Confidence = ConfidenceScore(math.Min(1.0, float64(baseConf)*rFactor))

	return d, nil
}

// ComposeForecastNarrative returns a 3-6 sentence plain-text forecast.
func ComposeForecastNarrative(d *ForecastData) string {
	var sb strings.Builder
	horizonStr := FormatSince(d.Horizon)

	fmt.Fprintf(&sb, "%s -- %s forecast\n\n", d.MetricFQ, horizonStr)

	dirLabel := forecastDirectionLabel(d.Directional, d.Direction)
	fmt.Fprintf(&sb, "Trend: %s (slope %.4g %s/day, R² = %.2f).\n",
		dirLabel, d.Slope, d.Unit, d.RSquared)

	fmt.Fprintf(&sb, "Projected %s mean: %.4g ± %.4g %s (%d-day horizon, n=%d observations).\n",
		horizonStr, d.ProjectedMean, d.BandWidth, d.Unit, int(d.Horizon.Hours()/24), d.SampleCount)

	if d.Baseline != nil {
		baselineDiff := d.ProjectedMean - d.Baseline.Mean
		pct := 0.0
		if d.Baseline.Mean != 0 {
			pct = baselineDiff / math.Abs(d.Baseline.Mean) * 100
		}
		sign := "+"
		if pct < 0 {
			sign = ""
		}
		fmt.Fprintf(&sb, "Relative to the %dd baseline (mean %.4g %s): %s%.1f%%.\n",
			d.Baseline.WindowDays, d.Baseline.Mean, d.Unit, sign, pct)
	}

	if d.RSquared < 0.3 {
		fmt.Fprintln(&sb, "Note: low R² indicates high variability; treat projection as directional only.")
	}

	confDetail := fmt.Sprintf("n=%d, R²=%.2f", d.SampleCount, d.RSquared)
	fmt.Fprintf(&sb, "\nSummary: %s\n", composeForecastSummary(d, confDetail))
	return sb.String()
}

// ToForecastJSON converts ForecastData to the structured output type.
func ToForecastJSON(d *ForecastData) *ForecastJSON {
	confDetail := fmt.Sprintf("n=%d, R²=%.2f", d.SampleCount, d.RSquared)
	j := &ForecastJSON{
		MetricFQ:      d.MetricFQ,
		Horizon:       FormatSince(d.Horizon),
		Direction:     d.Direction,
		Unit:          d.Unit,
		SampleCount:   d.SampleCount,
		Slope:         d.Slope,
		RSquared:      d.RSquared,
		ProjectedMean: d.ProjectedMean,
		BandWidth:     d.BandWidth,
		Directional:   d.Directional,
		Confidence:    d.Confidence.Float64(),
		Summary:       composeForecastSummary(d, confDetail),
	}
	if d.Baseline != nil {
		j.Baseline = &BaselineJSON{
			WindowDays:  d.Baseline.WindowDays,
			Mean:        d.Baseline.Mean,
			Stddev:      d.Baseline.Stddev,
			SampleCount: d.Baseline.SampleCount,
		}
	}
	return j
}

func composeForecastSummary(d *ForecastData, confDetail string) string {
	short := metricShortName(d.MetricFQ)
	horizonStr := FormatSince(d.Horizon)
	label := forecastDirectionLabel(d.Directional, d.Direction)
	confTag := d.Confidence.Tag(confDetail)
	return fmt.Sprintf("%s is %s; projected %s mean %.4g %s %s.",
		short, label, horizonStr, d.ProjectedMean, d.Unit, confTag)
}

func forecastLabel(slope float64, direction string, baseline *BaselineRow, horizonDays float64) string {
	// Determine magnitude of projected change relative to baseline or raw value.
	var refMean float64
	if baseline != nil && baseline.Mean != 0 {
		refMean = math.Abs(baseline.Mean)
	} else {
		refMean = 1 // avoid division by zero when no baseline
	}
	projectedChange := slope * horizonDays
	relChange := projectedChange / refMean

	const stableThreshold = 0.03 // <3% projected change is "stable"
	if math.Abs(relChange) < stableThreshold {
		return "stable"
	}

	improving := (slope > 0 && direction == "higher_is_better") ||
		(slope < 0 && direction == "lower_is_better")
	if improving {
		return "likely-improvement"
	}
	return "likely-decline"
}

func forecastDirectionLabel(directional, metricDirection string) string {
	switch directional {
	case "likely-improvement":
		return "trending toward improvement"
	case "likely-decline":
		return "trending toward decline"
	default:
		return "stable"
	}
}

// linearRegression returns the slope and intercept for y = slope*x + intercept.
func linearRegression(xs, ys []float64) (slope, intercept float64) {
	n := float64(len(xs))
	var sumX, sumY, sumXY, sumX2 float64
	for i := range xs {
		sumX += xs[i]
		sumY += ys[i]
		sumXY += xs[i] * ys[i]
		sumX2 += xs[i] * xs[i]
	}
	denom := n*sumX2 - sumX*sumX
	if denom == 0 {
		return 0, sumY / n
	}
	slope = (n*sumXY - sumX*sumY) / denom
	intercept = (sumY - slope*sumX) / n
	return slope, intercept
}

// rSquared returns the coefficient of determination for the fitted line.
func rSquared(xs, ys []float64, slope, intercept float64) float64 {
	if len(ys) == 0 {
		return 0
	}
	var sumY float64
	for _, y := range ys {
		sumY += y
	}
	mean := sumY / float64(len(ys))

	var ssTot, ssRes float64
	for i, y := range ys {
		ssTot += (y - mean) * (y - mean)
		resid := y - (slope*xs[i] + intercept)
		ssRes += resid * resid
	}
	if ssTot == 0 {
		return 1 // perfect fit (constant series)
	}
	r2 := 1 - ssRes/ssTot
	if r2 < 0 {
		return 0 // clamp; regression worse than mean
	}
	return r2
}

// residualBand returns the prediction band width at horizonDays ahead.
// It uses the standard error of the residuals and scales by sqrt(horizon)
// to account for increasing uncertainty over time.
func residualBand(xs, ys []float64, slope, intercept, horizonDays float64) float64 {
	n := len(xs)
	if n < 3 {
		return math.Abs(slope) * horizonDays
	}
	var ssRes float64
	for i, y := range ys {
		resid := y - (slope*xs[i] + intercept)
		ssRes += resid * resid
	}
	se := math.Sqrt(ssRes / float64(n-2))
	// Scale by sqrt(horizon) so uncertainty grows with projection distance.
	scale := math.Sqrt(horizonDays)
	if scale < 1 {
		scale = 1
	}
	return se * scale
}
