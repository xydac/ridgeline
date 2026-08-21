package memory

import (
	"fmt"
	"math"
)

// ConfidenceScore is a measure of evidence strength in [0, 1].
// Higher values indicate more data or stronger signal behind a claim.
type ConfidenceScore float64

const (
	confidenceHighThreshold   = 0.75
	confidenceMediumThreshold = 0.40

	// baselineSaturationSamples is the sample count at which baseline
	// confidence saturates at 1.0. 90 samples = a full 90-day window.
	baselineSaturationSamples = 90.0

	// anomalySaturationZ is the z-score at which anomaly confidence saturates.
	anomalySaturationZ = 3.0

	// proximityHorizon is the max hours over which proximity confidence decays
	// to zero. Matches causalProximityWindow.
	proximityHorizon = 48.0

	// correlationMinSamples is the minimum sample count for a correlation claim
	// to carry any confidence.
	correlationMinSamples = 5

	// correlationSaturationSamples is the sample count at which the sample-count
	// weight on correlation confidence saturates.
	correlationSaturationSamples = 30.0
)

// Level returns a human-readable label: "high", "medium", or "low".
func (s ConfidenceScore) Level() string {
	switch {
	case s >= confidenceHighThreshold:
		return "high"
	case s >= confidenceMediumThreshold:
		return "medium"
	default:
		return "low"
	}
}

// Tag returns a parenthetical confidence annotation for inline text.
// detail provides the human-readable evidence source, e.g. "90-day baseline, n=90".
func (s ConfidenceScore) Tag(detail string) string {
	if detail == "" {
		return fmt.Sprintf("(%s confidence: %.0f%%)", s.Level(), float64(s)*100)
	}
	return fmt.Sprintf("(%s confidence: %s)", s.Level(), detail)
}

// Float64 returns the score as a float64 suitable for JSON serialization.
func (s ConfidenceScore) Float64() float64 { return float64(s) }

// ScoreBaseline returns a confidence score for a baseline-backed claim.
// Confidence saturates at baselineSaturationSamples (90) samples.
func ScoreBaseline(sampleCount int) ConfidenceScore {
	if sampleCount <= 0 {
		return 0
	}
	return ConfidenceScore(math.Min(1.0, float64(sampleCount)/baselineSaturationSamples))
}

// ScoreAnomaly returns a confidence score for an anomaly claim.
// absZ is the absolute z-score of the anomaly. Saturates at anomalySaturationZ (3.0).
func ScoreAnomaly(absZ float64) ConfidenceScore {
	if absZ <= 0 {
		return 0
	}
	return ConfidenceScore(math.Min(1.0, absZ/anomalySaturationZ))
}

// ScoreProximity returns a confidence score for a causal claim based on
// the time gap between an event and an anomaly. Saturates at 0 hours and
// decays linearly to 0 at proximityHorizon (48h).
func ScoreProximity(proximityHours float64) ConfidenceScore {
	if proximityHours <= 0 {
		return 1.0
	}
	score := 1.0 - proximityHours/proximityHorizon
	if score < 0 {
		return 0
	}
	return ConfidenceScore(score)
}

// ScoreCorrelation returns a confidence score for a sibling-correlation claim.
// absR is |Pearson r| in [0,1]. Confidence is weighted by sample count and
// is zero below correlationMinSamples.
func ScoreCorrelation(absR float64, samples int) ConfidenceScore {
	if samples < correlationMinSamples || absR < 0 {
		return 0
	}
	sampleFactor := math.Min(1.0, float64(samples)/correlationSaturationSamples)
	return ConfidenceScore(absR * sampleFactor)
}

// baselineDetail returns a detail string for a confidence tag given a baseline,
// e.g. "90-day baseline, n=90".
func baselineDetail(b *BaselineRow) string {
	if b == nil {
		return "no baseline"
	}
	return fmt.Sprintf("%dd baseline, n=%d", b.WindowDays, b.SampleCount)
}
