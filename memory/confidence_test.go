package memory

import (
	"testing"
)

func TestScoreBaseline(t *testing.T) {
	cases := []struct {
		samples   int
		wantMin   float64
		wantMax   float64
		wantLevel string
	}{
		{0, 0, 0, "low"},
		{5, 0.05, 0.06, "low"},
		{30, 0.33, 0.34, "low"},
		{45, 0.49, 0.51, "medium"},
		{68, 0.75, 0.76, "high"},
		{90, 1.0, 1.0, "high"},
		{120, 1.0, 1.0, "high"},
	}
	for _, c := range cases {
		s := ScoreBaseline(c.samples)
		f := s.Float64()
		if f < c.wantMin || f > c.wantMax {
			t.Errorf("ScoreBaseline(%d) = %.4f; want [%.4f, %.4f]", c.samples, f, c.wantMin, c.wantMax)
		}
		if s.Level() != c.wantLevel {
			t.Errorf("ScoreBaseline(%d).Level() = %q; want %q", c.samples, s.Level(), c.wantLevel)
		}
	}
}

func TestScoreAnomaly(t *testing.T) {
	cases := []struct {
		z         float64
		wantMin   float64
		wantMax   float64
		wantLevel string
	}{
		{0, 0, 0, "low"},
		{1.0, 0.33, 0.34, "low"},
		{1.5, 0.49, 0.51, "medium"},
		{2.5, 0.83, 0.84, "high"},
		{3.0, 1.0, 1.0, "high"},
		{5.0, 1.0, 1.0, "high"},
	}
	for _, c := range cases {
		s := ScoreAnomaly(c.z)
		f := s.Float64()
		if f < c.wantMin || f > c.wantMax {
			t.Errorf("ScoreAnomaly(%.1f) = %.4f; want [%.4f, %.4f]", c.z, f, c.wantMin, c.wantMax)
		}
		if s.Level() != c.wantLevel {
			t.Errorf("ScoreAnomaly(%.1f).Level() = %q; want %q", c.z, s.Level(), c.wantLevel)
		}
	}
}

func TestScoreProximity(t *testing.T) {
	cases := []struct {
		hours     float64
		wantMin   float64
		wantMax   float64
		wantLevel string
	}{
		{0, 1.0, 1.0, "high"},
		{-1, 1.0, 1.0, "high"},
		{12, 0.74, 0.76, "high"},
		{24, 0.49, 0.51, "medium"},
		{36, 0.24, 0.26, "low"},
		{48, 0, 0, "low"},
		{100, 0, 0, "low"},
	}
	for _, c := range cases {
		s := ScoreProximity(c.hours)
		f := s.Float64()
		if f < c.wantMin || f > c.wantMax {
			t.Errorf("ScoreProximity(%.0f) = %.4f; want [%.4f, %.4f]", c.hours, f, c.wantMin, c.wantMax)
		}
		if s.Level() != c.wantLevel {
			t.Errorf("ScoreProximity(%.0f).Level() = %q; want %q", c.hours, s.Level(), c.wantLevel)
		}
	}
}

func TestScoreCorrelation(t *testing.T) {
	cases := []struct {
		r         float64
		samples   int
		wantMin   float64
		wantMax   float64
		wantLevel string
	}{
		{0.9, 3, 0, 0, "low"}, // below min samples
		{0.9, 5, 0.14, 0.16, "low"},
		{0.9, 15, 0.44, 0.46, "medium"},
		{0.9, 30, 0.89, 0.91, "high"},
		{0.9, 60, 0.89, 0.91, "high"}, // saturates at 30 samples
		{0.5, 30, 0.49, 0.51, "medium"},
		{0.2, 30, 0.19, 0.21, "low"},
	}
	for _, c := range cases {
		s := ScoreCorrelation(c.r, c.samples)
		f := s.Float64()
		if f < c.wantMin || f > c.wantMax {
			t.Errorf("ScoreCorrelation(%.1f, %d) = %.4f; want [%.4f, %.4f]", c.r, c.samples, f, c.wantMin, c.wantMax)
		}
		if s.Level() != c.wantLevel {
			t.Errorf("ScoreCorrelation(%.1f, %d).Level() = %q; want %q", c.r, c.samples, s.Level(), c.wantLevel)
		}
	}
}

func TestConfidenceScoreTag(t *testing.T) {
	s := ScoreBaseline(90)
	tag := s.Tag("90-day baseline, n=90")
	if tag != "(high confidence: 90-day baseline, n=90)" {
		t.Errorf("Tag = %q; want (high confidence: 90-day baseline, n=90)", tag)
	}

	low := ScoreBaseline(5)
	tag2 := low.Tag("7-day baseline, n=5")
	if tag2 != "(low confidence: 7-day baseline, n=5)" {
		t.Errorf("Tag = %q; want (low confidence: 7-day baseline, n=5)", tag2)
	}

	noDetail := ConfidenceScore(0.8).Tag("")
	if noDetail != "(high confidence: 80%)" {
		t.Errorf("Tag (no detail) = %q; want (high confidence: 80%%)", noDetail)
	}
}
