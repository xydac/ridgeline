package memory

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestGenerateDigest verifies that GenerateDigest produces a three-section
// document against a synthetic catalog with one notable metric.
func TestGenerateDigest(t *testing.T) {
	ctx := context.Background()
	cat := openTestCatalog(t)

	now := time.Now().UTC()
	window := 7 * 24 * time.Hour

	// Set up one metric with a baseline and a bad current value.
	if err := cat.UpsertMetric(ctx, "app.visitors", "count", "higher_is_better", "sum", nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		v := 500.0 + float64(i%3-1)*10
		insertValueAt(t, cat, "app.visitors", v, now.Add(-time.Duration(30-i)*24*time.Hour))
	}
	if err := cat.ComputeBaselines(ctx, "app.visitors", []int{30}); err != nil {
		t.Fatal(err)
	}
	// Current value: well below the baseline mean to register as anomalous.
	badVal := 200.0
	if err := cat.UpsertMetric(ctx, "app.visitors", "count", "higher_is_better", "sum", &badVal); err != nil {
		t.Fatal(err)
	}
	insertValueAt(t, cat, "app.visitors", badVal, now.Add(-time.Hour))

	d, err := cat.GenerateDigest(ctx, window, 5)
	if err != nil {
		t.Fatalf("GenerateDigest: %v", err)
	}

	// Must have exactly three sections.
	if len(d.Sections) != 3 {
		t.Fatalf("expected 3 sections, got %d", len(d.Sections))
	}

	titles := []string{"This Week", "Why It Moved", "What To Do"}
	for i, want := range titles {
		if d.Sections[i].Title != want {
			t.Errorf("section %d title: got %q, want %q", i, d.Sections[i].Title, want)
		}
		if strings.TrimSpace(d.Sections[i].Content) == "" {
			t.Errorf("section %d (%q) is empty", i, d.Sections[i].Title)
		}
	}

	if d.Since != window {
		t.Errorf("since: got %v, want %v", d.Since, window)
	}
	if d.GeneratedAt.IsZero() {
		t.Error("GeneratedAt is zero")
	}
}

// TestComposeDigestMarkdown verifies the Markdown output structure.
func TestComposeDigestMarkdown(t *testing.T) {
	d := &Digest{
		GeneratedAt: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
		Since:       7 * 24 * time.Hour,
		Sections: []DigestSection{
			{Title: "This Week", Content: "Nothing notable.\n"},
			{Title: "Why It Moved", Content: "No movers.\n"},
			{Title: "What To Do", Content: "Keep going.\n"},
		},
	}
	md := ComposeDigestMarkdown(d)

	checks := []string{
		"# Business Memory Digest",
		"2026-09-02",
		"## This Week",
		"## Why It Moved",
		"## What To Do",
		"Nothing notable.",
		"---",
	}
	for _, want := range checks {
		if !strings.Contains(md, want) {
			t.Errorf("markdown missing %q", want)
		}
	}
}

// TestToDigestJSON verifies the JSON conversion shape.
func TestToDigestJSON(t *testing.T) {
	d := &Digest{
		GeneratedAt: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
		Since:       7 * 24 * time.Hour,
		Sections: []DigestSection{
			{Title: "This Week", Content: "ok"},
		},
	}
	j := ToDigestJSON(d)
	if j.Since != "7d" {
		t.Errorf("since: got %q, want %q", j.Since, "7d")
	}
	if len(j.Sections) != 1 || j.Sections[0].Title != "This Week" {
		t.Errorf("sections: got %v", j.Sections)
	}
	if j.GeneratedAt == "" {
		t.Error("GeneratedAt is empty")
	}
}
