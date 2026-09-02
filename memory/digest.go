package memory

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// DigestSection is one labeled section of a Digest.
type DigestSection struct {
	Title   string
	Content string // human-readable prose or Markdown
}

// Digest is the assembled output of GenerateDigest.
type Digest struct {
	GeneratedAt time.Time
	Since       time.Duration
	Sections    []DigestSection
}

// DigestJSON is the structured JSON representation of a Digest.
type DigestJSON struct {
	GeneratedAt string              `json:"generated_at"`
	Since       string              `json:"since"`
	Sections    []DigestSectionJSON `json:"sections"`
}

// DigestSectionJSON is one section in DigestJSON.
type DigestSectionJSON struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

// GenerateDigest composes summarize + investigate + recommend into a single
// narrative document. topK controls the maximum items per section.
// Investigate is run on the top-3 highest-scoring metrics from summarize.
func (c *Catalog) GenerateDigest(ctx context.Context, since time.Duration, topK int) (*Digest, error) {
	if topK <= 0 {
		topK = 5
	}

	d := &Digest{
		GeneratedAt: time.Now().UTC(),
		Since:       since,
	}

	// --- This Week: top-line what happened --------------------------------
	summary, err := c.SummarizeAll(ctx, since, topK)
	if err != nil {
		return nil, fmt.Errorf("digest: summarize: %w", err)
	}
	d.Sections = append(d.Sections, DigestSection{
		Title:   "This Week",
		Content: ComposeSummaryNarrative(summary),
	})

	// --- Why It Moved: investigate the top movers -------------------------
	const maxInvestigate = 3
	limit := maxInvestigate
	if len(summary.TopMetrics) < limit {
		limit = len(summary.TopMetrics)
	}

	var investigateParts []string
	for i := 0; i < limit; i++ {
		m := summary.TopMetrics[i]
		inv, err := c.InvestigateMetric(ctx, m.FQName, since)
		if err != nil {
			// Non-fatal: skip this metric, include the rest.
			investigateParts = append(investigateParts, fmt.Sprintf("### %s\n\n(investigation unavailable: %v)\n", m.FQName, err))
			continue
		}
		investigateParts = append(investigateParts, fmt.Sprintf("### %s\n\n%s", m.FQName, ComposeCausalNarrative(inv)))
	}

	var investigateContent string
	if len(investigateParts) == 0 {
		investigateContent = "No notable movers to investigate in this window.\n"
	} else {
		investigateContent = strings.Join(investigateParts, "\n")
	}
	d.Sections = append(d.Sections, DigestSection{
		Title:   "Why It Moved",
		Content: investigateContent,
	})

	// --- What To Do: ranked recommendations --------------------------------
	rec, err := c.RecommendAll(ctx, since, topK)
	if err != nil {
		return nil, fmt.Errorf("digest: recommend: %w", err)
	}
	d.Sections = append(d.Sections, DigestSection{
		Title:   "What To Do",
		Content: ComposeRecommendNarrative(rec),
	})

	return d, nil
}

// ComposeDigestMarkdown renders a Digest as a Markdown document.
func ComposeDigestMarkdown(d *Digest) string {
	var sb strings.Builder
	sinceStr := FormatSince(d.Since)
	fmt.Fprintf(&sb, "# Business Memory Digest -- %s\n\n", d.GeneratedAt.Format("2006-01-02"))
	fmt.Fprintf(&sb, "_Generated %s | Window: last %s_\n\n", d.GeneratedAt.Format(time.RFC3339), sinceStr)
	fmt.Fprintln(&sb, "---")
	fmt.Fprintln(&sb)
	for _, sec := range d.Sections {
		fmt.Fprintf(&sb, "## %s\n\n", sec.Title)
		fmt.Fprintln(&sb, strings.TrimRight(sec.Content, "\n"))
		fmt.Fprintln(&sb)
		fmt.Fprintln(&sb, "---")
		fmt.Fprintln(&sb)
	}
	return sb.String()
}

// ToDigestJSON converts a Digest to its structured JSON representation.
func ToDigestJSON(d *Digest) *DigestJSON {
	j := &DigestJSON{
		GeneratedAt: d.GeneratedAt.Format(time.RFC3339),
		Since:       FormatSince(d.Since),
		Sections:    make([]DigestSectionJSON, 0, len(d.Sections)),
	}
	for _, sec := range d.Sections {
		j.Sections = append(j.Sections, DigestSectionJSON{
			Title:   sec.Title,
			Content: sec.Content,
		})
	}
	return j
}
