package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/xydac/ridgeline/memory"
)

// runDigest handles `ridgeline digest --config PATH [--since DUR] [--top N]
// [--out PATH] [--webhook URL] [--json]`.
//
// It composes summarize, investigate, and recommend into a single narrative
// document: "This Week / Why It Moved / What To Do". Output is Markdown by
// default; --json emits structured JSON for agent or webhook consumption.
func runDigest(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("digest", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	sinceStr := fs.String("since", "7d", "look-back window (e.g. 7d, 14d, 30d)")
	topN := fs.Int("top", 5, "max items per section")
	outPath := fs.String("out", "", "write output to this file (default: stdout); use 'auto' for date-stamped filename")
	webhook := fs.String("webhook", "", "POST digest JSON to this URL")
	asJSON := fs.Bool("json", false, "output as structured JSON instead of Markdown")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline digest --config PATH [--since DURATION] [--top N] [--out PATH] [--webhook URL] [--json]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Generate a Business Memory digest: a narrative document composed of")
		fmt.Fprintln(fs.Output(), "  'This Week'   -- top-line metric summary for the window")
		fmt.Fprintln(fs.Output(), "  'Why It Moved' -- investigation of the top 3 movers")
		fmt.Fprintln(fs.Output(), "  'What To Do'  -- ranked recommendations with suggested next commands")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Output is Markdown by default. Use --json for structured output,")
		fmt.Fprintln(fs.Output(), "--out to write to a file, or --webhook to POST JSON to a URL.")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Examples:")
		fmt.Fprintln(fs.Output(), "  ridgeline digest --config ridgeline.yaml --since 7d")
		fmt.Fprintln(fs.Output(), "  ridgeline digest --config ridgeline.yaml --out auto")
		fmt.Fprintln(fs.Output(), "  ridgeline digest --config ridgeline.yaml --webhook https://hooks.example.com/digest")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("digest: --config is required")
	}

	since, err := parseSinceDuration(*sinceStr)
	if err != nil {
		return usageErrorf("digest: invalid --since %q: %v", *sinceStr, err)
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	d, err := cat.GenerateDigest(ctx, since, *topN)
	if err != nil {
		return fmt.Errorf("digest: %w", err)
	}

	dj := memory.ToDigestJSON(d)

	// POST to webhook if requested.
	if *webhook != "" {
		if werr := postDigestWebhook(*webhook, dj); werr != nil {
			fmt.Fprintf(os.Stderr, "digest: webhook POST failed: %v\n", werr)
		}
	}

	// Render output.
	var output string
	if *asJSON {
		b, jerr := json.MarshalIndent(dj, "", "  ")
		if jerr != nil {
			return fmt.Errorf("digest: marshal json: %w", jerr)
		}
		output = string(b) + "\n"
	} else {
		output = memory.ComposeDigestMarkdown(d)
	}

	// Write to file or stdout.
	if *outPath == "" {
		fmt.Fprint(stdout, output)
		return nil
	}

	dest := *outPath
	if dest == "auto" {
		dest = fmt.Sprintf("digest-%s.md", d.GeneratedAt.Format("2006-01-02"))
	}
	if err := os.WriteFile(dest, []byte(output), 0o644); err != nil {
		return fmt.Errorf("digest: write %s: %w", dest, err)
	}
	fmt.Fprintf(stdout, "digest written to %s\n", filepath.Clean(dest))
	return nil
}

// postDigestWebhook marshals the digest as JSON and POSTs it to url.
func postDigestWebhook(url string, d *memory.DigestJSON) error {
	b, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b)) //nolint:gosec
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("server returned %s", resp.Status)
	}
	return nil
}
