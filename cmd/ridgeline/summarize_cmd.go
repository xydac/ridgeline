package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/xydac/ridgeline/memory"
)

// runSummarize handles `ridgeline summarize --config PATH [--since DUR] [--top N] [--json]`.
//
// It ranks all tracked metrics by directionality-adjusted deviation from their
// baselines and prints a narrative overview grouped by connector. Use --json
// for structured output suitable for agent consumption.
func runSummarize(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("summarize", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	sinceStr := fs.String("since", "7d", "time window to analyze (e.g. 7d, 30d, 24h)")
	topK := fs.Int("top", 5, "number of top metrics to show (0 = show all)")
	asJSON := fs.Bool("json", false, "output as structured JSON for agent consumption")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline summarize --config PATH [--since DURATION] [--top N] [--json]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Ranks all tracked metrics by deviation from their baselines and prints a")
		fmt.Fprintln(fs.Output(), "narrative overview grouped by connector. Surprise-bad events (e.g. a metric")
		fmt.Fprintln(fs.Output(), "that should be high but is low) rank above surprise-good events of the same")
		fmt.Fprintln(fs.Output(), "magnitude, so the most actionable information comes first.")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Example:")
		fmt.Fprintln(fs.Output(), "  ridgeline summarize --config ridgeline.yaml --since 7d --top 5")
		fmt.Fprintln(fs.Output(), "  ridgeline summarize --config ridgeline.yaml --since 30d --json")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("summarize: --config is required")
	}
	if fs.NArg() > 0 {
		return usageErrorf("summarize: unexpected argument %q (did you mean --since?)", fs.Arg(0))
	}

	since, err := parseSinceDuration(*sinceStr)
	if err != nil {
		return usageErrorf("summarize: invalid --since %q: %v", *sinceStr, err)
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	data, err := cat.SummarizeAll(ctx, since, *topK)
	if err != nil {
		return fmt.Errorf("summarize: %w", err)
	}

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(memory.ToSummaryJSON(data))
	}

	fmt.Fprint(stdout, memory.ComposeSummaryNarrative(data))
	return nil
}
