package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/xydac/ridgeline/memory"
)

// runRecommend handles `ridgeline recommend --config PATH [--since DUR] [--top N] [--json]`.
//
// It composes anomaly detection, forecast trajectory, and baseline deviation
// into a ranked list of focus areas, each with a suggested next command.
func runRecommend(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("recommend", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	sinceStr := fs.String("since", "7d", "look-back window (e.g. 7d, 14d, 30d)")
	topN := fs.Int("top", 5, "number of focus areas to show")
	asJSON := fs.Bool("json", false, "output as structured JSON for agent consumption")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline recommend --config PATH [--since DURATION] [--top N] [--json]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Composes anomaly detection, forecast trajectory, and baseline deviation")
		fmt.Fprintln(fs.Output(), "into a ranked list of focus areas. Each recommendation includes a")
		fmt.Fprintln(fs.Output(), "one-sentence reason and a suggested ridgeline command to run next.")
		fmt.Fprintln(fs.Output(), "Metrics sitting at baseline with stable forecasts are excluded.")
		fmt.Fprintln(fs.Output(), "Use --json for structured output suitable for agent consumption.")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Example:")
		fmt.Fprintln(fs.Output(), "  ridgeline recommend --config ridgeline.yaml --since 7d --top 5")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("recommend: --config is required")
	}

	since, err := parseSinceDuration(*sinceStr)
	if err != nil {
		return usageErrorf("recommend: invalid --since %q: %v", *sinceStr, err)
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	data, err := cat.RecommendAll(ctx, since, *topN)
	if err != nil {
		return fmt.Errorf("recommend: %w", err)
	}

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(memory.ToRecommendJSON(data))
	}

	fmt.Fprint(stdout, memory.ComposeRecommendNarrative(data))
	return nil
}
