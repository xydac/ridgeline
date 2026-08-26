package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/xydac/ridgeline/memory"
)

// runInvestigate handles: ridgeline investigate <metric> --config PATH [--since DURATION] [--json]
func runInvestigate(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("investigate", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	sinceStr := fs.String("since", "14d", "time window (e.g. 7d, 30d, 24h)")
	asJSON := fs.Bool("json", false, "output as structured JSON for agent consumption")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage:")
		fmt.Fprintln(fs.Output(), "  ridgeline investigate <metric> --config PATH [--since DURATION] [--json]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Produces a causal narrative for a metric: anomalies, correlated events,")
		fmt.Fprintln(fs.Output(), "and sibling-metric correlations over the requested window.")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Examples:")
		fmt.Fprintln(fs.Output(), "  ridgeline investigate plausible.daily.visitors --config ridgeline.yaml --since 14d")
		fmt.Fprintln(fs.Output(), "  ridgeline investigate myapp.daily.revenue --config ridgeline.yaml --since 30d --json")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("investigate: --config is required")
	}
	if fs.NArg() != 1 {
		return usageErrorf("investigate: exactly one metric argument is required")
	}

	since, err := parseSinceDuration(*sinceStr)
	if err != nil {
		return usageErrorf("investigate: invalid --since %q: %v", *sinceStr, err)
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	fq := fs.Arg(0)
	d, err := cat.InvestigateMetric(ctx, fq, since)
	if err != nil {
		return fmt.Errorf("investigate: %w", err)
	}

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(memory.ToInvestigateJSON(d))
	}
	fmt.Fprint(stdout, memory.ComposeCausalNarrative(d))
	return nil
}
