package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/xydac/ridgeline/memory"
)

// runCompare handles two modes:
//
//	ridgeline compare <metric-a> <metric-b> --config PATH [--since DURATION] [--json]
//	ridgeline compare <metric> --against RECENT,PRIOR --config PATH [--json]
//
// The first produces a pairwise narrative for two metrics over the same window.
// The second produces a period-over-period narrative for one metric.
func runCompare(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("compare", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	sinceStr := fs.String("since", "7d", "time window for pairwise comparison (e.g. 7d, 30d, 24h)")
	against := fs.String("against", "", "period-over-period windows as RECENT,PRIOR (e.g. 7d,14d)")
	asJSON := fs.Bool("json", false, "output as structured JSON for agent consumption")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage:")
		fmt.Fprintln(fs.Output(), "  ridgeline compare <metric-a> <metric-b> --config PATH [--since DURATION] [--json]")
		fmt.Fprintln(fs.Output(), "  ridgeline compare <metric> --against RECENT,PRIOR --config PATH [--json]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Pairwise: compares two metrics over the same time window.")
		fmt.Fprintln(fs.Output(), "Period-over-period: compares one metric against a prior window.")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Examples:")
		fmt.Fprintln(fs.Output(), "  ridgeline compare plausible.daily.visitors plausible.daily.pageviews --config ridgeline.yaml --since 7d")
		fmt.Fprintln(fs.Output(), "  ridgeline compare plausible.daily.visitors --against 7d,14d --config ridgeline.yaml --json")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("compare: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	// Period-over-period mode.
	if *against != "" {
		if fs.NArg() != 1 {
			return usageErrorf("compare: --against requires exactly one metric argument")
		}
		since, priorSince, err := parseAgainst(*against)
		if err != nil {
			return usageErrorf("compare: invalid --against %q: %v", *against, err)
		}
		fq := fs.Arg(0)
		data, err := cat.CompareMetricPeriods(ctx, fq, since, priorSince)
		if err != nil {
			return fmt.Errorf("compare: %w", err)
		}
		if *asJSON {
			enc := json.NewEncoder(stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(memory.ToPeriodOverPeriodJSON(data))
		}
		fmt.Fprint(stdout, memory.ComposePeriodOverPeriodNarrative(data))
		return nil
	}

	// Pairwise mode.
	if fs.NArg() != 2 {
		return usageErrorf("compare: requires two metric arguments, or one metric with --against")
	}
	since, err := parseSinceDuration(*sinceStr)
	if err != nil {
		return usageErrorf("compare: invalid --since %q: %v", *sinceStr, err)
	}
	a, b := fs.Arg(0), fs.Arg(1)
	data, err := cat.CompareMetrics(ctx, a, b, since)
	if err != nil {
		return fmt.Errorf("compare: %w", err)
	}
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(memory.ToCompareJSON(data))
	}
	fmt.Fprint(stdout, memory.ComposePairwiseNarrative(data))
	return nil
}

// parseAgainst parses "7d,14d" into two durations (recent, prior).
func parseAgainst(s string) (since, priorSince time.Duration, err error) {
	parts := strings.SplitN(s, ",", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected RECENT,PRIOR format (e.g. 7d,14d)")
	}
	since, err = parseSinceDuration(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("recent window: %w", err)
	}
	priorSince, err = parseSinceDuration(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("prior window: %w", err)
	}
	return since, priorSince, nil
}
