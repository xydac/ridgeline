package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/xydac/ridgeline/memory"
)

// runExplain handles `ridgeline explain <metric> --config PATH [--since DUR] [--json]`.
//
// It assembles a templated narrative from the Business Memory catalog covering
// the metric's current value, baseline comparison, prior-period change, and
// any anomalies in the requested window.
func runExplain(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("explain", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	sinceStr := fs.String("since", "7d", "time window to analyze (e.g. 7d, 30d, 24h)")
	asJSON := fs.Bool("json", false, "output as structured JSON for agent consumption")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline explain <metric> --config PATH [--since DURATION] [--json]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Produces a templated narrative for a metric from the Business Memory catalog.")
		fmt.Fprintln(fs.Output(), "Output covers: current value, baseline comparison, prior-period trend,")
		fmt.Fprintln(fs.Output(), "and any anomalies detected during sync. Use --json for structured output")
		fmt.Fprintln(fs.Output(), "suitable for agent consumption.")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Example:")
		fmt.Fprintln(fs.Output(), "  ridgeline explain plausible.daily.visitors --config ridgeline.yaml --since 7d")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("explain: --config is required")
	}
	if fs.NArg() == 0 {
		return usageErrorf("explain: metric name required (e.g. plausible.daily.visitors)")
	}
	fqName := fs.Arg(0)

	since, err := parseSinceDuration(*sinceStr)
	if err != nil {
		return usageErrorf("explain: invalid --since %q: %v", *sinceStr, err)
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	data, err := cat.ExplainMetric(ctx, fqName, since)
	if err != nil {
		return fmt.Errorf("explain: %w", err)
	}

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(memory.ToExplainJSON(data))
	}

	fmt.Fprint(stdout, memory.ComposeNarrative(data))
	return nil
}

// parseSinceDuration accepts "Nd" shorthand (e.g. "7d", "30d") as well as
// standard Go duration strings (e.g. "24h", "168h").
func parseSinceDuration(s string) (time.Duration, error) {
	var days int
	if n, _ := fmt.Sscanf(s, "%dd", &days); n == 1 && days > 0 {
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}
