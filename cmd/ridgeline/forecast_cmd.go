package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/xydac/ridgeline/memory"
)

// runForecast handles `ridgeline forecast <metric> --config PATH [--horizon DUR] [--json]`.
//
// It fits a linear regression to the metric's observed value history and
// produces a directional projection for the requested horizon, with a
// confidence score derived from sample count and R^2 fit quality.
func runForecast(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("forecast", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	horizonStr := fs.String("horizon", "7d", "projection horizon (e.g. 7d, 14d, 30d)")
	asJSON := fs.Bool("json", false, "output as structured JSON for agent consumption")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline forecast <metric> --config PATH [--horizon DURATION] [--json]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Produces a directional projection for a metric using linear regression")
		fmt.Fprintln(fs.Output(), "over its observed value history (up to 90 days). Output includes a")
		fmt.Fprintln(fs.Output(), "directionality label (likely-improvement, stable, likely-decline),")
		fmt.Fprintln(fs.Output(), "a projected mean with uncertainty band, and a confidence score based")
		fmt.Fprintln(fs.Output(), "on sample count and regression R^2. Use --json for structured output.")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Example:")
		fmt.Fprintln(fs.Output(), "  ridgeline forecast plausible.daily.visitors --config ridgeline.yaml --horizon 7d")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("forecast: --config is required")
	}
	if fs.NArg() == 0 {
		return usageErrorf("forecast: metric name required (e.g. plausible.daily.visitors)")
	}
	fqName := fs.Arg(0)

	horizon, err := parseSinceDuration(*horizonStr)
	if err != nil {
		return usageErrorf("forecast: invalid --horizon %q: %v", *horizonStr, err)
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	data, err := cat.ForecastMetric(ctx, fqName, horizon)
	if err != nil {
		return fmt.Errorf("forecast: %w", err)
	}

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(memory.ToForecastJSON(data))
	}

	fmt.Fprint(stdout, memory.ComposeForecastNarrative(data))
	return nil
}
