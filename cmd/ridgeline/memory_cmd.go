package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/memory"
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

// runMemory dispatches `ridgeline memory <subcommand>`.
//
//	memory streams   --config PATH           list all streams in the Business Memory catalog
//	memory metrics   --config PATH           list all metrics in the Business Memory catalog
//	memory baselines --config PATH <metric>  print rolling-window baselines + sparkline
//	memory recompute --config PATH           recompute all baselines from recorded history
//	memory events    --config PATH           list anomaly events detected during sync
func runMemory(ctx context.Context, args []string, stdout *os.File) error {
	if len(args) == 0 {
		return usageErrorf("subcommand required (streams, metrics, baselines, recompute, events)")
	}
	switch args[0] {
	case "streams":
		return runMemoryStreams(ctx, args[1:], stdout)
	case "metrics":
		return runMemoryMetrics(ctx, args[1:], stdout)
	case "baselines":
		return runMemoryBaselines(ctx, args[1:], stdout)
	case "recompute":
		return runMemoryRecompute(ctx, args[1:], stdout)
	case "events":
		return runMemoryEvents(ctx, args[1:], stdout)
	case "help", "--help", "-h":
		fmt.Fprintln(stdout, "Usage: ridgeline memory streams   --config PATH")
		fmt.Fprintln(stdout, "       ridgeline memory metrics   --config PATH")
		fmt.Fprintln(stdout, "       ridgeline memory baselines --config PATH <metric>")
		fmt.Fprintln(stdout, "       ridgeline memory recompute --config PATH [--since DURATION]")
		fmt.Fprintln(stdout, "       ridgeline memory events    --config PATH [--since DURATION]")
		fmt.Fprintln(stdout, "")
		fmt.Fprintln(stdout, "Query the Business Memory catalog.")
		return nil
	default:
		return usageErrorf("unknown subcommand %q (streams, metrics, baselines, recompute, events)", args[0])
	}
}

func runMemoryStreams(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("memory streams", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline memory streams --config PATH")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Lists all data streams recorded in the Business Memory catalog.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("streams: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	rows, err := cat.ListStreams(ctx)
	if err != nil {
		return fmt.Errorf("streams: %w", err)
	}
	if len(rows) == 0 {
		fmt.Fprintln(stdout, "No streams in Business Memory catalog. Run 'ridgeline sync' first.")
		return nil
	}

	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CONNECTOR\tSTREAM\tKIND\tFIRST SEEN\tLAST SEEN\tROWS (LIFETIME)")
	fmt.Fprintln(w, strings.Repeat("-", 10)+"\t"+strings.Repeat("-", 10)+"\t"+strings.Repeat("-", 12)+"\t"+strings.Repeat("-", 19)+"\t"+strings.Repeat("-", 19)+"\t"+strings.Repeat("-", 14))
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\n",
			r.Connector,
			r.Stream,
			r.Kind,
			r.FirstSeenAt.Format(time.RFC3339),
			r.LastSeenAt.Format(time.RFC3339),
			r.RowCountLifetime,
		)
	}
	return w.Flush()
}

func runMemoryMetrics(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("memory metrics", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline memory metrics --config PATH")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Lists all metrics recorded in the Business Memory catalog.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("metrics: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	rows, err := cat.ListMetrics(ctx)
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	if len(rows) == 0 {
		fmt.Fprintln(stdout, "No metrics in Business Memory catalog. Run 'ridgeline sync' against a connector with declared metric columns.")
		return nil
	}

	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "METRIC\tUNIT\tDIRECTION\tAGGREGATION\tLAST VALUE\tLAST SEEN")
	fmt.Fprintln(w, strings.Repeat("-", 10)+"\t"+strings.Repeat("-", 8)+"\t"+strings.Repeat("-", 16)+"\t"+strings.Repeat("-", 11)+"\t"+strings.Repeat("-", 10)+"\t"+strings.Repeat("-", 19))
	for _, r := range rows {
		lastVal := "-"
		if r.LastValue != nil {
			lastVal = fmt.Sprintf("%.4g", *r.LastValue)
		}
		lastAt := "-"
		if r.LastValueAt != nil {
			lastAt = r.LastValueAt.Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			r.FQName,
			r.Unit,
			r.Direction,
			r.Aggregation,
			lastVal,
			lastAt,
		)
	}
	return w.Flush()
}

func runMemoryBaselines(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("memory baselines", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline memory baselines --config PATH <metric>")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Prints rolling-window statistics and a 30-day sparkline for a metric.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("baselines: --config is required")
	}
	if fs.NArg() == 0 {
		return usageErrorf("baselines: metric name required (e.g. plausible.daily.visitors)")
	}
	fqName := fs.Arg(0)

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	rows, err := cat.ListBaselines(ctx, fqName)
	if err != nil {
		return fmt.Errorf("baselines: %w", err)
	}

	sparkline, err := cat.Sparkline(ctx, fqName, 30, 40)
	if err != nil {
		return fmt.Errorf("baselines: sparkline: %w", err)
	}

	if len(rows) == 0 {
		fmt.Fprintf(stdout, "No baselines for %s. Run 'ridgeline sync' against a connector with declared metric columns.\n", fqName)
		return nil
	}

	fmt.Fprintf(stdout, "Metric: %s\n", fqName)
	if sparkline != "" {
		fmt.Fprintf(stdout, "30d sparkline: %s\n", sparkline)
	}
	fmt.Fprintln(stdout, "")

	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "WINDOW\tSAMPLES\tMEAN\tSTDDEV\tMIN\tMAX\tCOMPUTED")
	fmt.Fprintln(w, strings.Repeat("-", 8)+"\t"+strings.Repeat("-", 7)+"\t"+strings.Repeat("-", 10)+"\t"+strings.Repeat("-", 8)+"\t"+strings.Repeat("-", 8)+"\t"+strings.Repeat("-", 8)+"\t"+strings.Repeat("-", 19))
	for _, r := range rows {
		fmt.Fprintf(w, "%dd\t%d\t%.4g\t%.4g\t%.4g\t%.4g\t%s\n",
			r.WindowDays,
			r.SampleCount,
			r.Mean,
			r.Stddev,
			r.Min,
			r.Max,
			r.LastComputedAt.Format(time.RFC3339),
		)
	}
	return w.Flush()
}

func runMemoryRecompute(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("memory recompute", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	sinceStr := fs.String("since", "", "only recompute metrics with observations in this window (e.g. 7d, 720h); omit for all")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline memory recompute --config PATH [--since DURATION]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Recomputes rolling-window baselines for all metrics from recorded history.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("recompute: --config is required")
	}

	var since time.Duration
	if *sinceStr != "" {
		since, err = time.ParseDuration(*sinceStr)
		if err != nil {
			return usageErrorf("recompute: invalid --since %q: %v", *sinceStr, err)
		}
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := cat.Recompute(ctx, since, memory.DefaultWindows); err != nil {
		return fmt.Errorf("recompute: %w", err)
	}
	fmt.Fprintln(stdout, "Baselines recomputed.")
	return nil
}

func runMemoryEvents(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("memory events", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	sinceStr := fs.String("since", "7d", "return events newer than this window (e.g. 24h, 7d, 30d); use 0 for all")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline memory events --config PATH [--since DURATION]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Lists anomaly events detected during sync, newest first.")
		fmt.Fprintln(fs.Output(), "Direction labels: surprise-good (metric moved in the desired direction),")
		fmt.Fprintln(fs.Output(), "                  surprise-bad  (metric moved against the desired direction),")
		fmt.Fprintln(fs.Output(), "                  surprise-neutral (metric has neutral directionality).")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("events: --config is required")
	}

	var since time.Duration
	if *sinceStr != "" && *sinceStr != "0" {
		since, err = time.ParseDuration(*sinceStr)
		if err != nil {
			// try "<N>d" shorthand: e.g. "7d"
			var days int
			if n, _ := fmt.Sscanf(*sinceStr, "%dd", &days); n == 1 {
				since = time.Duration(days) * 24 * time.Hour
			} else {
				return usageErrorf("events: invalid --since %q: %v", *sinceStr, err)
			}
		}
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	events, err := cat.ListEvents(ctx, since)
	if err != nil {
		return fmt.Errorf("events: %w", err)
	}
	if len(events) == 0 {
		if since > 0 {
			fmt.Fprintf(stdout, "No anomaly events in the last %s. Run 'ridgeline sync' to detect anomalies.\n", *sinceStr)
		} else {
			fmt.Fprintln(stdout, "No anomaly events recorded yet. Run 'ridgeline sync' to detect anomalies.")
		}
		return nil
	}

	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TIME\tMETRIC\tWINDOW\tOBSERVED\tMEAN\tDEVIATION\tDIRECTION")
	fmt.Fprintln(w, strings.Repeat("-", 19)+"\t"+strings.Repeat("-", 10)+"\t"+strings.Repeat("-", 6)+"\t"+strings.Repeat("-", 10)+"\t"+strings.Repeat("-", 10)+"\t"+strings.Repeat("-", 9)+"\t"+strings.Repeat("-", 16))
	for _, e := range events {
		sign := "+"
		if e.StddevFromMean < 0 {
			sign = ""
		}
		fmt.Fprintf(w, "%s\t%s\t%dd\t%.4g\t%.4g\t%s%.2fσ\t%s\n",
			e.At.Format(time.RFC3339),
			e.MetricFQ,
			e.WindowDays,
			e.ObservedValue,
			e.BaselineMean,
			sign,
			e.StddevFromMean,
			e.Direction,
		)
	}
	return w.Flush()
}

// openCatalogFromConfig loads the config at cfgPath, opens the state store,
// and returns a ready Catalog. The caller must close the returned store.
func openCatalogFromConfig(cfgPath string) (*memory.Catalog, *sqlitestate.Store, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, nil, err
	}
	store, err := sqlitestate.Open(cfg.StatePath)
	if err != nil {
		return nil, nil, err
	}
	return memory.New(store.DB()), store, nil
}
