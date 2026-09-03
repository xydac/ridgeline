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
//	memory streams   --config PATH                      list all streams
//	memory metrics   --config PATH                      list all metrics
//	memory baselines --config PATH <metric>             rolling-window baselines + sparkline
//	memory recompute --config PATH                      recompute all baselines
//	memory events    --config PATH                      list events (anomalies, deploys, commits)
//	memory note      --config PATH --kind K --description D  insert a manual event
//	memory patterns  --config PATH [--detect]           list detected recurring patterns
func runMemory(ctx context.Context, args []string, stdout *os.File) error {
	if len(args) == 0 {
		return usageErrorf("subcommand required (streams, metrics, baselines, recompute, events, note, patterns)")
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
	case "note":
		return runMemoryNote(ctx, args[1:], stdout)
	case "patterns":
		return runMemoryPatterns(ctx, args[1:], stdout)
	case "help", "--help", "-h":
		fmt.Fprintln(stdout, "Usage: ridgeline memory streams   --config PATH")
		fmt.Fprintln(stdout, "       ridgeline memory metrics   --config PATH")
		fmt.Fprintln(stdout, "       ridgeline memory baselines --config PATH <metric>")
		fmt.Fprintln(stdout, "       ridgeline memory recompute --config PATH [--since DURATION]")
		fmt.Fprintln(stdout, "       ridgeline memory events    --config PATH [--since DURATION]")
		fmt.Fprintln(stdout, "       ridgeline memory note      --config PATH --kind KIND --description TEXT [--at TIME]")
		fmt.Fprintln(stdout, "       ridgeline memory patterns  --config PATH [--detect]")
		fmt.Fprintln(stdout, "")
		fmt.Fprintln(stdout, "Query and update the Business Memory catalog.")
		return nil
	default:
		return usageErrorf("unknown subcommand %q (streams, metrics, baselines, recompute, events, note, patterns)", args[0])
	}
}

func runMemoryPatterns(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("memory patterns", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	detect := fs.Bool("detect", false, "re-run pattern detection before listing")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline memory patterns --config PATH [--detect]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "List recurring patterns detected across all tracked metrics.")
		fmt.Fprintln(fs.Output(), "Use --detect to re-run detection before listing.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("patterns: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	if *detect {
		if err := cat.RedetectAllPatterns(ctx); err != nil {
			return fmt.Errorf("detect patterns: %w", err)
		}
	}

	patterns, err := cat.ListPatterns(ctx)
	if err != nil {
		return fmt.Errorf("list patterns: %w", err)
	}
	if len(patterns) == 0 {
		fmt.Fprintln(stdout, "No patterns detected. Run with --detect to analyze your data, or sync more data first.")
		return nil
	}

	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "METRIC\tPATTERN\tCONFIDENCE\tSAMPLES\tEVIDENCE")
	for _, p := range patterns {
		evidence := p.EvidenceStart.Format("2006-01-02") + " to " + p.EvidenceEnd.Format("2006-01-02")
		fmt.Fprintf(tw, "%s\t%s\t%.0f%%\t%d\t%s\n",
			p.FQNAME, p.Pattern, p.Confidence*100, p.SampleCount, evidence)
	}
	return tw.Flush()
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
		declared, err := cat.MetricDeclared(ctx, fqName)
		if err != nil {
			return fmt.Errorf("baselines: %w", err)
		}
		if !declared {
			return fmt.Errorf("unknown metric %q: not in Business Memory catalog (run 'ridgeline memory metrics' to list known metrics)", fqName)
		}
		fmt.Fprintf(stdout, "No baselines yet for %s. Run 'ridgeline sync' to populate observations.\n", fqName)
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
		fmt.Fprintln(fs.Output(), "Lists all events in the Business Memory timeline (anomalies, deploys,")
		fmt.Fprintln(fs.Output(), "commits, and manual notes), newest first.")
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
			fmt.Fprintf(stdout, "No events in the last %s.\n", *sinceStr)
		} else {
			fmt.Fprintln(stdout, "No events recorded yet.")
		}
		return nil
	}

	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TIME\tKIND\tDETAIL")
	fmt.Fprintln(w, strings.Repeat("-", 19)+"\t"+strings.Repeat("-", 10)+"\t"+strings.Repeat("-", 40))
	for _, e := range events {
		detail := e.Description
		if e.Kind == "anomaly" {
			sign := "+"
			if e.StddevFromMean < 0 {
				sign = ""
			}
			detail = fmt.Sprintf("%s: %.4g (%s%.2fσ, %dd baseline) -- %s",
				e.MetricFQ, e.ObservedValue, sign, e.StddevFromMean, e.WindowDays, e.Direction)
		} else if detail == "" {
			detail = e.MetricFQ
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", e.At.Format(time.RFC3339), e.Kind, detail)
	}
	return w.Flush()
}

func runMemoryNote(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("memory note", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	kind := fs.String("kind", "", "event kind (e.g. deploy, release, incident, rollback)")
	desc := fs.String("description", "", "human-readable description of the event")
	atStr := fs.String("at", "", "event time in RFC3339 or YYYY-MM-DD (default: now)")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline memory note --config PATH --kind KIND --description TEXT [--at TIME]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Records a manual event in the Business Memory timeline.")
		fmt.Fprintln(fs.Output(), "Examples:")
		fmt.Fprintln(fs.Output(), `  ridgeline memory note --config ridgeline.yaml --kind deploy --description "shipped v1.4"`)
		fmt.Fprintln(fs.Output(), `  ridgeline memory note --config ridgeline.yaml --kind incident --description "db outage" --at 2026-08-01`)
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("note: --config is required")
	}
	if *kind == "" {
		return usageErrorf("note: --kind is required")
	}
	if *desc == "" {
		return usageErrorf("note: --description is required")
	}

	at := time.Now().UTC()
	if *atStr != "" {
		// try RFC3339 first, then YYYY-MM-DD
		if t, err2 := time.Parse(time.RFC3339, *atStr); err2 == nil {
			at = t
		} else if t, err2 := time.Parse("2006-01-02", *atStr); err2 == nil {
			at = t.UTC()
		} else {
			return usageErrorf("note: --at %q must be RFC3339 or YYYY-MM-DD", *atStr)
		}
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := cat.InsertManualEvent(ctx, *kind, *desc, at); err != nil {
		return fmt.Errorf("note: %w", err)
	}
	fmt.Fprintf(stdout, "Recorded %s event at %s: %s\n", *kind, at.Format("2006-01-02T15:04:05Z"), *desc)
	return nil
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
