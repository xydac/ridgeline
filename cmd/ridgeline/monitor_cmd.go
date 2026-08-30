package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/xydac/ridgeline/memory"
)

// runMonitor dispatches ridgeline monitor <add|list|rm|run>.
func runMonitor(ctx context.Context, args []string, stdout *os.File) error {
	if len(args) == 0 {
		return usageErrorf("monitor: subcommand required: add, list, rm, run")
	}
	switch args[0] {
	case "add":
		return runMonitorAdd(ctx, args[1:], stdout)
	case "list", "ls":
		return runMonitorList(ctx, args[1:], stdout)
	case "rm", "remove":
		return runMonitorRm(ctx, args[1:], stdout)
	case "run":
		return runMonitorRun(ctx, args[1:], stdout)
	default:
		return usageErrorf("monitor: unknown subcommand %q; valid: add, list, rm, run", args[0])
	}
}

func runMonitorAdd(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("monitor add", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	metric := fs.String("metric", "", "fully-qualified metric name (e.g. plausible.daily.visitors)")
	condition := fs.String("condition", "", "condition expression: above N | below N | deviates-by Nsigma")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline monitor add <name> --config PATH --metric METRIC --condition EXPR")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Persists a watch rule to Business Memory. Rules are evaluated by 'ridgeline monitor run'.")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Condition expressions:")
		fmt.Fprintln(fs.Output(), "  above N          triggers when the metric's last value exceeds N")
		fmt.Fprintln(fs.Output(), "  below N          triggers when the metric's last value falls below N")
		fmt.Fprintln(fs.Output(), "  deviates-by Nsigma  triggers when |last_value - baseline_mean| >= N * stddev")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Example:")
		fmt.Fprintln(fs.Output(), "  ridgeline monitor add visitors-low --config ridgeline.yaml \\")
		fmt.Fprintln(fs.Output(), "      --metric plausible.daily.visitors --condition 'below 500'")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	name := fs.Arg(0)
	if name == "" {
		return usageErrorf("monitor add: positional argument <name> is required")
	}
	if *cfgPath == "" {
		return usageErrorf("monitor add: --config is required")
	}
	if *metric == "" {
		return usageErrorf("monitor add: --metric is required")
	}
	if *condition == "" {
		return usageErrorf("monitor add: --condition is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := cat.AddWatch(ctx, name, *metric, *condition); err != nil {
		return fmt.Errorf("monitor add: %w", err)
	}
	fmt.Fprintf(stdout, "Watch %q registered: %s %s\n", name, *metric, *condition)
	return nil
}

func runMonitorList(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("monitor list", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline monitor list --config PATH")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Lists all registered watch rules with last-triggered timestamp.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("monitor list: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	watches, err := cat.ListWatches(ctx)
	if err != nil {
		return fmt.Errorf("monitor list: %w", err)
	}

	if len(watches) == 0 {
		fmt.Fprintln(stdout, "No watch rules registered. Use 'ridgeline monitor add' to create one.")
		return nil
	}

	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tMETRIC\tCONDITION\tLAST TRIGGERED")
	for _, w := range watches {
		last := "never"
		if w.LastTriggeredAt != nil {
			last = w.LastTriggeredAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", w.Name, w.MetricFQ, w.Condition, last)
	}
	return tw.Flush()
}

func runMonitorRm(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("monitor rm", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline monitor rm <name> --config PATH")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Removes a watch rule by name.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	name := fs.Arg(0)
	if name == "" {
		return usageErrorf("monitor rm: positional argument <name> is required")
	}
	if *cfgPath == "" {
		return usageErrorf("monitor rm: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := cat.RemoveWatch(ctx, name); err != nil {
		return fmt.Errorf("monitor rm: %w", err)
	}
	fmt.Fprintf(stdout, "Watch %q removed.\n", name)
	return nil
}

func runMonitorRun(ctx context.Context, args []string, stdout *os.File) error {
	fs := flag.NewFlagSet("monitor run", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	asJSON := fs.Bool("json", false, "output triggered events as structured JSON")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline monitor run --config PATH [--json]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Evaluates all registered watch rules against current Business Memory.")
		fmt.Fprintln(fs.Output(), "Triggered rules append events to 'bm_events' (visible via 'ridgeline memory events').")
		fmt.Fprintln(fs.Output(), "Run this after every sync to catch threshold violations automatically.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("monitor run: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	result, err := cat.RunWatches(ctx)
	if err != nil {
		return fmt.Errorf("monitor run: %w", err)
	}

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(memory.ToMonitorRunJSON(result))
	}

	fmt.Fprint(stdout, memory.ComposeMonitorRunNarrative(result))
	return nil
}
