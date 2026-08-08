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
//	memory streams --config PATH   list all streams in the Business Memory catalog
//	memory metrics --config PATH   list all metrics in the Business Memory catalog
func runMemory(ctx context.Context, args []string, stdout *os.File) error {
	if len(args) == 0 {
		return usageErrorf("memory: subcommand required (streams, metrics)")
	}
	switch args[0] {
	case "streams":
		return runMemoryStreams(ctx, args[1:], stdout)
	case "metrics":
		return runMemoryMetrics(ctx, args[1:], stdout)
	case "help", "--help", "-h":
		fmt.Fprintln(stdout, "Usage: ridgeline memory streams --config PATH")
		fmt.Fprintln(stdout, "       ridgeline memory metrics --config PATH")
		fmt.Fprintln(stdout, "")
		fmt.Fprintln(stdout, "Query the Business Memory catalog.")
		return nil
	default:
		return usageErrorf("memory: unknown subcommand %q (streams, metrics)", args[0])
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
		return usageErrorf("memory streams: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	rows, err := cat.ListStreams(ctx)
	if err != nil {
		return fmt.Errorf("memory streams: %w", err)
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
		return usageErrorf("memory metrics: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	rows, err := cat.ListMetrics(ctx)
	if err != nil {
		return fmt.Errorf("memory metrics: %w", err)
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

// openCatalogFromConfig loads the config at cfgPath, opens the state store,
// and returns a ready Catalog. The caller must close the returned store.
func openCatalogFromConfig(cfgPath string) (*memory.Catalog, *sqlitestate.Store, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, nil, fmt.Errorf("memory: %w", err)
	}
	store, err := sqlitestate.Open(cfg.StatePath)
	if err != nil {
		return nil, nil, fmt.Errorf("memory: %w", err)
	}
	return memory.New(store.DB()), store, nil
}
