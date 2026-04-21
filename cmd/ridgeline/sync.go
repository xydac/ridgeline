package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/testsrc"
	"github.com/xydac/ridgeline/pipeline"
	"github.com/xydac/ridgeline/sinks"
	"github.com/xydac/ridgeline/sinks/jsonl"
)

// runSync implements `ridgeline sync`. Today only --dry-run is wired,
// which runs the built-in testsrc connector through the jsonl sink.
// Real config-driven sync ships once the config parser lands.
func runSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	dryRun := fs.Bool("dry-run", false, "run the built-in testsrc connector against the jsonl sink")
	records := fs.Int("records", testsrc.DefaultRecords, "records per stream for dry-run")
	out := fs.String("out", "", "output directory (default: $TMPDIR/ridgeline-dryrun)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*dryRun {
		return fmt.Errorf("only --dry-run is implemented so far; try: ridgeline sync --dry-run")
	}
	dir := *out
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "ridgeline-dryrun")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	conn, ok := connectors.Get(testsrc.Name)
	if !ok {
		return fmt.Errorf("connector %q not registered", testsrc.Name)
	}
	sink, ok := sinks.Get(jsonl.Name)
	if !ok {
		return fmt.Errorf("sink %q not registered", jsonl.Name)
	}
	if err := sink.Init(ctx, sinks.SinkConfig{"dir": dir}); err != nil {
		return fmt.Errorf("sink init: %w", err)
	}
	defer sink.Close()

	req := pipeline.Request{
		Key:    "dryrun-" + testsrc.Name,
		Config: connectors.ConnectorConfig{"records": *records},
		Streams: []connectors.Stream{
			{Name: "pages", Mode: connectors.FullRefresh},
			{Name: "events", Mode: connectors.FullRefresh},
		},
	}
	res, err := pipeline.Run(ctx, conn, sink, pipeline.NewMemoryStateStore(), req)
	if err != nil {
		return err
	}
	fmt.Printf("wrote %d records across %d streams into %s\n", res.Records, len(res.PerStream), dir)
	for stream, sr := range res.PerStream {
		fmt.Printf("  %s: %d records\n", stream, sr.Records)
	}
	fmt.Printf("manifest: %s\n", filepath.Join(dir, "manifest.json"))
	return nil
}
