package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/testsrc"
	"github.com/xydac/ridgeline/pipeline"
	"github.com/xydac/ridgeline/sinks"
	"github.com/xydac/ridgeline/sinks/jsonl"
	"github.com/xydac/ridgeline/sinks/parquet"
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

// runSync implements `ridgeline sync`.
//
//	--config PATH     drive the pipeline from a ridgeline.yaml file with
//	                  durable SQLite state. Each configured connector is
//	                  run once, in product id then connector name order.
//	--dry-run         run the built-in testsrc connector against a
//	                  JSON-lines sink with an in-memory state store.
//
// The flags are mutually exclusive.
func runSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	dryRun := fs.Bool("dry-run", false, "run the built-in testsrc connector against the jsonl sink")
	records := fs.Int("records", testsrc.DefaultRecords, "records per stream for dry-run")
	out := fs.String("out", "", "output directory (default: $TMPDIR/ridgeline-dryrun)")
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *cfgPath != "" && *dryRun {
		return fmt.Errorf("--config and --dry-run are mutually exclusive")
	}
	// Explicit --out "" almost always means the caller passed an unset
	// shell variable. Refuse it so the bug surfaces here instead of
	// silently writing to the default temp dir.
	outSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "out" {
			outSet = true
		}
	})
	if outSet && *out == "" {
		return fmt.Errorf("--out must not be empty")
	}
	if *cfgPath != "" {
		return runConfigSync(ctx, *cfgPath)
	}
	if *dryRun {
		return runDryRun(ctx, *out, *records)
	}
	return fmt.Errorf("specify --config PATH or --dry-run")
}

// runDryRun is the in-memory smoke test. It stays available so a new
// user can observe the pipeline without writing a config.
func runDryRun(ctx context.Context, out string, records int) error {
	dir := out
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "ridgeline-dryrun")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		// os.MkdirAll already reports the path; don't double-wrap.
		return fmt.Errorf("create out dir: %w", err)
	}

	conn, ok := connectors.Get(testsrc.Name)
	if !ok {
		return fmt.Errorf("connector %q not registered", testsrc.Name)
	}
	sink := jsonl.New()
	if err := sink.Init(ctx, sinks.SinkConfig{"dir": dir}); err != nil {
		return fmt.Errorf("sink init: %w", err)
	}
	defer sink.Close()

	req := pipeline.Request{
		Key:    "dryrun-" + testsrc.Name,
		Config: connectors.ConnectorConfig{"records": records},
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

// runConfigSync loads cfgPath, opens the durable state store, and
// runs each configured connector through its configured sink. Each
// connector's state is keyed as "<product>/<connector>".
//
// Before any connector runs, every configured connector's Validate
// method is called in product + name order. If any Validate returns
// an error the whole sync is aborted with a non-zero exit: it is
// better to refuse a broken config up front than to have earlier
// connectors write partial data before a later one trips over its
// config at extract time.
func runConfigSync(ctx context.Context, cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	if err := validateConnectors(ctx, cfg); err != nil {
		return err
	}
	store, err := sqlitestate.Open(cfg.StatePath)
	if err != nil {
		return err
	}
	defer store.Close()

	fmt.Printf("loaded %s\n", cfgPath)
	fmt.Printf("state: %s\n", cfg.StatePath)

	var totalRecords int
	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		instances := append([]config.ConnectorInstance(nil), product.Connectors...)
		sort.Slice(instances, func(i, j int) bool { return instances[i].Name < instances[j].Name })
		for _, inst := range instances {
			n, err := runConnectorInstance(ctx, store, pid, inst)
			if err != nil {
				return fmt.Errorf("product %s connector %s: %w", pid, inst.Name, err)
			}
			totalRecords += n
		}
	}
	fmt.Printf("done: %d records total\n", totalRecords)
	return nil
}

// validateConnectors asks every registered connector in cfg to check
// its own config map before any sink is opened or record is written.
// Connector types that are not registered are also reported here, so
// the user does not have to wait for extract time to learn about the
// typo.
func validateConnectors(ctx context.Context, cfg *config.File) error {
	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		instances := append([]config.ConnectorInstance(nil), product.Connectors...)
		sort.Slice(instances, func(i, j int) bool { return instances[i].Name < instances[j].Name })
		for _, inst := range instances {
			conn, ok := connectors.Get(inst.Type)
			if !ok {
				return fmt.Errorf("product %s connector %s: type %q is not registered", pid, inst.Name, inst.Type)
			}
			connCfg := connectors.ConnectorConfig{}
			for k, v := range inst.Config {
				connCfg[k] = v
			}
			if err := conn.Validate(ctx, connCfg); err != nil {
				return fmt.Errorf("product %s connector %s: %w", pid, inst.Name, err)
			}
		}
	}
	return nil
}

// runConnectorInstance runs one connector from the config against its
// configured sink. Returns the number of records written.
func runConnectorInstance(ctx context.Context, store pipeline.StateStore, pid string, inst config.ConnectorInstance) (int, error) {
	conn, ok := connectors.Get(inst.Type)
	if !ok {
		return 0, fmt.Errorf("connector type %q is not registered", inst.Type)
	}
	sink, err := newSink(inst.Sink.Type)
	if err != nil {
		return 0, err
	}
	sinkCfg := sinks.SinkConfig{}
	for k, v := range inst.Sink.Options {
		sinkCfg[k] = v
	}
	if err := sink.Init(ctx, sinkCfg); err != nil {
		return 0, fmt.Errorf("sink init: %w", err)
	}
	defer sink.Close()

	streams := make([]connectors.Stream, 0, len(inst.Streams))
	for _, name := range inst.Streams {
		streams = append(streams, connectors.Stream{Name: name, Mode: connectors.Incremental})
	}
	connCfg := connectors.ConnectorConfig{}
	for k, v := range inst.Config {
		connCfg[k] = v
	}
	req := pipeline.Request{
		Key:     config.StateKey(pid, inst.Name),
		Config:  connCfg,
		Streams: streams,
	}
	res, err := pipeline.Run(ctx, conn, sink, store, req)
	if err != nil {
		return 0, err
	}
	fmt.Printf("%s/%s: %d records, %d states saved\n", pid, inst.Name, res.Records, res.States)
	return res.Records, nil
}

// newSink resolves a sink type name to a fresh Sink instance. The
// init-time registry in package sinks holds one singleton per type,
// which the lifecycle cannot reuse across multiple Init calls, so we
// construct fresh instances here.
func newSink(typ string) (sinks.Sink, error) {
	switch typ {
	case jsonl.Name:
		return jsonl.New(), nil
	case parquet.Name:
		return parquet.New(), nil
	}
	return nil, fmt.Errorf("sink type %q is not supported (known: %v)", typ, knownSinkTypes())
}

func knownSinkTypes() []string { return []string{jsonl.Name, parquet.Name} }
