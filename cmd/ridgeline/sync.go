package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/testsrc"
	"github.com/xydac/ridgeline/creds"
	"github.com/xydac/ridgeline/pipeline"
	"github.com/xydac/ridgeline/sinks"
	"github.com/xydac/ridgeline/sinks/jsonl"
	_ "github.com/xydac/ridgeline/sinks/parquet" // register parquet sink factory
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
	help, err := parseSubcommandFlags(fs, args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if err := rejectExtraArgs(fs); err != nil {
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
// Connectors inside a product run in the order the YAML file lists
// them, so a reader of ridgeline.yaml can predict execution order
// without cross-referencing names. Products themselves come from a
// YAML map and are iterated by sorted product id so runs are
// deterministic across Go releases; if top-level ordering ever needs
// to match declaration order, the schema can switch to a list form.
//
// Before any connector runs, every configured connector's Validate
// method is called. If any Validate returns an error the whole sync
// is aborted with a non-zero exit: it is better to refuse a broken
// config up front than to have earlier connectors write partial data
// before a later one trips over its config at extract time.
func runConfigSync(ctx context.Context, cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	store, err := sqlitestate.Open(cfg.StatePath)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := resolveConfigRefs(ctx, cfg, store, os.Stderr); err != nil {
		return err
	}
	if err := validateConnectors(ctx, cfg); err != nil {
		return err
	}

	fmt.Printf("loaded %s\n", cfgPath)
	fmt.Printf("state: %s\n", cfg.StatePath)

	var totalRecords int
	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		for _, inst := range product.Connectors {
			n, err := runConnectorInstance(ctx, store, pid, inst, os.Stdout)
			if err != nil {
				return fmt.Errorf("product %s connector %s: %w", pid, inst.Name, err)
			}
			totalRecords += n
		}
	}
	fmt.Printf("done: %d records total\n", totalRecords)
	return nil
}

// resolveConfigRefs rewrites every `*_ref` connector-config entry into
// the plaintext credential it names. Connectors see only the resolved
// key (for example `api_key`), never the `_ref` pointer, so they do
// not need to know about the credential store.
//
// The credential store is opened lazily: if no connector uses a ref,
// the key file is never touched. A missing credential fails the whole
// sync before any sink is opened or records are written. Collisions
// where both `api_key` and `api_key_ref` are present are logged to
// stderr and the ref wins.
func resolveConfigRefs(ctx context.Context, cfg *config.File, store *sqlitestate.Store, stderr io.Writer) error {
	if !needsCreds(cfg) {
		return nil
	}
	cs, err := openCredStore(cfg, store)
	if err != nil {
		return err
	}
	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		for i := range product.Connectors {
			inst := &product.Connectors[i]
			warns, err := creds.ResolveRefs(ctx, cs, inst.Config)
			if err != nil {
				return fmt.Errorf("product %s connector %s: %w", pid, inst.Name, err)
			}
			for _, w := range warns {
				fmt.Fprintf(stderr, "warn: product %s connector %s: %s\n", pid, inst.Name, w)
			}
		}
	}
	return nil
}

// needsCreds reports whether any connector config in cfg contains a
// key ending in creds.RefSuffix, so we can skip opening the key file
// entirely for sync runs that have no secrets.
func needsCreds(cfg *config.File) bool {
	for _, p := range cfg.Products {
		for _, inst := range p.Connectors {
			for k := range inst.Config {
				if k != creds.RefSuffix && strings.HasSuffix(k, creds.RefSuffix) {
					return true
				}
			}
		}
	}
	return false
}

// openCredStore loads the AES key at cfg.KeyPath and constructs a
// credential store sharing the same SQLite handle as state. Errors
// name the key path so a user missing the file knows where to put it.
func openCredStore(cfg *config.File, store *sqlitestate.Store) (*creds.Store, error) {
	if cfg.KeyPath == "" {
		return nil, fmt.Errorf("key_path must be set in the config to resolve *_ref credentials")
	}
	key, err := creds.KeyFromFile(cfg.KeyPath)
	if err != nil {
		return nil, err
	}
	cs, err := creds.New(store.DB(), key)
	if err != nil {
		return nil, err
	}
	return cs, nil
}

// validateConnectors asks every registered connector in cfg to check
// its own config map before any sink is opened or record is written.
// Connector types that are not registered are also reported here, so
// the user does not have to wait for extract time to learn about the
// typo. Validation walks products in sorted id order and connectors
// in their declared YAML order so error messages are stable.
func validateConnectors(ctx context.Context, cfg *config.File) error {
	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		for _, inst := range product.Connectors {
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
// configured sink. Returns the number of records written. The per-
// connector progress line is written to stdout so callers that want
// silent runs (TUI, tests) can pass io.Discard.
func runConnectorInstance(ctx context.Context, store pipeline.StateStore, pid string, inst config.ConnectorInstance, stdout io.Writer) (int, error) {
	conn, ok := connectors.Get(inst.Type)
	if !ok {
		return 0, fmt.Errorf("connector type %q is not registered", inst.Type)
	}
	sink, err := sinks.New(inst.Sink.Type)
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
	fmt.Fprintf(stdout, "%s/%s: %d records, %d states saved\n", pid, inst.Name, res.Records, res.States)
	return res.Records, nil
}
