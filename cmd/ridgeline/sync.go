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
	"github.com/xydac/ridgeline/enrichers"
	_ "github.com/xydac/ridgeline/enrichers/tsnormalize" // register ts_normalize enricher
	_ "github.com/xydac/ridgeline/enrichers/urlhost"     // register url_host enricher
	"github.com/xydac/ridgeline/pipeline"
	"github.com/xydac/ridgeline/sinks"
	"github.com/xydac/ridgeline/sinks/jsonl"
	_ "github.com/xydac/ridgeline/sinks/parquet" // register parquet sink factory
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

// syncFailure records one connector's runtime error during a
// --continue-on-error run.
type syncFailure struct {
	product   string
	connector string
	err       error
}

// PartialSyncError is returned by runSync when --continue-on-error is
// set and at least one connector failed while others were attempted.
// Call IsTotal to distinguish a run where every connector failed from
// one where only some did.
type PartialSyncError struct {
	failures  []syncFailure
	succeeded int
}

func (e *PartialSyncError) Error() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d of %d connector(s) failed:", len(e.failures), len(e.failures)+e.succeeded)
	for _, f := range e.failures {
		fmt.Fprintf(&sb, "\n  %s/%s: %s", f.product, f.connector, f.err)
	}
	return sb.String()
}

// IsTotal reports whether every connector failed (none succeeded).
func (e *PartialSyncError) IsTotal() bool { return e.succeeded == 0 }

// runSync implements `ridgeline sync`.
//
//	--config PATH           drive the pipeline from a ridgeline.yaml file
//	                        with durable SQLite state. Each configured
//	                        connector is run once, in product id then
//	                        connector name order.
//	--continue-on-error     when used with --config, continue running
//	                        remaining connectors after one fails. Exit
//	                        code 2 signals partial failure; exit code 1
//	                        signals total failure (all connectors failed).
//	--dry-run               run the built-in testsrc connector against a
//	                        JSON-lines sink with an in-memory state store.
//
// --config and --dry-run are mutually exclusive.
func runSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	dryRun := fs.Bool("dry-run", false, "run the built-in testsrc connector against the jsonl sink")
	records := fs.Int("records", testsrc.DefaultRecords, "records per stream for dry-run")
	out := fs.String("out", "", "output directory (default: $TMPDIR/ridgeline-dryrun)")
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	continueOnError := fs.Bool("continue-on-error", false, "continue after a connector failure; exit 2 on partial, 1 on total")
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: ridgeline sync --config PATH [--continue-on-error]")
		fmt.Fprintln(w, "       ridgeline sync --dry-run [--records N] [--out DIR]")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Runs the sync pipeline. With --config, drives all configured connectors")
		fmt.Fprintln(w, "against their configured sinks using durable SQLite state. With --dry-run,")
		fmt.Fprintln(w, "runs the built-in test source and prints results without writing state.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Flags:")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, os.Stdout, args)
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
		return runConfigSync(ctx, *cfgPath, *continueOnError)
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
		sink.Close()
		return err
	}
	// Close before printing the manifest path: Touch in Close writes
	// manifest.json so the path in the output is always valid.
	if err := sink.Close(); err != nil {
		return fmt.Errorf("sink close: %w", err)
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
//
// When continueOnError is true, a runtime failure from one connector
// is logged and the remaining connectors are still attempted. The
// caller receives a *PartialSyncError whose IsTotal method
// distinguishes a run where all connectors failed from a partial one.
func runConfigSync(ctx context.Context, cfgPath string, continueOnError bool) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	store, err := sqlitestate.Open(cfg.StatePath)
	if err != nil {
		return err
	}
	defer store.Close()

	cs, err := resolveConfigRefs(ctx, cfg, store, os.Stderr)
	if err != nil {
		return err
	}
	if err := validateConnectors(ctx, cfg); err != nil {
		return err
	}

	fmt.Printf("loaded %s\n", cfgPath)
	fmt.Printf("state: %s\n", cfg.StatePath)

	var totalRecords int
	var failures []syncFailure
	var succeeded int
	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		for _, inst := range product.Connectors {
			n, err := runConnectorInstance(ctx, store, pid, inst, os.Stdout, cs)
			if err != nil {
				if continueOnError {
					fmt.Fprintf(os.Stderr, "sync error (continuing): product %s connector %s: %v\n", pid, inst.Name, err)
					failures = append(failures, syncFailure{product: pid, connector: inst.Name, err: err})
					continue
				}
				return fmt.Errorf("product %s connector %s: %w", pid, inst.Name, err)
			}
			totalRecords += n
			succeeded++
		}
	}
	if len(failures) > 0 {
		fmt.Printf("done: %d records total (%d connector(s) failed)\n", totalRecords, len(failures))
		return &PartialSyncError{failures: failures, succeeded: succeeded}
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
//
// The returned *creds.Store is non-nil only when a store was opened;
// callers may pass it to connectors that implement TokenStorer.
func resolveConfigRefs(ctx context.Context, cfg *config.File, store *sqlitestate.Store, stderr io.Writer) (*creds.Store, error) {
	if !needsCreds(cfg) {
		return nil, nil
	}
	cs, err := openCredStore(cfg, store)
	if err != nil {
		return nil, err
	}
	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		for i := range product.Connectors {
			inst := &product.Connectors[i]
			warns, err := creds.ResolveRefs(ctx, cs, inst.Config)
			if err != nil {
				return nil, fmt.Errorf("product %s connector %s: %w", pid, inst.Name, err)
			}
			for _, w := range warns {
				fmt.Fprintf(stderr, "warn: product %s connector %s: %s\n", pid, inst.Name, w)
			}
		}
	}
	return cs, nil
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

// validateConnectors checks every connector, sink, and enricher type
// against their registries, then calls each connector's Validate method
// to verify its config map. Registry checks happen first via
// validateRegistrations so unknown types produce enumerated errors before
// any connector is asked to validate its specific options.
func validateConnectors(ctx context.Context, cfg *config.File) error {
	if err := validateRegistrations(cfg); err != nil {
		return err
	}
	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		for _, inst := range product.Connectors {
			conn, _ := connectors.Get(inst.Type) // safe: validateRegistrations confirmed presence
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

// buildEnricherSteps converts a connector instance's enricher refs into
// pipeline.EnricherStep values, resolving each type from the registry.
func buildEnricherSteps(inst config.ConnectorInstance) []pipeline.EnricherStep {
	if len(inst.Enrichers) == 0 {
		return nil
	}
	steps := make([]pipeline.EnricherStep, 0, len(inst.Enrichers))
	for _, er := range inst.Enrichers {
		e, ok := enrichers.Get(er.Type)
		if !ok {
			continue // already validated; should not happen
		}
		cfg := enrichers.EnrichConfig{}
		for k, v := range er.Config {
			cfg[k] = v
		}
		steps = append(steps, pipeline.EnricherStep{E: e, Cfg: cfg})
	}
	return steps
}

// runConnectorInstance runs one connector from the config against its
// configured sink. Returns the number of records written. The per-
// connector progress line is written to stdout so callers that want
// silent runs (TUI, tests) can pass io.Discard.
func runConnectorInstance(ctx context.Context, store pipeline.StateStore, pid string, inst config.ConnectorInstance, stdout io.Writer, cs *creds.Store) (int, error) {
	conn, ok := connectors.Get(inst.Type)
	if !ok {
		return 0, fmt.Errorf("connector type %q is not registered", inst.Type)
	}
	if cs != nil {
		if ts, ok := conn.(connectors.TokenStorer); ok {
			ts.SetTokenStore(cs)
		}
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
		Key:       config.StateKey(pid, inst.Name),
		Config:    connCfg,
		Streams:   streams,
		Enrichers: buildEnricherSteps(inst),
	}
	res, err := pipeline.Run(ctx, conn, sink, store, req)
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(stdout, "%s/%s: %d records, %d states saved\n", pid, inst.Name, res.Records, res.States)
	return res.Records, nil
}
