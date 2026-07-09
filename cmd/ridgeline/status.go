package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/connectors"
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

// runStatus implements `ridgeline status --config PATH`.
//
// It prints a read-only summary of every connector declared in the
// config, keyed by product/connector name, alongside the last
// persisted state and the wall-clock time of the last update.
//
// The state database is opened read-only semantics only: if the file
// does not exist yet, status reports every connector as "never
// synced" without creating an empty database as a side effect.
//
// State entries that no longer map to a configured connector are
// listed under an "orphan state entries" footer so a user renaming
// or removing a connector can see the leftover rows without having
// to open the sqlite file.
func runStatus(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: ridgeline status --config PATH")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Prints per-connector cursor and last-sync time from the SQLite state")
		fmt.Fprintln(w, "database. Opens the database read-only; safe to run at any time.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Flags:")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if err := rejectExtraArgs(fs); err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("--config PATH is required")
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	if err := validateConnectorsForStatus(ctx, cfg); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "loaded %s\n", *cfgPath)

	entries, stateExists, err := loadStateEntries(ctx, cfg.StatePath)
	if err != nil {
		return err
	}
	if stateExists {
		fmt.Fprintf(stdout, "state: %s\n", cfg.StatePath)
	} else {
		fmt.Fprintf(stdout, "state: %s (not created yet)\n", cfg.StatePath)
	}

	byKey := make(map[string]sqlitestate.Entry, len(entries))
	for _, e := range entries {
		byKey[e.Key] = e
	}

	seen := make(map[string]bool, len(entries))
	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		for _, inst := range product.Connectors {
			key := config.StateKey(pid, inst.Name)
			seen[key] = true
			entry, ok := byKey[key]
			fmt.Fprintf(stdout, "%s (%s)\n", key, inst.Type)
			fmt.Fprintf(stdout, "  streams: %s\n", formatStreams(inst.Streams))
			if !ok {
				fmt.Fprintln(stdout, "  never synced")
				continue
			}
			fmt.Fprintf(stdout, "  last sync: %s\n", entry.UpdatedAt)
			fmt.Fprintf(stdout, "  cursor: %s\n", formatCursor(entry.State))
		}
	}

	var orphans []sqlitestate.Entry
	for _, e := range entries {
		if !seen[e.Key] {
			orphans = append(orphans, e)
		}
	}
	if len(orphans) > 0 {
		fmt.Fprintln(stdout, "orphan state entries (not in this config):")
		for _, e := range orphans {
			fmt.Fprintf(stdout, "  %s  last sync: %s\n", e.Key, e.UpdatedAt)
		}
	}
	return nil
}

// loadStateEntries opens the state DB at path and returns every row.
// If the file does not exist, it returns no entries and a false flag
// without creating the file. Any other error (permission, corrupt
// schema) is surfaced.
func loadStateEntries(ctx context.Context, path string) ([]sqlitestate.Entry, bool, error) {
	if path == "" {
		return nil, false, fmt.Errorf("state_path is empty")
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("stat %s: %w", path, err)
	}
	store, err := sqlitestate.Open(path)
	if err != nil {
		return nil, true, err
	}
	defer store.Close()
	entries, err := store.List(ctx)
	if err != nil {
		return nil, true, err
	}
	return entries, true, nil
}

func formatStreams(streams []string) string {
	return "[" + strings.Join(streams, " ") + "]"
}

// validateConnectorsForStatus checks connector, sink, and enricher type
// registrations via validateRegistrations, then calls each connector's
// Validate method with synthesized credential refs (credentials are not
// loaded for status - it is a read-only operation). A non-empty *_ref
// value is treated as satisfying the corresponding bare requirement so
// a properly-configured _ref config does not produce a false failure.
func validateConnectorsForStatus(ctx context.Context, cfg *config.File) error {
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
			// Synthesize bare keys from _ref entries so validators do
			// not false-fail on ref-based configs (credentials remain
			// unloaded; we only need to signal presence). Strip the
			// _ref keys afterward so unknown-key checks do not reject
			// them.
			for k, v := range connCfg {
				if !strings.HasSuffix(k, "_ref") || k == "_ref" {
					continue
				}
				ref, ok := v.(string)
				if !ok || strings.TrimSpace(ref) == "" {
					continue
				}
				bare := strings.TrimSuffix(k, "_ref")
				if _, exists := connCfg[bare]; !exists {
					connCfg[bare] = "(ref: " + ref + ")"
				}
				delete(connCfg, k)
			}
			if err := conn.Validate(ctx, connCfg); err != nil {
				return fmt.Errorf("product %s connector %s: %w", pid, inst.Name, err)
			}
		}
	}
	return nil
}

// formatCursor renders the saved cursor for a connector. Empty maps
// are shown as "{}" so users can tell an incremental connector that
// has run at least once but not advanced any cursor from one that has
// never been seen. Malformed payloads surface as "<malformed>".
func formatCursor(state connectors.State) string {
	if state == nil {
		return "<malformed>"
	}
	if len(state) == 0 {
		return "{}"
	}
	b, err := json.Marshal(state)
	if err != nil {
		return "<unserializable>"
	}
	return string(b)
}
