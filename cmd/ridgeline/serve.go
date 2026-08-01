package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// permanentConfigError wraps a config-load or schema-validation failure that
// will not resolve without operator intervention. serve exits immediately on
// receipt instead of logging and retrying next tick.
type permanentConfigError struct{ err error }

func (e *permanentConfigError) Error() string { return e.err.Error() }
func (e *permanentConfigError) Unwrap() error { return e.err }

// timestampLineWriter wraps an io.Writer and prepends an RFC 3339 UTC
// timestamp to each Write call. log.New calls Write once per formatted
// line, so this produces one timestamp per log message.
type timestampLineWriter struct{ w io.Writer }

func (t *timestampLineWriter) Write(p []byte) (int, error) {
	ts := time.Now().UTC().Format(time.RFC3339)
	line := strings.TrimRight(string(p), "\n")
	_, err := fmt.Fprintf(t.w, "%s %s\n", ts, line)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// runServe implements `ridgeline serve`.
//
//	--config PATH     path to ridgeline.yaml
//	--interval DUR    how often to run sync (e.g. 30s, 5m, 1h)
//	--quiet           suppress per-sync preamble; emit one timestamped line per tick
//	--verbose         show per-sync preamble and per-connector lines on every tick
//
// The first sync runs immediately; subsequent syncs run on the interval.
// A single-line outcome is printed after each sync. SIGINT or SIGTERM
// exits cleanly between sync runs. Structural config errors (missing file,
// unparseable YAML, unknown connector type) fail fast on the first tick
// instead of looping forever; transient IO errors are retried each interval.
func runServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	interval := fs.Duration("interval", 0, "sync interval (e.g. 1h, 30m, 10s)")
	quiet := fs.Bool("quiet", false, "suppress per-sync preamble and per-connector lines; emit one timestamped line per tick; connector log lines are timestamped")
	verbose := fs.Bool("verbose", false, "show per-sync preamble and per-connector lines on every tick (default when --quiet is not set)")
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: ridgeline serve --config PATH --interval DUR [--quiet | --verbose]")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Runs sync on a repeating interval. The first sync runs immediately;")
		fmt.Fprintln(w, "subsequent syncs run after each interval elapses. Exits cleanly on")
		fmt.Fprintln(w, "SIGINT or SIGTERM. Does not daemonize; use systemd or launchd to")
		fmt.Fprintln(w, "keep the process alive.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "With --quiet, the per-sync preamble (loaded, state, per-connector")
		fmt.Fprintln(w, "record counts) is suppressed. One timestamped result line is written")
		fmt.Fprintln(w, "per tick. Connector log lines (warn:, info:) are also timestamped")
		fmt.Fprintln(w, "so a log tail remains machine-parseable. --verbose restores full output.")
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
	if *quiet && *verbose {
		return fmt.Errorf("--quiet and --verbose are mutually exclusive")
	}
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	if *interval <= 0 {
		return fmt.Errorf("--interval is required and must be positive")
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	var syncOut io.Writer = os.Stdout
	var logWriter io.Writer // nil = pipeline default (prefix-free stderr)
	if *quiet {
		syncOut = io.Discard
		logWriter = &timestampLineWriter{w: os.Stderr}
	}

	return serveLoop(ctx, *interval, func(ctx context.Context) error {
		start := time.Now()
		sum, err := runConfigSync(ctx, *cfgPath, false, false, syncOut, logWriter)
		elapsed := time.Since(start).Truncate(time.Millisecond)
		ts := time.Now().UTC().Format(time.RFC3339)
		if err != nil {
			var pce *permanentConfigError
			if errors.As(err, &pce) {
				return pce
			}
			if ctx.Err() != nil {
				// Shutdown signal received during sync; not a real failure.
				fmt.Printf("%s serve: shutting down\n", ts)
				return nil
			}
			fmt.Printf("%s serve: sync error (%s): %v\n", ts, elapsed, err)
		} else {
			fmt.Printf("%s serve: %d extracted, %d persisted, %d states saved (%s)\n",
				ts, sum.Extracted, sum.Persisted, sum.States, elapsed)
		}
		return nil
	})
}

// serveLoop runs syncFn once immediately, then repeats on interval until ctx
// is cancelled. If syncFn returns a *permanentConfigError the loop exits
// immediately with that error instead of logging and retrying. All other
// errors from syncFn are treated as transient: the caller is responsible for
// logging them and returning nil so the loop can continue.
func serveLoop(ctx context.Context, interval time.Duration, syncFn func(context.Context) error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if err := syncFn(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := syncFn(ctx); err != nil {
				return err
			}
		}
	}
}
