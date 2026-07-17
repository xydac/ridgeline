// Command ridgeline is the self-hosted intelligence platform binary.
//
// Subcommands:
//
//	version                 print the build version
//	sync --dry-run          run the pipeline against the built-in testsrc
//	                        connector and the jsonl sink, into a temp dir
//	sync --config PATH      run every connector configured in PATH against
//	                        its configured sink, persisting state to the
//	                        SQLite file at config.state_path
//	serve --config PATH     run sync on a repeating interval; exits cleanly
//	  --interval DUR        on SIGINT or SIGTERM; does not daemonize
//	status --config PATH    show per-connector state and last-sync time
//	                        recorded in the SQLite file at config.state_path
//	query <SQL>             run SQL against an in-process DuckDB and
//	                        print results as an aligned text table
//	creds list|put|get|rm   manage credentials in the AES-256-GCM
//	                        credential store backing the *_ref config keys
//	creds oauth PROVIDER    run the provider's browser OAuth flow and
//	                        store the resulting credentials (gsc)
//	tui --config PATH       interactive Bubble Tea view of configured
//	                        streams with last-sync and record counts
//
// Cobra will replace the hand-rolled argv dispatch once the command
// surface grows; for now flag + switch keeps the binary dep-free.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"

	// Side-effect imports register built-in connectors and sinks.
	_ "github.com/xydac/ridgeline/connectors/external"
	_ "github.com/xydac/ridgeline/connectors/github"
	_ "github.com/xydac/ridgeline/connectors/gsc"
	_ "github.com/xydac/ridgeline/connectors/hackernews"
	_ "github.com/xydac/ridgeline/connectors/plausible"
	_ "github.com/xydac/ridgeline/connectors/posthog"
	_ "github.com/xydac/ridgeline/connectors/testsrc"
	_ "github.com/xydac/ridgeline/connectors/umami"
	_ "github.com/xydac/ridgeline/sinks/jsonl"
	_ "github.com/xydac/ridgeline/sinks/parquet"
)

// Version is the build version. Overridden at release time via -ldflags.
// When the ldflags placeholder is unchanged, init() falls back to the module
// version from debug.ReadBuildInfo so that `go install` builds self-identify.
var Version = "0.0.0-dev"

func init() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	Version = resolveVersion(Version, info.Main.Version)
}

// resolveVersion returns moduleVersion when ldflagsVersion is still the dev
// placeholder and moduleVersion is a real version string (not empty or "(devel)").
// This lets `go install github.com/xydac/ridgeline@vX.Y.Z` self-identify; a
// plain `go build ./cmd/ridgeline` from a local checkout still returns the
// placeholder because Go reports "(devel)" for that case.
func resolveVersion(ldflagsVersion, moduleVersion string) string {
	if ldflagsVersion != "0.0.0-dev" {
		return ldflagsVersion
	}
	if moduleVersion == "" || moduleVersion == "(devel)" {
		return ldflagsVersion
	}
	return moduleVersion
}

// cmdExit exits with the appropriate code for err.
// Usage errors (missing required verb, unknown subcommand) exit 2.
// Runtime errors exit 1.
func cmdExit(err error) {
	var ue *usageError
	if errors.As(err, &ue) {
		os.Exit(2)
	}
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stderr)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version", "--version", "-v":
		extra := os.Args[2:]
		if len(extra) == 1 && (extra[0] == "--help" || extra[0] == "-h" || extra[0] == "help") {
			fmt.Fprintln(os.Stdout, "Usage: ridgeline version")
			fmt.Fprintln(os.Stdout, "")
			fmt.Fprintln(os.Stdout, "Prints the build version and exits.")
			fmt.Fprintln(os.Stdout, "")
			fmt.Fprintln(os.Stdout, "Release builds (Homebrew, goreleaser, go install @vX.Y.Z) print the")
			fmt.Fprintln(os.Stdout, "tagged version. A plain `go build ./cmd/ridgeline` from a local")
			fmt.Fprintln(os.Stdout, "checkout prints 0.0.0-dev because Go does not inject version info")
			fmt.Fprintln(os.Stdout, "for workspace builds; use `go install github.com/xydac/ridgeline@vX.Y.Z`")
			fmt.Fprintln(os.Stdout, "or pass -ldflags \"-X main.Version=vX.Y.Z\" to get a tagged string.")
			return
		}
		if len(extra) > 0 {
			fmt.Fprintf(os.Stderr, "version: unexpected argument %q\n", extra[0])
			os.Exit(2)
		}
		fmt.Println(Version)
	case "help", "--help", "-h":
		printUsage(os.Stdout)
	case "sync":
		if err := runSync(context.Background(), os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "sync: %v\n", err)
			var pse *PartialSyncError
			if errors.As(err, &pse) && !pse.IsTotal() {
				os.Exit(3)
			}
			cmdExit(err)
		}
	case "serve":
		if err := runServe(context.Background(), os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "serve: %v\n", err)
			cmdExit(err)
		}
	case "status":
		if err := runStatus(context.Background(), os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "status: %v\n", err)
			cmdExit(err)
		}
	case "query":
		if err := runQuery(context.Background(), os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "query: %v\n", err)
			cmdExit(err)
		}
	case "creds":
		if err := runCreds(context.Background(), os.Args[2:], os.Stdin, os.Stdout, os.Stderr); err != nil {
			fmt.Fprintf(os.Stderr, "creds: %v\n", err)
			cmdExit(err)
		}
	case "tui":
		if err := runTUI(context.Background(), os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "tui: %v\n", err)
			cmdExit(err)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", os.Args[1])
		fmt.Fprintln(os.Stderr, "run 'ridgeline --help' for usage.")
		os.Exit(2)
	}
}

func printUsage(w *os.File) {
	fmt.Fprintf(w, "ridgeline %s\n", Version)
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  ridgeline version")
	fmt.Fprintln(w, "  ridgeline sync --dry-run [--records N] [--out DIR]")
	fmt.Fprintln(w, "  ridgeline sync --config PATH [--continue-on-error]")
	fmt.Fprintln(w, "  ridgeline serve --config PATH --interval DUR")
	fmt.Fprintln(w, "  ridgeline status --config PATH")
	fmt.Fprintln(w, "  ridgeline query <SQL>")
	fmt.Fprintln(w, "  ridgeline creds list|put|get|rm --config PATH [NAME]")
	fmt.Fprintln(w, "  ridgeline creds oauth gsc --config PATH --client-id ID --client-secret SEC")
	fmt.Fprintln(w, "  ridgeline tui --config PATH [--render-once]")
	fmt.Fprintln(w, "  ridgeline help")
}
