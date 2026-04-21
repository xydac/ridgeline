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
//	status --config PATH    show per-connector state and last-sync time
//	                        recorded in the SQLite file at config.state_path
//	query <SQL>             run SQL against an in-process DuckDB and
//	                        print results as an aligned text table
//
// Cobra will replace the hand-rolled argv dispatch once the command
// surface grows; for now flag + switch keeps the binary dep-free.
package main

import (
	"context"
	"fmt"
	"os"

	// Side-effect imports register built-in connectors and sinks.
	_ "github.com/xydac/ridgeline/connectors/external"
	_ "github.com/xydac/ridgeline/connectors/hackernews"
	_ "github.com/xydac/ridgeline/connectors/testsrc"
	_ "github.com/xydac/ridgeline/connectors/umami"
	_ "github.com/xydac/ridgeline/sinks/jsonl"
	_ "github.com/xydac/ridgeline/sinks/parquet"
)

// Version is the build version. Overridden at release time via -ldflags.
var Version = "0.0.0-dev"

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stdout)
		return
	}
	switch os.Args[1] {
	case "version", "--version", "-v":
		if extra := os.Args[2:]; len(extra) > 0 {
			fmt.Fprintf(os.Stderr, "version: unexpected argument %q\n", extra[0])
			os.Exit(2)
		}
		fmt.Println(Version)
	case "help", "--help", "-h":
		printUsage(os.Stdout)
	case "sync":
		if err := runSync(context.Background(), os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "sync: %v\n", err)
			os.Exit(1)
		}
	case "status":
		if err := runStatus(context.Background(), os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "status: %v\n", err)
			os.Exit(1)
		}
	case "query":
		if err := runQuery(context.Background(), os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "query: %v\n", err)
			os.Exit(1)
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
	fmt.Fprintln(w, "  ridgeline sync --config PATH")
	fmt.Fprintln(w, "  ridgeline status --config PATH")
	fmt.Fprintln(w, "  ridgeline query <SQL>")
	fmt.Fprintln(w, "  ridgeline help")
}
