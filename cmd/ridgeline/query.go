package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/xydac/ridgeline/query"
)

// runQuery implements `ridgeline query <SQL>`.
//
// By default the command runs in read-only mode: only SELECT, EXPLAIN,
// DESCRIBE, SHOW, and PRAGMA statements are accepted, multi-statement
// input is rejected, and network reads (HTTP, S3, GCS) are blocked.
// Pass --write to permit modifications (INSERT, DELETE, COPY, DDL, ...)
// and to lift the network restriction.
//
// SQL must be supplied as a single quoted argument. No --config is
// required: DuckDB runs in-process against whatever paths the query
// references, typically read_parquet over a prior sync's output directory.
//
// SQL that begins with -- (a line comment) must be quoted as a single
// shell argument; the -- end-of-flags sentinel is also accepted:
//
//	ridgeline query "-- my comment
//	SELECT 1"
//	ridgeline query -- "-- my comment SELECT 1"
func runQuery(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	write := fs.Bool("write", false, "permit non-SELECT statements (INSERT, DELETE, COPY, DDL)")
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: ridgeline query [--write] <SQL>")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Runs a SQL query against an in-process DuckDB and")
		fmt.Fprintln(w, "prints the result as an aligned text table.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "By default, only SELECT and read-only statements are accepted,")
		fmt.Fprintln(w, "and network reads (HTTP, S3) are blocked.")
		fmt.Fprintln(w, "Pass --write to allow modifications (INSERT, DELETE, COPY, DDL)")
		fmt.Fprintln(w, "and to lift the network restriction.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "SQL may begin with a -- line comment when quoted as a single argument.")
		fmt.Fprintln(w, "Use -- to end flag parsing if needed:")
		fmt.Fprintln(w, "  ridgeline query -- \"-- comment SELECT 1\"")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Example:")
		fmt.Fprintln(w, "  ridgeline query \"SELECT count(*) FROM read_parquet('./out/*/*.parquet')\"")
	}

	// Partition args manually so that SQL starting with -- is not
	// misinterpreted by flag.FlagSet as a flag definition. Only the
	// flags this subcommand actually defines (-write/--write and the
	// help flags) are forwarded to fs.Parse; everything else becomes
	// a positional arg. A bare -- terminates flag scanning.
	var flagArgs, positional []string
	endOfFlags := false
	for _, a := range args {
		switch {
		case endOfFlags:
			positional = append(positional, a)
		case a == "--":
			endOfFlags = true
		case a == "--write" || a == "-write":
			flagArgs = append(flagArgs, a)
		case a == "--help" || a == "-help" || a == "-h":
			flagArgs = append(flagArgs, a)
		default:
			// Route flag-shaped args (starts with '-', no whitespace) through
			// the FlagSet so unrecognized flags produce the standard diagnostic
			// instead of a misleading "N words received" SQL error.
			if strings.HasPrefix(a, "-") && !strings.ContainsAny(a, " \t\n") {
				flagArgs = append(flagArgs, a)
			} else {
				positional = append(positional, a)
			}
		}
	}

	help, err := parseSubcommandFlags(fs, stdout, flagArgs)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if len(positional) == 0 {
		return usageErrorf("usage: ridgeline query [--write] \"<SQL>\"")
	}
	if len(positional) > 1 {
		return usageErrorf("SQL must be a single quoted argument (%d words received); use: ridgeline query \"<SQL>\"", len(positional))
	}
	return query.Run(ctx, positional[0], stdout, query.Options{Write: *write})
}
