package main

import (
	"context"
	"flag"
	"fmt"
	"io"

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
		fmt.Fprintln(w, "Example:")
		fmt.Fprintln(w, "  ridgeline query \"SELECT count(*) FROM read_parquet('./out/*/*.parquet')\"")
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: ridgeline query [--write] \"<SQL>\"")
	}
	if len(rest) > 1 {
		return fmt.Errorf("SQL must be a single quoted argument (%d words received); use: ridgeline query \"<SQL>\"", len(rest))
	}
	return query.Run(ctx, rest[0], stdout, query.Options{Write: *write})
}
