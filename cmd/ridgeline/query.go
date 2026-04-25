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
// DESCRIBE, SHOW, and PRAGMA statements are accepted, and
// multi-statement input is rejected. Pass --write to permit
// modifications (INSERT, DELETE, COPY, DDL, ...).
//
// SQL comes from the positional argument list, joined by a single
// space. This lets a caller quote the whole statement ("SELECT ...")
// or rely on the shell for word-splitting; either way produces the
// same query. No --config is required: DuckDB runs in-process against
// whatever the query references, typically read_parquet over a
// prior sync's output directory.
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
		fmt.Fprintln(w, "By default, only SELECT and read-only statements are accepted.")
		fmt.Fprintln(w, "Pass --write to allow modifications (INSERT, DELETE, COPY, DDL).")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Example:")
		fmt.Fprintln(w, "  ridgeline query \"SELECT count(*) FROM read_parquet('./out/*/*.parquet')\"")
	}
	help, err := parseSubcommandFlags(fs, args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: ridgeline query <SQL>")
	}
	stmt := strings.Join(rest, " ")
	return query.Run(ctx, stmt, stdout, query.Options{Write: *write})
}
