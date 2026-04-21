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
// SQL comes from the positional argument list, joined by a single
// space. This lets a caller quote the whole statement ("SELECT ...")
// or rely on the shell for word-splitting; either way produces the
// same query. No --config is required: DuckDB runs in-process against
// whatever the query references, typically read_parquet over a
// prior sync's output directory.
func runQuery(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: ridgeline query <SQL>")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Runs a SQL query against an in-process DuckDB and")
		fmt.Fprintln(w, "prints the result as an aligned text table.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Example:")
		fmt.Fprintln(w, "  ridgeline query \"SELECT count(*) FROM read_parquet('./out/*/*.parquet')\"")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: ridgeline query <SQL>")
	}
	sql := strings.Join(rest, " ")
	return query.Run(ctx, sql, stdout)
}
