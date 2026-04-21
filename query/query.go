package query

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// Run opens an in-memory DuckDB database, executes sql, and writes the
// result rows as an aligned text table to w. The database is closed
// before Run returns; no state persists between calls.
//
// Run is intended for SELECT-shaped queries. DuckDB will happily
// execute DDL and COPY statements too, but their result shape is a
// single status row rather than tabular data; run those via
// database/sql directly if the shape matters.
//
// Empty or whitespace-only SQL is rejected with a descriptive error so
// `ridgeline query ` with a blank argument does not look like it
// silently succeeded.
func Run(ctx context.Context, query string, w io.Writer) error {
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("query must not be empty")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("read columns: %w", err)
	}
	if len(cols) == 0 {
		return fmt.Errorf("query returned no columns; query expects a SELECT-like statement")
	}

	formatted := make([][]string, 0, 16)
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		row := make([]string, len(cols))
		for i, v := range raw {
			row[i] = formatValue(v)
		}
		formatted = append(formatted, row)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	writeTable(w, cols, formatted)
	return nil
}

// formatValue renders a single scanned column value. DuckDB NULLs come
// through as nil; []byte is rendered as a plain string; everything
// else uses the default fmt verb.
func formatValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(x)
	case string:
		return x
	default:
		return fmt.Sprintf("%v", v)
	}
}

// writeTable prints header and rows in a fixed-width aligned layout
// with space-separated columns. Zero rows still prints the header so a
// caller can see the schema of an empty result.
func writeTable(w io.Writer, cols []string, rows [][]string) {
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	for _, r := range rows {
		for i, cell := range r {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	writeRow(w, cols, widths)
	sep := make([]string, len(cols))
	for i, width := range widths {
		sep[i] = strings.Repeat("-", width)
	}
	writeRow(w, sep, widths)
	for _, r := range rows {
		writeRow(w, r, widths)
	}
	fmt.Fprintf(w, "(%d row%s)\n", len(rows), plural(len(rows)))
}

func writeRow(w io.Writer, cells []string, widths []int) {
	parts := make([]string, len(cells))
	for i, cell := range cells {
		parts[i] = padRight(cell, widths[i])
	}
	fmt.Fprintln(w, strings.Join(parts, "  "))
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
