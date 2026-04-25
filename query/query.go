package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// Options configures a Run call.
type Options struct {
	// Write disables the read-only guardrails. When false (the default),
	// only SELECT-type, EXPLAIN, PRAGMA, SHOW, and DESCRIBE statements
	// are accepted, and multi-statement input is rejected.
	Write bool
}

// safeNonSelectKeywords lists statement-opening keywords that are safe
// in read-only mode even though json_serialize_sql cannot parse them
// (it only handles SELECT-type statements). All of these produce
// read-only metadata operations in DuckDB.
var safeNonSelectKeywords = map[string]bool{
	"EXPLAIN":  true,
	"PRAGMA":   true,
	"SHOW":     true,
	"DESCRIBE": true,
}

// stmtMeta holds the subset of DuckDB json_serialize_sql output we inspect.
type stmtMeta struct {
	Error      bool   `json:"error"`
	ErrorMsg   string `json:"error_message"`
	Statements []struct {
		Node struct {
			Type string `json:"type"`
		} `json:"node"`
	} `json:"statements"`
}

// checkReadOnly validates stmt using DuckDB's json_serialize_sql:
//   - Multi-statement input is rejected.
//   - Non-SELECT statements not in the safe-keyword list are rejected.
//
// json_serialize_sql only handles SELECT-type statements; anything else
// returns an error JSON, which we treat as a non-SELECT indicator.
// EXPLAIN, PRAGMA, SHOW, and DESCRIBE are exempted via safeNonSelectKeywords.
//
// When the inspection query itself fails (syntax error, etc.), we return
// nil and let the actual execution surface the real error.
func checkReadOnly(ctx context.Context, db *sql.DB, stmt string) error {
	// Escape single quotes for embedding in a SQL literal (standard SQL: '' = one quote).
	escaped := strings.ReplaceAll(stmt, "'", "''")
	inspectSQL := "SELECT CAST(json_serialize_sql('" + escaped + "') AS VARCHAR)"

	var raw string
	if err := db.QueryRowContext(ctx, inspectSQL).Scan(&raw); err != nil {
		return nil
	}

	var meta stmtMeta
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return nil
	}

	if meta.Error {
		// Non-SELECT statement. Check the opening keyword against the safe list.
		kw := firstKeyword(stmt)
		if safeNonSelectKeywords[kw] {
			return nil
		}
		return fmt.Errorf("read-only mode rejects %s; pass --write to permit modifications", kw)
	}

	if len(meta.Statements) > 1 {
		return fmt.Errorf("multi-statement SQL is not permitted; run one statement at a time")
	}

	return nil
}

// firstKeyword returns the uppercased first whitespace-separated token
// of stmt, used for error messages.
func firstKeyword(stmt string) string {
	fields := strings.Fields(strings.TrimSpace(stmt))
	if len(fields) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(fields[0])
}

// Run opens an in-memory DuckDB database, executes stmt, and writes
// the result rows as an aligned text table to w. The database is closed
// before Run returns; no state persists between calls.
//
// By default (opts.Write == false), Run enforces read-only mode: only
// SELECT-type, EXPLAIN, PRAGMA, SHOW, and DESCRIBE statements are
// accepted, and multi-statement input is rejected. Pass Options{Write: true}
// to bypass these guardrails.
//
// Empty or whitespace-only input is rejected before any DuckDB call.
func Run(ctx context.Context, stmt string, w io.Writer, opts Options) error {
	if strings.TrimSpace(stmt) == "" {
		return fmt.Errorf("query must not be empty")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if !opts.Write {
		if err := checkReadOnly(ctx, db, stmt); err != nil {
			return err
		}
	}

	rows, err := db.QueryContext(ctx, stmt)
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

// formatValue renders a single scanned column value.
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

// writeTable prints header and rows in a fixed-width aligned layout.
// Zero rows still prints the header so the caller can see the schema.
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
