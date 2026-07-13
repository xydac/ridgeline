package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/mattn/go-runewidth"
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

// mutatingKeywords is the set of statement-opening keywords that indicate
// a write or DDL operation. Only when the leading keyword is in this set do
// we emit the "read-only mode rejects ... pass --write" message. An unknown
// keyword (e.g. a typo like "SELEKT") falls through to actual execution so
// DuckDB surfaces a real parse error rather than a misleading remediation hint.
var mutatingKeywords = map[string]bool{
	"ALTER":      true,
	"ATTACH":     true,
	"CALL":       true,
	"CHECKPOINT": true,
	"COPY":       true,
	"CREATE":     true,
	"DELETE":     true,
	"DETACH":     true,
	"DROP":       true,
	"EXPORT":     true,
	"IMPORT":     true,
	"INSERT":     true,
	"INSTALL":    true,
	"LOAD":       true,
	"MERGE":      true,
	"TRUNCATE":   true,
	"UPDATE":     true,
	"VACUUM":     true,
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

// hasExecutableContent reports whether s contains any executable SQL,
// treating whitespace, SQL comments (-- line and /* block */), and semicolons
// as non-content. Callers use it to reject inputs that carry no statement.
func hasExecutableContent(s string) bool {
	i := 0
	for i < len(s) {
		ch := s[i]
		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == ';':
			i++
		case ch == '-' && i+1 < len(s) && s[i+1] == '-':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case ch == '/' && i+1 < len(s) && s[i+1] == '*':
			i += 2
			for i+1 < len(s) && !(s[i] == '*' && s[i+1] == '/') {
				i++
			}
			i += 2
		default:
			return true
		}
	}
	return false
}

// hasNonCommentContent reports whether s contains any content outside of
// whitespace and SQL comments (-- line comments and /* block */ comments).
// Used to determine whether content follows a semicolon.
func hasNonCommentContent(s string) bool {
	i := 0
	for i < len(s) {
		ch := s[i]
		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			i++
		case ch == '-' && i+1 < len(s) && s[i+1] == '-':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case ch == '/' && i+1 < len(s) && s[i+1] == '*':
			i += 2
			for i+1 < len(s) && !(s[i] == '*' && s[i+1] == '/') {
				i++
			}
			i += 2
		default:
			return true
		}
	}
	return false
}

// hasStatementDelimiter reports whether s contains a semicolon that appears
// outside single-quoted strings, double-quoted identifiers, line comments
// (--...\n), or block comments (/* ... */), and is followed by non-comment
// SQL content. A trailing semicolon (followed only by whitespace/comments)
// does NOT count. This is a pre-flight check to reject multi-statement input
// before DuckDB's json_serialize_sql is called, closing the injection class
// where a leading allowed statement (SELECT, WITH) opens the gate for
// arbitrary trailing writes.
func hasStatementDelimiter(s string) bool {
	inSingle := false
	inDouble := false
	i := 0
	for i < len(s) {
		ch := s[i]
		switch {
		case !inSingle && !inDouble && ch == '-' && i+1 < len(s) && s[i+1] == '-':
			// Line comment: advance to end of line.
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case !inSingle && !inDouble && ch == '/' && i+1 < len(s) && s[i+1] == '*':
			// Block comment: advance to closing */.
			i += 2
			for i+1 < len(s) && !(s[i] == '*' && s[i+1] == '/') {
				i++
			}
			i += 2
		case !inDouble && ch == '\'':
			// Single-quoted string; '' is an escaped quote inside.
			if inSingle && i+1 < len(s) && s[i+1] == '\'' {
				i += 2
				continue
			}
			inSingle = !inSingle
			i++
		case !inSingle && ch == '"':
			inDouble = !inDouble
			i++
		case !inSingle && !inDouble && ch == ';':
			// Reject if actual SQL content (not just whitespace/comments) follows.
			if hasNonCommentContent(s[i+1:]) {
				return true
			}
			i++
		default:
			i++
		}
	}
	return false
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
	// Pre-scan for statement delimiters. This catches multi-statement
	// input like "SELECT 1; COPY ... TO '/tmp/x'" before json_serialize_sql
	// sees it. json_serialize_sql cannot serialize multi-type sequences and
	// returns an error; a leading SELECT keyword previously caused an early
	// nil return, opening the gate for trailing write statements.
	if hasStatementDelimiter(stmt) {
		return fmt.Errorf("multi-statement SQL is not permitted; run one statement at a time")
	}

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
		// json_serialize_sql could not parse the statement. This happens for
		// non-SELECT statements (DELETE, COPY, ATTACH, ...) and for malformed
		// input that the parser cannot reach. Only emit the "pass --write"
		// remediation when the opening keyword is a known mutating verb; an
		// unknown keyword (a typo, an unrecognized statement type) should fall
		// through to execution so DuckDB surfaces the real error.
		kw := firstKeyword(stmt)
		if safeNonSelectKeywords[kw] || kw == "SELECT" || kw == "WITH" {
			return nil
		}
		if mutatingKeywords[kw] {
			return fmt.Errorf("read-only mode rejects %s; pass --write to permit modifications", kw)
		}
		// Unknown keyword: let DuckDB produce the real parse error.
		return nil
	}

	if len(meta.Statements) > 1 {
		return fmt.Errorf("multi-statement SQL is not permitted; run one statement at a time")
	}

	return nil
}

// skipLeadingComments advances past any leading whitespace and SQL comments
// (-- line comments and /* block */ comments) in s, returning the rest.
// The result starts at the first non-comment, non-whitespace character.
func skipLeadingComments(s string) string {
	i := 0
	for i < len(s) {
		ch := s[i]
		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			i++
		case ch == '-' && i+1 < len(s) && s[i+1] == '-':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case ch == '/' && i+1 < len(s) && s[i+1] == '*':
			i += 2
			for i+1 < len(s) && !(s[i] == '*' && s[i+1] == '/') {
				i++
			}
			i += 2
		default:
			return s[i:]
		}
	}
	return ""
}

// firstKeyword returns the uppercased first non-comment token of stmt,
// used to build rejection messages. Leading SQL comments (-- and /* */)
// are skipped so the message names the real write verb rather than the
// comment delimiter.
func firstKeyword(stmt string) string {
	fields := strings.Fields(skipLeadingComments(stmt))
	if len(fields) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(fields[0])
}

// applySandbox configures the in-process DuckDB instance for safe
// read-only operation. It must be called immediately after opening the
// database and before any user SQL runs.
//
// Extension auto-install and auto-load are disabled, which blocks network
// reads (HTTP, S3, GCS) because they require the httpfs extension. Local
// filesystem reads (read_parquet, read_csv_auto, read_json_auto) continue
// to work; they are gated only by OS-level process permissions.
//
// Configuration is locked after these settings are applied, so subsequent
// SQL cannot undo the sandbox via SET statements.
func applySandbox(ctx context.Context, db *sql.DB) error {
	for _, s := range []string{
		"SET autoinstall_known_extensions = false",
		"SET autoload_known_extensions = false",
		"SET lock_configuration = true",
	} {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("sandbox: %w", err)
		}
	}
	return nil
}

// Run opens an in-memory DuckDB database, executes stmt, and writes
// the result rows as an aligned text table to w. The database is closed
// before Run returns; no state persists between calls.
//
// By default (opts.Write == false), Run enforces read-only mode: only
// SELECT-type, EXPLAIN, PRAGMA, SHOW, and DESCRIBE statements are
// accepted; multi-statement input is rejected; and network reads
// (HTTP, S3, GCS) are blocked by disabling extension auto-load.
// Local filesystem reads remain unrestricted. Pass Options{Write: true}
// to bypass these guardrails for full DuckDB access.
//
// Input that contains no executable SQL (empty, whitespace-only,
// comment-only, or a bare semicolon) is rejected before any DuckDB call.
func Run(ctx context.Context, stmt string, w io.Writer, opts Options) error {
	if !hasExecutableContent(stmt) {
		return fmt.Errorf("query must not be empty")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if !opts.Write {
		if err := applySandbox(ctx, db); err != nil {
			return err
		}
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

// formatValue renders a single scanned column value in SQL-faithful notation.
// SQL NULL renders as an empty string (visually distinct from the string "NULL").
// Lists render as [a, b, c]; structs as {k: v}; blobs as blob:\xHH...;
// TIME/TIMESTAMP columns render as RFC 3339; DATE (midnight UTC) as YYYY-MM-DD.
func formatValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return formatBlob(x)
	case time.Time:
		if x.Hour() == 0 && x.Minute() == 0 && x.Second() == 0 && x.Nanosecond() == 0 && x.Location() == time.UTC {
			return x.Format("2006-01-02")
		}
		return x.Format(time.RFC3339)
	case []any:
		return formatList(x)
	case map[string]any:
		return formatStruct(x)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// formatBlob renders a binary blob as blob:\xHH... hex notation.
func formatBlob(b []byte) string {
	var sb strings.Builder
	sb.WriteString("blob:")
	for _, byt := range b {
		fmt.Fprintf(&sb, `\x%02x`, byt)
	}
	return sb.String()
}

// formatList renders a DuckDB LIST value as [a, b, c] with recursive formatting.
func formatList(items []any) string {
	parts := make([]string, len(items))
	for i, item := range items {
		parts[i] = formatValue(item)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// formatStruct renders a DuckDB STRUCT value as {k1: v1, k2: v2}.
// Keys are sorted for deterministic output.
func formatStruct(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + ": " + formatValue(m[k])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// escapeCell replaces control characters that would break the table layout
// with their backslash representations.
func escapeCell(s string) string {
	s = strings.ReplaceAll(s, "\r", `\r`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\t", `\t`)
	return s
}

// writeTable prints header and rows in a fixed-width aligned layout.
// Column widths are measured in terminal display columns (not UTF-8 bytes)
// so multi-byte and wide characters (CJK, emoji) align correctly.
// Control characters in cell values are escaped before rendering.
// Zero rows still prints the header so the caller can see the schema.
func writeTable(w io.Writer, cols []string, rows [][]string) {
	// Escape control characters before measuring or rendering anything.
	eCols := make([]string, len(cols))
	for i, c := range cols {
		eCols[i] = escapeCell(c)
	}
	eRows := make([][]string, len(rows))
	for i, r := range rows {
		ec := make([]string, len(r))
		for j, cell := range r {
			ec[j] = escapeCell(cell)
		}
		eRows[i] = ec
	}

	widths := make([]int, len(eCols))
	for i, c := range eCols {
		widths[i] = runewidth.StringWidth(c)
	}
	for _, r := range eRows {
		for i, cell := range r {
			if dw := runewidth.StringWidth(cell); dw > widths[i] {
				widths[i] = dw
			}
		}
	}
	writeRow(w, eCols, widths)
	sep := make([]string, len(eCols))
	for i, width := range widths {
		sep[i] = strings.Repeat("-", width)
	}
	writeRow(w, sep, widths)
	for _, r := range eRows {
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

// padRight pads s with spaces to reach the target display width.
// Width is measured in terminal columns, not bytes, so multi-byte
// characters (accented Latin, CJK, emoji) are accounted for correctly.
func padRight(s string, width int) string {
	dw := runewidth.StringWidth(s)
	if dw >= width {
		return s
	}
	return s + strings.Repeat(" ", width-dw)
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
