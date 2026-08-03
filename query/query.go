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

// allowedNonSelectKeywords is the closed set of statement-opening keywords
// accepted in read-only mode when json_serialize_sql cannot parse the
// statement (it only handles SELECT-type statements). EXPLAIN is handled
// separately because EXPLAIN ANALYZE executes its wrapped statement.
// Everything outside this set is rejected, including session-control
// statements like USE, BEGIN, COMMIT, and ROLLBACK.
var allowedNonSelectKeywords = map[string]bool{
	"PRAGMA":   true,
	"SHOW":     true,
	"DESCRIBE": true,
}

// mutatingKeywords is the set of statement-opening keywords that indicate
// a write or DDL operation. Used only to produce a more specific rejection
// message; the closed allow-list rejects all non-allowed keywords anyway.
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

// splitStatements splits sql into individual statement strings by scanning for
// semicolons outside single-quoted strings, double-quoted identifiers, line
// comments (-- to newline), and block comments (/* ... */). Segments that
// contain no executable content are omitted from the result.
func splitStatements(sql string) []string {
	var stmts []string
	var cur strings.Builder
	inSingle := false
	inDouble := false
	i := 0
	for i < len(sql) {
		ch := sql[i]
		switch {
		case !inSingle && !inDouble && ch == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				cur.WriteByte(sql[i])
				i++
			}
		case !inSingle && !inDouble && ch == '/' && i+1 < len(sql) && sql[i+1] == '*':
			cur.WriteByte(sql[i])
			cur.WriteByte(sql[i+1])
			i += 2
			for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
				cur.WriteByte(sql[i])
				i++
			}
			if i+1 < len(sql) {
				cur.WriteByte(sql[i])
				cur.WriteByte(sql[i+1])
				i += 2
			}
		case !inDouble && ch == '\'':
			if inSingle && i+1 < len(sql) && sql[i+1] == '\'' {
				cur.WriteByte(ch)
				cur.WriteByte(ch)
				i += 2
				continue
			}
			inSingle = !inSingle
			cur.WriteByte(ch)
			i++
		case !inSingle && ch == '"':
			inDouble = !inDouble
			cur.WriteByte(ch)
			i++
		case !inSingle && !inDouble && ch == ';':
			if hasExecutableContent(cur.String()) {
				stmts = append(stmts, strings.TrimSpace(cur.String()))
			}
			cur.Reset()
			i++
		default:
			cur.WriteByte(ch)
			i++
		}
	}
	if hasExecutableContent(cur.String()) {
		stmts = append(stmts, strings.TrimSpace(cur.String()))
	}
	return stmts
}

// checkSingleStatement validates one SQL statement using a closed allow-list.
//
// DuckDB's json_serialize_sql is called first: if it parses successfully,
// the statement is SELECT-type and is always allowed. When it cannot parse
// (non-SELECT or malformed input), the first keyword determines acceptance
// under the closed allow-list: SELECT, WITH (for malformed inputs that start
// with SELECT/WITH), EXPLAIN (without ANALYZE), DESCRIBE, SHOW, and PRAGMA
// are allowed. Everything else -- including session-control statements like
// USE, BEGIN, and COMMIT -- is rejected. This is an allow-list, not a
// deny-list: unrecognized verbs are rejected rather than passed through.
//
// When the inspection query itself fails (DB error), the function returns nil
// and lets actual execution surface the real error.
func checkSingleStatement(ctx context.Context, db *sql.DB, stmt string) error {
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

	if !meta.Error {
		// json_serialize_sql succeeded: statement is SELECT-type, allowed.
		return nil
	}

	// Non-SELECT statement: apply the closed allow-list.
	kw := firstKeyword(stmt)
	switch kw {
	case "SELECT", "WITH":
		// json_serialize_sql failed but the leading keyword is SELECT/WITH,
		// which means the input is malformed SQL. Let DuckDB surface the
		// real parse error rather than producing a misleading gate message.
		return nil
	case "EXPLAIN":
		// EXPLAIN ANALYZE (keyword or parenthesized form) executes its wrapped
		// statement, so it must be rejected even if the inner statement would be
		// allowed. Plain EXPLAIN only prints the plan without executing.
		if explainHasAnalyze(stmt) {
			return fmt.Errorf("read-only mode rejects EXPLAIN ANALYZE; pass --write to permit modifications")
		}
		return nil
	case "DESCRIBE", "SHOW", "PRAGMA":
		return nil
	default:
		// Closed allow-list: reject everything not explicitly permitted above,
		// including session-control statements (USE, BEGIN, COMMIT, ROLLBACK)
		// and any other verb not in the enumerated read-only set.
		return fmt.Errorf("read-only mode rejects %s; pass --write to permit modifications", kw)
	}
}

// checkReadOnly validates stmt for read-only mode. It splits the input into
// individual statements using splitStatements and checks each one with
// checkSingleStatement. Multi-statement input is permitted when every
// statement is read-only; a leading SELECT does not license a trailing write.
// Any statement that would be rejected on its own causes the whole batch to
// be rejected before DuckDB executes anything.
func checkReadOnly(ctx context.Context, db *sql.DB, stmt string) error {
	for _, s := range splitStatements(stmt) {
		if err := checkSingleStatement(ctx, db, s); err != nil {
			return err
		}
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

// explainHasAnalyze reports whether stmt begins with EXPLAIN and carries
// an ANALYZE flag in either the keyword form (EXPLAIN ANALYZE ...) or the
// parenthesized-options form (EXPLAIN (ANALYZE) or EXPLAIN (ANALYZE, FORMAT JSON)).
// EXPLAIN without ANALYZE only prints the execution plan and is safe in
// read-only mode. EXPLAIN ANALYZE actually executes the wrapped statement,
// so EXPLAIN ANALYZE COPY would bypass the read-only gate if allowed.
func explainHasAnalyze(stmt string) bool {
	rest := skipLeadingComments(stmt)
	if !strings.HasPrefix(strings.ToUpper(rest), "EXPLAIN") {
		return false
	}
	rest = strings.TrimSpace(rest[len("EXPLAIN"):])
	if len(rest) == 0 {
		return false
	}
	upper := strings.ToUpper(rest)

	// Keyword form: EXPLAIN ANALYZE
	if strings.HasPrefix(upper, "ANALYZE") {
		tail := rest[len("ANALYZE"):]
		if len(tail) == 0 || tail[0] == ' ' || tail[0] == '\t' || tail[0] == '\n' || tail[0] == '\r' || tail[0] == '(' {
			return true
		}
	}

	// Parenthesized form: EXPLAIN (ANALYZE ...) or EXPLAIN (FORMAT JSON, ANALYZE)
	if rest[0] != '(' {
		return false
	}
	depth := 0
	var inside strings.Builder
	for _, ch := range rest {
		if ch == '(' {
			depth++
			if depth == 1 {
				continue // skip opening paren
			}
		} else if ch == ')' {
			depth--
			if depth == 0 {
				break
			}
		}
		if depth >= 1 {
			inside.WriteRune(ch)
		}
	}
	for _, part := range strings.Split(inside.String(), ",") {
		words := strings.Fields(strings.TrimSpace(part))
		if len(words) > 0 && strings.ToUpper(words[0]) == "ANALYZE" {
			return true
		}
	}
	return false
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
// sqlite_scanner is loaded before the lock so that sqlite_scan() and
// sqlite_attach() work in read-only mode without requiring --write.
// If the extension is not installed the load silently no-ops and the
// user will see DuckDB's own "extension not installed" message when they
// try to use it.
//
// Extension auto-install and auto-load are disabled after the explicit
// load, which blocks network reads (HTTP, S3, GCS) because they require
// the httpfs extension. Local filesystem reads (read_parquet, read_csv_auto,
// read_json_auto, sqlite_scan) continue to work.
//
// Configuration is locked after these settings are applied, so subsequent
// SQL cannot undo the sandbox via SET statements.
func applySandbox(ctx context.Context, db *sql.DB) error {
	// Pre-load sqlite_scanner while auto-install is still permitted, before
	// the configuration lock prevents extension loading.
	_, _ = db.ExecContext(ctx, "LOAD sqlite_scanner")
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

// pinTimezoneUTC forces the DuckDB session TimeZone to UTC so that
// date_trunc, extract, and other TIMESTAMPTZ-aware functions bucket
// consistently across hosts. Without this, DuckDB inherits the host's
// TZ, producing different results for the same query on different
// machines and contradicting the README's "(UTC)" claim.
func pinTimezoneUTC(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "SET TimeZone='UTC'"); err != nil {
		return fmt.Errorf("pin timezone: %w", err)
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

	if err := pinTimezoneUTC(ctx, db); err != nil {
		return err
	}
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
		if !opts.Write && strings.Contains(err.Error(), "httpfs") {
			return fmt.Errorf("network reads are disabled in read-only mode; pass --write to opt in")
		}
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

	// Extract struct field ordering from column type metadata so STRUCT
	// columns render fields in SQL-declared order (not sorted).
	colTypes, _ := rows.ColumnTypes()
	structKeys := make([][]string, len(cols))
	for i, ct := range colTypes {
		structKeys[i] = parseStructFieldNames(ct.DatabaseTypeName())
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
			if structKeys[i] != nil {
				if m, ok := v.(map[string]any); ok {
					row[i] = formatStruct(m, structKeys[i])
					continue
				}
			}
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
		return formatStruct(x, nil)
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
// When orderedKeys is non-nil the fields are rendered in that order;
// otherwise keys are sorted for deterministic output.
func formatStruct(m map[string]any, orderedKeys []string) string {
	keys := orderedKeys
	if keys == nil {
		keys = make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
	}
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v, ok := m[k]
		if !ok {
			continue
		}
		parts = append(parts, k+": "+formatValue(v))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// parseStructFieldNames parses the field names from a DuckDB STRUCT type
// name (e.g. STRUCT("z" INTEGER, "a" INTEGER, "m" INTEGER)) and returns
// them in declared order. Returns nil if the input is not a STRUCT type or
// cannot be parsed, causing the caller to fall back to sorted output.
func parseStructFieldNames(typeName string) []string {
	const prefix = "STRUCT("
	if !strings.HasPrefix(typeName, prefix) {
		return nil
	}
	inner := typeName[len(prefix):]
	if !strings.HasSuffix(inner, ")") {
		return nil
	}
	inner = inner[:len(inner)-1]

	var names []string
	i := 0
	for i < len(inner) {
		// Skip whitespace before each field definition.
		for i < len(inner) && (inner[i] == ' ' || inner[i] == '\t') {
			i++
		}
		if i >= len(inner) {
			break
		}
		// Read the field name: either "quoted" or unquoted.
		var name string
		if inner[i] == '"' {
			i++ // opening quote
			start := i
			for i < len(inner) && inner[i] != '"' {
				i++
			}
			name = inner[start:i]
			if i < len(inner) {
				i++ // closing quote
			}
		} else {
			start := i
			for i < len(inner) && inner[i] != ' ' && inner[i] != '\t' {
				i++
			}
			name = inner[start:i]
		}
		if name == "" {
			return nil
		}
		names = append(names, name)

		// Skip the field's type definition, which may contain nested parens.
		// Advance until a comma at depth 0 (or end of string).
		depth := 0
		for i < len(inner) {
			ch := inner[i]
			if ch == '(' {
				depth++
			} else if ch == ')' {
				if depth == 0 {
					break
				}
				depth--
			} else if ch == ',' && depth == 0 {
				i++ // consume comma
				break
			}
			i++
		}
	}
	return names
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
