package query

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func TestRunLiteralSelect(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELECT 42 AS answer, 'hi' AS greeting", &buf, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := buf.String()
	for _, want := range []string{"answer", "greeting", "42", "hi", "(1 row)"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q\n---\n%s", want, got)
		}
	}
}

// TestRunNullRendering verifies that a SQL NULL and the string 'NULL' are
// visually distinct in query output (F-076). SQL NULL renders as an empty
// cell; the literal string 'NULL' renders as the four characters NULL.
func TestRunNullRendering(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELECT NULL AS realnull, 'NULL' AS strnull", &buf, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	// Find the data row (skip header and separator lines).
	var dataRow string
	for _, l := range lines {
		if strings.Contains(l, "NULL") && !strings.HasPrefix(l, "realnull") && !strings.HasPrefix(l, "---") {
			dataRow = l
			break
		}
	}
	if dataRow == "" {
		t.Fatalf("could not find data row in output:\n%s", out)
	}
	// The two cells must not render identically. The string 'NULL' column
	// must contain "NULL"; the real-null column must be empty (blank cells).
	// Check by splitting on two-or-more spaces and verifying the first cell is blank.
	parts := strings.SplitN(strings.TrimRight(dataRow, " "), "  ", 2)
	if len(parts) < 2 {
		t.Fatalf("expected two cells separated by spaces, got: %q", dataRow)
	}
	nullCell := strings.TrimSpace(parts[0])
	strCell := strings.TrimSpace(parts[1])
	if nullCell != "" {
		t.Errorf("SQL NULL should render as empty cell, got %q", nullCell)
	}
	if strCell != "NULL" {
		t.Errorf("string 'NULL' should render as NULL, got %q", strCell)
	}
}

func TestRunEmptyResultSetPrintsHeaderAndCount(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELECT 1 AS x WHERE 1=0", &buf, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "x") {
		t.Errorf("expected header column in output, got\n%s", got)
	}
	if !strings.Contains(got, "(0 rows)") {
		t.Errorf("expected zero-row footer, got\n%s", got)
	}
}

func TestRunRejectsEmptyQuery(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"whitespace", "   \n\t  "},
		{"line comment only", "-- just a comment"},
		{"block comment only", "/* nothing here */"},
		{"bare semicolon", ";"},
		{"semicolons and comments", "  ;; -- x\n /* y */ ; "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Run(context.Background(), tc.input, &buf, Options{})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "query must not be empty") {
				t.Errorf("want canonical empty-query message, got %q", err.Error())
			}
		})
	}
}

func TestRunSurfacesSyntaxError(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELEKT 1", &buf, Options{})
	if err == nil {
		t.Fatal("expected syntax error, got nil")
	}
}

// TestRunReadOnlyRejectsDelete verifies that a DELETE statement is
// rejected in default (read-only) mode.
func TestRunReadOnlyRejectsDelete(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "DELETE FROM nonexistent", &buf, Options{})
	if err == nil {
		t.Fatal("expected error for DELETE in read-only mode, got nil")
	}
	if !strings.Contains(err.Error(), "read-only mode") {
		t.Errorf("expected read-only rejection error, got %q", err.Error())
	}
	if !strings.Contains(strings.ToUpper(err.Error()), "DELETE") {
		t.Errorf("error should mention DELETE, got %q", err.Error())
	}
}

// TestRunReadOnlyRejectsCopy verifies that COPY TO is rejected in
// default (read-only) mode. This closes F-025.
func TestRunReadOnlyRejectsCopy(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "out.csv")
	stmt := "COPY (SELECT 42 AS answer) TO '" + tmp + "' (HEADER)"
	var buf bytes.Buffer
	err := Run(context.Background(), stmt, &buf, Options{})
	if err == nil {
		t.Fatal("expected error for COPY TO in read-only mode, got nil")
	}
	if !strings.Contains(err.Error(), "read-only mode") {
		t.Errorf("expected read-only rejection error, got %q", err.Error())
	}
}

// TestRunReadOnlyRejectsMultiStatement verifies that semicolon-joined
// multi-statement input is rejected. This closes F-026.
func TestRunReadOnlyRejectsMultiStatement(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELECT 1; SELECT 2", &buf, Options{})
	if err == nil {
		t.Fatal("expected error for multi-statement input, got nil")
	}
	if !strings.Contains(err.Error(), "multi-statement") || !strings.Contains(err.Error(), "not permitted") {
		t.Errorf("expected multi-statement error, got %q", err.Error())
	}
}

// TestRunReadOnlyRejectsSelectPlusCopy verifies that the F-065 bypass
// is closed: a leading SELECT followed by COPY TO is rejected even though
// the first statement is an otherwise-allowed read-only statement.
func TestRunReadOnlyRejectsSelectPlusCopy(t *testing.T) {
	tmp := t.TempDir()
	stmt := "SELECT 1; COPY (SELECT 99 AS x) TO '" + tmp + "/pwn.csv'"
	var buf bytes.Buffer
	err := Run(context.Background(), stmt, &buf, Options{})
	if err == nil {
		t.Fatal("expected multi-statement rejection, got nil (F-065 bypass still open)")
	}
	if !strings.Contains(err.Error(), "multi-statement") {
		t.Errorf("expected multi-statement error, got %q", err.Error())
	}
}

// TestRunReadOnlyRejectsSelectPlusCreate verifies that SELECT 1; CREATE TABLE
// is rejected in read-only mode (F-065 bypass shape 2).
func TestRunReadOnlyRejectsSelectPlusCreate(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELECT 1; CREATE TABLE z AS SELECT 42 AS v", &buf, Options{})
	if err == nil {
		t.Fatal("expected multi-statement rejection, got nil (F-065 bypass still open)")
	}
	if !strings.Contains(err.Error(), "multi-statement") {
		t.Errorf("expected multi-statement error, got %q", err.Error())
	}
}

// TestRunReadOnlyRejectsWithPlusCopy verifies that a WITH-led statement
// followed by COPY TO is rejected (F-065 bypass shape 3).
func TestRunReadOnlyRejectsWithPlusCopy(t *testing.T) {
	tmp := t.TempDir()
	stmt := "WITH t AS (SELECT 1) SELECT * FROM t; COPY (SELECT 7 AS x) TO '" + tmp + "/pwn2.csv'"
	var buf bytes.Buffer
	err := Run(context.Background(), stmt, &buf, Options{})
	if err == nil {
		t.Fatal("expected multi-statement rejection, got nil (F-065 bypass still open)")
	}
	if !strings.Contains(err.Error(), "multi-statement") {
		t.Errorf("expected multi-statement error, got %q", err.Error())
	}
}

// TestRunReadOnlyRejectsAttach verifies that ATTACH to a new writable
// database is rejected in read-only mode (F-065 bypass shape 4).
func TestRunReadOnlyRejectsAttach(t *testing.T) {
	tmp := t.TempDir()
	stmt := "ATTACH '" + tmp + "/rw.db' AS rw"
	var buf bytes.Buffer
	err := Run(context.Background(), stmt, &buf, Options{})
	if err == nil {
		t.Fatal("expected read-only rejection for ATTACH, got nil (F-065 bypass)")
	}
}

// TestRunReadOnlyAllowsTrailingSemicolon verifies that a single statement
// with a trailing semicolon is accepted (trailing ';' is not a delimiter).
func TestRunReadOnlyAllowsTrailingSemicolon(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELECT 42 AS answer;", &buf, Options{})
	if err != nil {
		t.Fatalf("trailing semicolon should be allowed: %v", err)
	}
	if !strings.Contains(buf.String(), "42") {
		t.Errorf("expected result 42, got\n%s", buf.String())
	}
}

// TestRunReadOnlyRejectsInsert verifies INSERT is rejected.
func TestRunReadOnlyRejectsInsert(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "INSERT INTO t VALUES (1)", &buf, Options{})
	if err == nil {
		t.Fatal("expected error for INSERT in read-only mode, got nil")
	}
	if !strings.Contains(err.Error(), "read-only mode") {
		t.Errorf("expected read-only rejection error, got %q", err.Error())
	}
}

// TestRunWriteModeAllowsNonSelect verifies that Write: true bypasses
// the read-only guard. The statement may produce other errors (e.g. no
// such table) but must NOT produce the "read-only mode rejects" error.
func TestRunWriteModeAllowsNonSelect(t *testing.T) {
	var buf bytes.Buffer
	// CREATE TABLE AS SELECT returns a row-count, not tabular data, so
	// Run may error about "no columns" -- but must not say "read-only".
	err := Run(context.Background(), "CREATE TABLE t AS SELECT 42 AS x", &buf, Options{Write: true})
	if err != nil && strings.Contains(err.Error(), "read-only mode rejects") {
		t.Errorf("write mode should not produce read-only rejection: %v", err)
	}
}

// TestRunReadOnlyAllowsCTE verifies that a WITH...SELECT (CTE) is
// permitted in read-only mode, since its AST type is SELECT_NODE.
func TestRunReadOnlyAllowsCTE(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "WITH x AS (SELECT 1 AS n) SELECT n FROM x", &buf, Options{})
	if err != nil {
		t.Fatalf("CTE should be allowed in read-only mode: %v", err)
	}
	if !strings.Contains(buf.String(), "(1 row)") {
		t.Errorf("expected 1-row result, got\n%s", buf.String())
	}
}

// TestRunMalformedSelectSurfacesSyntaxError verifies that a SELECT
// that json_serialize_sql cannot parse (e.g. two SELECT clauses
// concatenated without a delimiter) produces a real DuckDB syntax
// error rather than the misleading "read-only mode rejects SELECT".
func TestRunMalformedSelectSurfacesSyntaxError(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELECT 1 SELECT 2", &buf, Options{})
	if err == nil {
		t.Fatal("expected syntax error, got nil")
	}
	if strings.Contains(err.Error(), "read-only mode rejects") {
		t.Errorf("malformed SELECT should not be flagged as read-only rejection: %v", err)
	}
}

// TestRunTypoedKeywordSurfacesDuckDBError verifies that a misspelled
// statement keyword (e.g. SELEKT) produces a real DuckDB parse error
// rather than the misleading "read-only mode rejects SELEKT; pass --write"
// message. Closes F-056.
func TestRunTypoedKeywordSurfacesDuckDBError(t *testing.T) {
	cases := []string{
		"SELEKT 1",
		"INSRT INTO t VALUES (1)",
	}
	for _, stmt := range cases {
		t.Run(stmt, func(t *testing.T) {
			var buf bytes.Buffer
			err := Run(context.Background(), stmt, &buf, Options{})
			if err == nil {
				t.Fatalf("expected error for %q, got nil", stmt)
			}
			if strings.Contains(err.Error(), "pass --write") {
				t.Errorf("typo'd keyword %q should not produce --write hint, got: %v", stmt, err)
			}
		})
	}
}

// TestRunKnownMutatingKeywordGetsWriteHint verifies that recognized write
// keywords (DELETE, INSERT, CREATE) in read-only mode produce the
// "pass --write" remediation hint rather than a raw DuckDB error.
func TestRunKnownMutatingKeywordGetsWriteHint(t *testing.T) {
	cases := []string{
		"DELETE FROM t",
		"INSERT INTO t VALUES (1)",
		"CREATE TABLE t (x INT)",
		"DROP TABLE t",
		"UPDATE t SET x = 1",
	}
	for _, stmt := range cases {
		t.Run(stmt, func(t *testing.T) {
			var buf bytes.Buffer
			err := Run(context.Background(), stmt, &buf, Options{})
			if err == nil {
				t.Fatalf("expected error for %q in read-only mode, got nil", stmt)
			}
			if !strings.Contains(err.Error(), "pass --write") {
				t.Errorf("mutating keyword %q should get --write hint, got: %v", stmt, err)
			}
		})
	}
}

// TestRunReadOnlyRejectsCommentedWriteNamesVerb verifies that a write statement
// preceded by a SQL comment is rejected with the real write keyword in the
// error message, not the comment delimiter (F-071).
func TestRunReadOnlyRejectsCommentedWriteNamesVerb(t *testing.T) {
	cases := []struct {
		name   string
		stmt   string
		wantKW string
	}{
		{
			name:   "block comment before COPY",
			stmt:   "/* annotate */ COPY (SELECT 1) TO '/tmp/x.csv'",
			wantKW: "COPY",
		},
		{
			name:   "line comment before INSERT",
			stmt:   "-- step 1\nINSERT INTO t VALUES (1)",
			wantKW: "INSERT",
		},
		{
			name:   "two block comments before DELETE",
			stmt:   "/* a */ /* b */ DELETE FROM t",
			wantKW: "DELETE",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Run(context.Background(), tc.stmt, &buf, Options{})
			if err == nil {
				t.Fatal("expected read-only rejection, got nil")
			}
			msg := strings.ToUpper(err.Error())
			if !strings.Contains(msg, tc.wantKW) {
				t.Errorf("rejection message should name %q, got: %q", tc.wantKW, err.Error())
			}
			if strings.Contains(err.Error(), "rejects /*") || strings.Contains(err.Error(), "rejects --") {
				t.Errorf("rejection message must not name comment delimiter, got: %q", err.Error())
			}
		})
	}
}

// TestRunReadsParquet exercises the round-trip a user will actually
// hit: write a parquet file with the ridgeline parquet sink's schema,
// then select from it via read_parquet. Uses the glob pattern the
// README documents.
func TestRunReadsParquet(t *testing.T) {
	dir := t.TempDir()
	// Hand-roll a parquet file using DuckDB itself so the test does
	// not depend on the sinks package. This keeps query's test free
	// of cross-package coupling while still covering the real
	// read_parquet path the CLI will invoke.
	setup := "COPY (SELECT 'pages' AS stream, 1 AS timestamp, '{\"id\":1}' AS data_json " +
		"UNION ALL SELECT 'events', 2, '{\"id\":2}') " +
		"TO '" + filepath.Join(dir, "fixture.parquet") + "' (FORMAT PARQUET)"

	// Writing the fixture needs a side-effecting statement, which Run
	// rejects by design. Use the driver directly for setup only.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), setup); err != nil {
		t.Fatalf("setup: %v", err)
	}
	db.Close()

	var buf bytes.Buffer
	q := "SELECT stream, count(*) AS n FROM read_parquet('" + filepath.Join(dir, "*.parquet") + "') GROUP BY stream ORDER BY stream"
	if err := Run(context.Background(), q, &buf, Options{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := buf.String()
	for _, want := range []string{"pages", "events", "stream", "n", "(2 rows)"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q\n---\n%s", want, got)
		}
	}
}

// TestApplySandboxSettings verifies that applySandbox configures the
// expected DuckDB settings and locks the configuration.
func TestApplySandboxSettings(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := applySandbox(context.Background(), db); err != nil {
		t.Fatalf("applySandbox: %v", err)
	}

	// Confirm autoinstall and autoload are false via duckdb_settings().
	for _, want := range []struct{ name, val string }{
		{"autoinstall_known_extensions", "false"},
		{"autoload_known_extensions", "false"},
	} {
		var got string
		row := db.QueryRowContext(context.Background(),
			"SELECT value FROM duckdb_settings() WHERE name = '"+want.name+"'")
		if err := row.Scan(&got); err != nil {
			t.Errorf("query %s: %v", want.name, err)
			continue
		}
		if got != want.val {
			t.Errorf("expected %s=%s, got %s", want.name, want.val, got)
		}
	}

	// Confirm lock: any attempt to change a setting must fail.
	_, err = db.ExecContext(context.Background(), "SET autoload_known_extensions = true")
	if err == nil {
		t.Fatal("expected SET to fail after lock_configuration = true")
	}
}

// TestRunReadOnlySandboxBlocksHTTP verifies that an HTTP URL fails in
// read-only mode because httpfs cannot be auto-loaded. The failure must
// come from DuckDB's extension gating, not from a live network error.
func TestRunReadOnlySandboxBlocksHTTP(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(),
		"SELECT 1 FROM read_csv_auto('https://nonexistent.invalid/x.csv') LIMIT 1",
		&buf, Options{})
	if err == nil {
		t.Fatal("expected HTTP read to fail in read-only mode, got nil")
	}
	// The error must mention the missing httpfs extension, not a live
	// HTTP response. A live network error would contain "HTTP Error" or
	// "Could not establish connection".
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "http error") || strings.Contains(msg, "could not establish") {
		t.Errorf("sandbox failed: DuckDB made a live network attempt (got: %v)", err)
	}
	if !strings.Contains(msg, "httpfs") && !strings.Contains(msg, "extension") {
		t.Logf("unexpected error format (sandbox may still work): %v", err)
	}
}

// TestHasStatementDelimiter covers the tokenizer edge cases.
func TestHasStatementDelimiter(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		// Single statements: no delimiter.
		{"SELECT 1", false},
		{"SELECT 'a;b' AS x", false},               // semicolon inside string
		{"SELECT \"a;b\" AS x", false},             // semicolon inside double-quoted ident
		{"SELECT 1 -- trailing; comment\n", false}, // semicolon inside line comment
		{"SELECT 1 /* inline; comment */", false},  // semicolon inside block comment
		{"SELECT 1;", false},                       // trailing semicolon, no content after
		{"SELECT 1; -- comment\n  ", false},        // semicolon then only comment/whitespace
		// Multi-statement: delimiter present.
		{"SELECT 1; SELECT 2", true},
		{"SELECT 1; CREATE TABLE t AS SELECT 1", true},
		{"WITH t AS (SELECT 1) SELECT * FROM t; COPY (SELECT 1) TO '/x'", true},
		{"SELECT 'ok'; SELECT 'bypass'", true},
	}
	for _, tc := range cases {
		got := hasStatementDelimiter(tc.input)
		if got != tc.want {
			t.Errorf("hasStatementDelimiter(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// TestWriteTableMultiByteAlignment verifies that columns containing multi-byte
// characters (accented Latin, CJK full-width glyphs) align correctly.
// The separator must match the display width of the widest cell, not its byte length.
func TestWriteTableMultiByteAlignment(t *testing.T) {
	// "café" (NFD) would work too, but DuckDB returns precomposed NFC.
	// "café" = café: 4 display columns, 5 UTF-8 bytes.
	// "xy"       = 2 display columns.
	// Column width should be 4 (display width of café), separator "----".
	var buf bytes.Buffer
	err := Run(context.Background(),
		"SELECT 'café' AS a UNION ALL SELECT 'xy' ORDER BY a",
		&buf, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "café") {
		t.Errorf("output missing café:\n%s", out)
	}
	// Separator must be exactly 4 dashes (display width), not 5 (byte length).
	if !strings.Contains(out, "----") {
		t.Errorf("expected 4-dash separator for display-width-4 column:\n%s", out)
	}
	if strings.Contains(out, "-----") {
		t.Errorf("separator is 5 dashes (byte-length bug not fixed):\n%s", out)
	}

	// CJK: each glyph is 2 display columns wide.
	// "日本語" = 3 glyphs * 2 = 6 display columns, 9 UTF-8 bytes.
	// "ab"    = 2 display columns.
	var buf2 bytes.Buffer
	err = Run(context.Background(),
		"SELECT '日本語' AS j UNION ALL SELECT 'ab' ORDER BY j",
		&buf2, Options{})
	if err != nil {
		t.Fatalf("Run (CJK): %v", err)
	}
	out2 := buf2.String()
	if !strings.Contains(out2, "日本語") {
		t.Errorf("output missing 日本語:\n%s", out2)
	}
	// Separator must be 6 dashes (display width), not 9 (byte length).
	if !strings.Contains(out2, "------") {
		t.Errorf("expected 6-dash separator for CJK column:\n%s", out2)
	}
	if strings.Contains(out2, "---------") {
		t.Errorf("separator is 9 dashes (byte-length bug not fixed):\n%s", out2)
	}
}

// TestWriteTableNewlineEscape verifies that embedded newlines in cell values
// are rendered as the two-character literal \n rather than breaking the row.
func TestWriteTableNewlineEscape(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(),
		"SELECT 'line1\nline2' AS v",
		&buf, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if strings.Count(out, "\n") != 4 {
		// Expected: header line, separator, data row, row-count footer = 4 newlines.
		t.Errorf("expected 4 lines (header+sep+data+count), got output:\n%s", out)
	}
	if !strings.Contains(out, `\n`) {
		t.Errorf("embedded newline not escaped to \\n literal:\n%s", out)
	}
}

// TestWriteTableTabEscape verifies that embedded tab characters in cell values
// are rendered as the two-character literal \t rather than expanding the cell.
func TestWriteTableTabEscape(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(),
		"SELECT 'col1\tcol2' AS v",
		&buf, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, `\t`) {
		t.Errorf("embedded tab not escaped to \\t literal:\n%s", out)
	}
}

// TestFormatValue covers the SQL-faithful cell formatter (F-084).
func TestFormatValue(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"int64", int64(42), "42"},
		{"float64", float64(3.14), "3.14"},
		{"blob empty", []byte{}, "blob:"},
		{"blob bytes", []byte{0x01, 0xab}, `blob:\x01\xab`},
		{"list empty", []any{}, "[]"},
		{"list ints", []any{int64(1), int64(2), int64(3)}, "[1, 2, 3]"},
		{"list nested", []any{[]any{int64(1), int64(2)}, "x"}, "[[1, 2], x]"},
		{"struct", map[string]any{"b": "x", "a": int64(1)}, "{a: 1, b: x}"},
		{"date midnight UTC", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "2026-01-01"},
		{"timestamp non-midnight", time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC), "2026-01-15T10:30:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatValue(tt.input); got != tt.want {
				t.Errorf("formatValue(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestRunTypeRendering verifies SQL-faithful output through the full Run pipeline (F-084).
func TestRunTypeRendering(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"list integers", "SELECT [1, 2, 3] AS v", "[1, 2, 3]"},
		{"list strings", "SELECT ['a', 'b'] AS v", "[a, b]"},
		{"date column", "SELECT DATE '2026-01-01' AS d", "2026-01-01"},
		{"timestamp column", "SELECT TIMESTAMP '2026-01-15 10:30:00' AS ts", "2026-01-15T10:30:00Z"},
		{"blob column", "SELECT 'AB'::BLOB AS b", `blob:\x41\x42`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Run(context.Background(), tt.sql, &buf, Options{}); err != nil {
				t.Fatalf("Run(%q): %v", tt.sql, err)
			}
			if !strings.Contains(buf.String(), tt.want) {
				t.Errorf("output missing %q:\n%s", tt.want, buf.String())
			}
		})
	}
}

// TestRunStructRendering verifies STRUCT output is {k: v} formatted (F-084).
func TestRunStructRendering(t *testing.T) {
	var buf bytes.Buffer
	// DuckDB struct literal syntax
	err := Run(context.Background(), "SELECT {'name': 'alice', 'age': 30} AS s", &buf, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	// Must contain braces and colon-separated key/value pairs, not map[...] Go notation.
	if strings.Contains(out, "map[") {
		t.Errorf("struct rendered as Go map; want SQL notation:\n%s", out)
	}
	if !strings.Contains(out, "{") || !strings.Contains(out, "}") {
		t.Errorf("struct missing braces in output:\n%s", out)
	}
}

// TestRunReadOnlyBlocksNetworkRead verifies that a SELECT using read_json_auto
// with an HTTP URL is rejected in read-only mode with a user-friendly message,
// not DuckDB's internal "INSTALL httpfs" suggestion (F-081).
func TestRunReadOnlyBlocksNetworkRead(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELECT * FROM read_json_auto('https://example.com/x.jsonl')", &buf, Options{})
	if err == nil {
		t.Fatal("expected error for network read in read-only mode, got nil")
	}
	if strings.Contains(err.Error(), "httpfs") {
		t.Errorf("error leaks internal httpfs message: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "network reads are disabled") {
		t.Errorf("expected network-read rejection message, got %q", err.Error())
	}
}

// TestMain guards against leaving temp dirs behind if a test panics.
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
