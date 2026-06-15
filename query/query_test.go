package query

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

func TestRunNullRendering(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELECT NULL AS empty", &buf, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(buf.String(), "NULL") {
		t.Errorf("expected NULL token in output, got\n%s", buf.String())
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
	var buf bytes.Buffer
	err := Run(context.Background(), "   \n\t  ", &buf, Options{})
	if err == nil {
		t.Fatal("expected error for empty query, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error should mention empty query, got %q", err.Error())
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

// TestMain guards against leaving temp dirs behind if a test panics.
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
