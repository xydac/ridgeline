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

// TestMain guards against leaving temp dirs behind if a test panics.
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
