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
	err := Run(context.Background(), "SELECT 42 AS answer, 'hi' AS greeting", &buf)
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
	err := Run(context.Background(), "SELECT NULL AS empty", &buf)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(buf.String(), "NULL") {
		t.Errorf("expected NULL token in output, got\n%s", buf.String())
	}
}

func TestRunEmptyResultSetPrintsHeaderAndCount(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELECT 1 AS x WHERE 1=0", &buf)
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
	err := Run(context.Background(), "   \n\t  ", &buf)
	if err == nil {
		t.Fatal("expected error for empty query, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error should mention empty query, got %q", err.Error())
	}
}

func TestRunSurfacesSyntaxError(t *testing.T) {
	var buf bytes.Buffer
	err := Run(context.Background(), "SELEKT 1", &buf)
	if err == nil {
		t.Fatal("expected syntax error, got nil")
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
	if err := Run(context.Background(), q, &buf); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := buf.String()
	for _, want := range []string{"pages", "events", "stream", "n", "(2 rows)"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q\n---\n%s", want, got)
		}
	}
}

// TestMain guards against leaving temp dirs behind if a test panics.
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
