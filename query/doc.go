// Package query runs a single SQL statement against an in-process
// DuckDB database and writes the result as an aligned text table.
//
// The package is intentionally thin. It exists so the `ridgeline query`
// CLI has a small, testable surface that does not leak the underlying
// driver type into the rest of the codebase. A caller that needs more
// control (batching, prepared statements, multiple statements) should
// open their own *sql.DB with the "duckdb" driver.
//
// Typical use:
//
//	err := query.Run(ctx, `
//	    SELECT stream, count(*) AS n
//	    FROM read_parquet('./out/*/*.parquet')
//	    GROUP BY stream
//	`, os.Stdout)
//
// DuckDB reads Parquet, CSV, JSON, and SQLite files directly via its
// built-in table functions (read_parquet, read_csv, etc.), so a single
// Run call can query anything a prior sync produced without a separate
// load step.
package query
