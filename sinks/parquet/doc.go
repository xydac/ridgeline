// Package parquet provides a Sink that writes records as Apache
// Parquet files.
//
// One file per (run, stream) combination is written under the
// configured dir, and a manifest.json at the dir root lists every
// file written. Each file has the fixed schema
//
//	stream      (UTF8):  the stream name
//	timestamp   (int64): record timestamp in unix microseconds, UTC
//	data_json   (UTF8):  the record's data map encoded as JSON
//
// Storing the record body as a JSON column keeps the sink usable for
// any connector without a per-stream schema declaration, while still
// producing real Parquet that DuckDB, pandas, and Arrow-based tools
// can read. Typed-column schema inference is a follow-up.
//
// Config keys (all strings unless noted):
//
//	dir      (required): output directory. Created if absent.
//	run_id   (optional): identifier for this run. Default is the
//	                     sink's initialization unix-nano timestamp.
//
// The sink registers itself with sinks.Register under the name
// "parquet" on package import.
package parquet
