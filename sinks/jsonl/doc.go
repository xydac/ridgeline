// Package jsonl provides a Sink that writes records as JSON-lines files.
//
// One file per (run, stream) combination is written under the
// configured dir, and a manifest.json at the dir root lists every file
// written. The sink is intentionally low-ceremony: no compression, no
// partitioning scheme beyond the run id, no schema enforcement. Its
// job is to give a Connector somewhere to land records quickly so that
// the end-to-end pipeline can be smoke-tested without the heavier
// Parquet sink.
//
// Config keys (all strings unless noted):
//
//	dir      (required): output directory. Created if absent.
//	run_id   (optional): identifier for this run. Default is the
//	                     sink's initialization unix-nano timestamp.
//
// The sink registers itself with sinks.Register under the name "jsonl"
// on package import.
package jsonl
