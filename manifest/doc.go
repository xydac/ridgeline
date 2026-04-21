// Package manifest records the set of data files a sink has written,
// so downstream readers (DuckDB, report generators, the sync checker)
// can locate partitions without walking the directory tree.
//
// A Manifest is a single JSON file, typically stored at the root of a
// sink's output directory. Writes are atomic: the Store renders to a
// tempfile then renames, so concurrent readers always see a fully
// formed manifest.
//
// Partitions record the stream, path, row count, size, and the time
// range they cover. The time range supports partition pruning: a query
// for records in [t0, t1) can skip any partition whose [StartTime,
// EndTime] does not overlap.
package manifest
