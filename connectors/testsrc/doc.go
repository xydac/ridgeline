// Package testsrc provides a deterministic, zero-dependency connector
// that emits synthetic records. It is the source used by
// `ridgeline sync --dry-run` so a new user can observe the full
// pipeline without configuring a real data source.
//
// Streams:
//
//	pages  - synthetic pageview events
//	events - synthetic custom events
//
// Config keys:
//
//	records (int, optional): total records per stream. Default 5.
//
// The connector registers itself under the name "testsrc" on import.
package testsrc
