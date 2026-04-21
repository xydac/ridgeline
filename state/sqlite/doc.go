// Package sqlite provides a SQLite-backed implementation of
// pipeline.StateStore, plus a shared handle used by the credential
// store in package creds.
//
// A single Store instance owns one SQLite file on disk. The schema is
// created and migrated automatically on Open. The underlying driver is
// modernc.org/sqlite, a pure-Go translation that needs no CGO and no C
// toolchain at build time.
//
// Typical usage from a sync command:
//
//	store, err := sqlite.Open("~/.ridgeline/ridgeline.db")
//	if err != nil { return err }
//	defer store.Close()
//
//	_, err = pipeline.Run(ctx, conn, sink, store, pipeline.Request{
//	    Key: "myapp_gsc",
//	    // ...
//	})
//
// Concurrency: Store is safe for concurrent use. Reads and writes
// serialize through the underlying database/sql connection pool.
package sqlite
