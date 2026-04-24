// Package sinks defines the contract that every Ridgeline storage
// backend implements, plus a registry for native (in-binary) sinks.
//
// A sink receives Records grouped by stream and writes them to a
// destination: Parquet on local disk (default), DuckDB, Postgres, S3,
// a webhook, stdout, and so on. Multiple sinks can be configured to
// fan out the same data to several destinations.
//
// Records are imported from the connectors package to keep the data
// shape canonical across the pipeline.
//
// A minimal sink looks like this:
//
//	package counter
//
//	import (
//		"context"
//		"sync/atomic"
//
//		"github.com/xydac/ridgeline/connectors"
//		"github.com/xydac/ridgeline/sinks"
//	)
//
//	type Sink struct{ count atomic.Int64 }
//
//	func (s *Sink) Name() string { return "counter" }
//	func (s *Sink) Init(context.Context, sinks.SinkConfig) error { return nil }
//	func (s *Sink) Write(_ context.Context, _ string, recs []connectors.Record) error {
//		s.count.Add(int64(len(recs)))
//		return nil
//	}
//	func (s *Sink) Flush(context.Context) error { return nil }
//	func (s *Sink) Close() error                { return nil }
//
//	func init() { sinks.Register("counter", func() sinks.Sink { return &Sink{} }) }
package sinks
