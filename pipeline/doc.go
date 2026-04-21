// Package pipeline wires a Connector to a Sink and drives an ETL run.
//
// The pipeline is intentionally small: it pulls Messages from a
// Connector, batches Records by stream, hands each batch to a Sink,
// flushes the Sink on every StateMsg, and persists the emitted State to
// a StateStore once the flush succeeds. This "flush then save" order is
// the durability guarantee: a connector never sees a saved state it
// cannot recover from, because the records that justify the state are
// already on disk before the state is persisted.
//
// The pipeline does not own the Sink's lifecycle. Callers call Init and
// Close themselves so they can reuse a single Sink across multiple
// connector runs (for example, one Parquet sink receiving records from
// the GSC and Umami connectors back to back).
package pipeline
