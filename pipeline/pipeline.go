package pipeline

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/enrichers"
	"github.com/xydac/ridgeline/sinks"
)

// defaultLogger writes connector log messages to stderr without the
// stdlib "YYYY/MM/DD HH:MM:SS" prefix, so a `warn: [key] level: msg`
// line lines up with the rest of the CLI's plain output instead of
// standing out as the only timestamped line on the terminal.
var defaultLogger = log.New(os.Stderr, "", 0)

// DefaultBatchSize is used when Request.BatchSize is zero.
const DefaultBatchSize = 500

// StreamDeclarer is implemented by sinks that accept per-stream
// schema declarations. A sink that implements this interface receives
// one DeclareStream call per requested stream that the connector has
// published a non-empty Schema for, before the first Extract call.
// Sinks that do not implement StreamDeclarer keep their default
// behavior.
type StreamDeclarer interface {
	DeclareStream(stream string, schema connectors.Schema)
}

// StreamSpecDeclarer is an optional extension of StreamDeclarer. Sinks
// that implement this interface receive the full StreamSpec (including
// semantic metadata) in addition to the column schema, enabling them to
// persist richer metadata alongside the data files.
type StreamSpecDeclarer interface {
	DeclareStreamSpec(stream string, spec connectors.StreamSpec)
}

// EnricherStep pairs a registered enricher with its per-connector config.
// The pipeline applies steps in slice order to each record batch before
// passing the batch to the sink.
type EnricherStep struct {
	E   enrichers.Enricher
	Cfg enrichers.EnrichConfig
}

// Request describes one pipeline run.
type Request struct {
	// Key identifies this connector instance for state persistence.
	// Typically the connector instance name from the config
	// (for example "gsc_mainsite").
	Key string
	// Config is the connector-specific configuration.
	Config connectors.ConnectorConfig
	// Streams is the set of streams the connector should extract.
	Streams []connectors.Stream
	// BatchSize caps the number of records passed to Sink.Write in a
	// single call. Zero means DefaultBatchSize.
	BatchSize int
	// Logger receives log messages emitted by the connector and the
	// pipeline itself. Nil means a prefix-free stderr logger so warn
	// lines match the CLI's plain output instead of carrying the stdlib
	// date/time prefix.
	Logger *log.Logger
	// Enrichers is the ordered list of transforms applied to each
	// record batch before the sink writes them. An empty slice is a
	// no-op.
	Enrichers []EnricherStep
}

// StreamResult is the per-stream outcome of a Run.
type StreamResult struct {
	Records int
}

// MetricPoint is one (timestamp, column, value) observation captured from a
// metric-kind stream during a pipeline run. The timestamp comes from the
// record's own Timestamp field (set by the connector from its primary key),
// not the ingest time.
type MetricPoint struct {
	At     time.Time
	Column string
	Value  float64
}

// Result summarizes a Run.
type Result struct {
	// Records is the total number of records extracted from the connector
	// and passed to the sink (before any sink-side partition pruning).
	Records int
	// Persisted is the number of records the sink actually wrote to
	// durable storage. This is less than Records when the sink prunes
	// already-covered partitions on a re-run.
	Persisted int
	// States is the number of checkpoints persisted during this run.
	States int
	// PerStream breaks Records down by stream name.
	PerStream map[string]StreamResult
	// SchemaMessages counts schema announcements received. Useful for
	// tests that assert connectors emit schemas.
	SchemaMessages int
	// ObservedSchemas holds the most-recently-announced schema per stream.
	// Populated from SchemaMsg messages during extraction; callers can use
	// this to access runtime-declared kind and column semantics for
	// connectors (e.g. external) whose static Spec().Streams is empty.
	ObservedSchemas map[string]connectors.Schema
	// Skipped is the number of records dropped before reaching the
	// pipeline (e.g. external RECORD messages with a missing data field).
	Skipped int
	// LastObserved holds the last record seen per stream. Callers can use
	// this to sample metric values without a post-sync storage query.
	// Nil when no records were observed for that stream.
	LastObserved map[string]connectors.Record
	// MetricTimeSeries holds all per-record metric observations for streams
	// that declare metric columns. Key is stream name. Each MetricPoint
	// carries the record's declared timestamp (not the ingest time) so the
	// Business Memory layer can record one observation per record day rather
	// than one per sync run.
	MetricTimeSeries map[string][]MetricPoint
}

// Run drives one extraction from conn through sink, persisting state
// via store. Run does not call sink.Init or sink.Close; the caller owns
// the sink's lifecycle.
//
// Run returns when the connector's Message channel closes, when the
// connector returns an error from Extract, or when ctx is cancelled.
// In the cancelled case Run still returns the partial Result along with
// ctx.Err().
func Run(ctx context.Context, conn connectors.Connector, sink sinks.Sink, store StateStore, req Request) (Result, error) {
	if conn == nil {
		return Result{}, fmt.Errorf("pipeline: nil Connector")
	}
	if sink == nil {
		return Result{}, fmt.Errorf("pipeline: nil Sink")
	}
	if store == nil {
		return Result{}, fmt.Errorf("pipeline: nil StateStore")
	}
	if req.Key == "" {
		return Result{}, fmt.Errorf("pipeline: Request.Key is required")
	}

	logger := req.Logger
	if logger == nil {
		logger = defaultLogger
	}
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	// Tag the sink with the connector name for per-connector manifest
	// attribution. A sink that does not implement ConnectorNamer is
	// unaffected; its partitions carry an empty Connector field.
	if cn, ok := sink.(sinks.ConnectorNamer); ok && req.Key != "" {
		cn.SetConnector(req.Key)
	}

	// Declare typed schemas on the sink when the connector has
	// published them. A sink that does not implement StreamDeclarer
	// keeps the default {stream, timestamp, data_json} shape.
	if decl, ok := sink.(StreamDeclarer); ok {
		spec := conn.Spec()
		specByName := map[string]connectors.StreamSpec{}
		for _, ss := range spec.Streams {
			specByName[ss.Name] = ss
		}
		specDecl, hasSpecDecl := sink.(StreamSpecDeclarer)
		for _, rs := range req.Streams {
			ss, known := specByName[rs.Name]
			if !known {
				continue
			}
			if len(ss.Schema.Columns) > 0 {
				decl.DeclareStream(rs.Name, ss.Schema)
			}
			if hasSpecDecl {
				specDecl.DeclareStreamSpec(rs.Name, ss)
			}
		}
	}

	state, err := store.Load(ctx, req.Key)
	if err != nil {
		return Result{}, fmt.Errorf("pipeline: load state: %w", err)
	}

	ch, err := conn.Extract(ctx, req.Config, req.Streams, state)
	if err != nil {
		return Result{}, fmt.Errorf("pipeline: extract: %w", err)
	}

	// Build a per-stream index of metric column names so flushStream can
	// capture per-record observations without re-scanning the spec on each call.
	metricCols := map[string][]string{}
	for _, ss := range conn.Spec().Streams {
		for _, col := range ss.Schema.Columns {
			if col.Semantics != nil {
				metricCols[ss.Name] = append(metricCols[ss.Name], col.Name)
			}
		}
	}

	result := Result{
		PerStream:        map[string]StreamResult{},
		LastObserved:     map[string]connectors.Record{},
		MetricTimeSeries: map[string][]MetricPoint{},
	}
	buffers := map[string][]connectors.Record{}

	// flushStream applies any configured enrichers to the buffered
	// records for stream, then writes the batch to the sink and clears
	// the buffer.
	flushStream := func(stream string) error {
		batch := buffers[stream]
		if len(batch) == 0 {
			return nil
		}
		for _, step := range req.Enrichers {
			var enrichErr error
			batch, enrichErr = step.E.Enrich(ctx, step.Cfg, batch)
			if enrichErr != nil {
				return fmt.Errorf("enricher %s: %w", step.E.Name(), enrichErr)
			}
		}
		n, err := sink.Write(ctx, stream, batch)
		if err != nil {
			return fmt.Errorf("sink.Write(%s): %w", stream, err)
		}
		sr := result.PerStream[stream]
		sr.Records += len(batch)
		result.PerStream[stream] = sr
		result.Records += len(batch)
		result.Persisted += n
		result.LastObserved[stream] = batch[len(batch)-1]

		// Capture per-record metric observations at each record's own
		// timestamp so callers can record historical data at its declared
		// date rather than at ingest time.
		if cols := metricCols[stream]; len(cols) > 0 {
			for _, rec := range batch {
				if rec.Timestamp.IsZero() {
					continue
				}
				for _, col := range cols {
					v, ok := asFloat64(rec.Data[col])
					if !ok {
						continue
					}
					result.MetricTimeSeries[stream] = append(
						result.MetricTimeSeries[stream],
						MetricPoint{At: rec.Timestamp, Column: col, Value: v},
					)
				}
			}
		}

		buffers[stream] = buffers[stream][:0]
		return nil
	}

	flushAll := func() error {
		for stream := range buffers {
			if err := flushStream(stream); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// Best-effort final flush before returning.
			_ = flushAll()
			return result, ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				// If ctx was cancelled, the channel close may be the
				// connector's response to that cancellation; surface
				// ctx.Err() rather than reporting clean completion.
				if err := ctx.Err(); err != nil {
					_ = flushAll()
					return result, err
				}
				if err := flushAll(); err != nil {
					return result, err
				}
				if err := sink.Flush(ctx); err != nil {
					return result, fmt.Errorf("sink.Flush: %w", err)
				}
				return result, nil
			}
			switch msg.Type {
			case connectors.RecordMsg:
				if msg.Record == nil {
					return result, fmt.Errorf("pipeline: RecordMsg with nil Record")
				}
				stream := msg.Record.Stream
				buffers[stream] = append(buffers[stream], *msg.Record)
				if len(buffers[stream]) >= batchSize {
					if err := flushStream(stream); err != nil {
						return result, err
					}
				}
			case connectors.StateMsg:
				if msg.State == nil {
					return result, fmt.Errorf("pipeline: StateMsg with nil State")
				}
				if err := flushAll(); err != nil {
					return result, err
				}
				if err := sink.Flush(ctx); err != nil {
					return result, fmt.Errorf("sink.Flush: %w", err)
				}
				if err := store.Save(ctx, req.Key, *msg.State); err != nil {
					return result, fmt.Errorf("store.Save: %w", err)
				}
				result.States++
			case connectors.LogMsg:
				if msg.Log != nil {
					logger.Printf("%s: [%s] %s", msg.Log.Level, req.Key, msg.Log.Message)
				}
			case connectors.SchemaMsg:
				result.SchemaMessages++
				if msg.Schema != nil {
					if result.ObservedSchemas == nil {
						result.ObservedSchemas = make(map[string]connectors.Schema)
					}
					result.ObservedSchemas[msg.Stream] = *msg.Schema
				}
			case connectors.SkippedMsg:
				result.Skipped++
			case connectors.ErrorMsg:
				// Terminal: discard any records still in memory,
				// do not flush, do not save state, surface the
				// error to the caller. Records already written
				// by prior flushes stay written; STATE messages
				// committed before this error stay committed.
				err := msg.Err
				if err == nil {
					err = fmt.Errorf("pipeline: ErrorMsg with nil Err")
				}
				return result, err
			}
		}
	}
}

// asFloat64 converts common numeric interface values to float64.
// Returns (value, true) for supported types; (0, false) otherwise.
// This avoids a dependency on the cmd layer's toFloat64 helper.
func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}
