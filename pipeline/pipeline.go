package pipeline

import (
	"context"
	"fmt"
	"log"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/sinks"
)

// DefaultBatchSize is used when Request.BatchSize is zero.
const DefaultBatchSize = 500

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
	// pipeline itself. Nil means log.Default().
	Logger *log.Logger
}

// StreamResult is the per-stream outcome of a Run.
type StreamResult struct {
	Records int
}

// Result summarizes a Run.
type Result struct {
	// Records is the total number of records written across all streams.
	Records int
	// States is the number of checkpoints persisted during this run.
	States int
	// PerStream breaks Records down by stream name.
	PerStream map[string]StreamResult
	// SchemaMessages counts schema announcements received. Useful for
	// tests that assert connectors emit schemas.
	SchemaMessages int
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
		logger = log.Default()
	}
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	state, err := store.Load(ctx, req.Key)
	if err != nil {
		return Result{}, fmt.Errorf("pipeline: load state: %w", err)
	}

	ch, err := conn.Extract(ctx, req.Config, req.Streams, state)
	if err != nil {
		return Result{}, fmt.Errorf("pipeline: extract: %w", err)
	}

	result := Result{PerStream: map[string]StreamResult{}}
	buffers := map[string][]connectors.Record{}

	// flushStream writes the buffered records for stream to the sink
	// and clears the buffer.
	flushStream := func(stream string) error {
		batch := buffers[stream]
		if len(batch) == 0 {
			return nil
		}
		if err := sink.Write(ctx, stream, batch); err != nil {
			return fmt.Errorf("sink.Write(%s): %w", stream, err)
		}
		sr := result.PerStream[stream]
		sr.Records += len(batch)
		result.PerStream[stream] = sr
		result.Records += len(batch)
		buffers[stream] = batch[:0]
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
					logger.Printf("[%s] %s: %s", req.Key, msg.Log.Level, msg.Log.Message)
				}
			case connectors.SchemaMsg:
				result.SchemaMessages++
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
