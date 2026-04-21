package parquet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	pq "github.com/parquet-go/parquet-go"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/manifest"
	"github.com/xydac/ridgeline/sinks"
)

// Name is the sink name registered with the sinks package.
const Name = "parquet"

func init() {
	sinks.Register(New())
}

// Row is the fixed Parquet schema used by this sink. It is exported so
// that downstream Go code can read the files back with a matching
// GenericReader if it wants to stay in-process instead of going
// through DuckDB.
type Row struct {
	Stream    string `parquet:"stream,snappy"`
	Timestamp int64  `parquet:"timestamp,snappy"`
	DataJSON  string `parquet:"data_json,snappy"`
}

// New returns an uninitialized Sink. Call Init before Write.
func New() *Sink { return &Sink{} }

// Sink writes records as Parquet files under a per-run directory.
type Sink struct {
	mu       sync.Mutex
	dir      string
	runID    string
	manifest *manifest.Store
	streams  map[string]*streamFile
	inited   bool
	closed   bool
}

type streamFile struct {
	path      string
	file      *os.File
	writer    *pq.GenericWriter[Row]
	rows      int64
	startTime time.Time
	endTime   time.Time
}

// Name returns the registered sink name.
func (s *Sink) Name() string { return Name }

// Init opens the sink for writing. cfg must contain a "dir" entry.
// Unknown option keys are rejected at Init with a did-you-mean hint,
// so a mistyped "dirr" surfaces here instead of downstream as
// "dir is required".
func (s *Sink) Init(_ context.Context, cfg sinks.SinkConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inited {
		return fmt.Errorf("parquet: Init called twice")
	}
	if err := sinks.CheckUnknownKeys(cfg, "dir", "run_id"); err != nil {
		return fmt.Errorf("parquet: %w", err)
	}
	dir := cfg.String("dir")
	if dir == "" {
		return fmt.Errorf("parquet: config key %q is required", "dir")
	}
	runID := cfg.String("run_id")
	if runID == "" {
		runID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	runDir := filepath.Join(dir, runID)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("parquet: %w", err)
	}
	s.dir = dir
	s.runID = runID
	s.manifest = manifest.NewStore(filepath.Join(dir, "manifest.json"))
	s.streams = map[string]*streamFile{}
	s.inited = true
	return nil
}

// Dir returns the root output directory. Useful for tests and callers
// that want to locate the manifest after Init.
func (s *Sink) Dir() string { return s.dir }

// RunID returns the run id used for this sink's output subdirectory.
func (s *Sink) RunID() string { return s.runID }

// Write appends records to the stream's Parquet file, opening the
// file lazily on first write.
func (s *Sink) Write(_ context.Context, stream string, records []connectors.Record) error {
	if stream == "" {
		return fmt.Errorf("parquet: empty stream name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inited {
		return fmt.Errorf("parquet: Write before Init")
	}
	if s.closed {
		return fmt.Errorf("parquet: Write after Close")
	}
	sf, err := s.streamFileLocked(stream)
	if err != nil {
		return err
	}
	rows := make([]Row, 0, len(records))
	for _, r := range records {
		data := r.Data
		if data == nil {
			data = map[string]any{}
		}
		b, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("parquet: marshal %s: %w", stream, err)
		}
		rows = append(rows, Row{
			Stream:    stream,
			Timestamp: r.Timestamp.UTC().UnixMicro(),
			DataJSON:  string(b),
		})
		if sf.startTime.IsZero() || r.Timestamp.Before(sf.startTime) {
			sf.startTime = r.Timestamp
		}
		if r.Timestamp.After(sf.endTime) {
			sf.endTime = r.Timestamp
		}
	}
	n, err := sf.writer.Write(rows)
	if err != nil {
		return fmt.Errorf("parquet: write %s: %w", stream, err)
	}
	sf.rows += int64(n)
	return nil
}

func (s *Sink) streamFileLocked(stream string) (*streamFile, error) {
	if sf, ok := s.streams[stream]; ok {
		return sf, nil
	}
	rel := filepath.Join(s.runID, stream+".parquet")
	abs := filepath.Join(s.dir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, fmt.Errorf("parquet: stream %s: %w", stream, err)
	}
	f, err := os.OpenFile(abs, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("parquet: stream %s: %w", stream, err)
	}
	w := pq.NewGenericWriter[Row](f)
	sf := &streamFile{path: rel, file: f, writer: w}
	s.streams[stream] = sf
	return sf, nil
}

// Flush forces the current Parquet row group to close on each open
// stream. It does not close the file itself; Write may be called
// again afterward and will land in a new row group.
func (s *Sink) Flush(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inited {
		return nil
	}
	for stream, sf := range s.streams {
		if err := sf.writer.Flush(); err != nil {
			return fmt.Errorf("parquet: flush %s: %w", stream, err)
		}
	}
	return nil
}

// Close finalizes every open Parquet writer, closes the underlying
// files, and appends a manifest entry per non-empty stream. After
// Close the sink cannot be reused.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inited {
		return nil
	}
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	for stream, sf := range s.streams {
		if err := sf.writer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("parquet: close writer %s: %w", stream, err)
		}
		info, statErr := sf.file.Stat()
		if err := sf.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("parquet: close file %s: %w", stream, err)
		}
		if sf.rows > 0 {
			var size int64
			if statErr == nil {
				size = info.Size()
			}
			part := manifest.Partition{
				Stream:    stream,
				Path:      sf.path,
				Format:    "parquet",
				Rows:      sf.rows,
				SizeBytes: size,
				StartTime: sf.startTime.UTC(),
				EndTime:   sf.endTime.UTC(),
			}
			if err := s.manifest.Append(part); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	s.streams = nil
	return firstErr
}
