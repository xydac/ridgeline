package jsonl

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/manifest"
	"github.com/xydac/ridgeline/sinks"
)

// Name is the sink name registered with the sinks package.
const Name = "jsonl"

func init() {
	sinks.Register(New())
}

// New returns an uninitialized Sink. Call Init before Write.
func New() *Sink { return &Sink{} }

// Sink writes records as JSON-lines files.
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
	writer    *bufio.Writer
	rows      int64
	bytes     int64
	startTime time.Time
	endTime   time.Time
}

// Name returns the registered sink name.
func (s *Sink) Name() string { return Name }

// Init opens the sink for writing. cfg must contain a "dir" entry.
func (s *Sink) Init(_ context.Context, cfg sinks.SinkConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inited {
		return fmt.Errorf("jsonl: Init called twice")
	}
	dir := cfg.String("dir")
	if dir == "" {
		return fmt.Errorf("jsonl: config key %q is required", "dir")
	}
	runID := cfg.String("run_id")
	if runID == "" {
		runID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	runDir := filepath.Join(dir, runID)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("jsonl: mkdir %s: %w", runDir, err)
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

// Write appends records for stream as JSON-lines to the stream's file,
// opening the file lazily on first write.
func (s *Sink) Write(_ context.Context, stream string, records []connectors.Record) error {
	if stream == "" {
		return fmt.Errorf("jsonl: empty stream name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inited {
		return fmt.Errorf("jsonl: Write before Init")
	}
	if s.closed {
		return fmt.Errorf("jsonl: Write after Close")
	}
	sf, err := s.streamFileLocked(stream)
	if err != nil {
		return err
	}
	for _, r := range records {
		payload := map[string]any{
			"stream":    stream,
			"timestamp": r.Timestamp,
			"data":      r.Data,
		}
		line, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("jsonl: marshal record: %w", err)
		}
		n, err := sf.writer.Write(line)
		if err != nil {
			return fmt.Errorf("jsonl: write %s: %w", stream, err)
		}
		sf.bytes += int64(n)
		if err := sf.writer.WriteByte('\n'); err != nil {
			return fmt.Errorf("jsonl: write newline: %w", err)
		}
		sf.bytes++
		sf.rows++
		if sf.startTime.IsZero() || r.Timestamp.Before(sf.startTime) {
			sf.startTime = r.Timestamp
		}
		if r.Timestamp.After(sf.endTime) {
			sf.endTime = r.Timestamp
		}
	}
	return nil
}

func (s *Sink) streamFileLocked(stream string) (*streamFile, error) {
	if sf, ok := s.streams[stream]; ok {
		return sf, nil
	}
	rel := filepath.Join(s.runID, stream+".jsonl")
	abs := filepath.Join(s.dir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, fmt.Errorf("jsonl: mkdir for %s: %w", stream, err)
	}
	f, err := os.OpenFile(abs, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("jsonl: open %s: %w", abs, err)
	}
	sf := &streamFile{path: rel, file: f, writer: bufio.NewWriter(f)}
	s.streams[stream] = sf
	return sf, nil
}

// Flush durably writes any buffered records to disk. It does NOT close
// stream files; subsequent Writes continue appending in memory until
// the next Flush or Close.
func (s *Sink) Flush(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for stream, sf := range s.streams {
		if err := sf.writer.Flush(); err != nil {
			return fmt.Errorf("jsonl: flush %s: %w", stream, err)
		}
		if err := sf.file.Sync(); err != nil {
			return fmt.Errorf("jsonl: sync %s: %w", stream, err)
		}
	}
	return nil
}

// Close flushes every open stream, appends a manifest entry for each,
// and releases file handles. After Close the sink cannot be reused.
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
		if err := sf.writer.Flush(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("jsonl: flush %s: %w", stream, err)
		}
		if err := sf.file.Sync(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("jsonl: sync %s: %w", stream, err)
		}
		if err := sf.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("jsonl: close %s: %w", stream, err)
		}
		if sf.rows > 0 {
			part := manifest.Partition{
				Stream:    stream,
				Path:      sf.path,
				Format:    "jsonl",
				Rows:      sf.rows,
				SizeBytes: sf.bytes,
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
