package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Version is the on-disk schema version for a Manifest. Incremented
// whenever the JSON layout changes in a backward-incompatible way.
const Version = 1

// Partition records one data file written by a sink.
type Partition struct {
	// Stream is the stream name this partition belongs to.
	Stream string `json:"stream"`
	// Path is the location of the data file. Conventionally relative
	// to the manifest file's directory, so the whole directory is
	// relocatable.
	Path string `json:"path"`
	// Format names the on-disk format (for example "jsonl", "parquet").
	Format string `json:"format"`
	// Rows is the number of records in the file.
	Rows int64 `json:"rows"`
	// SizeBytes is the file size on disk.
	SizeBytes int64 `json:"size_bytes"`
	// StartTime is the minimum record timestamp in the file. Zero when
	// the sink did not observe any timestamps.
	StartTime time.Time `json:"start_time"`
	// EndTime is the maximum record timestamp in the file. Zero when
	// the sink did not observe any timestamps.
	EndTime time.Time `json:"end_time"`
	// CreatedAt is when the file was finalized.
	CreatedAt time.Time `json:"created_at"`
}

// Manifest is the on-disk index of partitions for one output directory.
type Manifest struct {
	Version    int         `json:"version"`
	UpdatedAt  time.Time   `json:"updated_at"`
	Partitions []Partition `json:"partitions"`
}

// ForStream returns the partitions whose Stream equals name, in the
// order they were appended.
func (m *Manifest) ForStream(name string) []Partition {
	out := make([]Partition, 0)
	for _, p := range m.Partitions {
		if p.Stream == name {
			out = append(out, p)
		}
	}
	return out
}

// Store reads and writes a Manifest atomically to a file on disk.
// Store is safe for concurrent use.
type Store struct {
	// Path is the manifest file location. The parent directory must
	// exist before the first Save; Append creates it if missing.
	Path string

	mu sync.Mutex
}

// NewStore returns a Store that reads and writes a manifest at path.
func NewStore(path string) *Store {
	return &Store{Path: path}
}

// Load reads the manifest from disk. Load returns an empty Manifest
// (with Version set) when the file does not exist, so callers can
// treat a missing manifest as "no partitions yet" without a separate
// existence check.
func (s *Store) Load() (Manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

func (s *Store) loadLocked() (Manifest, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return Manifest{Version: Version}, nil
	}
	if err != nil {
		return Manifest{}, fmt.Errorf("manifest: read %s: %w", s.Path, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("manifest: parse %s: %w", s.Path, err)
	}
	if m.Version == 0 {
		m.Version = Version
	}
	return m, nil
}

// Save writes m atomically to s.Path. The parent directory must exist.
// Save updates m.UpdatedAt and m.Version before writing, so callers do
// not have to.
func (s *Store) Save(m Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked(m)
}

func (s *Store) saveLocked(m Manifest) error {
	m.Version = Version
	m.UpdatedAt = time.Now().UTC()
	if m.Partitions == nil {
		m.Partitions = []Partition{}
	}
	data, err := json.MarshalIndent(&m, "", "  ")
	if err != nil {
		return fmt.Errorf("manifest: marshal: %w", err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(s.Path)
	tmp, err := os.CreateTemp(dir, ".manifest-*.json")
	if err != nil {
		return fmt.Errorf("manifest: tempfile: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("manifest: write tempfile: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("manifest: sync tempfile: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("manifest: close tempfile: %w", err)
	}
	if err := os.Rename(tmpName, s.Path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("manifest: rename: %w", err)
	}
	return nil
}

// Append loads the manifest, appends p, and writes it back atomically.
// Append creates the parent directory when it does not yet exist.
func (s *Store) Append(p Partition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o755); err != nil {
		return fmt.Errorf("manifest: mkdir: %w", err)
	}
	m, err := s.loadLocked()
	if err != nil {
		return err
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now().UTC()
	}
	m.Partitions = append(m.Partitions, p)
	return s.saveLocked(m)
}
