package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// SchemaVersion is the currently supported top-level version field.
// Configs without a version default to SchemaVersion; configs with a
// higher version are rejected at Load.
const SchemaVersion = 1

// File is the in-memory shape of a parsed ridgeline.yaml.
type File struct {
	// Version is the schema version the file was written against.
	// Zero is treated as SchemaVersion for forward-compatibility with
	// older configs that predate the field.
	Version int `yaml:"version"`

	// StatePath points at the SQLite file that holds pipeline state
	// and credentials. Defaults to ~/.ridgeline/ridgeline.db if
	// empty at Load time.
	StatePath string `yaml:"state_path"`

	// KeyPath points at the hex-encoded AES-256 key file used by
	// package creds. Defaults to ~/.ridgeline/key if empty.
	KeyPath string `yaml:"key_path"`

	// Products maps a stable product id (used as the first segment
	// of state keys) to its connector and sink configuration.
	Products map[string]Product `yaml:"products"`
}

// Product groups connectors and sinks under a shared namespace.
type Product struct {
	// Connectors lists every connector instance configured for this
	// product. An empty list is a validation error.
	Connectors []ConnectorInstance `yaml:"connectors"`
}

// ConnectorInstance is one configured connector within a product.
type ConnectorInstance struct {
	// Name is the instance-unique identifier. It must be unique
	// within its product. Combined with the product id it forms the
	// pipeline state key used by package pipeline.
	Name string `yaml:"name"`

	// Type is the registered connector type (for example "testsrc"
	// or "gsc"). Must match a connector registered with package
	// connectors.
	Type string `yaml:"type"`

	// Config is the connector-specific options map. Its shape is
	// defined by the connector itself. Loaded as generic YAML so
	// every connector can validate it against its own schema.
	Config map[string]any `yaml:"config"`

	// Streams is the list of stream names to pull. Empty means pull
	// every stream the connector publishes.
	Streams []string `yaml:"streams"`

	// Sink is the destination for records emitted by this connector.
	Sink SinkRef `yaml:"sink"`
}

// SinkRef selects a registered sink and carries its options.
type SinkRef struct {
	// Type is the registered sink type (for example "jsonl" or
	// "parquet"). Must match a sink registered with package sinks.
	Type string `yaml:"type"`

	// Options is the sink-specific configuration map.
	Options map[string]any `yaml:"options"`
}

// Load reads path, expands paths beginning with ~/, applies defaults,
// and validates the result. The returned File is safe to use directly;
// no further defaulting is required.
func Load(path string) (*File, error) {
	if path == "" {
		return nil, fmt.Errorf("config: path must not be empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	return Parse(raw)
}

// Parse decodes b as YAML and validates it. Callers who want to
// embed the config in a larger document or read from a non-file
// source can use Parse directly.
func Parse(b []byte) (*File, error) {
	var f File
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	if err := f.applyDefaults(); err != nil {
		return nil, err
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return &f, nil
}

// Validate checks every required field and returns the first problem
// it finds. Validate is safe to call on a File built in memory; it
// does not apply defaults, so callers constructing a File directly
// must also call applyDefaults (unexported) or supply all fields.
func (f *File) Validate() error {
	if f.Version != 0 && f.Version != SchemaVersion {
		return fmt.Errorf("config: unsupported version %d (this binary understands %d)", f.Version, SchemaVersion)
	}
	if len(f.Products) == 0 {
		return fmt.Errorf("config: at least one product is required under products:")
	}
	for id, p := range f.Products {
		if id == "" {
			return fmt.Errorf("config: product id must not be empty")
		}
		if strings.ContainsAny(id, " \t/\\") {
			return fmt.Errorf("config: product id %q must not contain whitespace or slashes", id)
		}
		if len(p.Connectors) == 0 {
			return fmt.Errorf("config: product %q has no connectors", id)
		}
		seen := map[string]struct{}{}
		for i, c := range p.Connectors {
			where := fmt.Sprintf("product %q connector #%d", id, i)
			if c.Name == "" {
				return fmt.Errorf("config: %s: name is required", where)
			}
			if _, dup := seen[c.Name]; dup {
				return fmt.Errorf("config: %s: duplicate connector name %q within product", where, c.Name)
			}
			seen[c.Name] = struct{}{}
			if c.Type == "" {
				return fmt.Errorf("config: %s (%q): type is required", where, c.Name)
			}
			if c.Sink.Type == "" {
				return fmt.Errorf("config: %s (%q): sink.type is required", where, c.Name)
			}
		}
	}
	return nil
}

// StateKey returns the canonical state key for a connector instance
// within a product: "<product>/<connector>".
func StateKey(productID, connectorName string) string {
	return productID + "/" + connectorName
}

// ProductIDs returns every configured product id, sorted.
// Useful for deterministic iteration in the CLI.
func (f *File) ProductIDs() []string {
	ids := make([]string, 0, len(f.Products))
	for id := range f.Products {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// applyDefaults fills in empty optional fields with their documented
// defaults and expands leading ~/ in path-valued fields.
func (f *File) applyDefaults() error {
	if f.Version == 0 {
		f.Version = SchemaVersion
	}
	home, _ := os.UserHomeDir()
	expand := func(p string) string {
		if p == "" || home == "" {
			return p
		}
		if p == "~" {
			return home
		}
		if strings.HasPrefix(p, "~/") {
			return filepath.Join(home, p[2:])
		}
		return p
	}
	if f.StatePath == "" {
		if home == "" {
			return fmt.Errorf("config: state_path is empty and $HOME is unset; specify state_path explicitly")
		}
		f.StatePath = filepath.Join(home, ".ridgeline", "ridgeline.db")
	} else {
		f.StatePath = expand(f.StatePath)
	}
	if f.KeyPath == "" {
		if home == "" {
			return fmt.Errorf("config: key_path is empty and $HOME is unset; specify key_path explicitly")
		}
		f.KeyPath = filepath.Join(home, ".ridgeline", "key")
	} else {
		f.KeyPath = expand(f.KeyPath)
	}
	for id, p := range f.Products {
		for i := range p.Connectors {
			// Nothing to expand inside connector config today, but
			// leaving the loop for future fields that take paths.
			_ = p.Connectors[i]
		}
		f.Products[id] = p
	}
	return nil
}
