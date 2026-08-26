package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// SchemaVersion is the currently supported top-level version field.
// Every ridgeline.yaml must declare it explicitly; Load rejects
// missing, zero, and non-equal values.
const SchemaVersion = 1

// File is the in-memory shape of a parsed ridgeline.yaml.
type File struct {
	// Version is the schema version the file was written against.
	// It must equal SchemaVersion. A missing or zero value is rejected
	// at Load so a future v2 binary can break compatibility cleanly
	// instead of silently accepting today's unversioned configs.
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

	// Memory configures Business Memory anomaly detection.
	// All fields are optional; zero values apply the documented defaults.
	Memory MemoryConfig `yaml:"memory"`
}

// MemoryConfig holds global and per-metric anomaly detection settings.
type MemoryConfig struct {
	// AnomalyK is the stddev multiplier used to flag anomalies.
	// A value of 2.5 flags observations more than 2.5 standard deviations
	// from the rolling mean. Defaults to 2.5 when zero.
	AnomalyK float64 `yaml:"anomaly_k"`

	// MinSamples is the minimum number of baseline samples required
	// before a metric is eligible for anomaly detection.
	// Defaults to 14 when zero.
	MinSamples int `yaml:"min_samples"`

	// MetricOverrides maps a fully-qualified metric name to per-metric
	// overrides. Keys must match the fq_name in the Business Memory
	// catalog (e.g. "plausible.daily.visitors").
	MetricOverrides map[string]MetricAnomalyOverride `yaml:"metric_overrides"`
}

// MetricAnomalyOverride overrides anomaly detection parameters for one metric.
type MetricAnomalyOverride struct {
	// AnomalyK overrides the global AnomalyK for this metric.
	// Ignored when zero (global value applies).
	AnomalyK float64 `yaml:"anomaly_k"`
	// MinSamples overrides the global MinSamples for this metric.
	// Ignored when zero (global value applies).
	MinSamples int `yaml:"min_samples"`
}

// AnomalyKFor returns the effective k threshold for fqName, applying any
// per-metric override. Falls back to the global value, then the package default.
func (m MemoryConfig) AnomalyKFor(fqName string) float64 {
	if ov, ok := m.MetricOverrides[fqName]; ok && ov.AnomalyK > 0 {
		return ov.AnomalyK
	}
	if m.AnomalyK > 0 {
		return m.AnomalyK
	}
	return 2.5
}

// MinSamplesFor returns the effective minimum sample count for fqName, applying
// any per-metric override. Falls back to the global value, then the package default.
func (m MemoryConfig) MinSamplesFor(fqName string) int {
	if ov, ok := m.MetricOverrides[fqName]; ok && ov.MinSamples > 0 {
		return ov.MinSamples
	}
	if m.MinSamples > 0 {
		return m.MinSamples
	}
	return 14
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

	// Streams is the list of stream names to pull. Must be non-empty;
	// a connector that processes zero streams is almost always a typo.
	// Discovery-driven "pull every stream" semantics are not yet
	// supported and will be gated behind an explicit keyword when added.
	Streams []string `yaml:"streams"`

	// Sink is the destination for records emitted by this connector.
	Sink SinkRef `yaml:"sink"`

	// Enrichers is an optional list of transforms applied to each
	// record batch after extraction and before the sink writes them.
	// Enrichers run in the order listed; an empty slice is a no-op.
	Enrichers []EnricherRef `yaml:"enrichers"`
}

// SinkRef selects a registered sink and carries its options.
type SinkRef struct {
	// Type is the registered sink type (for example "jsonl" or
	// "parquet"). Must match a sink registered with package sinks.
	Type string `yaml:"type"`

	// Options is the sink-specific configuration map.
	Options map[string]any `yaml:"options"`
}

// EnricherRef selects a registered enricher and carries its options.
// Enrichers are applied in order after extraction and before the sink
// writes each batch of records.
type EnricherRef struct {
	// Type is the registered enricher type (for example "url_host").
	// Must match an enricher registered with package enrichers.
	Type string `yaml:"type"`

	// Config is the enricher-specific configuration map.
	Config map[string]any `yaml:"config"`
}

// LoadCreds reads path, applies defaults, and validates only the fields
// required for credential operations (version, state_path, key_path).
// Products are not required, so a config that has no connectors configured
// yet is still accepted. Use this for all ridgeline creds subcommands.
func LoadCreds(path string) (*File, error) {
	if path == "" {
		return nil, fmt.Errorf("config: path must not be empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return ParseCreds(raw)
}

// ParseCreds is like Parse but skips the products validation so a minimal
// config (version + state_path + key_path only) is accepted.
func ParseCreds(b []byte) (*File, error) {
	if strings.TrimSpace(string(b)) == "" {
		return nil, fmt.Errorf("config: file is empty; add 'version: 1' and at least one product under 'products:'")
	}
	var f File
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("config: file is empty or contains only comments; add 'version: 1' and at least one product under 'products:'")
		}
		return nil, translateYAMLErr(err)
	}
	if err := f.applyDefaults(); err != nil {
		return nil, err
	}
	if f.Version != SchemaVersion {
		return nil, fmt.Errorf("config: version must be %d (got %d); add 'version: %d' at the top of the file", SchemaVersion, f.Version, SchemaVersion)
	}
	return &f, nil
}

// Load reads path, expands paths beginning with ~/, applies defaults,
// and validates the result. The returned File is safe to use directly;
// no further defaulting is required.
//
// Relative state_path and key_path values are resolved against the
// directory that contains path, not the process working directory. This
// matches how users configure these fields: relative to the config file,
// not relative to wherever they happen to invoke the binary.
func Load(path string) (*File, error) {
	if path == "" {
		return nil, fmt.Errorf("config: path must not be empty")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("config: resolve path: %w", err)
	}
	raw, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	f, err := Parse(raw)
	if err != nil {
		return nil, err
	}
	configDir := filepath.Dir(absPath)
	if !filepath.IsAbs(f.StatePath) {
		f.StatePath = filepath.Join(configDir, f.StatePath)
	}
	if !filepath.IsAbs(f.KeyPath) {
		f.KeyPath = filepath.Join(configDir, f.KeyPath)
	}
	return f, nil
}

// Parse decodes b as YAML and validates it. Callers who want to
// embed the config in a larger document or read from a non-file
// source can use Parse directly.
func Parse(b []byte) (*File, error) {
	if strings.TrimSpace(string(b)) == "" {
		return nil, fmt.Errorf("config: file is empty; add 'version: 1' and at least one product under 'products:'")
	}
	var f File
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("config: file is empty or contains only comments; add 'version: 1' and at least one product under 'products:'")
		}
		return nil, translateYAMLErr(err)
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
	if f.Version != SchemaVersion {
		return fmt.Errorf("config: version must be %d (got %d); add 'version: %d' at the top of the file", SchemaVersion, f.Version, SchemaVersion)
	}
	if len(f.Products) == 0 {
		return fmt.Errorf("config: at least one product is required under products:")
	}
	if f.Memory.AnomalyK < 0 {
		return fmt.Errorf("config: memory.anomaly_k must be a positive number (got %g); omit the field to use the default of 2.5", f.Memory.AnomalyK)
	}
	if f.Memory.MinSamples < 0 {
		return fmt.Errorf("config: memory.min_samples must be a non-negative integer (got %d); omit the field to use the default of 14", f.Memory.MinSamples)
	}
	for key, ov := range f.Memory.MetricOverrides {
		if key == "" {
			return fmt.Errorf("config: memory.metric_overrides: metric key must not be empty")
		}
		if ov.AnomalyK < 0 {
			return fmt.Errorf("config: memory.metric_overrides[%q]: anomaly_k must be a positive number (got %g)", key, ov.AnomalyK)
		}
		if ov.MinSamples < 0 {
			return fmt.Errorf("config: memory.metric_overrides[%q]: min_samples must be a non-negative integer (got %d)", key, ov.MinSamples)
		}
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
			where := fmt.Sprintf("product %q connector #%d", id, i+1)
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
			if len(c.Streams) == 0 {
				return fmt.Errorf("config: %s (%q): streams must not be empty", where, c.Name)
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

// translateYAMLErr converts a yaml.TypeError into a Ridgeline-vocabulary
// error that does not expose Go type names or yaml internal tags. For any
// other error type it falls back to a generic "config: parse" wrap.
func translateYAMLErr(err error) error {
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		return fmt.Errorf("config: parse: %w", err)
	}
	msgs := make([]string, 0, len(te.Errors))
	for _, m := range te.Errors {
		msgs = append(msgs, cleanYAMLMsg(m))
	}
	if len(msgs) == 1 {
		return fmt.Errorf("config: %s", msgs[0])
	}
	return fmt.Errorf("config: multiple errors in config file:\n  %s", strings.Join(msgs, "\n  "))
}

var (
	reGoMapType    = regexp.MustCompile(`map\[string\][^\s,)]*`)
	reGoSliceType  = regexp.MustCompile(`\[][^\s,)]*`)
	reUnknownField = regexp.MustCompile(`\bnot found in type [^\s,]+`)
)

var yamlTagNames = map[string]string{
	"!!str":       "a string",
	"!!seq":       "a list",
	"!!map":       "a mapping",
	"!!int":       "an integer",
	"!!bool":      "a boolean",
	"!!float":     "a number",
	"!!null":      "null",
	"!!binary":    "binary data",
	"!!timestamp": "a timestamp",
	"!!merge":     "a merge key",
}

func cleanYAMLMsg(msg string) string {
	// "field X not found in type pkg.T" -> "field X is not recognized"
	msg = reUnknownField.ReplaceAllString(msg, "is not recognized")
	// Replace !!tag with human-readable type name.
	for tag, name := range yamlTagNames {
		msg = strings.ReplaceAll(msg, tag+" ", name+" ")
		msg = strings.ReplaceAll(msg, tag, name)
	}
	// Replace Go composite type names.
	msg = reGoMapType.ReplaceAllString(msg, "a mapping")
	msg = reGoSliceType.ReplaceAllString(msg, "a list")
	msg = strings.ReplaceAll(msg, "interface {}", "a value")
	return strings.TrimRight(msg, " ")
}

// applyDefaults fills in empty optional fields with their documented
// defaults and expands leading ~/ in path-valued fields.
func (f *File) applyDefaults() error {
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
