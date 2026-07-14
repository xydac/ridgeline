package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xydac/ridgeline/config"
)

const minimalValid = `
version: 1
state_path: /tmp/ridgeline.db
key_path: /tmp/ridgeline.key
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        config:
          records: 5
        streams: [pages, events]
        sink:
          type: jsonl
          options:
            dir: ./out
`

func TestParse_Minimal(t *testing.T) {
	f, err := config.Parse([]byte(minimalValid))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.Version != config.SchemaVersion {
		t.Fatalf("version: want %d, got %d", config.SchemaVersion, f.Version)
	}
	if f.StatePath != "/tmp/ridgeline.db" {
		t.Fatalf("StatePath: got %q", f.StatePath)
	}
	if got := f.ProductIDs(); len(got) != 1 || got[0] != "myapp" {
		t.Fatalf("ProductIDs: got %v", got)
	}
	p := f.Products["myapp"]
	if len(p.Connectors) != 1 {
		t.Fatalf("connectors: got %d", len(p.Connectors))
	}
	c := p.Connectors[0]
	if c.Name != "demo" || c.Type != "testsrc" || c.Sink.Type != "jsonl" {
		t.Fatalf("connector: got %+v", c)
	}
	if got, _ := c.Config["records"].(int); got != 5 {
		t.Fatalf("config.records: got %v", c.Config["records"])
	}
}

func TestParse_ExpandsHomePaths(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	src := `
version: 1
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        streams: [pages]
        sink:
          type: jsonl
`
	f, err := config.Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	wantState := filepath.Join(home, ".ridgeline", "ridgeline.db")
	if f.StatePath != wantState {
		t.Fatalf("default StatePath: want %q got %q", wantState, f.StatePath)
	}
	wantKey := filepath.Join(home, ".ridgeline", "key")
	if f.KeyPath != wantKey {
		t.Fatalf("default KeyPath: want %q got %q", wantKey, f.KeyPath)
	}
}

func TestParse_ExplicitTildePath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	src := `
version: 1
state_path: ~/custom.db
key_path: ~/custom.key
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        streams: [pages]
        sink:
          type: jsonl
`
	f, err := config.Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.StatePath != filepath.Join(home, "custom.db") {
		t.Fatalf("tilde expansion: got %q", f.StatePath)
	}
	if f.KeyPath != filepath.Join(home, "custom.key") {
		t.Fatalf("tilde expansion key: got %q", f.KeyPath)
	}
}

func TestParse_RejectsUnknownFields(t *testing.T) {
	src := `
version: 1
state_path: /tmp/x.db
key_path: /tmp/x.key
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        sink:
          type: jsonl
nonsense_field: true
`
	if _, err := config.Parse([]byte(src)); err == nil {
		t.Fatalf("expected error on unknown field")
	}
}

func TestParse_RejectsFutureVersion(t *testing.T) {
	src := `
version: 99
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        streams: [pages]
        sink:
          type: jsonl
`
	_, err := config.Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "version must be") {
		t.Fatalf("expected version error, got %v", err)
	}
}

func TestParse_RejectsMissingVersion(t *testing.T) {
	src := `
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        streams: [pages]
        sink:
          type: jsonl
`
	_, err := config.Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "version must be") {
		t.Fatalf("expected version error, got %v", err)
	}
}

func TestParse_RejectsZeroVersion(t *testing.T) {
	src := `
version: 0
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        streams: [pages]
        sink:
          type: jsonl
`
	_, err := config.Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "version must be") {
		t.Fatalf("expected version error, got %v", err)
	}
}

func TestParse_RejectsEmptyInput(t *testing.T) {
	for _, src := range []string{"", "   ", "\n\n\t\n"} {
		_, err := config.Parse([]byte(src))
		if err == nil {
			t.Fatalf("expected error for empty input %q, got nil", src)
		}
		if !strings.Contains(err.Error(), "empty") {
			t.Errorf("expected actionable empty-file message, got %v", err)
		}
	}
}

func TestParse_RejectsNoProducts(t *testing.T) {
	src := `version: 1`
	_, err := config.Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "products") {
		t.Fatalf("expected products error, got %v", err)
	}
}

func TestParse_RejectsConnectorMissingType(t *testing.T) {
	src := `
version: 1
products:
  myapp:
    connectors:
      - name: demo
        streams: [pages]
        sink:
          type: jsonl
`
	_, err := config.Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "type is required") {
		t.Fatalf("expected type-required error, got %v", err)
	}
}

func TestParse_RejectsSinkMissingType(t *testing.T) {
	src := `
version: 1
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        streams: [pages]
`
	_, err := config.Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "sink.type is required") {
		t.Fatalf("expected sink.type error, got %v", err)
	}
}

func TestParse_RejectsEmptyStreams(t *testing.T) {
	src := `
version: 1
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        streams: []
        sink: { type: jsonl }
`
	_, err := config.Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "streams must not be empty") {
		t.Fatalf("expected empty-streams error, got %v", err)
	}
}

func TestParse_RejectsMissingStreams(t *testing.T) {
	src := `
version: 1
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        sink: { type: jsonl }
`
	_, err := config.Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "streams must not be empty") {
		t.Fatalf("expected empty-streams error, got %v", err)
	}
}

func TestParse_RejectsDuplicateConnectorNames(t *testing.T) {
	src := `
version: 1
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        streams: [pages]
        sink: { type: jsonl }
      - name: demo
        type: testsrc
        streams: [pages]
        sink: { type: jsonl }
`
	_, err := config.Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestParse_RejectsBadProductID(t *testing.T) {
	src := `
version: 1
products:
  "my app":
    connectors:
      - name: demo
        type: testsrc
        streams: [pages]
        sink: { type: jsonl }
`
	_, err := config.Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "whitespace") {
		t.Fatalf("expected whitespace error, got %v", err)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := config.Load("/tmp/definitely-not-a-ridgeline-config-xyz.yaml")
	if err == nil {
		t.Fatalf("expected error for missing file")
	}
}

func TestLoad_RealFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ridgeline.yaml")
	if err := os.WriteFile(path, []byte(minimalValid), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if f.Products["myapp"].Connectors[0].Name != "demo" {
		t.Fatalf("unexpected: %+v", f)
	}
}

func TestStateKey(t *testing.T) {
	if got := config.StateKey("myapp", "demo"); got != "myapp/demo" {
		t.Fatalf("StateKey: got %q", got)
	}
}

func TestParse_YAMLTypeErrorNoGoTypes(t *testing.T) {
	// products expects a mapping; passing a scalar triggers a yaml.TypeError
	// that historically leaked "map[string]config.Product" and "!!str".
	src := `
version: 1
state_path: /tmp/x.db
products: broken_not_a_map
`
	_, err := config.Parse([]byte(src))
	if err == nil {
		t.Fatalf("expected parse error")
	}
	msg := err.Error()
	for _, leak := range []string{"map[string]", "config.Product", "!!str", "!!seq", "!!map"} {
		if strings.Contains(msg, leak) {
			t.Errorf("error leaks Go internal %q: %s", leak, msg)
		}
	}
}

func TestParse_YAMLTypeErrorNoGoTypesOnList(t *testing.T) {
	// connectors expects a list; passing a scalar leaks "[]config.ConnectorInstance"
	src := `
version: 1
state_path: /tmp/x.db
products:
  myapp:
    connectors: not_a_list
`
	_, err := config.Parse([]byte(src))
	if err == nil {
		t.Fatalf("expected parse error")
	}
	msg := err.Error()
	for _, leak := range []string{"[]config.", "config.ConnectorInstance", "!!str"} {
		if strings.Contains(msg, leak) {
			t.Errorf("error leaks Go internal %q: %s", leak, msg)
		}
	}
}

func TestLoad_READMEExamplesParseClean(t *testing.T) {
	// Each file in testdata/ is a verbatim copy of a README example block.
	// This test ensures every example parses without error so a user can
	// copy-paste from the README and have it work.
	files := []string{
		"testdata/readme_posthog.yaml",
		"testdata/readme_hackernews.yaml",
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatalf("read %s: %v", f, err)
			}
			if _, err := config.Parse(raw); err != nil {
				t.Fatalf("Parse(%s): %v", f, err)
			}
		})
	}
}

func TestParse_UnknownFieldNoGoType(t *testing.T) {
	// KnownFields(true) emits "field X not found in type config.T"
	src := `
version: 1
state_path: /tmp/x.db
key_path: /tmp/x.key
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        streams: [pages]
        sink:
          type: jsonl
nonsense_field: true
`
	_, err := config.Parse([]byte(src))
	if err == nil {
		t.Fatalf("expected parse error")
	}
	msg := err.Error()
	if strings.Contains(msg, "config.") || strings.Contains(msg, "in type") {
		t.Errorf("error leaks Go type name: %s", msg)
	}
}

// TestParse_CommentsOnlyYAML verifies that a YAML file containing only comments
// returns an actionable message instead of the raw "config: parse: EOF" (F-062).
func TestParse_CommentsOnlyYAML(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"single comment", "# hello\n"},
		{"multiple comments", "# line1\n# line2\n# line3\n"},
		{"comment with blank lines", "\n# comment\n\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse([]byte(tc.input))
			if err == nil {
				t.Fatal("expected error for comment-only config, got nil")
			}
			msg := err.Error()
			if !strings.Contains(msg, "empty") && !strings.Contains(msg, "comments") {
				t.Errorf("want actionable message mentioning 'empty' or 'comments', got %q", msg)
			}
			if strings.Contains(msg, "EOF") {
				t.Errorf("should not expose raw EOF to user, got %q", msg)
			}
		})
	}
}
