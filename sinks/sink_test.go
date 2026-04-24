package sinks

import (
	"context"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/xydac/ridgeline/connectors"
)

type fakeSink struct {
	name    string
	written atomic.Int64
	flushed atomic.Int64
	closed  atomic.Bool
}

func (f *fakeSink) Name() string                           { return f.name }
func (f *fakeSink) Init(context.Context, SinkConfig) error { return nil }
func (f *fakeSink) Write(_ context.Context, _ string, recs []connectors.Record) error {
	f.written.Add(int64(len(recs)))
	return nil
}
func (f *fakeSink) Flush(context.Context) error {
	f.flushed.Add(1)
	return nil
}
func (f *fakeSink) Close() error {
	f.closed.Store(true)
	return nil
}

func TestRegisterAndNew(t *testing.T) {
	t.Cleanup(reset)
	reset()

	Register("alpha", func() Sink { return &fakeSink{name: "alpha"} })
	Register("bravo", func() Sink { return &fakeSink{name: "bravo"} })

	got, err := New("alpha")
	if err != nil || got.Name() != "alpha" {
		t.Fatalf("New(alpha) = (%v, %v); want sink alpha", got, err)
	}
	// Each New call must return a fresh instance.
	other, err := New("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if got == other {
		t.Fatalf("New(alpha) returned the same pointer twice; want fresh instances")
	}
	if _, err := New("missing"); err == nil {
		t.Fatalf("New(missing) returned nil error; want not-registered error")
	}
	if want, got := []string{"alpha", "bravo"}, List(); !reflect.DeepEqual(want, got) {
		t.Fatalf("List() = %v; want %v", got, want)
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	t.Cleanup(reset)
	reset()
	Register("dup", func() Sink { return &fakeSink{name: "dup"} })
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on duplicate Register")
		}
	}()
	Register("dup", func() Sink { return &fakeSink{name: "dup"} })
}

func TestRegisterNilFactoryPanics(t *testing.T) {
	t.Cleanup(reset)
	reset()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on nil factory")
		}
	}()
	Register("x", nil)
}

func TestRegisterEmptyNamePanics(t *testing.T) {
	t.Cleanup(reset)
	reset()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on empty-name Register")
		}
	}()
	Register("", func() Sink { return &fakeSink{name: "x"} })
}

func TestSinkConfigAccessors(t *testing.T) {
	cfg := SinkConfig{
		"path":     "./data",
		"size":     float64(100),
		"size_int": 25,
		"enabled":  true,
	}
	if got := cfg.String("path"); got != "./data" {
		t.Errorf("String(path) = %q; want ./data", got)
	}
	if got := cfg.String("missing"); got != "" {
		t.Errorf("String(missing) = %q; want empty", got)
	}
	if got := cfg.Int("size", 0); got != 100 {
		t.Errorf("Int(size) = %d; want 100", got)
	}
	if got := cfg.Int("size_int", 0); got != 25 {
		t.Errorf("Int(size_int) = %d; want 25", got)
	}
	if got := cfg.Int("missing", 7); got != 7 {
		t.Errorf("Int(missing, 7) = %d; want 7", got)
	}
	if got := cfg.Bool("enabled", false); !got {
		t.Errorf("Bool(enabled) = %v; want true", got)
	}
	if got := cfg.Bool("missing", true); !got {
		t.Errorf("Bool(missing, true) = %v; want true", got)
	}
}

func TestCheckUnknownKeys(t *testing.T) {
	if err := CheckUnknownKeys(SinkConfig{"dir": "/tmp"}, "dir", "run_id"); err != nil {
		t.Fatalf("known-only keys should pass, got %v", err)
	}
	if err := CheckUnknownKeys(nil, "dir"); err != nil {
		t.Fatalf("nil cfg should pass, got %v", err)
	}
	err := CheckUnknownKeys(SinkConfig{"dirr": "/tmp"}, "dir", "run_id")
	if err == nil {
		t.Fatal("typo must be rejected")
	}
	if !strings.Contains(err.Error(), `"dir"`) || !strings.Contains(err.Error(), `"dirr"`) {
		t.Errorf("missing did-you-mean hint: %v", err)
	}
	err = CheckUnknownKeys(SinkConfig{"zzzzzz": "x"}, "dir", "run_id")
	if err == nil {
		t.Fatal("unknown key must be rejected")
	}
	if strings.Contains(err.Error(), "did you mean") {
		t.Errorf("unrelated key should not carry a suggestion: %v", err)
	}
}

func TestFakeSinkLifecycle(t *testing.T) {
	t.Cleanup(reset)
	reset()
	s := &fakeSink{name: "lifecycle"}
	Register("lifecycle", func() Sink { return s })
	ctx := context.Background()
	if err := s.Init(ctx, SinkConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, "events", []connectors.Record{{Stream: "events"}, {Stream: "events"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := s.written.Load(); got != 2 {
		t.Errorf("written = %d; want 2", got)
	}
	if got := s.flushed.Load(); got != 1 {
		t.Errorf("flushed = %d; want 1", got)
	}
	if !s.closed.Load() {
		t.Errorf("closed = false; want true")
	}
}
