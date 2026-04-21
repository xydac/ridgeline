package enrichers

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/xydac/ridgeline/connectors"
)

type upperEnricher struct{}

func (upperEnricher) Name() string { return "upper" }
func (upperEnricher) Enrich(_ context.Context, _ EnrichConfig, recs []connectors.Record) ([]connectors.Record, error) {
	for i := range recs {
		if s, ok := recs[i].Data["text"].(string); ok {
			recs[i].Data["text_upper"] = strings.ToUpper(s)
		}
	}
	return recs, nil
}

func TestRegisterAndGet(t *testing.T) {
	t.Cleanup(reset)
	reset()

	Register(upperEnricher{})
	got, ok := Get("upper")
	if !ok || got.Name() != "upper" {
		t.Fatalf("Get(upper) = (%v, %v); want enricher upper", got, ok)
	}
	if want, got := []string{"upper"}, List(); !reflect.DeepEqual(want, got) {
		t.Fatalf("List() = %v; want %v", got, want)
	}
}

func TestEnrichAddsField(t *testing.T) {
	t.Cleanup(reset)
	reset()

	Register(upperEnricher{})
	e, _ := Get("upper")
	in := []connectors.Record{{Data: map[string]any{"text": "hello"}}}
	out, err := e.Enrich(context.Background(), EnrichConfig{}, in)
	if err != nil {
		t.Fatal(err)
	}
	if got := out[0].Data["text_upper"]; got != "HELLO" {
		t.Errorf("text_upper = %v; want HELLO", got)
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	t.Cleanup(reset)
	reset()
	Register(upperEnricher{})
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on duplicate Register")
		}
	}()
	Register(upperEnricher{})
}

func TestRegisterNilPanics(t *testing.T) {
	t.Cleanup(reset)
	reset()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on nil Register")
		}
	}()
	Register(nil)
}

func TestEnrichConfigAccessors(t *testing.T) {
	cfg := EnrichConfig{
		"model":         "claude-sonnet",
		"batch_size":    float64(10),
		"output_fields": []any{"sentiment", "score"},
	}
	if got := cfg.String("model"); got != "claude-sonnet" {
		t.Errorf("String(model) = %q; want claude-sonnet", got)
	}
	if got := cfg.Int("batch_size", 0); got != 10 {
		t.Errorf("Int(batch_size) = %d; want 10", got)
	}
	if got := cfg.Int("missing", 5); got != 5 {
		t.Errorf("Int(missing, 5) = %d; want 5", got)
	}
	if got := cfg.StringSlice("output_fields"); len(got) != 2 || got[0] != "sentiment" || got[1] != "score" {
		t.Errorf("StringSlice(output_fields) = %v; want [sentiment score]", got)
	}
}
