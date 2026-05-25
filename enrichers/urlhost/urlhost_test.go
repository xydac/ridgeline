package urlhost_test

import (
	"context"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/enrichers"
	_ "github.com/xydac/ridgeline/enrichers/urlhost"
)

func TestURLHostExtraction(t *testing.T) {
	e, ok := enrichers.Get("url_host")
	if !ok {
		t.Fatal("url_host enricher not registered")
	}

	now := time.Now()
	in := []connectors.Record{
		{Stream: "pages", Timestamp: now, Data: map[string]any{"url": "https://example.com/path?q=1"}},
		{Stream: "pages", Timestamp: now, Data: map[string]any{"url": "http://sub.domain.org/a/b"}},
		{Stream: "pages", Timestamp: now, Data: map[string]any{"url": "not-a-url"}},
		{Stream: "pages", Timestamp: now, Data: map[string]any{"other": "field"}},
	}

	out, err := e.Enrich(context.Background(), enrichers.EnrichConfig{}, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 4 {
		t.Fatalf("len = %d; want 4", len(out))
	}
	if got := out[0].Data["host"]; got != "example.com" {
		t.Errorf("record[0] host = %v; want example.com", got)
	}
	if got := out[1].Data["host"]; got != "sub.domain.org" {
		t.Errorf("record[1] host = %v; want sub.domain.org", got)
	}
	if _, ok := out[2].Data["host"]; ok {
		t.Errorf("record[2] should have no host field for unparseable URL")
	}
	if _, ok := out[3].Data["host"]; ok {
		t.Errorf("record[3] should have no host field when url_field is absent")
	}
}

func TestURLHostCustomFields(t *testing.T) {
	e, _ := enrichers.Get("url_host")
	now := time.Now()
	in := []connectors.Record{
		{Stream: "events", Timestamp: now, Data: map[string]any{"page_url": "https://acme.io/login"}},
	}
	cfg := enrichers.EnrichConfig{"url_field": "page_url", "host_field": "domain"}
	out, err := e.Enrich(context.Background(), cfg, in)
	if err != nil {
		t.Fatal(err)
	}
	if got := out[0].Data["domain"]; got != "acme.io" {
		t.Errorf("domain = %v; want acme.io", got)
	}
	if _, ok := out[0].Data["host"]; ok {
		t.Errorf("should not write default host field when host_field is custom")
	}
}
