// Package urlhost provides an enricher that derives a hostname field
// from a URL field already present in each record.
//
// Configuration keys:
//
//	url_field   - source field containing the full URL (default: "url")
//	host_field  - destination field written with the extracted hostname
//	              (default: "host")
//
// Records whose url_field is missing or not a string are passed through
// unchanged. Records where the URL cannot be parsed are also passed
// through unchanged.
//
// Register the enricher by importing this package for its side effects:
//
//	import _ "github.com/xydac/ridgeline/enrichers/urlhost"
package urlhost

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/enrichers"
)

func init() { enrichers.Register(&Enricher{}) }

// Enricher extracts the hostname from a URL field and writes it to a
// separate field.
type Enricher struct{}

// Name returns the stable registered name of this enricher.
func (e *Enricher) Name() string { return "url_host" }

// ValidateConfig rejects configs where url_field or host_field are present
// but not strings, surfacing YAML type-mismatch errors at pipeline-build time.
func (e *Enricher) ValidateConfig(cfg enrichers.EnrichConfig) error {
	for _, key := range []string{"url_field", "host_field"} {
		v, ok := cfg[key]
		if !ok {
			continue
		}
		if _, isStr := v.(string); !isStr {
			return fmt.Errorf("url_host: config key %q must be a string field name, got %T", key, v)
		}
	}
	return nil
}

// Enrich reads cfg["url_field"] (default "url") from each record's
// Data map, parses it as a URL, and writes the hostname to
// cfg["host_field"] (default "host"). Records that have no parseable
// URL in url_field are passed through unchanged.
func (e *Enricher) Enrich(_ context.Context, cfg enrichers.EnrichConfig, recs []connectors.Record) ([]connectors.Record, error) {
	urlField := cfg.String("url_field")
	if urlField == "" {
		urlField = "url"
	}
	hostField := cfg.String("host_field")
	if hostField == "" {
		hostField = "host"
	}
	for i := range recs {
		raw, ok := recs[i].Data[urlField].(string)
		if !ok || raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" {
			continue
		}
		recs[i].Data[hostField] = strings.ToLower(u.Hostname())
	}
	return recs, nil
}
