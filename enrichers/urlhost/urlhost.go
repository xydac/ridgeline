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
	"net/url"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/enrichers"
)

func init() { enrichers.Register(&Enricher{}) }

// Enricher extracts the hostname from a URL field and writes it to a
// separate field.
type Enricher struct{}

// Name returns the stable registered name of this enricher.
func (e *Enricher) Name() string { return "url_host" }

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
		recs[i].Data[hostField] = u.Hostname()
	}
	return recs, nil
}
