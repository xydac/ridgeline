// Package enrichers defines the contract for transforms that augment
// records between extract and load, plus a registry for native
// (in-binary) enrichers.
//
// An enricher takes a batch of records and returns a batch of records
// with additional fields: sentiment scores, extracted entities, LLM
// classifications, custom NLP output, and so on. Enrichers process
// records in batches to amortize the cost of model calls.
//
// External enrichers follow the same JSON-lines protocol as external
// connectors (package protocol), with one extra inbound message type:
// "enrich".
//
// A minimal enricher looks like this:
//
//	package upper
//
//	import (
//		"context"
//		"strings"
//
//		"github.com/xydac/ridgeline/connectors"
//		"github.com/xydac/ridgeline/enrichers"
//	)
//
//	type Enricher struct{}
//
//	func (e *Enricher) Name() string { return "upper" }
//	func (e *Enricher) Enrich(_ context.Context, _ enrichers.EnrichConfig, recs []connectors.Record) ([]connectors.Record, error) {
//		for i := range recs {
//			if s, ok := recs[i].Data["text"].(string); ok {
//				recs[i].Data["text_upper"] = strings.ToUpper(s)
//			}
//		}
//		return recs, nil
//	}
//
//	func init() { enrichers.Register(&Enricher{}) }
package enrichers
