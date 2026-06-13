package main

import (
	"fmt"
	"strings"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/enrichers"
	"github.com/xydac/ridgeline/sinks"
)

// validateRegistrations checks that every connector, sink, and enricher
// type referenced in cfg is registered in its respective registry. It
// does not validate connector configs (credentials are not loaded here);
// call per-connector Validate methods separately after resolving refs.
//
// All three registries produce the same enumerated error message format
// on an unknown type, so the user can self-correct without reading source.
func validateRegistrations(cfg *config.File) error {
	knownSinks := sinks.List()
	sinkSet := make(map[string]bool, len(knownSinks))
	for _, s := range knownSinks {
		sinkSet[s] = true
	}

	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		for _, inst := range product.Connectors {
			if _, ok := connectors.Get(inst.Type); !ok {
				return fmt.Errorf("product %s connector %s: unknown connector type %q (known: %s)",
					pid, inst.Name, inst.Type, strings.Join(connectors.List(), ", "))
			}
			if !sinkSet[inst.Sink.Type] {
				return fmt.Errorf("product %s connector %s: unknown sink type %q (known: %s)",
					pid, inst.Name, inst.Sink.Type, strings.Join(knownSinks, ", "))
			}
			for _, er := range inst.Enrichers {
				if _, ok := enrichers.Get(er.Type); !ok {
					return fmt.Errorf("product %s connector %s: unknown enricher type %q (known: %s)",
						pid, inst.Name, er.Type, strings.Join(enrichers.List(), ", "))
				}
			}
		}
	}
	return nil
}
