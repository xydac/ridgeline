// Package connectors defines the contract that every Ridgeline data source
// implements, plus a process-wide registry for native (in-binary) connectors.
//
// Two connector flavors share this interface:
//
//   - Native connectors are written in Go and registered via Register at
//     init time. They are compiled into the ridgeline binary.
//   - External connectors are separate executables in any language. They
//     speak the JSON-lines protocol defined in package protocol; the
//     external runner adapts that wire format to the Connector interface.
//
// A minimal native connector looks like this:
//
//	package noop
//
//	import (
//		"context"
//
//		"github.com/xydac/ridgeline/connectors"
//	)
//
//	type Connector struct{}
//
//	func (c *Connector) Spec() connectors.ConnectorSpec {
//		return connectors.ConnectorSpec{
//			Name:        "noop",
//			DisplayName: "No-op",
//			Version:     "0.1.0",
//			AuthType:    connectors.AuthNone,
//			Streams: []connectors.StreamSpec{{
//				Name:      "events",
//				SyncModes: []connectors.SyncMode{connectors.FullRefresh},
//			}},
//		}
//	}
//
//	func (c *Connector) Validate(_ context.Context, _ connectors.ConnectorConfig) error {
//		return nil
//	}
//
//	func (c *Connector) Discover(_ context.Context, _ connectors.ConnectorConfig) (*connectors.Catalog, error) {
//		return &connectors.Catalog{Streams: []connectors.DiscoveredStream{
//			{StreamSpec: c.Spec().Streams[0], Available: true},
//		}}, nil
//	}
//
//	func (c *Connector) Extract(_ context.Context, _ connectors.ConnectorConfig, _ []connectors.Stream, _ connectors.State) (<-chan connectors.Message, error) {
//		ch := make(chan connectors.Message)
//		close(ch)
//		return ch, nil
//	}
//
//	func init() { connectors.Register(&Connector{}) }
package connectors
