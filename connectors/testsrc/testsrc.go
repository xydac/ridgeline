package testsrc

import (
	"context"
	"fmt"
	"time"

	"github.com/xydac/ridgeline/connectors"
)

// Name is the connector name registered with the connectors package.
const Name = "testsrc"

// DefaultRecords is the number of records emitted per stream when the
// "records" config key is absent or zero.
const DefaultRecords = 5

func init() {
	connectors.Register(New())
}

// Connector is a synthetic data source. It is safe to reuse the same
// value across many Extract calls.
type Connector struct{}

// New returns a ready-to-register Connector.
func New() *Connector { return &Connector{} }

// Spec returns the connector's self-description.
func (c *Connector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{
		Name:        Name,
		DisplayName: "Test Source",
		Description: "Synthetic connector for dry-run smoke tests. Emits deterministic records on the pages and events streams.",
		Version:     "0.1.0",
		AuthType:    connectors.AuthNone,
		Streams: []connectors.StreamSpec{
			{Name: "pages", Description: "Synthetic pageview events", SyncModes: []connectors.SyncMode{connectors.FullRefresh}},
			{Name: "events", Description: "Synthetic custom events", SyncModes: []connectors.SyncMode{connectors.FullRefresh}},
		},
	}
}

// Validate always succeeds; this connector has no external dependency.
func (c *Connector) Validate(context.Context, connectors.ConnectorConfig) error { return nil }

// Discover returns both streams as available.
func (c *Connector) Discover(_ context.Context, _ connectors.ConnectorConfig) (*connectors.Catalog, error) {
	spec := c.Spec()
	out := make([]connectors.DiscoveredStream, 0, len(spec.Streams))
	for _, ss := range spec.Streams {
		out = append(out, connectors.DiscoveredStream{StreamSpec: ss, Available: true})
	}
	return &connectors.Catalog{Streams: out}, nil
}

// Extract emits the configured number of records for each requested
// stream, a StateMsg per stream, and then closes the channel. All
// records are produced synchronously so tests are deterministic.
func (c *Connector) Extract(ctx context.Context, cfg connectors.ConnectorConfig, streams []connectors.Stream, _ connectors.State) (<-chan connectors.Message, error) {
	n := cfg.Int("records", DefaultRecords)
	if n < 0 {
		return nil, fmt.Errorf("testsrc: records must be >= 0")
	}
	ch := make(chan connectors.Message, 16)
	go func() {
		defer close(ch)
		start := time.Unix(1700000000, 0).UTC()
		for _, s := range streams {
			for i := 0; i < n; i++ {
				rec := connectors.Record{
					Timestamp: start.Add(time.Duration(i) * time.Minute),
					Data: map[string]any{
						"stream": s.Name,
						"index":  i,
						"id":     fmt.Sprintf("%s-%d", s.Name, i),
					},
				}
				select {
				case <-ctx.Done():
					return
				case ch <- connectors.RecordMessage(s.Name, rec):
				}
			}
			select {
			case <-ctx.Done():
				return
			case ch <- connectors.StateMessage(connectors.State{"last_stream": s.Name, "count": n}):
			}
		}
	}()
	return ch, nil
}
