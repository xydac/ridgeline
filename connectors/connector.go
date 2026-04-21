package connectors

import "context"

// Connector is the contract every native data source implements.
//
// Lifecycle:
//
//  1. Spec is called once at startup and must be cheap and pure (no I/O).
//  2. Validate is called when the user sets up or refreshes credentials.
//     It must return nil only when the connector can reach its source.
//  3. Discover lists what streams are available. The orchestrator may
//     call Discover frequently, so it should be O(1) network calls.
//  4. Extract returns a channel of Messages. Implementations close the
//     channel when extraction is complete. Implementations must respect
//     ctx cancellation and stop sending on the channel promptly when
//     ctx is done.
//
// Implementations must be safe for concurrent calls to different
// methods on the same value.
type Connector interface {
	Spec() ConnectorSpec
	Validate(ctx context.Context, cfg ConnectorConfig) error
	Discover(ctx context.Context, cfg ConnectorConfig) (*Catalog, error)
	Extract(ctx context.Context, cfg ConnectorConfig, streams []Stream, state State) (<-chan Message, error)
}
