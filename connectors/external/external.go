package external

import (
	"context"
	"fmt"
	"strings"

	"github.com/xydac/ridgeline/connectors"
)

// Name is the connector type registered with the connectors package.
// Use it as the `type:` value in ridgeline.yaml when wiring an external
// child process as a connector.
const Name = "external"

// version is the wire version of this runner. Bumped on protocol or
// behavior changes that an external process might care about.
const version = "0.1.0"

func init() {
	connectors.Register(New())
}

// Connector is the runner for external child-process connectors. A
// single Connector value is safe to share across many Extract calls;
// each call spawns its own child process.
type Connector struct{}

// New returns a ready-to-register Connector.
func New() *Connector { return &Connector{} }

// Spec returns the runner's self-description. The runner cannot know
// what streams an arbitrary child process exposes, so Streams is empty;
// the user names the streams to extract in their ridgeline.yaml and the
// child process honours them via the streams field of the extract command.
func (c *Connector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{
		Name:        Name,
		DisplayName: "External Process",
		Description: "Runs a child process that speaks the JSON-lines protocol on stdin/stdout.",
		Version:     version,
		AuthType:    connectors.AuthNone,
	}
}

// Validate checks that a command is configured. It does not invoke the
// child process; reachability of the underlying source is the child's
// responsibility, and a "command exists" check would not be meaningful
// (the user might have a virtualenv-relative or PATH-relative entry).
func (c *Connector) Validate(_ context.Context, cfg connectors.ConnectorConfig) error {
	if strings.TrimSpace(cfg.String("command")) == "" {
		return fmt.Errorf("external: command must not be empty")
	}
	return nil
}

// Discover returns an empty catalog. The runner does not currently
// invoke the child to ask what streams it exposes; the user names the
// streams they want in ridgeline.yaml. A future enhancement will issue
// the spec command to the child and translate its SPEC reply into a
// catalog.
func (c *Connector) Discover(_ context.Context, _ connectors.ConnectorConfig) (*connectors.Catalog, error) {
	return &connectors.Catalog{}, nil
}

// Extract is not yet implemented; the next change wires the child
// process lifecycle.
func (c *Connector) Extract(_ context.Context, _ connectors.ConnectorConfig, _ []connectors.Stream, _ connectors.State) (<-chan connectors.Message, error) {
	return nil, fmt.Errorf("external: Extract not yet implemented")
}
