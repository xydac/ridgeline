package connectors

// AuthType enumerates the authentication mechanisms a connector supports.
type AuthType int

const (
	// AuthNone means the connector requires no credentials.
	AuthNone AuthType = iota
	// AuthAPIKey means the connector authenticates with a static key or token.
	AuthAPIKey
	// AuthOAuth2 means the connector uses OAuth 2.0 (authorization code or PKCE).
	AuthOAuth2
	// AuthJWT means the connector signs requests with a JWT (e.g. App Store Connect).
	AuthJWT
	// AuthBasic means the connector uses HTTP basic auth.
	AuthBasic
)

// String returns the lowercase name of the AuthType, suitable for config files.
func (a AuthType) String() string {
	switch a {
	case AuthNone:
		return "none"
	case AuthAPIKey:
		return "api_key"
	case AuthOAuth2:
		return "oauth2"
	case AuthJWT:
		return "jwt"
	case AuthBasic:
		return "basic"
	}
	return "unknown"
}

// AuthConfig describes how to authenticate with a connector. Only the
// fields relevant to AuthType are populated.
type AuthConfig struct {
	// OAuth2 fields
	AuthURL  string
	TokenURL string
	Scopes   []string

	// API key / JWT fields
	KeyFields []string
}

// SyncMode defines how a stream syncs.
type SyncMode int

const (
	// FullRefresh replaces all data each sync.
	FullRefresh SyncMode = iota
	// Incremental fetches only new or changed records, using state as a cursor.
	Incremental
	// CDC is reserved for future change-data-capture sources.
	CDC
)

// String returns the lowercase name of the SyncMode.
func (s SyncMode) String() string {
	switch s {
	case FullRefresh:
		return "full_refresh"
	case Incremental:
		return "incremental"
	case CDC:
		return "cdc"
	}
	return "unknown"
}

// ColumnType is the logical type of a column in a stream's schema. The
// physical Parquet type is chosen by the sink.
type ColumnType int

// ColumnType values, covering the logical types a stream schema can
// declare. Int is a 64-bit signed integer; Float is IEEE-754 double
// precision; Timestamp is a UTC instant; JSON carries opaque nested
// structure that sinks store as an encoded JSON string.
const (
	String ColumnType = iota
	Int
	Float
	Bool
	Timestamp
	JSON
)

// String returns the lowercase name of the ColumnType.
func (c ColumnType) String() string {
	switch c {
	case String:
		return "string"
	case Int:
		return "int"
	case Float:
		return "float"
	case Bool:
		return "bool"
	case Timestamp:
		return "timestamp"
	case JSON:
		return "json"
	}
	return "unknown"
}

// Column describes one field in a stream's schema.
type Column struct {
	Name     string
	Type     ColumnType
	Required bool
	// Key indicates the column is part of the stream's primary key.
	// Sinks use this for deduplication and upsert.
	Key bool
}

// Schema describes the shape of records in a stream. Schema is advisory:
// connectors may emit extra fields not declared here, but sinks should
// preserve declared columns even when records omit them.
type Schema struct {
	Columns []Column
}

// StreamSpec declares one data stream a connector can produce.
type StreamSpec struct {
	Name        string
	Description string
	Schema      Schema
	SyncModes   []SyncMode
	// DefaultCron is a suggested sync schedule in standard cron syntax.
	// Empty means the connector does not recommend a schedule.
	DefaultCron string
}

// Stream identifies a stream the orchestrator wants to extract.
type Stream struct {
	Name string
	// Mode is the sync mode the orchestrator selected. The connector must
	// support this mode (must appear in the matching StreamSpec.SyncModes).
	Mode SyncMode
}

// ConnectorSpec is a connector's self-description.
type ConnectorSpec struct {
	Name        string
	DisplayName string
	Description string
	Version     string
	AuthType    AuthType
	AuthConfig  *AuthConfig
	// GoLibrary names the upstream Go SDK the connector wraps, if any.
	// Used in generated docs.
	GoLibrary string
	Streams   []StreamSpec
}

// DiscoveredStream is a stream returned by Discover, annotated with
// runtime availability.
type DiscoveredStream struct {
	StreamSpec
	// Available indicates the stream can be synced with the current
	// configuration and credentials.
	Available bool
	// RowCount is an estimated row count when the source can cheaply
	// provide one, otherwise nil.
	RowCount *int64
}

// Catalog is the result of discovery, listing streams the connector can
// currently produce.
type Catalog struct {
	Streams []DiscoveredStream
}

// State is an opaque checkpoint a connector emits and consumes to resume
// incremental syncs. State values must be JSON-marshalable so they can
// be persisted by the orchestrator and shipped over the JSON-lines
// protocol to external connectors.
type State map[string]any

// String returns the string value at key, or fallback if missing or not
// a string. This is a convenience for connectors reading their own state
// without type-asserting at every call site.
func (s State) String(key, fallback string) string {
	if v, ok := s[key].(string); ok {
		return v
	}
	return fallback
}

// ConnectorConfig is the user-supplied configuration for one connector
// instance, loaded from ridgeline.yaml. The shape is connector-specific;
// helpers below provide typed access without forcing every connector to
// define its own struct.
type ConnectorConfig map[string]any

// String returns the string value at key, or "" if missing.
func (c ConnectorConfig) String(key string) string {
	if v, ok := c[key].(string); ok {
		return v
	}
	return ""
}

// StringSlice returns the []string value at key, or nil if missing or
// not a slice of strings. JSON decodes string slices as []any, so the
// helper handles both shapes.
func (c ConnectorConfig) StringSlice(key string) []string {
	switch v := c[key].(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// Int returns the int value at key, or fallback if missing or not numeric.
func (c ConnectorConfig) Int(key string, fallback int) int {
	switch v := c[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return fallback
}
