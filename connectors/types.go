package connectors

import "fmt"

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

// SemanticKind classifies what a stream represents in business terms.
// The zero value is Unstructured so streams without an explicit Kind
// still have a valid, conservative classification.
type SemanticKind int

const (
	// Unstructured means records have no standardized quantitative meaning
	// (free-form logs, raw events without aggregation semantics, etc.).
	Unstructured SemanticKind = iota
	// Metric means every record is a measurement that can be aggregated
	// over time (page views per day, revenue per hour, etc.).
	Metric
	// Event means every record is a discrete occurrence with a timestamp
	// (a click, a deploy, a user sign-up). Events are counted, not summed.
	Event
	// Dimension means every record is a reference entity (a user, a
	// product, a country) that other streams join against.
	Dimension
)

// String returns the lowercase name of the SemanticKind.
func (k SemanticKind) String() string {
	switch k {
	case Metric:
		return "metric"
	case Event:
		return "event"
	case Dimension:
		return "dimension"
	}
	return "unstructured"
}

// Directionality indicates whether higher or lower values of a metric
// column are preferable. It enables anomaly detection and explain
// output to frame deviations as good-news or bad-news.
type Directionality int

const (
	// Neutral means there is no preferred direction (e.g. a duration
	// that could legitimately go either way depending on the product goal).
	Neutral Directionality = iota
	// HigherIsBetter means an increase is a positive signal (revenue,
	// signups, page views, etc.).
	HigherIsBetter
	// LowerIsBetter means a decrease is a positive signal (bounce rate,
	// error rate, latency, etc.).
	LowerIsBetter
)

// String returns the lowercase name of the Directionality.
func (d Directionality) String() string {
	switch d {
	case HigherIsBetter:
		return "higher_is_better"
	case LowerIsBetter:
		return "lower_is_better"
	}
	return "neutral"
}

// AggregationHint suggests how a metric column should be rolled up when
// querying across multiple rows. Sinks and reasoning primitives use
// this to produce sensible default aggregations without guessing.
type AggregationHint int

const (
	// AggNone means no standard aggregation applies (e.g. string labels).
	AggNone AggregationHint = iota
	// AggSum means values should be added (total page views over a week).
	AggSum
	// AggAvg means values should be averaged (average bounce rate).
	AggAvg
	// AggLast means only the most recent value matters (current balance).
	AggLast
	// AggCount means the number of records is the metric, not the value.
	AggCount
)

// String returns the lowercase name of the AggregationHint.
func (a AggregationHint) String() string {
	switch a {
	case AggSum:
		return "sum"
	case AggAvg:
		return "avg"
	case AggLast:
		return "last"
	case AggCount:
		return "count"
	}
	return "none"
}

// ColumnSemantics carries the optional business-meaning metadata for a
// metric column. Nil means the column has no declared quantitative semantics.
type ColumnSemantics struct {
	// Unit is a human-readable label for the column's unit of measure
	// (e.g. "seconds", "%", "USD", "requests").
	Unit string
	// Direction indicates whether higher or lower values are preferable.
	Direction Directionality
	// Aggregation is the recommended rollup function for this column.
	Aggregation AggregationHint
}

// Column describes one field in a stream's schema.
type Column struct {
	Name     string
	Type     ColumnType
	Required bool
	// Key indicates the column is part of the stream's primary key.
	// Sinks use this for deduplication and upsert.
	Key bool
	// Semantics carries optional business-meaning metadata for metric
	// columns. Nil for columns with no quantitative interpretation.
	Semantics *ColumnSemantics
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
	// Kind classifies what this stream represents in business terms.
	// The zero value (Unstructured) is valid for streams without a
	// quantitative or categorical interpretation.
	Kind      SemanticKind
	Schema    Schema
	SyncModes []SyncMode
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

// CheckUnknownKeys returns an error describing any key present in cfg
// that is not listed in known. It is intended for use in Validate to
// turn typo'd config keys into a fail-fast error at load time instead
// of the less-actionable "required field X is missing" downstream.
// The error includes a did-you-mean suggestion when an unknown key is
// within edit distance 2 of a known key.
//
// Connector authors should prefer calling this at the top of Validate
// so the user sees the most specific error first.
func CheckUnknownKeys(cfg ConnectorConfig, known ...string) error {
	if len(cfg) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(known))
	for _, k := range known {
		set[k] = struct{}{}
	}
	for k := range cfg {
		if _, ok := set[k]; ok {
			continue
		}
		if suggest := nearestKey(k, known); suggest != "" {
			return fmt.Errorf("unknown config key %q (did you mean %q?)", k, suggest)
		}
		return fmt.Errorf("unknown config key %q (known: %v)", k, known)
	}
	return nil
}

// nearestKey returns the entry in candidates with the smallest edit
// distance to query, provided that distance is at most 2. It returns ""
// when no candidate is close enough.
func nearestKey(query string, candidates []string) string {
	best := ""
	bestDist := 3
	for _, c := range candidates {
		d := editDistance(query, c)
		if d < bestDist {
			best = c
			bestDist = d
		}
	}
	return best
}

// editDistance is the Levenshtein distance between a and b. It is used
// only for user-facing "did you mean" hints, so clarity beats speed.
func editDistance(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	la, lb := len(ra), len(rb)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			curr[j] = minInt(del, minInt(ins, sub))
		}
		prev, curr = curr, prev
	}
	return prev[lb]
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
