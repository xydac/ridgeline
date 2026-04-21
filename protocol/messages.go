package protocol

import "encoding/json"

// Command names sent from the orchestrator to an external process on
// stdin. These are the lowercase tags that appear in the "type" field.
const (
	CmdSpec     = "spec"
	CmdValidate = "validate"
	CmdDiscover = "discover"
	CmdExtract  = "extract"
	CmdEnrich   = "enrich"
)

// Output message tags returned from an external process on stdout.
// These are the uppercase tags that appear in the "type" field.
const (
	MsgSpec   = "SPEC"
	MsgRecord = "RECORD"
	MsgState  = "STATE"
	MsgLog    = "LOG"
	MsgSchema = "SCHEMA"
	MsgDone   = "DONE"
	MsgError  = "ERROR"
)

// Command is a message sent from the orchestrator to an external
// process. Only fields relevant to the named Type are populated.
//
// Wire shape:
//
//	{"type":"extract","config":{...},"streams":[...],"state":{...}}
type Command struct {
	Type    string         `json:"type"`
	Config  map[string]any `json:"config,omitempty"`
	Streams []StreamRef    `json:"streams,omitempty"`
	State   map[string]any `json:"state,omitempty"`
	// Records carries input records for the "enrich" command.
	Records []Record `json:"records,omitempty"`
}

// StreamRef identifies one stream the orchestrator wants extracted.
type StreamRef struct {
	Name string `json:"name"`
	Mode string `json:"mode,omitempty"`
}

// Output is a message sent from an external process back to the
// orchestrator. Only fields relevant to the named Type are populated.
//
// Wire shape (RECORD):
//
//	{"type":"RECORD","stream":"events","timestamp":"2026-04-20T00:00:00Z","data":{...}}
type Output struct {
	Type      string         `json:"type"`
	Spec      *Spec          `json:"spec,omitempty"`
	Stream    string         `json:"stream,omitempty"`
	Timestamp string         `json:"timestamp,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	State     map[string]any `json:"state,omitempty"`
	Level     string         `json:"level,omitempty"`
	Message   string         `json:"message,omitempty"`
	Schema    *Schema        `json:"schema,omitempty"`
	Error     string         `json:"error,omitempty"`
}

// Record is the wire form of a single data row. The orchestrator's
// runner converts this into connectors.Record before handing it to
// downstream stages.
type Record struct {
	Stream    string         `json:"stream"`
	Timestamp string         `json:"timestamp,omitempty"`
	Data      map[string]any `json:"data"`
}

// Spec is the wire form of a connector or enricher's self-description,
// returned in response to the "spec" command.
type Spec struct {
	Name        string       `json:"name"`
	DisplayName string       `json:"display_name,omitempty"`
	Description string       `json:"description,omitempty"`
	Version     string       `json:"version"`
	AuthType    string       `json:"auth_type,omitempty"`
	Streams     []StreamSpec `json:"streams,omitempty"`
}

// StreamSpec is the wire form of one declared stream in a Spec.
type StreamSpec struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	SyncModes   []string `json:"sync_modes,omitempty"`
	DefaultCron string   `json:"default_cron,omitempty"`
	Schema      *Schema  `json:"schema,omitempty"`
}

// Schema is the wire form of a stream's schema.
type Schema struct {
	Columns []Column `json:"columns"`
}

// Column is the wire form of one schema column.
type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
	Key      bool   `json:"key,omitempty"`
}

// MarshalLine returns v encoded as a single JSON line terminated by '\n'.
// This is a thin convenience over json.Marshal for callers that only
// need to write one message.
func MarshalLine(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
