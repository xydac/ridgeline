package connectors

import "time"

// MessageType discriminates the variants of Message.
type MessageType int

const (
	// RecordMsg carries a single Record.
	RecordMsg MessageType = iota
	// StateMsg carries an incremental checkpoint. Connectors should emit a
	// StateMsg after every batch of records that can be safely resumed
	// from, and the orchestrator persists it after the records are durably
	// written.
	StateMsg
	// LogMsg carries a structured log entry.
	LogMsg
	// SchemaMsg carries a schema declaration or change for a stream.
	SchemaMsg
	// ErrorMsg carries a fatal connector error. The pipeline treats the
	// first ErrorMsg on the channel as terminal: it stops consuming,
	// discards any uncommitted buffered records, does not persist any
	// pending state, and returns the error to its caller. Use ErrorMsg
	// when a connector has decided it cannot continue. Non-fatal
	// conditions belong in LogMsg at an appropriate level.
	ErrorMsg
)

// LogLevel categorizes LogEntry severity.
type LogLevel int

// LogLevel values, from least to most severe.
const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

// String returns the lowercase name of the LogLevel.
func (l LogLevel) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	}
	return "unknown"
}

// LogEntry is a single structured log message from a connector.
type LogEntry struct {
	Level   LogLevel
	Message string
	Fields  map[string]any
}

// Record is a single data row emitted by a connector.
type Record struct {
	Stream    string
	Timestamp time.Time
	Data      map[string]any
}

// Message is the unit of communication on the channel returned by
// Connector.Extract. Exactly one of the pointer fields is non-nil,
// matching Type.
type Message struct {
	Type   MessageType
	Record *Record
	State  *State
	Log    *LogEntry
	Schema *Schema
	// Err carries the fatal error when Type == ErrorMsg.
	Err error
	// Stream names the stream this Schema applies to. Only meaningful
	// when Type == SchemaMsg.
	Stream string
}

// RecordMessage builds a Message wrapping rec.
func RecordMessage(stream string, rec Record) Message {
	rec.Stream = stream
	return Message{Type: RecordMsg, Record: &rec}
}

// StateMessage builds a Message wrapping a checkpoint.
func StateMessage(state State) Message {
	return Message{Type: StateMsg, State: &state}
}

// LogMessage builds a Message wrapping a log entry at the given level.
func LogMessage(level LogLevel, msg string) Message {
	return Message{Type: LogMsg, Log: &LogEntry{Level: level, Message: msg}}
}

// SchemaMessage builds a Message announcing the schema for a stream.
func SchemaMessage(stream string, schema Schema) Message {
	return Message{Type: SchemaMsg, Stream: stream, Schema: &schema}
}

// ErrorMessage builds a Message wrapping a fatal connector error. The
// pipeline treats this as terminal: no further messages will be consumed,
// buffered records are discarded, and err is returned to the caller.
func ErrorMessage(err error) Message {
	return Message{Type: ErrorMsg, Err: err}
}
