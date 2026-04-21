package connectors

import (
	"errors"
	"testing"
	"time"
)

func TestRecordMessage(t *testing.T) {
	r := Record{Timestamp: time.Unix(0, 0), Data: map[string]any{"k": "v"}}
	m := RecordMessage("events", r)
	if m.Type != RecordMsg {
		t.Fatalf("Type = %v; want RecordMsg", m.Type)
	}
	if m.Record == nil || m.Record.Stream != "events" {
		t.Fatalf("Record.Stream = %q; want events", m.Record.Stream)
	}
}

func TestStateMessage(t *testing.T) {
	m := StateMessage(State{"cursor": "abc"})
	if m.Type != StateMsg || m.State == nil || (*m.State)["cursor"] != "abc" {
		t.Fatalf("StateMessage round-trip failed: %+v", m)
	}
}

func TestLogMessage(t *testing.T) {
	m := LogMessage(LevelWarn, "uh oh")
	if m.Type != LogMsg || m.Log == nil || m.Log.Level != LevelWarn || m.Log.Message != "uh oh" {
		t.Fatalf("LogMessage round-trip failed: %+v", m)
	}
}

func TestSchemaMessage(t *testing.T) {
	s := Schema{Columns: []Column{{Name: "id", Type: String, Key: true}}}
	m := SchemaMessage("events", s)
	if m.Type != SchemaMsg || m.Stream != "events" || m.Schema == nil || len(m.Schema.Columns) != 1 {
		t.Fatalf("SchemaMessage round-trip failed: %+v", m)
	}
}

func TestErrorMessage(t *testing.T) {
	want := errors.New("connector gave up")
	m := ErrorMessage(want)
	if m.Type != ErrorMsg {
		t.Fatalf("Type = %v; want ErrorMsg", m.Type)
	}
	if m.Err != want {
		t.Fatalf("Err = %v; want %v", m.Err, want)
	}
}

func TestLogLevelString(t *testing.T) {
	cases := map[LogLevel]string{
		LevelDebug:  "debug",
		LevelInfo:   "info",
		LevelWarn:   "warn",
		LevelError:  "error",
		LogLevel(7): "unknown",
	}
	for in, want := range cases {
		if got := in.String(); got != want {
			t.Errorf("LogLevel(%d).String() = %q; want %q", in, got, want)
		}
	}
}
