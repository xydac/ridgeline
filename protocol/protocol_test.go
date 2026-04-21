package protocol

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := []Output{
		{Type: MsgSpec, Spec: &Spec{Name: "noop", Version: "0.1.0", AuthType: "none"}},
		{Type: MsgRecord, Stream: "events", Timestamp: "2026-04-20T00:00:00Z", Data: map[string]any{"id": "1", "n": float64(2)}},
		{Type: MsgState, State: map[string]any{"cursor": "abc"}},
		{Type: MsgLog, Level: "info", Message: "hello"},
		{Type: MsgSchema, Stream: "events", Schema: &Schema{Columns: []Column{{Name: "id", Type: "string", Key: true}}}},
		{Type: MsgDone},
		{Type: MsgError, Error: "boom"},
	}

	var buf bytes.Buffer
	for _, o := range in {
		line, err := MarshalLine(o)
		if err != nil {
			t.Fatalf("MarshalLine: %v", err)
		}
		buf.Write(line)
	}

	dec := NewDecoder(&buf)
	for i, want := range in {
		got, err := dec.Read()
		if err != nil {
			t.Fatalf("Read[%d]: %v", i, err)
		}
		if got.Type != want.Type {
			t.Errorf("Read[%d].Type = %q; want %q", i, got.Type, want.Type)
		}
	}
	if _, err := dec.Read(); err != io.EOF {
		t.Errorf("expected EOF after last line, got %v", err)
	}
}

func TestEncoderRoundTripCommands(t *testing.T) {
	cmd := Command{
		Type:    CmdExtract,
		Config:  map[string]any{"keywords": []any{"foo", "bar"}},
		Streams: []StreamRef{{Name: "events", Mode: "incremental"}},
		State:   map[string]any{"cursor": "abc"},
	}

	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	if err := enc.Write(cmd); err != nil {
		t.Fatal(err)
	}
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}

	if !strings.HasSuffix(buf.String(), "\n") {
		t.Errorf("encoded line should end with newline; got %q", buf.String())
	}
	if !strings.Contains(buf.String(), `"type":"extract"`) {
		t.Errorf("expected type=extract in output; got %q", buf.String())
	}
}

func TestDecoderSkipsBlankLines(t *testing.T) {
	input := "\n\n" + `{"type":"DONE"}` + "\n\n"
	dec := NewDecoder(strings.NewReader(input))
	out, err := dec.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if out.Type != MsgDone {
		t.Errorf("Type = %q; want DONE", out.Type)
	}
	if _, err := dec.Read(); err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestDecoderRejectsMissingType(t *testing.T) {
	dec := NewDecoder(strings.NewReader(`{"stream":"x"}` + "\n"))
	_, err := dec.Read()
	if err == nil || !strings.Contains(err.Error(), "missing required field") {
		t.Errorf("expected missing-type error, got %v", err)
	}
}

func TestDecoderRejectsMalformedJSON(t *testing.T) {
	dec := NewDecoder(strings.NewReader("{not json}\n"))
	_, err := dec.Read()
	if err == nil || !strings.Contains(err.Error(), "decode line") {
		t.Errorf("expected decode error, got %v", err)
	}
}

func TestDecoderRejectsOversizedLine(t *testing.T) {
	big := strings.Repeat("a", 200)
	dec := NewDecoderSize(strings.NewReader(`{"type":"LOG","message":"`+big+`"}`+"\n"), 64)
	_, err := dec.Read()
	if err == nil {
		t.Fatal("expected oversized-line error, got nil")
	}
	// bufio.Scanner.Err() returns ErrTooLong; we wrap it.
	if !strings.Contains(err.Error(), "scan") {
		t.Errorf("expected wrapped scan error, got %v", err)
	}
}

func TestDecoderStrictRejectsUnknownFields(t *testing.T) {
	dec := NewDecoder(strings.NewReader(`{"type":"DONE","mystery":1}` + "\n"))
	dec.Strict = true
	_, err := dec.Read()
	if err == nil {
		t.Fatal("expected unknown-field error, got nil")
	}
}

func TestDecoderLooseAllowsUnknownFields(t *testing.T) {
	dec := NewDecoder(strings.NewReader(`{"type":"DONE","mystery":1}` + "\n"))
	out, err := dec.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if out.Type != MsgDone {
		t.Errorf("Type = %q; want DONE", out.Type)
	}
}

func TestMarshalLineErrorsOnUnencodable(t *testing.T) {
	// channels can't be JSON-encoded
	_, err := MarshalLine(make(chan int))
	if err == nil {
		t.Fatal("expected encoding error, got nil")
	}
}

// Make sure errors.Is plays nicely with our wrapped scan error so
// callers can distinguish line-too-long from other failures.
func TestDecoderErrorIsWrapped(t *testing.T) {
	dec := NewDecoderSize(strings.NewReader(strings.Repeat("a", 5000)+"\n"), 64)
	_, err := dec.Read()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// The underlying error should still be retrievable via errors.Unwrap.
	if errors.Unwrap(err) == nil {
		t.Errorf("expected wrapped error, got %v", err)
	}
}
