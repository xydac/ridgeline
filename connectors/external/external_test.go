package external_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/external"
	"github.com/xydac/ridgeline/protocol"
)

func TestRegistered(t *testing.T) {
	t.Parallel()
	if _, ok := connectors.Get(external.Name); !ok {
		t.Fatalf("external connector not registered")
	}
}

func TestSpec(t *testing.T) {
	t.Parallel()
	spec := external.New().Spec()
	if spec.Name != external.Name {
		t.Errorf("Name = %q, want %q", spec.Name, external.Name)
	}
	if spec.AuthType != connectors.AuthNone {
		t.Errorf("AuthType = %v, want AuthNone", spec.AuthType)
	}
	if spec.Version == "" {
		t.Error("Version must not be empty")
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	c := external.New()
	cases := []struct {
		name    string
		cfg     connectors.ConnectorConfig
		wantErr bool
		wantSub string
	}{
		{"empty", connectors.ConnectorConfig{}, true, "command"},
		{"whitespace-command", connectors.ConnectorConfig{"command": "   "}, true, "command"},
		{"valid", connectors.ConnectorConfig{"command": "/bin/echo"}, false, ""},
		{"typo-commad", connectors.ConnectorConfig{"commad": "/bin/echo"}, true, "command"},
		{"valid-all-fields", connectors.ConnectorConfig{
			"command": "/bin/echo",
			"args":    []any{"hi"},
			"env":     map[string]any{"K": "V"},
			"dir":     "/tmp",
		}, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.Validate(context.Background(), tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate(%v) err=%v, wantErr=%v", tc.cfg, err, tc.wantErr)
			}
			if err != nil && tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestDiscover(t *testing.T) {
	t.Parallel()
	c := external.New()
	cat, err := c.Discover(context.Background(), connectors.ConnectorConfig{"command": "/bin/echo"})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if cat == nil {
		t.Fatal("Discover returned nil catalog")
	}
}

// helperConfig builds a ConnectorConfig that re-execs the test binary
// in helper mode. The mode key tells TestHelperProcess which canned
// behavior to perform.
func helperConfig(mode string) connectors.ConnectorConfig {
	return connectors.ConnectorConfig{
		"command": os.Args[0],
		"args":    []any{"-test.run=TestHelperProcess", "--"},
		"env": map[string]any{
			"GO_WANT_HELPER_PROCESS": "1",
			"HELPER_MODE":            mode,
		},
	}
}

func collect(ch <-chan connectors.Message) (records []connectors.Record, states []connectors.State, logs []connectors.LogEntry, errs []error) {
	for m := range ch {
		switch m.Type {
		case connectors.RecordMsg:
			records = append(records, *m.Record)
		case connectors.StateMsg:
			states = append(states, *m.State)
		case connectors.LogMsg:
			logs = append(logs, *m.Log)
		case connectors.ErrorMsg:
			errs = append(errs, m.Err)
		}
	}
	return
}

func TestExtractRecordsAndState(t *testing.T) {
	t.Parallel()
	c := external.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streams := []connectors.Stream{{Name: "events", Mode: connectors.Incremental}}
	ch, err := c.Extract(ctx, helperConfig("records-and-state"), streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, states, _, _ := collect(ch)
	if len(records) != 3 {
		t.Errorf("records = %d, want 3", len(records))
	}
	for i, r := range records {
		if r.Stream != "events" {
			t.Errorf("records[%d].Stream = %q, want events", i, r.Stream)
		}
		if r.Data["i"] == nil {
			t.Errorf("records[%d] missing data.i", i)
		}
	}
	if len(states) != 1 {
		t.Fatalf("states = %d, want 1", len(states))
	}
	if states[0]["cursor"] != "abc" {
		t.Errorf("state cursor = %v, want abc", states[0]["cursor"])
	}
}

func TestExtractDoneEndsStream(t *testing.T) {
	t.Parallel()
	c := external.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streams := []connectors.Stream{{Name: "events", Mode: connectors.FullRefresh}}
	ch, err := c.Extract(ctx, helperConfig("done-then-extra"), streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, _, _, _ := collect(ch)
	if len(records) != 1 {
		t.Errorf("records = %d, want 1 (DONE should stop the stream before the second record)", len(records))
	}
}

func TestExtractProtocolErrorEmitsErrorMsg(t *testing.T) {
	t.Parallel()
	c := external.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streams := []connectors.Stream{{Name: "events", Mode: connectors.FullRefresh}}
	ch, err := c.Extract(ctx, helperConfig("error-mid-stream"), streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, _, _, errs := collect(ch)
	if len(records) != 1 {
		t.Errorf("records = %d, want 1 (the RECORD before the ERROR)", len(records))
	}
	if len(errs) == 0 {
		t.Fatal("expected an ErrorMsg carrying the child's protocol error")
	}
	if !strings.Contains(errs[0].Error(), "boom") {
		t.Errorf("ErrorMsg err = %v, want it to mention boom", errs[0])
	}
}

func TestExtractContextCancelKillsChild(t *testing.T) {
	t.Parallel()
	c := external.New()
	ctx, cancel := context.WithCancel(context.Background())
	streams := []connectors.Stream{{Name: "events", Mode: connectors.FullRefresh}}
	ch, err := c.Extract(ctx, helperConfig("hang"), streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	cancel()
	done := make(chan struct{})
	go func() { _, _, _, _ = collect(ch); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Extract did not return after ctx cancel")
	}
}

func TestExtractCommandNotFoundEmitsErrorMsg(t *testing.T) {
	t.Parallel()
	c := external.New()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	streams := []connectors.Stream{{Name: "events", Mode: connectors.FullRefresh}}
	cfg := connectors.ConnectorConfig{"command": "/no/such/command/at/all"}
	ch, err := c.Extract(ctx, cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	_, _, _, errs := collect(ch)
	if len(errs) == 0 {
		t.Fatal("expected at least one ErrorMsg reporting the spawn failure")
	}
	if !strings.Contains(errs[0].Error(), "start") {
		t.Errorf("err = %v, want to mention start", errs[0])
	}
}

func TestExtractChildNonZeroExitEmitsErrorMsg(t *testing.T) {
	t.Parallel()
	c := external.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streams := []connectors.Stream{{Name: "events", Mode: connectors.FullRefresh}}
	// /bin/sh -c 'exit 42' prints nothing and exits non-zero.
	cfg := connectors.ConnectorConfig{
		"command": "/bin/sh",
		"args":    []any{"-c", "exit 42"},
	}
	ch, err := c.Extract(ctx, cfg, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	_, _, _, errs := collect(ch)
	if len(errs) == 0 {
		t.Fatal("expected an ErrorMsg when child exits non-zero")
	}
	if !strings.Contains(errs[0].Error(), "exit") {
		t.Errorf("err = %v, want to mention exit", errs[0])
	}
}

func TestExtractDecodeErrorEmitsErrorMsgAndDropsPartialState(t *testing.T) {
	t.Parallel()
	c := external.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streams := []connectors.Stream{{Name: "events", Mode: connectors.Incremental}}
	ch, err := c.Extract(ctx, helperConfig("record-garbage-state"), streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	records, states, _, errs := collect(ch)
	if len(records) != 1 {
		t.Errorf("records = %d, want 1 (the RECORD before the garbage)", len(records))
	}
	if len(states) != 0 {
		t.Errorf("states = %d, want 0 (STATE after the garbage must not leak through)", len(states))
	}
	if len(errs) == 0 {
		t.Fatal("expected an ErrorMsg for the mid-stream decode failure")
	}
}

func TestExtractStdinCarriesStreamsAndState(t *testing.T) {
	t.Parallel()
	c := external.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streams := []connectors.Stream{
		{Name: "alpha", Mode: connectors.Incremental},
		{Name: "beta", Mode: connectors.FullRefresh},
	}
	state := connectors.State{"cursor": "x"}
	ch, err := c.Extract(ctx, helperConfig("echo-stdin"), streams, state)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	_, _, logs, _ := collect(ch)
	if len(logs) == 0 {
		t.Fatal("expected at least one info log echoing stdin")
	}
	var found bool
	for _, l := range logs {
		if strings.Contains(l.Message, "alpha") && strings.Contains(l.Message, "beta") && strings.Contains(l.Message, `"cursor":"x"`) {
			found = true
		}
	}
	if !found {
		t.Errorf("did not find echoed extract command in logs; got %v", logs)
	}
}

// TestHelperProcess is the entrypoint the external connector spawns when
// HELPER_MODE is set. It must live next to the regular tests so the
// test binary contains the necessary code paths.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	defer os.Exit(0)
	switch os.Getenv("HELPER_MODE") {
	case "records-and-state":
		writeOutputs(
			outRecord("events", map[string]any{"i": 1}),
			outRecord("events", map[string]any{"i": 2}),
			outRecord("events", map[string]any{"i": 3}),
			outState(map[string]any{"cursor": "abc"}),
		)
	case "done-then-extra":
		writeOutputs(
			outRecord("events", map[string]any{"i": 1}),
			protocol.Output{Type: protocol.MsgDone},
			outRecord("events", map[string]any{"i": 2}),
		)
	case "error-mid-stream":
		writeOutputs(
			outRecord("events", map[string]any{"i": 1}),
			protocol.Output{Type: protocol.MsgError, Error: "boom"},
		)
	case "record-garbage-state":
		// Emit one valid RECORD, one garbage line that will break
		// the JSON decoder, then one valid STATE that the decoder
		// will never reach. Exercises the "partial batch must be
		// dropped" path.
		writeOutputs(outRecord("events", map[string]any{"i": 1}))
		os.Stdout.Write([]byte("not-json-garbage-line\n"))
		writeOutputs(outState(map[string]any{"cursor": "ghost"}))
	case "hang":
		// Drain stdin so the parent's writes do not SIGPIPE us, then
		// block until killed.
		go io.Copy(io.Discard, os.Stdin)
		select {}
	case "echo-stdin":
		buf, _ := io.ReadAll(os.Stdin)
		writeOutputs(protocol.Output{
			Type:    protocol.MsgLog,
			Level:   "info",
			Message: string(buf),
		})
	}
}

// writeOutputs writes one Output per line on stdout. Use json.Marshal
// directly because protocol.Encoder is typed for Command, not Output.
func writeOutputs(outs ...protocol.Output) {
	for _, o := range outs {
		b, _ := json.Marshal(o)
		os.Stdout.Write(append(b, '\n'))
	}
}

func outRecord(stream string, data map[string]any) protocol.Output {
	return protocol.Output{Type: protocol.MsgRecord, Stream: stream, Data: data}
}

func outState(state map[string]any) protocol.Output {
	return protocol.Output{Type: protocol.MsgState, State: state}
}
