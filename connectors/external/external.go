package external

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/protocol"
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

// knownConfigKeys enumerates every config key this connector reads.
var knownConfigKeys = []string{"command", "args", "env", "dir"}

// Validate checks that a command is configured and that every supplied
// config key is one the runner recognizes. It does not invoke the
// child process; reachability of the underlying source is the child's
// responsibility, and a "command exists" check would not be meaningful
// (the user might have a virtualenv-relative or PATH-relative entry).
func (c *Connector) Validate(_ context.Context, cfg connectors.ConnectorConfig) error {
	if err := connectors.CheckUnknownKeys(cfg, knownConfigKeys...); err != nil {
		return fmt.Errorf("external: %w", err)
	}
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

// Extract spawns the configured child process, sends an extract command
// on its stdin, and translates the JSON-lines messages on its stdout
// into connectors.Message values on the returned channel. The channel
// is closed when the child exits, when stdout reaches EOF, when the
// child sends a DONE or ERROR message, or when ctx is cancelled.
//
// Errors that occur before the channel is even returned (config
// validation, pipe creation) are returned synchronously. Fatal errors
// that arise mid-stream (spawn failure, decode failure, child exit
// code, child ERROR message) are forwarded as a final ErrorMessage so
// the pipeline aborts the sync with a non-zero exit; stderr noise and
// other non-fatal output still surface as warn-level LogMessages.
func (c *Connector) Extract(ctx context.Context, cfg connectors.ConnectorConfig, streams []connectors.Stream, state connectors.State) (<-chan connectors.Message, error) {
	command := strings.TrimSpace(cfg.String("command"))
	if command == "" {
		return nil, fmt.Errorf("external: command must not be empty")
	}
	args := cfg.StringSlice("args")
	env := stringMap(cfg["env"])
	workDir := strings.TrimSpace(cfg.String("dir"))

	ch := make(chan connectors.Message, 64)
	go run(ctx, ch, command, args, env, workDir, streams, state)
	return ch, nil
}

// run is the body of Extract's goroutine. It owns the lifecycle of one
// child process and is the only place that closes ch.
func run(ctx context.Context, ch chan<- connectors.Message, command string, args []string, env map[string]string, workDir string, streams []connectors.Stream, state connectors.State) {
	defer close(ch)

	cmd := exec.CommandContext(ctx, command, args...)
	if workDir != "" {
		cmd.Dir = workDir
	}
	cmd.Env = mergedEnv(env)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		emitError(ctx, ch, fmt.Errorf("external: stdin pipe: %w", err))
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		emitError(ctx, ch, fmt.Errorf("external: stdout pipe: %w", err))
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		emitError(ctx, ch, fmt.Errorf("external: stderr pipe: %w", err))
		return
	}

	if err := cmd.Start(); err != nil {
		emitError(ctx, ch, fmt.Errorf("external: start %q: %w", command, err))
		return
	}

	// Drain stderr in the background so a chatty child cannot block on
	// a full pipe. Stderr lines surface as warn-level logs.
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		drainStderr(ctx, ch, stderr)
	}()

	// Send the extract command on stdin and close it. writeExtractCommand
	// failure is reported as a log for now, because we still want to read
	// whatever the child managed to produce; the terminal error (if any)
	// is raised downstream from decode or wait.
	if err := writeExtractCommand(stdin, streams, state); err != nil {
		emitLog(ctx, ch, connectors.LevelError, fmt.Sprintf("external: write extract command: %v", err))
	}
	_ = stdin.Close()

	decodeErr := decodeOutputs(ctx, ch, stdout)

	// Wait for stderr drain to settle before reaping, so all log lines
	// are emitted in order.
	<-stderrDone
	waitErr := cmd.Wait()

	// Context cancel is not a failure; everything else might be.
	if ctx.Err() != nil {
		return
	}

	// decodeOutputs surfaces an ERROR protocol message by returning
	// protocolError; everything else (malformed JSON, truncated stream)
	// is a decode failure. Both are fatal.
	if decodeErr != nil && !errors.Is(decodeErr, io.EOF) {
		emitError(ctx, ch, fmt.Errorf("external: %w", decodeErr))
		return
	}
	if waitErr != nil {
		emitError(ctx, ch, fmt.Errorf("external: child %q exited: %w", command, waitErr))
		return
	}
}

// writeExtractCommand encodes a single "extract" Command on w.
func writeExtractCommand(w io.Writer, streams []connectors.Stream, state connectors.State) error {
	enc := protocol.NewEncoder(w)
	refs := make([]protocol.StreamRef, 0, len(streams))
	for _, s := range streams {
		refs = append(refs, protocol.StreamRef{Name: s.Name, Mode: s.Mode.String()})
	}
	cmd := protocol.Command{
		Type:    protocol.CmdExtract,
		Streams: refs,
		State:   map[string]any(state),
	}
	if err := enc.Write(cmd); err != nil {
		return err
	}
	return enc.Flush()
}

// decodeOutputs reads JSON-lines Outputs from r and writes them on ch
// as connectors.Messages. It returns when stdout closes, when ctx is
// done, when a DONE or ERROR message arrives, or when a decode error
// is hit.
func decodeOutputs(ctx context.Context, ch chan<- connectors.Message, r io.Reader) error {
	dec := protocol.NewDecoder(r)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		out, err := dec.Read()
		if err != nil {
			return err
		}
		switch out.Type {
		case protocol.MsgRecord:
			rec := connectors.Record{
				Stream: out.Stream,
				Data:   out.Data,
			}
			if out.Timestamp != "" {
				if t, perr := time.Parse(time.RFC3339Nano, out.Timestamp); perr == nil {
					rec.Timestamp = t
				}
			}
			if !send(ctx, ch, connectors.RecordMessage(out.Stream, rec)) {
				return ctx.Err()
			}
		case protocol.MsgState:
			if !send(ctx, ch, connectors.StateMessage(connectors.State(out.State))) {
				return ctx.Err()
			}
		case protocol.MsgLog:
			level := levelFromString(out.Level)
			if !send(ctx, ch, connectors.LogMessage(level, out.Message)) {
				return ctx.Err()
			}
		case protocol.MsgSchema:
			if out.Schema == nil {
				continue
			}
			schema := connectors.Schema{Columns: columnsFromProtocol(out.Schema.Columns)}
			if !send(ctx, ch, connectors.SchemaMessage(out.Stream, schema)) {
				return ctx.Err()
			}
		case protocol.MsgError:
			// Protocol ERROR messages are the child telling us it
			// has given up. Surface them as a fatal decode error so
			// the run-loop turns them into an ErrorMsg on the
			// channel.
			return &protocolError{msg: out.Error}
		case protocol.MsgDone:
			return nil
		default:
			// Forward unknown types as warn-level logs so a forward-
			// compatible child does not silently disappear into a hole.
			emitLog(ctx, ch, connectors.LevelWarn, fmt.Sprintf("external: ignoring unknown message type %q", out.Type))
		}
	}
}

// drainStderr reads lines from r and forwards them as warn-level logs.
// It returns when r closes (the child exits or we kill it).
func drainStderr(ctx context.Context, ch chan<- connectors.Message, r io.Reader) {
	buf := make([]byte, 4096)
	var carry []byte
	for {
		n, err := r.Read(buf)
		if n > 0 {
			carry = append(carry, buf[:n]...)
			for {
				i := indexByte(carry, '\n')
				if i < 0 {
					break
				}
				line := strings.TrimRight(string(carry[:i]), "\r")
				carry = carry[i+1:]
				if line == "" {
					continue
				}
				if !send(ctx, ch, connectors.LogMessage(connectors.LevelWarn, line)) {
					return
				}
			}
		}
		if err != nil {
			if len(carry) > 0 {
				_ = send(ctx, ch, connectors.LogMessage(connectors.LevelWarn, string(carry)))
			}
			return
		}
	}
}

// columnsFromProtocol converts wire schema columns into the connectors
// shape. Type strings map to the connectors.ColumnType enum; unknown
// types fall back to connectors.JSON so no data is silently dropped.
func columnsFromProtocol(cols []protocol.Column) []connectors.Column {
	out := make([]connectors.Column, 0, len(cols))
	for _, c := range cols {
		out = append(out, connectors.Column{
			Name:     c.Name,
			Type:     columnTypeFromString(c.Type),
			Required: c.Required,
			Key:      c.Key,
		})
	}
	return out
}

func columnTypeFromString(s string) connectors.ColumnType {
	switch strings.ToLower(s) {
	case "string":
		return connectors.String
	case "int", "integer":
		return connectors.Int
	case "float", "double":
		return connectors.Float
	case "bool", "boolean":
		return connectors.Bool
	case "timestamp", "datetime":
		return connectors.Timestamp
	}
	return connectors.JSON
}

func levelFromString(s string) connectors.LogLevel {
	switch strings.ToLower(s) {
	case "debug":
		return connectors.LevelDebug
	case "warn", "warning":
		return connectors.LevelWarn
	case "error":
		return connectors.LevelError
	}
	return connectors.LevelInfo
}

// send writes m on ch unless ctx is cancelled first.
func send(ctx context.Context, ch chan<- connectors.Message, m connectors.Message) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- m:
		return true
	}
}

// emitLog is a non-blocking-on-cancel convenience wrapper.
func emitLog(ctx context.Context, ch chan<- connectors.Message, level connectors.LogLevel, msg string) {
	send(ctx, ch, connectors.LogMessage(level, msg))
}

// emitError sends err to the pipeline as a fatal ErrorMsg. Callers
// should return immediately after emitError; the pipeline will stop
// consuming the channel once it sees the ErrorMsg.
func emitError(ctx context.Context, ch chan<- connectors.Message, err error) {
	send(ctx, ch, connectors.ErrorMessage(err))
}

// protocolError wraps a child-emitted ERROR message so the run-loop
// can distinguish "child told us it failed" from "we failed to decode
// the child's output". Both are fatal.
type protocolError struct{ msg string }

func (p *protocolError) Error() string { return "child error: " + p.msg }

// stringMap coerces an arbitrary value (typically the YAML-decoded
// `env:` map) into map[string]string. Unsupported shapes yield nil.
func stringMap(v any) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, val := range m {
			if s, ok := val.(string); ok {
				out[k] = s
			} else {
				out[k] = fmt.Sprint(val)
			}
		}
		return out
	}
	return nil
}

// mergedEnv returns the parent environment with the per-connector env
// overrides applied. A nil overrides map yields os.Environ() unchanged.
func mergedEnv(overrides map[string]string) []string {
	if len(overrides) == 0 {
		return os.Environ()
	}
	parent := os.Environ()
	out := make([]string, 0, len(parent)+len(overrides))
	skip := make(map[string]bool, len(overrides))
	for k := range overrides {
		skip[k] = true
	}
	for _, kv := range parent {
		if i := strings.IndexByte(kv, '='); i > 0 {
			if skip[kv[:i]] {
				continue
			}
		}
		out = append(out, kv)
	}
	for k, v := range overrides {
		out = append(out, k+"="+v)
	}
	return out
}

// indexByte is a tiny helper so the file does not pull in bytes for one call.
func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}
