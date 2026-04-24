package pipeline_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/pipeline"
	"github.com/xydac/ridgeline/sinks"
)

// fakeConnector emits a caller-provided slice of Messages on Extract.
type fakeConnector struct {
	msgs       []connectors.Message
	extractErr error
	// gotState captures the state passed in at Extract.
	gotState connectors.State
}

func (f *fakeConnector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{Name: "fake", Version: "0.0.0"}
}
func (f *fakeConnector) Validate(context.Context, connectors.ConnectorConfig) error { return nil }
func (f *fakeConnector) Discover(context.Context, connectors.ConnectorConfig) (*connectors.Catalog, error) {
	return &connectors.Catalog{}, nil
}
func (f *fakeConnector) Extract(_ context.Context, _ connectors.ConnectorConfig, _ []connectors.Stream, s connectors.State) (<-chan connectors.Message, error) {
	f.gotState = s
	if f.extractErr != nil {
		return nil, f.extractErr
	}
	ch := make(chan connectors.Message, len(f.msgs))
	for _, m := range f.msgs {
		ch <- m
	}
	close(ch)
	return ch, nil
}

// recordingSink captures every Write/Flush/Close for assertions.
type recordingSink struct {
	mu       sync.Mutex
	writes   map[string][][]connectors.Record
	flushes  int
	writeErr error
	flushErr error
}

func newRecordingSink() *recordingSink {
	return &recordingSink{writes: map[string][][]connectors.Record{}}
}

func (r *recordingSink) Name() string                                 { return "rec" }
func (r *recordingSink) Init(context.Context, sinks.SinkConfig) error { return nil }
func (r *recordingSink) Write(_ context.Context, stream string, recs []connectors.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writeErr != nil {
		return r.writeErr
	}
	cp := make([]connectors.Record, len(recs))
	copy(cp, recs)
	r.writes[stream] = append(r.writes[stream], cp)
	return nil
}
func (r *recordingSink) Flush(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.flushes++
	return r.flushErr
}
func (r *recordingSink) Close() error { return nil }

func (r *recordingSink) totalRecords(stream string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, batch := range r.writes[stream] {
		n += len(batch)
	}
	return n
}

func rec(stream, id string) connectors.Message {
	return connectors.RecordMessage(stream, connectors.Record{
		Timestamp: time.Unix(0, 0),
		Data:      map[string]any{"id": id},
	})
}

func TestRun_HappyPath(t *testing.T) {
	t.Parallel()
	msgs := []connectors.Message{
		rec("pages", "a"),
		rec("pages", "b"),
		connectors.StateMessage(connectors.State{"cursor": "2"}),
		rec("events", "e1"),
	}
	conn := &fakeConnector{msgs: msgs}
	sink := newRecordingSink()
	store := pipeline.NewMemoryStateStore()

	res, err := pipeline.Run(context.Background(), conn, sink, store, pipeline.Request{Key: "fake"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Records != 3 {
		t.Fatalf("Records = %d, want 3", res.Records)
	}
	if res.States != 1 {
		t.Fatalf("States = %d, want 1", res.States)
	}
	if sink.totalRecords("pages") != 2 {
		t.Errorf("pages total = %d, want 2", sink.totalRecords("pages"))
	}
	if sink.totalRecords("events") != 1 {
		t.Errorf("events total = %d, want 1", sink.totalRecords("events"))
	}
	// Final close-of-channel flush + StateMsg flush = at least 2.
	if sink.flushes < 2 {
		t.Errorf("flushes = %d, want >= 2", sink.flushes)
	}
	got, _ := store.Load(context.Background(), "fake")
	if got["cursor"] != "2" {
		t.Errorf("persisted state = %v, want cursor=2", got)
	}
}

func TestRun_LoadsPriorState(t *testing.T) {
	t.Parallel()
	conn := &fakeConnector{}
	sink := newRecordingSink()
	store := pipeline.NewMemoryStateStore()
	_ = store.Save(context.Background(), "fake", connectors.State{"cursor": "prior"})

	if _, err := pipeline.Run(context.Background(), conn, sink, store, pipeline.Request{Key: "fake"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if conn.gotState["cursor"] != "prior" {
		t.Errorf("connector saw state %v, want cursor=prior", conn.gotState)
	}
}

func TestRun_EmptyStream(t *testing.T) {
	t.Parallel()
	conn := &fakeConnector{}
	sink := newRecordingSink()
	store := pipeline.NewMemoryStateStore()

	res, err := pipeline.Run(context.Background(), conn, sink, store, pipeline.Request{Key: "fake"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Records != 0 {
		t.Errorf("Records = %d, want 0", res.Records)
	}
	if sink.flushes != 1 {
		t.Errorf("flushes = %d, want 1", sink.flushes)
	}
}

func TestRun_BatchSizeFlushes(t *testing.T) {
	t.Parallel()
	const n = 250
	msgs := make([]connectors.Message, 0, n)
	for i := 0; i < n; i++ {
		msgs = append(msgs, rec("s", fmt.Sprintf("%d", i)))
	}
	conn := &fakeConnector{msgs: msgs}
	sink := newRecordingSink()
	store := pipeline.NewMemoryStateStore()

	res, err := pipeline.Run(context.Background(), conn, sink, store, pipeline.Request{Key: "fake", BatchSize: 100})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Records != n {
		t.Fatalf("Records = %d, want %d", res.Records, n)
	}
	// Expect: 100, 100, 50 = three Write calls for stream "s".
	got := len(sink.writes["s"])
	if got != 3 {
		t.Errorf("Write batches for s = %d, want 3", got)
	}
}

func TestRun_StateMsgWithNoRecords(t *testing.T) {
	t.Parallel()
	msgs := []connectors.Message{
		connectors.StateMessage(connectors.State{"cursor": "0"}),
	}
	conn := &fakeConnector{msgs: msgs}
	sink := newRecordingSink()
	store := pipeline.NewMemoryStateStore()

	res, err := pipeline.Run(context.Background(), conn, sink, store, pipeline.Request{Key: "fake"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.States != 1 {
		t.Fatalf("States = %d, want 1", res.States)
	}
	got, _ := store.Load(context.Background(), "fake")
	if got["cursor"] != "0" {
		t.Errorf("persisted state = %v, want cursor=0", got)
	}
}

func TestRun_LogAndSchemaMessages(t *testing.T) {
	t.Parallel()
	msgs := []connectors.Message{
		connectors.LogMessage(connectors.LevelInfo, "hi"),
		connectors.SchemaMessage("s", connectors.Schema{}),
		rec("s", "1"),
	}
	conn := &fakeConnector{msgs: msgs}
	sink := newRecordingSink()
	store := pipeline.NewMemoryStateStore()

	logger := log.New(discardWriter{}, "", 0)
	res, err := pipeline.Run(context.Background(), conn, sink, store, pipeline.Request{Key: "fake", Logger: logger})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.SchemaMessages != 1 {
		t.Errorf("SchemaMessages = %d, want 1", res.SchemaMessages)
	}
	if res.Records != 1 {
		t.Errorf("Records = %d, want 1", res.Records)
	}
}

// TestRun_LogFormat_NoStdlibPrefix pins the warn-line format at
// `level: [key] message`, with no leading YYYY/MM/DD HH:MM:SS from the
// stdlib logger. Regressing this turns every sync terminal into a
// grep-unfriendly timestamp stream (F-014).
func TestRun_LogFormat_NoStdlibPrefix(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	msgs := []connectors.Message{
		connectors.LogMessage(connectors.LevelWarn, "hackernews: unknown stream \"bogus\""),
	}
	_, err := pipeline.Run(context.Background(),
		&fakeConnector{msgs: msgs}, newRecordingSink(),
		pipeline.NewMemoryStateStore(),
		pipeline.Request{Key: "myapp/hn", Logger: logger})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := strings.TrimRight(buf.String(), "\n")
	want := "warn: [myapp/hn] hackernews: unknown stream \"bogus\""
	if got != want {
		t.Errorf("log line\n got:  %q\n want: %q", got, want)
	}
}

func TestRun_ExtractError(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	conn := &fakeConnector{extractErr: boom}
	_, err := pipeline.Run(context.Background(), conn, newRecordingSink(), pipeline.NewMemoryStateStore(), pipeline.Request{Key: "fake"})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want to wrap boom", err)
	}
}

func TestRun_WriteError(t *testing.T) {
	t.Parallel()
	msgs := []connectors.Message{rec("s", "1")}
	conn := &fakeConnector{msgs: msgs}
	sink := newRecordingSink()
	sink.writeErr = errors.New("disk full")
	_, err := pipeline.Run(context.Background(), conn, sink, pipeline.NewMemoryStateStore(), pipeline.Request{Key: "fake"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestRun_ErrorMsgTerminates(t *testing.T) {
	t.Parallel()
	boom := errors.New("external: boom")
	// Emit one record, then a fatal ErrorMsg, then a STATE that should
	// be ignored. The pipeline should return boom, not save state, and
	// not flush the buffered record.
	msgs := []connectors.Message{
		rec("s", "1"),
		connectors.ErrorMessage(boom),
		connectors.StateMessage(connectors.State{"cursor": "should-not-persist"}),
	}
	conn := &fakeConnector{msgs: msgs}
	sink := newRecordingSink()
	store := pipeline.NewMemoryStateStore()

	res, err := pipeline.Run(context.Background(), conn, sink, store, pipeline.Request{Key: "fake"})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want to wrap boom", err)
	}
	if res.Records != 0 {
		t.Errorf("Records = %d, want 0 (buffered record must not count as written)", res.Records)
	}
	if sink.totalRecords("s") != 0 {
		t.Errorf("sink got %d records, want 0 (buffered record must not be flushed after ErrorMsg)", sink.totalRecords("s"))
	}
	if res.States != 0 {
		t.Errorf("States = %d, want 0 (STATE after ErrorMsg must be ignored)", res.States)
	}
	got, _ := store.Load(context.Background(), "fake")
	if len(got) != 0 {
		t.Errorf("persisted state = %v, want empty (STATE after ErrorMsg must not be saved)", got)
	}
}

func TestRun_ErrorMsgNilErr(t *testing.T) {
	t.Parallel()
	conn := &fakeConnector{msgs: []connectors.Message{{Type: connectors.ErrorMsg}}}
	_, err := pipeline.Run(context.Background(), conn, newRecordingSink(), pipeline.NewMemoryStateStore(), pipeline.Request{Key: "fake"})
	if err == nil {
		t.Fatal("expected error for ErrorMsg with nil Err, got nil")
	}
}

func TestRun_RequiresKey(t *testing.T) {
	t.Parallel()
	_, err := pipeline.Run(context.Background(), &fakeConnector{}, newRecordingSink(), pipeline.NewMemoryStateStore(), pipeline.Request{})
	if err == nil {
		t.Fatalf("expected error for empty Key")
	}
}

func TestRun_NilArgs(t *testing.T) {
	t.Parallel()
	req := pipeline.Request{Key: "fake"}
	if _, err := pipeline.Run(context.Background(), nil, newRecordingSink(), pipeline.NewMemoryStateStore(), req); err == nil {
		t.Error("nil Connector should error")
	}
	if _, err := pipeline.Run(context.Background(), &fakeConnector{}, nil, pipeline.NewMemoryStateStore(), req); err == nil {
		t.Error("nil Sink should error")
	}
	if _, err := pipeline.Run(context.Background(), &fakeConnector{}, newRecordingSink(), nil, req); err == nil {
		t.Error("nil StateStore should error")
	}
}

func TestRun_ContextCancel(t *testing.T) {
	t.Parallel()
	// Connector that never closes its channel until ctx is done.
	blocking := &blockingConnector{ready: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := pipeline.Run(ctx, blocking, newRecordingSink(), pipeline.NewMemoryStateStore(), pipeline.Request{Key: "fake"})
		done <- err
	}()
	<-blocking.ready
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestMemoryStateStore_ReturnsCopies(t *testing.T) {
	t.Parallel()
	store := pipeline.NewMemoryStateStore()
	in := connectors.State{"k": "v"}
	_ = store.Save(context.Background(), "key", in)
	in["k"] = "mutated"
	out, _ := store.Load(context.Background(), "key")
	if out["k"] != "v" {
		t.Errorf("state was mutated via input map: got %v", out)
	}
	out["k"] = "mutated_out"
	out2, _ := store.Load(context.Background(), "key")
	if out2["k"] != "v" {
		t.Errorf("state was mutated via output map: got %v", out2)
	}
}

type blockingConnector struct {
	ready chan struct{}
}

func (b *blockingConnector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{Name: "block", Version: "0.0.0"}
}
func (b *blockingConnector) Validate(context.Context, connectors.ConnectorConfig) error { return nil }
func (b *blockingConnector) Discover(context.Context, connectors.ConnectorConfig) (*connectors.Catalog, error) {
	return &connectors.Catalog{}, nil
}
func (b *blockingConnector) Extract(ctx context.Context, _ connectors.ConnectorConfig, _ []connectors.Stream, _ connectors.State) (<-chan connectors.Message, error) {
	ch := make(chan connectors.Message)
	go func() {
		close(b.ready)
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
