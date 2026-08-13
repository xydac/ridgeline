package git

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/xydac/ridgeline/connectors"
)

// initTestRepo creates a temporary git repo with n commits and returns the path.
func initTestRepo(t *testing.T, n int) string {
	t.Helper()
	dir := t.TempDir()

	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test",
			"GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=Test",
			"GIT_COMMITTER_EMAIL=test@example.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}

	run("init", "-b", "main")
	run("config", "user.email", "test@example.com")
	run("config", "user.name", "Test")
	for i := range n {
		file := filepath.Join(dir, "f")
		if err := os.WriteFile(file, []byte{byte(i)}, 0644); err != nil {
			t.Fatal(err)
		}
		run("add", ".")
		run("commit", "-m", "commit "+string(rune('A'+i)))
	}
	return dir
}

func TestGitConnector_Spec(t *testing.T) {
	c := New()
	spec := c.Spec()
	if spec.Name != Name {
		t.Errorf("Name: got %q, want %q", spec.Name, Name)
	}
	if len(spec.Streams) != 1 || spec.Streams[0].Name != StreamCommits {
		t.Error("expected one stream: commits")
	}
}

func TestGitConnector_Validate(t *testing.T) {
	c := New()
	dir := initTestRepo(t, 1)

	if err := c.Validate(context.Background(), connectors.ConnectorConfig{"path": dir}); err != nil {
		t.Errorf("Validate on valid repo: %v", err)
	}
	if err := c.Validate(context.Background(), connectors.ConnectorConfig{"path": t.TempDir()}); err == nil {
		t.Error("Validate on non-repo should fail")
	}
}

func TestGitConnector_Extract_AllCommits(t *testing.T) {
	c := New()
	dir := initTestRepo(t, 3)

	cfg := connectors.ConnectorConfig{"path": dir}
	streams := []connectors.Stream{{Name: StreamCommits, Mode: connectors.Incremental}}
	ch, err := c.Extract(context.Background(), cfg, streams, connectors.State{})
	if err != nil {
		t.Fatal(err)
	}
	var msgs []connectors.Message
	for m := range ch {
		msgs = append(msgs, m)
	}
	var records int
	for _, m := range msgs {
		if m.Type == connectors.RecordMsg {
			records++
		}
	}
	if records != 3 {
		t.Errorf("expected 3 commit records, got %d", records)
	}
	for _, m := range msgs {
		if m.Type != connectors.RecordMsg {
			continue
		}
		if m.Record.Stream != StreamCommits {
			t.Errorf("unexpected stream %q", m.Record.Stream)
		}
		if m.Record.Timestamp.IsZero() {
			t.Error("timestamp is zero")
		}
		if _, ok := m.Record.Data["hash"]; !ok {
			t.Error("missing hash field")
		}
		if _, ok := m.Record.Data["subject"]; !ok {
			t.Error("missing subject field")
		}
	}
}

func TestGitConnector_Extract_Incremental(t *testing.T) {
	c := New()
	dir := initTestRepo(t, 2)

	cfg := connectors.ConnectorConfig{"path": dir}
	streams := []connectors.Stream{{Name: StreamCommits, Mode: connectors.Incremental}}

	// First run: drain channel, capture the state message.
	ch, err := c.Extract(context.Background(), cfg, streams, connectors.State{})
	if err != nil {
		t.Fatal(err)
	}
	var cursor connectors.State
	var recordCount int
	for m := range ch {
		if m.Type == connectors.RecordMsg {
			recordCount++
		}
		if m.Type == connectors.StateMsg && m.State != nil {
			cursor = *m.State
		}
	}
	if recordCount != 2 {
		t.Fatalf("expected 2 record messages, got %d", recordCount)
	}

	// Second run with cursor: nothing new.
	ch2, err := c.Extract(context.Background(), cfg, streams, cursor)
	if err != nil {
		t.Fatal(err)
	}
	var second []connectors.Message
	for m := range ch2 {
		second = append(second, m)
	}
	if len(second) != 0 {
		t.Errorf("expected 0 messages after cursor, got %d", len(second))
	}
}

func TestGitConnector_EmitEvents(t *testing.T) {
	c := New()
	dir := initTestRepo(t, 2)

	cfg := connectors.ConnectorConfig{"path": dir}
	events, err := c.EmitEvents(context.Background(), cfg, connectors.State{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	for _, e := range events {
		if e.Kind != "commit" {
			t.Errorf("kind: got %q, want commit", e.Kind)
		}
		if e.Hash == "" {
			t.Error("hash is empty")
		}
		if e.Description == "" {
			t.Error("description is empty")
		}
		if e.At.Equal(time.Time{}) {
			t.Error("At is zero")
		}
	}
}

func TestGitConnector_EmitEvents_Incremental(t *testing.T) {
	c := New()
	dir := initTestRepo(t, 3)
	cfg := connectors.ConnectorConfig{"path": dir}

	// Get all events; use the first (newest) hash as cursor.
	all, err := c.EmitEvents(context.Background(), cfg, connectors.State{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3, got %d", len(all))
	}
	cursor := connectors.State{cursorKey: all[0].Hash}

	// Second call: nothing new.
	none, err := c.EmitEvents(context.Background(), cfg, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 0 {
		t.Errorf("expected 0 after cursor, got %d", len(none))
	}
}
