package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

func TestRunTUI_RequiresConfig(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runTUI(context.Background(), nil, &buf)
	if err == nil || !strings.Contains(err.Error(), "--config") {
		t.Fatalf("want --config required error, got %v", err)
	}
}

func TestRunTUI_RenderOnce_NeverSynced(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ridgeline.db")
	outDir := filepath.Join(dir, "out")
	cfgPath := configFixture(t, dir, dbPath, outDir)

	var buf bytes.Buffer
	if err := runTUI(context.Background(), []string{"--config", cfgPath, "--render-once"}, &buf); err != nil {
		t.Fatalf("runTUI: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"PRODUCT", "CONNECTOR", "STREAM", "STATUS", "LAST SYNC", "RECORDS",
		"myapp", "demo", "testsrc", "pages", "events",
		"never", "q/ctrl+c", "s  sync",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("render missing %q:\n%s", want, out)
		}
	}
}

func TestRunTUI_RenderOnce_AfterSync_ShowsRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ridgeline.db")
	outDir := filepath.Join(dir, "out")
	cfgPath := configFixture(t, dir, dbPath, outDir)

	if err := runSync(context.Background(), []string{"--config", cfgPath}); err != nil {
		t.Fatalf("runSync: %v", err)
	}

	var buf bytes.Buffer
	if err := runTUI(context.Background(), []string{"--config", cfgPath, "--render-once"}, &buf); err != nil {
		t.Fatalf("runTUI: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, " never ") {
		t.Errorf("should not say 'never' after sync:\n%s", out)
	}
	// testsrc configured with records: 2, two streams, so 2 records
	// per stream in the manifest.
	lines := strings.Split(out, "\n")
	var dataLines []string
	for _, ln := range lines {
		if strings.Contains(ln, "myapp") && strings.Contains(ln, "demo") {
			dataLines = append(dataLines, ln)
		}
	}
	if len(dataLines) != 2 {
		t.Fatalf("want 2 data lines, got %d:\n%s", len(dataLines), out)
	}
	for _, ln := range dataLines {
		if !strings.Contains(ln, " 2") {
			t.Errorf("expected '2' record count on line: %q", ln)
		}
		if !strings.Contains(ln, tuiStatusOK) {
			t.Errorf("expected status %q on line: %q", tuiStatusOK, ln)
		}
	}
}

func TestTUIModel_QuitKeys(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		msg  tea.KeyMsg
	}{
		{"q", tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}}},
		{"ctrl+c", tea.KeyMsg{Type: tea.KeyCtrlC}},
		{"esc", tea.KeyMsg{Type: tea.KeyEsc}},
	}
	for _, tc := range cases {
		m := tuiModel{cfgPath: "x", rows: nil}
		next, cmd := m.Update(tc.msg)
		if cmd == nil {
			t.Errorf("%s: expected tea.Quit cmd", tc.name)
		}
		if !next.(tuiModel).quit {
			t.Errorf("%s: expected quit=true", tc.name)
		}
	}

	// Unknown keys leave the model alone and emit no command.
	m := tuiModel{cfgPath: "x"}
	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'z'}})
	if cmd != nil {
		t.Errorf("unknown key should not emit a cmd, got %v", cmd)
	}
	if next.(tuiModel).quit {
		t.Errorf("unknown key should not quit")
	}
}

func TestTUIModel_CursorMovement(t *testing.T) {
	t.Parallel()
	rows := []tuiRow{
		{Product: "a", Connector: "c1", Stream: "s1"},
		{Product: "a", Connector: "c1", Stream: "s2"},
		{Product: "a", Connector: "c2", Stream: "s3"},
	}
	m := tuiModel{cfgPath: "x", rows: rows}

	down := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}}
	up := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}}

	// Cursor clamps at 0 on up.
	next, _ := m.Update(up)
	if next.(tuiModel).cursor != 0 {
		t.Errorf("up at top should clamp to 0, got %d", next.(tuiModel).cursor)
	}

	// j advances.
	var model tea.Model = next
	model, _ = model.(tuiModel).Update(down)
	if model.(tuiModel).cursor != 1 {
		t.Errorf("after j, cursor=%d, want 1", model.(tuiModel).cursor)
	}
	model, _ = model.(tuiModel).Update(down)
	model, _ = model.(tuiModel).Update(down)
	if model.(tuiModel).cursor != 2 {
		t.Errorf("cursor should clamp at last row (2), got %d", model.(tuiModel).cursor)
	}

	// k goes back.
	model, _ = model.(tuiModel).Update(up)
	if model.(tuiModel).cursor != 1 {
		t.Errorf("after k, cursor=%d, want 1", model.(tuiModel).cursor)
	}
}

func TestDeriveStatus(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		lastSync string
		want     string
	}{
		{"empty", "", tuiStatusNever},
		{"unparseable", "nonsense", tuiStatusNever},
		{"recent", now.Add(-1 * time.Hour).Format(time.RFC3339), tuiStatusOK},
		{"stale", now.Add(-48 * time.Hour).Format(time.RFC3339), tuiStatusStale},
		{"ms format", now.Add(-30 * time.Minute).UTC().Format("2006-01-02T15:04:05.000Z07:00"), tuiStatusOK},
	}
	for _, tc := range cases {
		got := deriveStatus(tc.lastSync, now)
		if got != tc.want {
			t.Errorf("%s: deriveStatus(%q)=%q, want %q", tc.name, tc.lastSync, got, tc.want)
		}
	}
}

func TestTUIModel_SyncKeyTriggersSyncerAndRefreshes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ridgeline.db")
	outDir := filepath.Join(dir, "out")
	cfgPath := configFixture(t, dir, dbPath, outDir)

	rows, err := collectTUIRows(context.Background(), cfgPath, time.Now().UTC())
	if err != nil {
		t.Fatalf("collectTUIRows: %v", err)
	}
	for _, r := range rows {
		if r.Status != tuiStatusNever {
			t.Fatalf("pre-sync rows should all be 'never', got %q", r.Status)
		}
	}

	called := 0
	syncer := func(ctx context.Context, p, pid, conn string) (int, error) {
		called++
		if pid != "myapp" || conn != "demo" {
			t.Errorf("syncer got pid=%s conn=%s, want myapp/demo", pid, conn)
		}
		// Actually run the sync so refreshRows sees real data.
		return realTUISyncer(ctx, p, pid, conn)
	}

	m := tuiModel{cfgPath: cfgPath, rows: rows, syncer: syncer}
	nextModel, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	if cmd == nil {
		t.Fatal("expected a tea.Cmd from s keypress")
	}
	mm := nextModel.(tuiModel)
	// All rows belonging to the highlighted connector should flip to "syncing".
	for _, r := range mm.rows {
		if r.Product == "myapp" && r.Connector == "demo" && r.Status != tuiStatusSyncing {
			t.Errorf("row %+v should be syncing", r)
		}
	}

	msg := cmd()
	done, ok := msg.(syncDoneMsg)
	if !ok {
		t.Fatalf("cmd returned %T, want syncDoneMsg", msg)
	}
	if done.err != nil {
		t.Fatalf("sync failed: %v", done.err)
	}
	nextModel, _ = mm.Update(done)
	mm = nextModel.(tuiModel)
	if called != 1 {
		t.Errorf("syncer called %d times, want 1", called)
	}
	for _, r := range mm.rows {
		if r.Records != 2 {
			t.Errorf("row %+v: expected records=2 after sync", r)
		}
		if r.Status != tuiStatusOK {
			t.Errorf("row %+v: expected status=%q after sync", r, tuiStatusOK)
		}
	}

	// A second `s` while already syncing is a no-op (guard against
	// re-entrancy). Force the syncing flag on the key manually.
	mm.syncing = map[string]bool{"myapp/demo": true}
	_, cmd2 := mm.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	if cmd2 != nil {
		t.Error("second s while syncing should not dispatch a new cmd")
	}
}

func TestTUIModel_SyncError_SetsErrorStatus(t *testing.T) {
	t.Parallel()
	rows := []tuiRow{
		{Product: "p", Connector: "c", Stream: "s", Status: tuiStatusNever},
	}
	syncer := func(ctx context.Context, cfgPath, pid, conn string) (int, error) {
		return 0, errors.New("boom")
	}
	m := tuiModel{cfgPath: "missing.yaml", rows: rows, syncer: syncer}
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	if cmd == nil {
		t.Fatal("expected a cmd")
	}
	msg := cmd()
	done, ok := msg.(syncDoneMsg)
	if !ok || done.err == nil {
		t.Fatalf("want syncDoneMsg with err, got %T %+v", msg, msg)
	}
	// refreshRows will also fail because the cfg is missing; its
	// failure is swallowed and the existing rows remain.
	nextModel, _ := m.Update(done)
	mm := nextModel.(tuiModel)
	if mm.rows[0].Status != tuiStatusError {
		t.Errorf("row status=%q, want %q", mm.rows[0].Status, tuiStatusError)
	}
	view := mm.View()
	if !strings.Contains(view, "boom") {
		t.Errorf("view should surface error message, got:\n%s", view)
	}
}
