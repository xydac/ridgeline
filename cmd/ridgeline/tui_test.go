package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

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
		"PRODUCT", "CONNECTOR", "STREAM", "LAST SYNC", "RECORDS",
		"myapp", "demo", "testsrc", "pages", "events",
		"never", "q / ctrl+c",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("render missing %q:\n%s", want, out)
		}
	}
	// Records unknown (no manifest yet) should render as "-".
	if !strings.Contains(out, " - ") && !strings.HasSuffix(strings.TrimRight(out, "\n"), "-") {
		t.Errorf("records column should be '-' before first sync:\n%s", out)
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
	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	if cmd != nil {
		t.Errorf("unknown key should not emit a cmd, got %v", cmd)
	}
	if next.(tuiModel).quit {
		t.Errorf("unknown key should not quit")
	}
}
