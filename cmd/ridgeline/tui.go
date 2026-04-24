package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/manifest"
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

// Status values shown in the STATUS column. "never" means no state
// has been recorded; "ok" means the last sync was recent; "stale"
// means the last sync was older than tuiStaleAfter; "error" means
// the last in-TUI sync trigger returned an error; "syncing" means a
// sync is currently running on that row.
const (
	tuiStatusNever   = "never"
	tuiStatusOK      = "ok"
	tuiStatusStale   = "stale"
	tuiStatusError   = "error"
	tuiStatusSyncing = "syncing"
)

// tuiStaleAfter is the threshold past which a row's status flips from
// ok to stale. One day matches the cadence most connectors are
// expected to run at; per-connector overrides can come later.
const tuiStaleAfter = 24 * time.Hour

// tuiRow is one row in the products view: a single stream within a
// connector within a product, with its most recent sync timestamp,
// the cumulative record count recorded in the sink's manifest, and
// a derived health status.
type tuiRow struct {
	Product   string
	Connector string
	Type      string
	Stream    string
	LastSync  string
	Records   int64
	Status    string
	ErrorMsg  string
}

// collectTUIRows loads the config, state, and every referenced sink
// manifest, and returns one row per (product, connector, stream).
//
// LastSync is empty for a connector that has never been synced.
// Records is -1 when the sink manifest could not be read (not yet
// created, missing, or malformed); otherwise it is the sum of Rows
// across every manifest partition tagged with the stream name.
// Status is derived from LastSync against now.
func collectTUIRows(ctx context.Context, cfgPath string, now time.Time) ([]tuiRow, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}

	stateEntries, _, err := loadStateEntries(ctx, cfg.StatePath)
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]sqlitestate.Entry, len(stateEntries))
	for _, e := range stateEntries {
		byKey[e.Key] = e
	}

	manifestCache := map[string]map[string]int64{}
	readManifest := func(dir string) map[string]int64 {
		if dir == "" {
			return nil
		}
		if cached, ok := manifestCache[dir]; ok {
			return cached
		}
		path := filepath.Join(dir, "manifest.json")
		counts, ok := readManifestRows(path)
		if !ok {
			manifestCache[dir] = nil
			return nil
		}
		manifestCache[dir] = counts
		return counts
	}

	var rows []tuiRow
	for _, pid := range cfg.ProductIDs() {
		product := cfg.Products[pid]
		for _, inst := range product.Connectors {
			key := config.StateKey(pid, inst.Name)
			entry, haveState := byKey[key]
			sinkDir := sinkDirFromOptions(inst.Sink.Options)
			counts := readManifest(sinkDir)
			for _, stream := range inst.Streams {
				row := tuiRow{
					Product:   pid,
					Connector: inst.Name,
					Type:      inst.Type,
					Stream:    stream,
					Records:   -1,
				}
				if haveState {
					row.LastSync = entry.UpdatedAt
				}
				if counts != nil {
					if n, ok := counts[stream]; ok {
						row.Records = n
					} else {
						row.Records = 0
					}
				}
				row.Status = deriveStatus(row.LastSync, now)
				rows = append(rows, row)
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Product != rows[j].Product {
			return rows[i].Product < rows[j].Product
		}
		if rows[i].Connector != rows[j].Connector {
			return rows[i].Connector < rows[j].Connector
		}
		return rows[i].Stream < rows[j].Stream
	})
	return rows, nil
}

// deriveStatus maps a last-sync timestamp (RFC 3339 ms) plus a
// reference time to one of the plain status labels. An unparseable
// timestamp is treated as never synced so a corrupt state row never
// hides behind a stale "ok".
func deriveStatus(lastSync string, now time.Time) string {
	if lastSync == "" {
		return tuiStatusNever
	}
	t, err := time.Parse(time.RFC3339, lastSync)
	if err != nil {
		// Try the millisecond variant state/sqlite writes.
		t, err = time.Parse("2006-01-02T15:04:05.000Z07:00", lastSync)
		if err != nil {
			return tuiStatusNever
		}
	}
	if now.Sub(t) > tuiStaleAfter {
		return tuiStatusStale
	}
	return tuiStatusOK
}

// sinkDirFromOptions returns the "dir" option if the sink declares
// one as a string. Both built-in sinks (jsonl, parquet) use this key;
// future sinks that use a different key will show unknown record
// counts until they are wired in here.
func sinkDirFromOptions(opts map[string]any) string {
	if opts == nil {
		return ""
	}
	if v, ok := opts["dir"].(string); ok {
		return v
	}
	return ""
}

// readManifestRows reads the manifest at path and returns the sum of
// Rows per stream. The second return is false when the manifest does
// not exist or cannot be parsed, so the caller can render "-".
func readManifestRows(path string) (map[string]int64, bool) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false
		}
		return nil, false
	}
	defer f.Close()
	var m manifest.Manifest
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return nil, false
	}
	out := map[string]int64{}
	for _, p := range m.Partitions {
		out[p.Stream] += p.Rows
	}
	return out, true
}

// runTUI implements `ridgeline tui --config PATH`.
//
// It renders a products view: one line per configured stream, with
// product, connector type, connector name, stream name, status,
// last-sync timestamp, and cumulative record count. Keybindings:
//
//	j / down, k / up   move the highlight
//	s                  trigger a sync on the highlighted connector
//	q / ctrl+c / esc   quit
//
// The --render-once flag writes the static view to stdout and exits
// without starting an interactive program. It exists so the view
// can be exercised in non-TTY environments (tests, CI, pipes).
func runTUI(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	renderOnce := fs.Bool("render-once", false, "render the current view to stdout and exit")
	help, err := parseSubcommandFlags(fs, args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if err := rejectExtraArgs(fs); err != nil {
		return err
	}
	if *cfgPath == "" {
		return fmt.Errorf("--config PATH is required")
	}

	now := time.Now().UTC()
	rows, err := collectTUIRows(ctx, *cfgPath, now)
	if err != nil {
		return err
	}

	if *renderOnce {
		fmt.Fprint(stdout, renderTUIView(*cfgPath, rows, 0))
		return nil
	}

	model := tuiModel{
		cfgPath: *cfgPath,
		rows:    rows,
		syncer:  realTUISyncer,
	}
	_, err = tea.NewProgram(model, tea.WithContext(ctx)).Run()
	return err
}

// tuiSyncer runs a single connector end-to-end, identified by the
// product id and connector name from the current config. It returns
// the record count written and any error. It is injected into the
// model so tests can substitute a fake.
type tuiSyncer func(ctx context.Context, cfgPath, pid, connName string) (int, error)

// syncDoneMsg is emitted after a triggered sync finishes. The TUI
// uses it to clear the "syncing" flag, refresh the row data from
// disk, and stamp an error message if the sync failed.
type syncDoneMsg struct {
	pid      string
	connName string
	err      error
}

// tuiModel is the Bubble Tea model for the products view.
type tuiModel struct {
	cfgPath string
	rows    []tuiRow
	cursor  int
	syncing map[string]bool
	errors  map[string]string
	syncer  tuiSyncer
	quit    bool
}

func (m tuiModel) Init() tea.Cmd { return nil }

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			m.quit = true
			return m, tea.Quit
		case "down", "j":
			if m.cursor < len(m.rows)-1 {
				m.cursor++
			}
			return m, nil
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
			return m, nil
		case "s":
			if len(m.rows) == 0 {
				return m, nil
			}
			row := m.rows[m.cursor]
			key := row.Product + "/" + row.Connector
			if m.syncing == nil {
				m.syncing = map[string]bool{}
			}
			if m.syncing[key] {
				return m, nil
			}
			m.syncing[key] = true
			m = m.applySyncingStatus(row.Product, row.Connector)
			cfgPath := m.cfgPath
			pid := row.Product
			connName := row.Connector
			syncer := m.syncer
			return m, func() tea.Msg {
				_, err := syncer(context.Background(), cfgPath, pid, connName)
				return syncDoneMsg{pid: pid, connName: connName, err: err}
			}
		}
	case syncDoneMsg:
		key := msg.pid + "/" + msg.connName
		if m.syncing != nil {
			delete(m.syncing, key)
		}
		if m.errors == nil {
			m.errors = map[string]string{}
		}
		if msg.err != nil {
			m.errors[key] = msg.err.Error()
		} else {
			delete(m.errors, key)
		}
		m = m.refreshRows()
		return m, nil
	}
	return m, nil
}

// applySyncingStatus stamps "syncing" onto every row belonging to the
// given product/connector so the view reflects the in-flight state
// before the actual sync finishes.
func (m tuiModel) applySyncingStatus(pid, connName string) tuiModel {
	for i := range m.rows {
		if m.rows[i].Product == pid && m.rows[i].Connector == connName {
			m.rows[i].Status = tuiStatusSyncing
			m.rows[i].ErrorMsg = ""
		}
	}
	return m
}

// refreshRows re-reads state and manifests from disk and overlays
// the in-TUI syncing and error flags. Any failure to re-read is
// silently ignored; a stale view is preferred over a crashed TUI.
func (m tuiModel) refreshRows() tuiModel {
	rows, err := collectTUIRows(context.Background(), m.cfgPath, time.Now().UTC())
	if err == nil {
		m.rows = rows
	}
	for i := range m.rows {
		key := m.rows[i].Product + "/" + m.rows[i].Connector
		if m.syncing[key] {
			m.rows[i].Status = tuiStatusSyncing
		}
		if msg, ok := m.errors[key]; ok {
			m.rows[i].Status = tuiStatusError
			m.rows[i].ErrorMsg = msg
		}
	}
	if m.cursor >= len(m.rows) {
		if len(m.rows) == 0 {
			m.cursor = 0
		} else {
			m.cursor = len(m.rows) - 1
		}
	}
	return m
}

func (m tuiModel) View() string {
	if m.quit {
		return ""
	}
	out := renderTUIView(m.cfgPath, m.rows, m.cursor)
	// Surface the first error below the legend so a failed sync is
	// visible even after the row's color cue scrolls off.
	for _, r := range m.rows {
		if r.Status == tuiStatusError && r.ErrorMsg != "" {
			out += "error on " + r.Product + "/" + r.Connector + ": " + r.ErrorMsg + "\n"
			break
		}
	}
	return out
}

var (
	tuiHeaderStyle   = lipgloss.NewStyle().Bold(true)
	tuiLegendStyle   = lipgloss.NewStyle().Faint(true)
	tuiSelectedStyle = lipgloss.NewStyle().Reverse(true)
	tuiStatusStyles  = map[string]lipgloss.Style{
		tuiStatusOK:      lipgloss.NewStyle().Foreground(lipgloss.Color("2")),
		tuiStatusStale:   lipgloss.NewStyle().Foreground(lipgloss.Color("3")),
		tuiStatusError:   lipgloss.NewStyle().Foreground(lipgloss.Color("1")),
		tuiStatusSyncing: lipgloss.NewStyle().Foreground(lipgloss.Color("4")),
		tuiStatusNever:   lipgloss.NewStyle().Faint(true),
	}
)

// renderTUIView is the pure rendering function. It produces the full
// screen (header, table, legend) so it can be diffed in tests. The
// cursor argument is the index of the highlighted row.
func renderTUIView(cfgPath string, rows []tuiRow, cursor int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ridgeline %s\n", Version)
	fmt.Fprintf(&b, "config: %s\n\n", cfgPath)

	headers := []string{"PRODUCT", "CONNECTOR", "TYPE", "STREAM", "STATUS", "LAST SYNC", "RECORDS"}
	data := make([][]string, 0, len(rows))
	for _, r := range rows {
		last := r.LastSync
		if last == "" {
			last = "never"
		}
		records := "-"
		if r.Records >= 0 {
			records = fmt.Sprintf("%d", r.Records)
		}
		data = append(data, []string{r.Product, r.Connector, r.Type, r.Stream, r.Status, last, records})
	}

	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, row := range data {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	// Header.
	{
		parts := make([]string, len(headers))
		for i, c := range headers {
			parts[i] = padRight(c, widths[i])
		}
		b.WriteString(tuiHeaderStyle.Render(strings.Join(parts, "  ")))
		b.WriteByte('\n')
	}

	if len(data) == 0 {
		fmt.Fprintln(&b, "(no connectors configured)")
	}
	for i, row := range data {
		parts := make([]string, len(row))
		for j, cell := range row {
			parts[j] = padRight(cell, widths[j])
		}
		// Status column (index 4) gets its status color.
		if style, ok := tuiStatusStyles[rows[i].Status]; ok {
			parts[4] = style.Render(parts[4])
		}
		line := strings.Join(parts, "  ")
		if i == cursor {
			line = tuiSelectedStyle.Render(line)
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}

	b.WriteByte('\n')
	b.WriteString(tuiLegendStyle.Render("j/k up/down  move   s  sync   q/ctrl+c  quit"))
	b.WriteByte('\n')
	return b.String()
}

func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}

// realTUISyncer runs a single connector against its configured sink,
// reusing the same pipeline path as the `sync` CLI. The per-connector
// chatter that runConnectorInstance prints is discarded so it does
// not corrupt the TUI's alternate-screen buffer.
func realTUISyncer(ctx context.Context, cfgPath, pid, connName string) (int, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return 0, err
	}
	store, err := sqlitestate.Open(cfg.StatePath)
	if err != nil {
		return 0, err
	}
	defer store.Close()
	if err := resolveConfigRefs(ctx, cfg, store, io.Discard); err != nil {
		return 0, err
	}
	if err := validateConnectors(ctx, cfg); err != nil {
		return 0, err
	}
	product, ok := cfg.Products[pid]
	if !ok {
		return 0, fmt.Errorf("product %q not in config", pid)
	}
	for _, inst := range product.Connectors {
		if inst.Name == connName {
			return runConnectorInstance(ctx, store, pid, inst, io.Discard)
		}
	}
	return 0, fmt.Errorf("connector %q not found under product %q", connName, pid)
}
