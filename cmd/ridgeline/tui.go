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

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/manifest"
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

// tuiRow is one row in the products view: a single stream within a
// connector within a product, with its most recent sync timestamp and
// the cumulative record count recorded in the sink's manifest.
type tuiRow struct {
	Product   string
	Connector string
	Type      string
	Stream    string
	LastSync  string
	Records   int64
}

// collectTUIRows loads the config, state, and every referenced sink
// manifest, and returns one row per (product, connector, stream).
//
// LastSync is empty for a connector that has never been synced.
// Records is -1 when the sink manifest could not be read (not yet
// created, missing, or malformed); otherwise it is the sum of Rows
// across every manifest partition tagged with the stream name.
func collectTUIRows(ctx context.Context, cfgPath string) ([]tuiRow, error) {
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
// It renders a read-only products view: one line per configured
// stream, with product, connector type, connector name, stream name,
// last-sync timestamp, and cumulative record count. Keybindings:
// `q` or ctrl-c to quit.
//
// The --render-once flag writes the static view to stdout and exits
// without starting an interactive program. It exists so the view
// can be exercised in non-TTY environments (tests, CI, pipes) and
// so users can pipe the snapshot into other tools.
func runTUI(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	renderOnce := fs.Bool("render-once", false, "render the current view to stdout and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *cfgPath == "" {
		return fmt.Errorf("--config PATH is required")
	}

	rows, err := collectTUIRows(ctx, *cfgPath)
	if err != nil {
		return err
	}

	if *renderOnce {
		fmt.Fprint(stdout, renderTUIView(*cfgPath, rows))
		return nil
	}

	model := tuiModel{cfgPath: *cfgPath, rows: rows}
	_, err = tea.NewProgram(model, tea.WithContext(ctx)).Run()
	return err
}

// tuiModel is the Bubble Tea model for the products view. It is a
// pure read-only snapshot in this first iteration; refreshes and
// sync-trigger keybindings land in a follow-up.
type tuiModel struct {
	cfgPath string
	rows    []tuiRow
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
		}
	}
	return m, nil
}

func (m tuiModel) View() string {
	if m.quit {
		return ""
	}
	return renderTUIView(m.cfgPath, m.rows)
}

var (
	tuiHeaderStyle = lipgloss.NewStyle().Bold(true)
	tuiLegendStyle = lipgloss.NewStyle().Faint(true)
)

// renderTUIView is the pure rendering function. It produces the full
// screen (header, table, legend) so it can be diffed in tests.
func renderTUIView(cfgPath string, rows []tuiRow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ridgeline %s\n", Version)
	fmt.Fprintf(&b, "config: %s\n\n", cfgPath)

	headers := []string{"PRODUCT", "CONNECTOR", "TYPE", "STREAM", "LAST SYNC", "RECORDS"}
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
		data = append(data, []string{r.Product, r.Connector, r.Type, r.Stream, last, records})
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

	writeRow := func(cells []string, style lipgloss.Style) {
		parts := make([]string, len(cells))
		for i, c := range cells {
			parts[i] = padRight(c, widths[i])
		}
		line := strings.Join(parts, "  ")
		if style.GetBold() || style.GetFaint() {
			line = style.Render(line)
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}

	writeRow(headers, tuiHeaderStyle)
	if len(data) == 0 {
		fmt.Fprintln(&b, "(no connectors configured)")
	}
	for _, row := range data {
		writeRow(row, lipgloss.Style{})
	}

	b.WriteByte('\n')
	b.WriteString(tuiLegendStyle.Render("q / ctrl+c  quit"))
	b.WriteByte('\n')
	return b.String()
}

func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}
