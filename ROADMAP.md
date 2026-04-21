# Roadmap

## Phase 1: Framework + Core Pipeline

- [ ] Go module scaffold, goreleaser config, Homebrew tap stub
- [x] Config parser (YAML to Go structs)
- [x] Connector interface + protocol (native Go + external JSON-lines runner)
- [x] Sink interface
- [x] ETL lifecycle manager (extract, transform, load, checkpoint)
- [x] Manifest file writer (partition pruning)
- [x] In-memory state store
- [x] `ridgeline sync --dry-run` against a built-in test source
- [x] `ridgeline sync --config` against a ridgeline.yaml
- [x] State and checkpoint store (SQLite, persistent)
- [x] Credential store (SQLite + AES-256-GCM encryption)
- [ ] Parquet sink (default)
- [ ] DuckDB integration (go-duckdb, static linked)
- [ ] First native connectors: GSC, Umami, Hacker News
- [ ] First external connector in Python
- [ ] `ridgeline status`, `ridgeline query`, `ridgeline creds` CLI commands
- [ ] TUI shell (Bubble Tea): products view, health bars, keybindings

## Phase 2+

Further phases are tracked privately during bootstrap and will be published here once Phase 1 ships.
