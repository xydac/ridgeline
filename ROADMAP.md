# Roadmap

## Phase 1: Framework + Core Pipeline

- [ ] Go module scaffold, goreleaser config, Homebrew tap stub
- [ ] Config parser (YAML to Go structs, viper)
- [x] Connector interface + protocol (native Go + external JSON-lines runner)
- [x] Sink interface
- [x] ETL lifecycle manager (extract, transform, load, checkpoint)
- [x] Manifest file writer (partition pruning)
- [x] In-memory state store (interface; SQLite impl upcoming)
- [x] `ridgeline sync --dry-run` against a built-in test source
- [ ] Parquet sink (default)
- [ ] DuckDB integration (go-duckdb, static linked)
- [ ] State and checkpoint store (SQLite, persistent)
- [ ] Credential store (SQLite + AES-256-GCM encryption)
- [ ] First native connectors: GSC, Umami, Hacker News
- [ ] First external connector in Python
- [ ] `ridgeline status`, `ridgeline query` CLI commands
- [ ] TUI shell (Bubble Tea): products view, health bars, keybindings

## Phase 2+

Further phases are tracked privately during bootstrap and will be published here once Phase 1 ships.
