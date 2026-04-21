# Roadmap

## Phase 1: Framework + Core Pipeline

- [ ] Go module scaffold, goreleaser config, Homebrew tap stub
- [ ] Config parser (YAML to Go structs, viper)
- [ ] Connector interface + protocol (native Go + external JSON-lines runner)
- [ ] Sink interface + Parquet sink (default)
- [ ] DuckDB integration (go-duckdb, static linked)
- [ ] ETL lifecycle manager (extract, transform, load, checkpoint)
- [ ] State and checkpoint store (SQLite)
- [ ] Credential store (SQLite + AES-256-GCM encryption)
- [ ] Manifest file writer (partition pruning)
- [ ] First native connectors: GSC, Umami, Hacker News
- [ ] First external connector in Python
- [ ] `ridgeline sync`, `ridgeline status`, `ridgeline query` CLI commands
- [ ] TUI shell (Bubble Tea): products view, health bars, keybindings

## Phase 2+

Further phases are tracked privately during bootstrap and will be published here once Phase 1 ships.
