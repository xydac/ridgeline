# Roadmap

## Phase 1: Framework + Core Pipeline

- [x] Go module scaffold and CI (ubuntu + macOS, Go 1.25 + 1.26)
- [ ] goreleaser config and Homebrew tap stub
- [x] Config parser (YAML to Go structs)
- [x] Connector interface (native Go)
- [x] JSON-lines protocol spec and codec for external connectors
- [ ] External JSON-lines runner that spawns and drives child processes
- [x] Sink interface
- [x] ETL lifecycle manager (extract, transform, load, checkpoint)
- [x] Manifest file writer with time-range metadata
- [ ] Partition pruning on re-run (today the manifest grows unbounded)
- [x] In-memory state store
- [x] `ridgeline sync --dry-run` against a built-in test source
- [x] `ridgeline sync --config` against a ridgeline.yaml
- [x] State and checkpoint store (SQLite, persistent)
- [x] Credential store (SQLite + AES-256-GCM encryption)
- [ ] Parquet sink (default)
- [ ] DuckDB integration (go-duckdb, static linked)
- [x] Native connector: Hacker News (Algolia public API)
- [ ] Native connector: Umami (self-hosted analytics)
- [ ] Native connector: Google Search Console (OAuth 2.0)
- [ ] First external connector in Python
- [ ] `ridgeline status`, `ridgeline query`, `ridgeline creds` CLI commands
- [ ] TUI shell (Bubble Tea): products view, health bars, keybindings

## Phase 2+

Further phases are tracked privately during bootstrap and will be published here once Phase 1 ships.
