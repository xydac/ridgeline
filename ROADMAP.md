# Roadmap

## Phase 1: Framework + Core Pipeline

- [x] Go module scaffold and CI (ubuntu + macOS, Go 1.25 + 1.26)
- [ ] goreleaser config and Homebrew tap stub
- [x] Config parser (YAML to Go structs)
- [x] Connector interface (native Go)
- [x] JSON-lines protocol spec and codec for external connectors
- [x] External JSON-lines runner that spawns and drives child processes
- [x] Sink interface
- [x] ETL lifecycle manager (extract, transform, load, checkpoint)
- [x] Manifest file writer with time-range metadata
- [ ] Partition pruning on re-run (today the manifest grows unbounded)
- [x] In-memory state store
- [x] `ridgeline sync --dry-run` against a built-in test source
- [x] `ridgeline sync --config` against a ridgeline.yaml
- [x] State and checkpoint store (SQLite, persistent)
- [x] Credential store (SQLite + AES-256-GCM encryption)
- [x] Parquet sink (writes `{stream, timestamp, data_json}` files; typed-column inference is a follow-up)
- [x] DuckDB integration (in-process, via go-duckdb/v2)
- [x] Native connector: Hacker News (Algolia public API)
- [ ] Native connector: Umami (self-hosted analytics)
- [ ] Native connector: Google Search Console (OAuth 2.0)
- [x] First external connector in Python (worked example under `examples/external/`)
- [x] `ridgeline status` CLI command (per-connector cursor and last-sync time)
- [x] `ridgeline query` CLI command (runs SQL against DuckDB)
- [ ] `ridgeline creds` CLI command
- [ ] TUI shell (Bubble Tea): products view, health bars, keybindings

## Known gaps

- Route all CLI output through one formatter so warn lines stop carrying the stdlib `log` prefix
- Factory registry for sinks, replacing the hand-rolled switch in the sync command
- Defer partition-directory creation until the first record lands, avoiding empty dirs on zero-record runs
- `sync --help` exits 0 with usage, not 1 with a `flag: help requested` line

## Phase 2+

Further phases are tracked privately during bootstrap and will be published here once Phase 1 ships.
