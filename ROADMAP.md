# Roadmap

## Phase 1: Framework + Core Pipeline

- [x] Go module scaffold and CI (ubuntu + macOS, Go 1.25 + 1.26)
- [x] goreleaser config and Homebrew tap (multi-arch builds via goreleaser-cross on `v*` tags; `v0.1.0` published, formula in `xydac/homebrew-tap`)
- [x] Config parser (YAML to Go structs)
- [x] Connector interface (native Go)
- [x] JSON-lines protocol spec and codec for external connectors
- [x] External JSON-lines runner that spawns and drives child processes
- [x] Sink interface
- [x] ETL lifecycle manager (extract, transform, load, checkpoint)
- [x] Manifest file writer with time-range metadata
- [x] Partition pruning on re-run (sinks drop records whose timestamps are already covered by a manifest partition, so a no-op re-run adds no file and no manifest entry)
- [x] In-memory state store
- [x] `ridgeline sync --dry-run` against a built-in test source
- [x] `ridgeline sync --config` against a ridgeline.yaml
- [x] State and checkpoint store (SQLite, persistent)
- [x] Credential store (SQLite + AES-256-GCM encryption)
- [x] Parquet sink (writes `{stream, timestamp, data_json}` files; connectors that declare a stream Schema get typed columns for the declared fields alongside the data_json payload)
- [x] DuckDB integration (in-process, via go-duckdb/v2)
- [x] Native connector: Hacker News (Algolia public API)
- [x] Native connector: Umami (self-hosted analytics, API key or username/password login)
- [x] Native connector: Google Search Console (OAuth 2.0 via `creds oauth gsc` browser PKCE flow or a bring-your-own refresh token)
- [x] Native connector: Plausible Analytics (daily timeseries via API token; typed `visitors`, `pageviews`, `bounce_rate`, `visit_duration` columns)
- [x] Native connector: GitHub repository traffic (daily views and clones via PAT; typed `count` and `uniques` columns; incremental cursor per stream)
- [x] First external connector in Python (worked example under `examples/external/`)
- [x] `ridgeline status` CLI command (per-connector cursor and last-sync time)
- [x] `ridgeline query` CLI command (runs SQL against DuckDB)
- [x] `ridgeline creds` CLI command (list, put, get, rm)
- [x] TUI shell (Bubble Tea): products view, health bars, keybindings (`ridgeline tui` ships a products view with colored status, cursor navigation, and an `s` key that triggers a real sync on the highlighted connector)
- [x] `sync --continue-on-error`: partial failure mode that runs remaining connectors after one fails, exits 2 on partial and 1 on total failure
- [x] Enricher transform stage: `Enricher` interface with per-batch semantics, init-time registry, `enrichers:` config section on each connector, built-in `url_host` enricher (hostname extraction from URL fields)


## Phase 2+

Further phases are tracked privately during bootstrap and will be published here once Phase 1 ships.
