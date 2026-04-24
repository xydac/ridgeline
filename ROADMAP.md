# Roadmap

## Phase 1: Framework + Core Pipeline

- [x] Go module scaffold and CI (ubuntu + macOS, Go 1.25 + 1.26)
- [x] goreleaser config and Homebrew tap stub (multi-arch builds via goreleaser-cross on `v*` tags; first tag still pending)
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
- [x] Parquet sink (writes `{stream, timestamp, data_json}` files; typed-column inference is a follow-up)
- [x] DuckDB integration (in-process, via go-duckdb/v2)
- [x] Native connector: Hacker News (Algolia public API)
- [x] Native connector: Umami (self-hosted analytics, API key or username/password login)
- [x] Native connector: Google Search Console (OAuth 2.0 via `creds oauth gsc` browser PKCE flow or a bring-your-own refresh token)
- [x] First external connector in Python (worked example under `examples/external/`)
- [x] `ridgeline status` CLI command (per-connector cursor and last-sync time)
- [x] `ridgeline query` CLI command (runs SQL against DuckDB)
- [x] `ridgeline creds` CLI command (list, put, get, rm)
- [x] TUI shell (Bubble Tea): products view, health bars, keybindings (`ridgeline tui` ships a products view with colored status, cursor navigation, and an `s` key that triggers a real sync on the highlighted connector)

## Known gaps

- `ridgeline query` is a full DuckDB session with write access to the filesystem
  and to any `ATTACH`'d SQLite database. An `ATTACH ...; DELETE FROM state`
  on the state DB succeeds silently; a `COPY ... TO '/path'` writes files.
  The subcommand should enforce a read-only statement surface by default
  and open attached SQLite databases read-only, with an explicit opt-in
  flag for write access.
- `ridgeline creds rm` exits 0 when the named credential does not exist,
  so a typo looks like a successful delete. It should report a miss
  with a non-zero exit.
- `ridgeline creds put` silently overwrites an existing value. It
  should either refuse without an explicit overwrite flag, or print a
  visible "replaced" line so the previous value is not lost unnoticed.
- `ridgeline creds put` strips a single trailing newline from stdin,
  which is friendly for `echo "secret" | creds put` but silently
  mutates a secret whose bytes really do end with a newline. The
  strip should be opt-in, or a raw-bytes mode should be offered.
- `creds oauth gsc --client-secret VALUE` takes the secret on the
  command line and therefore leaks it to shell history. An
  alternative that reads the secret from stdin or a file is needed.
- Output lines from the sync pipeline's warning path carry the Go
  stdlib log prefix (`2026/04/21 01:00:38 ...`); info and done lines
  print without a prefix. Output should go through one formatter.
- YAML decode errors surface the Go destination type
  (`map[string]config.Product`) and yaml tags (`!!str`) in error
  messages. SQLite driver errors surface numeric errnos like `(14)`
  and `(26)`. Both should be translated into Ridgeline vocabulary at
  the CLI boundary.
- A `sync` that produces zero records still creates an empty
  timestamped partition directory under the sink's output root.
  Repeated idempotent runs accumulate empty directories. The mkdir
  should be deferred until the first partition is actually written.

## Phase 2+

Further phases are tracked privately during bootstrap and will be published here once Phase 1 ships.
