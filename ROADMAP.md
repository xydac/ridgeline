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


## Known gaps

- `sync` reports `done: N records total` from records extracted, not records persisted. On a steady-state re-run the sink writes nothing new but the CLI still prints a non-zero count; only the manifest's `updated_at` refresh signals freshness. Either distinguish "N new" from "N extracted" in the CLI output or document the counting rule.
- `umami` login mode caches its JWT in cleartext in the state DB. Anything that can read the state file (including `ridgeline query` via `ATTACH`) can read the token. The cached credential should travel through the encrypted `credentials` table, or the query runner should refuse `ATTACH` against the state DB.
- `ridgeline query` rejects SQL that begins with a `--` line comment because Go's flag parser claims the argument as an unknown flag. The end-of-flags `--` separator works but is undocumented. Stop flag parsing after the `query` subcommand, or document the separator in `--help` and the README.
- `ridgeline query` misclassifies syntax errors as read-only-mode rejections. A typo'd verb like `SELEKT 1` is reported as "read-only mode rejects SELEKT; pass --write", which steers the user to drop safety guards to debug a spelling mistake. Only emit the "pass --write" message when the leading token is a recognized mutating or DDL keyword.
- The `url_host` enricher preserves the parsed host's letter case, so `example.com`, `Example.com`, and `EXAMPLE.COM` land in three distinct GROUP BY buckets even though the README's stated rationale is "group by domain in DuckDB". Normalize to lowercase per RFC 3986, or document that SQL must wrap the field in `lower()`.
- Unknown `connector` and `enricher` `type:` values reject with no list of valid types. Sinks already enumerate known types on rejection; connectors and enrichers should match that, or expose a discovery verb.
- An empty or whitespace-only `ridgeline.yaml` returns `config: parse: EOF` instead of an actionable "file is empty; add `version: 1` and at least one product" message.
- `creds oauth gsc --client-secret-file` stores the file contents verbatim, but the README tells users to point it at Google's `client_secret.json` wrapper. Either extract the secret from the JSON wrapper or document that the file must contain just the secret string.
- GitHub 401 error responses are logged as raw multi-line JSON. Parse the response and surface only `message` and `documentation_url`.

## Phase 2+

Further phases are tracked privately during bootstrap and will be published here once Phase 1 ships.
