# Roadmap

## Phase 1: Framework + Core Pipeline

- [x] Go module scaffold and CI (ubuntu + macOS, Go 1.25 + 1.26)
- [x] goreleaser config and Homebrew tap (multi-arch builds via goreleaser-cross on `v*` tags; `v0.1.0` published, cask in `xydac/homebrew-tap`)
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
- [x] Native connector: PostHog (individual events via Personal API Key; typed `timestamp`, `event`, and `distinct_id` columns; cursor-based incremental with pagination)
- [x] First external connector in Python (worked example under `examples/external/`)
- [x] `ridgeline status` CLI command (per-connector cursor and last-sync time)
- [x] `ridgeline query` CLI command (runs SQL against DuckDB)
- [x] `ridgeline creds` CLI command (list, put, get, rm)
- [x] TUI shell (Bubble Tea): products view, health bars, keybindings (`ridgeline tui` ships a products view with colored status, cursor navigation, and an `s` key that triggers a real sync on the highlighted connector)
- [x] `sync --continue-on-error`: partial failure mode that runs remaining connectors after one fails, exits 3 on partial and 1 on total failure
- [x] Enricher transform stage: `Enricher` interface with per-batch semantics, init-time registry, `enrichers:` config section on each connector, built-in `url_host` enricher (hostname extraction from URL fields), built-in `ts_normalize` enricher (timestamp normalization to UTC RFC 3339)

## Phase 2: Business Memory

- [x] Business Memory catalog: `bm_streams` and `bm_metrics` tables persist observed streams and metric columns across sync runs
- [x] `ridgeline memory streams` -- list all streams with first_seen_at, last_seen_at, lifetime row count
- [x] `ridgeline memory metrics` -- list all metric columns with unit, directionality, aggregation, last value
- [x] Baselines: rolling window statistics per metric (7d, 30d, 90d mean/stddev/min/max); `ridgeline memory baselines <metric>` with ASCII sparkline; `ridgeline memory recompute`
- [x] Anomaly detection: deviations from baseline surface as events in `bm_events`; `ridgeline memory events --since 7d` with directional labels (surprise-good/bad/neutral); configurable k and min_samples globally and per-metric
- [x] `ridgeline explain <metric> --since <window>` -- templated narrative from the memory catalog
- [x] `ridgeline compare` -- pairwise and period-over-period narrative across two metrics or windows
- [x] `ridgeline investigate <metric>` -- cross-source causal narrative correlating anomalies with events by temporal proximity and ranking sibling metrics by Pearson correlation
- [x] `ridgeline summarize` -- ranked narrative overview of all tracked metrics; directionality-adjusted scoring surfaces surprise-bad events first; grouped by connector; `--top N` and `--json` flags
- [x] `ridgeline forecast <metric> --horizon <window>` -- directional projection via linear regression over up to 90 days of metric history; directional label (likely-improvement / stable / likely-decline), projected mean with uncertainty band, confidence from sample count and R^2
- [x] Cross-connector event log: deploys, releases, and git commits land in `bm_events`
- [x] MCP server (`ridgeline mcp`) exposing `list_metrics`, `explain`, `investigate`, `compare`, and `summarize` as agent tools

## Known gaps

- Baselines bucket samples by ingest time rather than by each record's timestamp or declared `key` column, so a single sync that backfills 40 days of daily records collapses into one baseline sample (`n=1`) instead of ~40. Anomaly detection cannot fire until at least three separate sync runs land on three separate days, so the reasoning layer answers "near baseline, low confidence" on the exact happy path a new user hits after a first backfill.
- Business Memory identifies streams and metrics by `<connector type>.<stream>.<column>`, ignoring the connector's configured `name` and its parent product. Two connectors of the same type (two Plausible sites, staging + production, two external plugins declaring the same stream name) silently merge their rows into one catalog entry, so baselines are computed over an interleaved mixture and there is no name a user can pass to `explain` to isolate one instance.
- The `url_host` enricher preserves the parsed host's letter case, so `example.com`, `Example.com`, and `EXAMPLE.COM` land in three distinct GROUP BY buckets even though the README's stated rationale is "group by domain in DuckDB". Normalize to lowercase per RFC 3986, or document that SQL must wrap the field in `lower()`.
- `creds oauth gsc --client-secret-file` stores the file contents verbatim, but the README tells users to point it at Google's `client_secret.json` wrapper. Either extract the secret from the JSON wrapper or document that the file must contain just the secret string.
- CLI exit codes for misinvocation (missing required flag, unknown flag, unexpected positional) are inconsistent across subcommands: some exit 1, others exit 2. Establish a single convention (2 for usage errors, 1 for runtime failures) and apply it uniformly.
- The external JSON-lines protocol rejects string-encoded numeric epochs (e.g. `"timestamp":"1710495000"`) with the generic message `unparseable timestamp <value>`. Either accept the same string-encoded epoch forms the `ts_normalize` enricher already documents, or return an error that names the string type as the reason for rejection so authors of external connectors can act on it.
