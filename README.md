# Ridgeline

Self-hosted intelligence platform for indie developers.

Extract from anywhere. Enrich with AI. Query with SQL. Alert on what
matters. One binary. Pluggable connectors in any language. DuckDB-powered.

> **Status: early bootstrap.** The ETL core runs end-to-end from a
> ridgeline.yaml config: SQLite-backed state (durable across restarts),
> an AES-256-GCM credential store with a `ridgeline creds` CLI, JSON-lines
> and Parquet sinks, native connectors for Hacker News (Algolia public
> API), Umami (self-hosted analytics, API key or username/password
> login), and Google Search Console (OAuth 2.0 with a bring-your-own
> refresh token for now), an external
> runner that lets you wire any executable that speaks JSON-lines as a
> connector, and an in-process DuckDB `ridgeline query` command. Next up
> are more native connectors. See [ROADMAP.md](ROADMAP.md). Built in
> public.

## Try it now

### Dry-run (no config, in-memory state)

```sh
git clone https://github.com/xydac/ridgeline.git
cd ridgeline
go build -o ridgeline ./cmd/ridgeline
./ridgeline sync --dry-run --out ./out --records 3
# wrote 6 records across 2 streams into ./out
#   pages: 3 records
#   events: 3 records
# manifest: out/manifest.json
```

### Config-driven sync (durable state)

Write a `ridgeline.yaml`:

```yaml
version: 1
state_path: ./ridgeline.db
products:
  myapp:
    connectors:
      - name: demo
        type: testsrc
        config:
          records: 2
        streams: [pages, events]
        sink:
          type: jsonl
          options:
            dir: ./out
```

Run it:

```sh
./ridgeline sync --config ridgeline.yaml
# loaded ridgeline.yaml
# state: ./ridgeline.db
# myapp/demo: 4 records, 2 states saved
# done: 4 records total
```

State lives in `./ridgeline.db` (SQLite, 0600 permissions, schema
created on first run). A second invocation reuses the same database,
so connector checkpoints survive process restarts.

### Inspecting state

`ridgeline status` reads the same `ridgeline.yaml` and prints each
configured connector alongside its stored cursor and the last-sync
wall-clock time, without opening a Parquet viewer or the SQLite file:

```sh
./ridgeline status --config ridgeline.yaml
# loaded ridgeline.yaml
# state: ./ridgeline.db
# myapp/demo (testsrc)
#   streams: [pages events]
#   last sync: 2026-04-21T11:29:26.366Z
#   cursor: {"count":2,"last_stream":"events"}
```

Status is read-only: if `state_path` does not exist yet, the command
reports every connector as `never synced` without creating an empty
database. State entries that no longer map to a configured connector
are listed under an `orphan state entries` footer so a rename or
removal is visible without inspecting the database by hand.

Credentials live in the same file under the `credentials` table,
sealed with AES-256-GCM. The 32-byte key is loaded from the optional
`key_path` field (hex encoded; defaults to `~/.ridgeline/key`). The
`ridgeline creds` subcommand (below) creates the key file on first
use, so no pre-setup is required.

### Managing credentials

`ridgeline creds` owns the encrypted credential store: `put` to write a
secret, `get` to read it back, `list` to enumerate names, `rm` to
delete:

```sh
echo "my-umami-api-key" | ./ridgeline creds put --config ridgeline.yaml umami_main
# stored credential "umami_main" (16 bytes)

./ridgeline creds list --config ridgeline.yaml
# umami_main

./ridgeline creds get --config ridgeline.yaml umami_main
# my-umami-api-key

./ridgeline creds rm --config ridgeline.yaml umami_main
```

Any connector config that declares a key ending in `_ref` pulls its
value from this store at sync time. `api_key_ref: umami_main` on a
connector resolves to `api_key: <plaintext>` before Validate runs, so
the YAML file never carries the secret on disk.

### Pulling Umami analytics

The `umami` connector reads the events feed from a self-hosted Umami
install. It supports two auth modes: an API key (Umami v2 cloud or any
install that exposes Settings -> API Keys) and username/password login
(the default for most self-hosted installs, which POSTs to
`/api/auth/login` and caches the returned JWT).

**API key mode** (default). Create the key in the Umami UI, store it,
reference it from the config:

```yaml
version: 1
state_path: ./ridgeline.db
key_path: ./ridgeline.key
products:
  myapp:
    connectors:
      - name: web
        type: umami
        config:
          base_url: https://stats.example.com
          website_id: 00000000-0000-0000-0000-000000000000
          api_key_ref: umami_main     # resolves via the creds store
          page_size: 100              # optional, default 100, max 1000
          max_pages: 10               # optional, default 10
        streams: [events]
        sink:
          type: parquet
          options:
            dir: ./umami-out
```

**Login mode**. Store the username and password with `ridgeline creds put`,
then declare `auth: login` plus `username_ref` and `password_ref`:

```sh
echo "alice" | ./ridgeline creds put --config ridgeline.yaml umami_user
echo "hunter2" | ./ridgeline creds put --config ridgeline.yaml umami_pass
```

```yaml
- name: web
  type: umami
  config:
    base_url: https://stats.example.com
    website_id: 00000000-0000-0000-0000-000000000000
    auth: login
    username_ref: umami_user
    password_ref: umami_pass
  streams: [events]
  sink:
    type: parquet
    options:
      dir: ./umami-out
```

The JWT is cached in the SQLite state store under `auth_token` so a
typical sync makes one bearer request; on 401 the connector re-logs in
once and retries. A fresh token is persisted immediately after login,
so a crash mid-sync still saves the new credential for the next run.

The incremental cursor is the RFC 3339 `createdAt` high-water mark
(key `last_created_at` in the state entry), so re-runs only fetch
events strictly newer than the last one seen. First sync falls back to
a 30-day lookback.

### Pulling real Hacker News data

The `hackernews` connector queries the public Algolia HN search API,
no auth required. Drop this into a `ridgeline.yaml`:

```yaml
version: 1
state_path: ./ridgeline.db
products:
  myapp:
    connectors:
      - name: hn
        type: hackernews
        config:
          query: golang        # any Algolia search query
          hits_per_page: 50    # default 50, max 1000
          max_pages: 1         # raise this for a backfill sync
        streams: [stories]     # also: comments
        sink:
          type: jsonl
          options:
            dir: ./hn-out
```

```sh
./ridgeline sync --config ridgeline.yaml
# loaded ridgeline.yaml
# state: ./ridgeline.db
# myapp/hn: 50 records, 1 states saved
# done: 50 records total

./ridgeline sync --config ridgeline.yaml
# loaded ridgeline.yaml
# state: ./ridgeline.db
# myapp/hn: 0 records, 1 states saved      # cursor sees no new items yet
# done: 0 records total
```

Each sync persists a `created_at_i` high-water mark per stream into
the SQLite state store, so re-runs only fetch records strictly newer
than the last one seen.

### Pulling Google Search Console data

The `gsc` connector reads daily Search Analytics rows for a configured
property via the webmasters/v3 API. Auth is OAuth 2.0. The full browser
PKCE flow is not wired into the binary yet, so today the user obtains a
long-lived refresh token out of band (for example with the Google
OAuth Playground, selecting the
`https://www.googleapis.com/auth/webmasters.readonly` scope) and stores
it in the credential store alongside the OAuth client id and secret.
The connector exchanges the refresh token for short-lived access
tokens at sync time and caches the access token in the per-connector
state map so a typical hourly run makes one token call per hour.

```sh
echo "1234567890-xxxxx.apps.googleusercontent.com" | \
  ./ridgeline creds put --config ridgeline.yaml gsc_client_id
echo "GOCSPX-..."   | ./ridgeline creds put --config ridgeline.yaml gsc_client_secret
echo "1//0g-long-refresh-token" | \
  ./ridgeline creds put --config ridgeline.yaml gsc_refresh_token
```

```yaml
version: 1
state_path: ./ridgeline.db
products:
  myapp:
    connectors:
      - name: search
        type: gsc
        config:
          site_url: sc-domain:example.com   # or https://example.com/
          client_id_ref: gsc_client_id
          client_secret_ref: gsc_client_secret
          refresh_token_ref: gsc_refresh_token
          dimensions: [date, query, page]   # default; also: country, device, searchAppearance
          row_limit: 1000                   # rows per page, 1..25000
          max_pages: 10                     # page cap per extract
          lookback_days: 28                 # initial lookback on first sync
          end_offset_days: 2                # Google embargoes the most recent ~2 days
        streams: [search_analytics]
        sink:
          type: parquet
          options:
            dir: ./gsc-out
```

The incremental cursor is a YYYY-MM-DD date stored under `last_date`.
Subsequent syncs request startDate = last_date + 1 day and stop at
today minus `end_offset_days`. On a 401 the connector forces one
refresh and retries once; a second 401 surfaces the original error
rather than looping.

### Wiring an external connector (any language)

The `external` connector type spawns any executable that speaks the
JSON-lines protocol on stdin and stdout. A worked Python example lives
under [`examples/external/`](examples/external/); the wiring looks like:

```yaml
version: 1
state_path: ./ridgeline.db
products:
  myapp:
    connectors:
      - name: pydemo
        type: external
        config:
          command: python3
          args: ["./examples/external/myconnector.py"]
        streams: [events]
        sink:
          type: jsonl
          options:
            dir: ./py-out
```

```sh
./ridgeline sync --config ridgeline.yaml
# loaded ridgeline.yaml
# state: ./ridgeline.db
# myapp/pydemo: 3 records, 1 states saved
# done: 3 records total
```

The runner sends one `extract` command on the child's stdin (with the
configured streams and the persisted incremental state) and reads
RECORD, STATE, LOG, SCHEMA, ERROR, and DONE messages back. Anything
the child writes to stderr is surfaced as a warn-level log.
Cancelling the parent kills the child, so a stuck connector cannot
block the orchestrator.

### Writing Parquet

Swap `type: jsonl` for `type: parquet` on any sink block to write
Apache Parquet files instead of JSON-lines:

```yaml
sink:
  type: parquet
  options:
    dir: ./pq-out
```

Each file has a fixed three-column schema:

| Column      | Type           | Meaning                                    |
|-------------|----------------|--------------------------------------------|
| `stream`    | UTF8           | Stream name (also encoded in the filename) |
| `timestamp` | INT64          | Record timestamp, unix microseconds, UTC   |
| `data_json` | UTF8           | Record body encoded as JSON                |

Storing the record body as a JSON column keeps the sink usable for
every connector without a per-stream schema declaration. Typed-column
Parquet inference is on the roadmap.

### Querying with `ridgeline query`

`ridgeline query <SQL>` runs a SQL statement against an in-process
DuckDB and prints the result as an aligned text table. DuckDB reads
Parquet, CSV, JSON, and SQLite files directly through its built-in
table functions, so a single command can query any prior sync's output
without a separate load step:

```sh
./ridgeline query "SELECT stream, count(*) AS n FROM read_parquet('./pq-out/*/*.parquet') GROUP BY stream ORDER BY stream"
# stream  n
# ------  -
# events  4
# pages   4
# (2 rows)

# field-level query via JSON extraction
./ridgeline query "SELECT json_extract(data_json, '\$.id') AS id, stream FROM read_parquet('./pq-out/*/*.parquet') ORDER BY stream, id"
```

pandas, pyarrow, and the external `duckdb` CLI read the same files
with no translation layer, so `ridgeline query` is one convenient
option rather than the only option.

Run the test suite:

```sh
go test ./...
```

## What exists today

| Package                     | Status                                                                   |
|-----------------------------|--------------------------------------------------------------------------|
| `connectors`                | `Connector` interface, types, message variants, init-time registry.      |
| `connectors/testsrc`        | Synthetic source used by `sync --dry-run`.                               |
| `connectors/hackernews`     | Incremental Algolia-backed Hacker News search (stories, comments).       |
| `connectors/umami`          | Incremental Umami events feed; API-key or login (username/password) auth.|
| `connectors/gsc`            | Google Search Console daily Search Analytics; OAuth 2.0 refresh token.   |
| `connectors/external`       | Runs any executable that speaks the JSON-lines protocol as a connector.  |
| `sinks`                     | `Sink` interface, `SinkConfig` accessors, init-time registry.            |
| `sinks/jsonl`               | JSON-lines file sink. Registers manifest partitions on Close.            |
| `sinks/parquet`             | Apache Parquet file sink with a `{stream, timestamp, data_json}` schema. |
| `enrichers`                 | `Enricher` interface, `EnrichConfig` accessors, init-time registry.      |
| `protocol`                  | JSON-lines `Encoder`/`Decoder` for external plugins.                     |
| `pipeline`                  | ETL lifecycle: Connector -> batch -> Sink -> Flush -> StateStore.Save.   |
| `manifest`                  | Atomic partition index written alongside sink output.                    |
| `state/sqlite`              | Durable `StateStore` on pure-Go SQLite (modernc.org/sqlite).             |
| `creds`                     | AES-256-GCM credential store, shares the SQLite database.                |
| `config`                    | YAML loader for ridgeline.yaml (products, connectors, sinks).            |
| `query`                     | In-process DuckDB runner. Backs the `ridgeline query` CLI.               |
| `cmd/ridgeline`             | Binary. `version`, `sync`, `status`, `query`, `creds`.                   |

The wire format that lets external plugins be written in any language
is specified in [docs/protocol.md](docs/protocol.md).

## What is coming

See [ROADMAP.md](ROADMAP.md). Next up: partition pruning on re-run,
goreleaser + Homebrew, the full OAuth browser + PKCE flow for GSC so
new users can skip the refresh-token-out-of-band step, and a Bubble
Tea TUI shell.

## Install

A `brew install` release is in progress; for now
`go build ./cmd/ridgeline` is the install path.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [CLA.md](CLA.md).

## License

MIT. See [LICENSE](LICENSE).
