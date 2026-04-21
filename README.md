# Ridgeline

Self-hosted intelligence platform for indie developers.

Extract from anywhere. Enrich with AI. Query with SQL. Alert on what
matters. One binary. Pluggable connectors in any language. DuckDB-powered.

> **Status: early bootstrap.** The ETL core runs end-to-end from a
> ridgeline.yaml config: SQLite-backed state (durable across restarts),
> an AES-256-GCM credential store, JSON-lines and Parquet sinks,
> the first real native connector (Hacker News, via the public Algolia
> search API), and an external runner that lets you wire any
> executable that speaks JSON-lines as a connector. Next up are more
> native connectors and DuckDB-backed query. See
> [ROADMAP.md](ROADMAP.md). Built in public.

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
`key_path` field (hex encoded; defaults to `~/.ridgeline/key`) when
a connector actually reads a credential. Today credentials are wired
programmatically via the `creds` package; the CLI never opens the key
file on its own, so `key_path` can be omitted from a sync-only config.
A `ridgeline creds` command is on the roadmap.

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
Parquet inference is on the roadmap. DuckDB reads the files directly:

```sh
duckdb -c "select count(*), stream from read_parquet('./pq-out/*/*.parquet') group by stream;"

# field-level query via JSON extraction
duckdb -c "select json_extract(data_json, '\$.url') as url, count(*) from read_parquet('./pq-out/*/pages.parquet') group by url;"
```

pandas and pyarrow read the same files with no translation layer.

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
| `cmd/ridgeline`             | Binary. `version`, `sync --dry-run`, `sync --config`, `status --config`. |

The wire format that lets external plugins be written in any language
is specified in [docs/protocol.md](docs/protocol.md).

## What is coming

See [ROADMAP.md](ROADMAP.md). Next up: more native connectors
(GSC, Umami), DuckDB integration, and a `ridgeline query` command
backed by it.

## Install

A `brew install` release ships once the query path lands; for now
`go build ./cmd/ridgeline` is the install path.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [CLA.md](CLA.md).

## License

MIT. See [LICENSE](LICENSE).
