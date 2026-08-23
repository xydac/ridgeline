# Ridgeline

Self-hosted intelligence platform for indie developers.

Extract from anywhere. Enrich with AI. Query with SQL. Alert on what
matters. One binary. Pluggable connectors in any language. DuckDB-powered.

> **Status: early bootstrap.** The ETL core runs end-to-end from a
> ridgeline.yaml config: SQLite-backed state (durable across restarts),
> an AES-256-GCM credential store with a `ridgeline creds` CLI, JSON-lines
> and Parquet sinks, native connectors for Hacker News (Algolia public
> API), Umami (self-hosted analytics, API key or username/password
> login), Google Search Console (OAuth 2.0 via a browser PKCE flow
> or a bring-your-own refresh token), Plausible Analytics (daily
> timeseries via API token), GitHub repository traffic (daily views
> and clones via PAT), and PostHog (individual events via Personal API
> Key), an external runner that lets you wire any
> executable that speaks JSON-lines as a connector, and an in-process
> DuckDB `ridgeline query` command. See [ROADMAP.md](ROADMAP.md). Built
> in public.

## Try it now

### Dry-run (no config, in-memory state)

```sh
git clone https://github.com/xydac/ridgeline.git
cd ridgeline
go build -o ridgeline ./cmd/ridgeline
./ridgeline sync --dry-run --out ./out --records 3
# wrote 6 records across 2 streams into ./out
#   events: 3 records
#   pages: 3 records
# manifest: out/manifest.json
```

By default, output files are nested under a per-run directory
(`out/<run-id>/stream.jsonl`) so consecutive runs append without
overwriting. Query with the `*/*` glob to match all runs:

```sh
./ridgeline query "SELECT count(*) FROM read_json_auto('out/*/*.jsonl')"
```

For one-shot use where you want files directly at `out/stream.jsonl`, add
`--out-dir-root`. This overwrites any previous output on the next run:

```sh
./ridgeline sync --dry-run --out ./out --records 3 --out-dir-root
./ridgeline query "SELECT count(*) FROM read_json_auto('out/*.jsonl')"
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
# starting myapp/demo (testsrc)...
# myapp/demo: 4 extracted, 4 persisted, 2 states saved
# done: 4 extracted, 4 persisted
```

A `starting <product>/<name> (<type>)...` line prints before each
connector runs, so an unattended log shows which connector is in flight
when the process is interrupted or hangs.

State lives in `./ridgeline.db` (SQLite, 0600 permissions, schema
created on first run). A second invocation reuses the same database,
so connector checkpoints survive process restarts.

**Partial failure:** by default a single connector error aborts the
whole run. Pass `--continue-on-error` to keep going and commit the
data from connectors that succeed:

```sh
./ridgeline sync --config ridgeline.yaml --continue-on-error
# loaded ridgeline.yaml
# state: ./ridgeline.db
# starting myapp/analytics (umami)...
# myapp/analytics: 150 extracted, 150 persisted, 1 states saved
# starting myapp/events (umami)...
# sync error (continuing): product myapp connector events: ...
# starting myapp/search (gsc)...
# myapp/search: 42 extracted, 42 persisted, 1 states saved
# done: 192 extracted, 192 persisted (1 connector(s) failed)
```

Exit code is `0` when all connectors pass, `3` when some fail and some
succeed (partial), and `1` when all connectors fail or a configuration
error prevents any connector from starting. Exit code `2` is reserved
for misinvocation (unknown flag, missing required argument).

### Run on a schedule

`ridgeline serve` runs the sync pipeline on a repeating interval. The
first sync starts immediately; subsequent syncs run after each interval
elapses. A single-line outcome is printed after each run:

```sh
./ridgeline serve --config ridgeline.yaml --interval 1h
# loaded ridgeline.yaml
# state: ./ridgeline.db
# starting myapp/analytics (umami)...
# myapp/analytics: 150 extracted, 150 persisted, 1 states saved
# done: 150 extracted, 150 persisted
# 2026-06-17T12:00:03Z serve: 150 extracted, 150 persisted, 1 states saved (2.1s)
# ...repeats every hour...
```

The tick line shows **extracted** (records read from the connector) and
**persisted** (records the sink actually wrote). On a re-run over an
exhausted connector the persisted count will be 0, making an idle loop
immediately distinguishable from one that is still making progress.

For unattended use (systemd, cron, log aggregators), add `--quiet` to
suppress the per-sync preamble and per-connector lines. One timestamped
result line is written per tick, plus any connector log lines (warn:,
info:) which are also timestamped so a log tail remains parseable:

```sh
./ridgeline serve --config ridgeline.yaml --interval 1h --quiet
# 2026-06-17T12:00:03Z serve: 150 extracted, 150 persisted, 1 states saved (2.1s)
# 2026-06-17T13:00:04Z serve: 0 extracted, 0 persisted, 0 states saved (1.8s)
```

Use `--verbose` to explicitly enable the full preamble output (the
default when `--quiet` is not set). `--quiet` and `--verbose` are
mutually exclusive.

`serve` does not daemonize. Use systemd, launchd, or any process
supervisor to keep it alive. SIGINT or SIGTERM exits cleanly; a signal
during a sync prints `serve: shutting down` and exits 0 instead of
reporting a spurious sync error.

Structural config errors (missing file, unparseable YAML, unknown
connector type) cause `serve` to exit non-zero on the first tick so a
process supervisor with `Restart=on-failure` will page you instead of
silently looping. Transient IO errors (a momentary permissions flip, an
editor writing the config mid-tick) are logged and retried on the next
interval.

```sh
# Example systemd service (save as ~/.config/systemd/user/ridgeline.service)
# [Unit]
# Description=Ridgeline sync daemon
# [Service]
# ExecStart=/usr/local/bin/ridgeline serve --config /home/alice/ridgeline.yaml --interval 1h --quiet
# Restart=on-failure
# [Install]
# WantedBy=default.target
```

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

`status` runs the same connector and sink schema validation as `sync`:
unknown option keys, missing required fields, and a sink `dir` that
points at a regular file are all reported by `status` before the first
sync ever runs.

### Products view (TUI)

`ridgeline tui --config ridgeline.yaml` opens a terminal UI that
lists every configured stream with its product, connector type,
connector name, status, last-sync timestamp, and cumulative record
count pulled from the sink manifest. Status is derived from the last
sync time: `never` before a first run, `ok` within the last day,
`stale` after that, `error` when the last in-TUI sync trigger failed.

Keybindings:

- `j` / `down`, `k` / `up`: move the highlight between rows.
- `s`: run a real sync on the highlighted connector through the same
  pipeline path as the CLI; the row updates live with a new status,
  record count, and last-sync time when the run finishes.
- `q`, `ctrl+c`, or `esc`: quit.

Pass `--render-once` to print a single snapshot and exit without
starting an interactive program; useful in pipes or CI.

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

# Overwriting an existing name prints "replaced" so the change is visible:
echo "new-key" | ./ridgeline creds put --config ridgeline.yaml umami_main
# replaced credential "umami_main" (7 bytes)

# --raw preserves trailing bytes verbatim (skips newline strip):
printf 'secret-with-newline\n' | ./ridgeline creds put --raw --config ridgeline.yaml umami_raw

./ridgeline creds list --config ridgeline.yaml
# umami_main
# umami_raw

./ridgeline creds get --config ridgeline.yaml umami_main
# my-umami-api-key

# --raw retrieves the exact bytes stored without appending a trailing newline:
./ridgeline creds get --raw --config ridgeline.yaml umami_raw | wc -c
# 20  (exactly the bytes that were put, newline included)

# --config may appear before or after the credential name in all verbs:
./ridgeline creds get umami_main --config ridgeline.yaml

./ridgeline creds rm --config ridgeline.yaml umami_main
```

**Key file protection.** The credential store is encrypted with an
AES-256 key stored in the file named by `key_path`. If that file is
absent and the store contains existing secrets, any `creds` command
errors rather than minting a new key (which would make existing secrets
unrecoverable):

```
credential store exists but key file missing at ./ridgeline.key;
refusing to mint a replacement key (would orphan 3 existing secret(s));
either restore the key or run `ridgeline creds init --force-new-key` to wipe the store
```

To deliberately start over with a new key (all stored credentials will
be lost):

```sh
ridgeline creds init --config ridgeline.yaml --force-new-key
# credential store wiped and new key written to ./ridgeline.key
```

On a fresh machine with no key file and an empty store, any `creds`
verb creates the key automatically - no explicit `init` step is
required.

Any connector config that declares a key ending in `_ref` pulls its
value from this store at sync time. `api_key_ref: umami_main` on a
connector resolves to `api_key: <plaintext>` before Validate runs, so
the YAML file never carries the secret on disk.

### Pulling Umami analytics

The `umami` connector reads the events feed from a self-hosted Umami
install. It supports two auth modes: an API key (Umami v2 cloud or any
install that exposes Settings -> API Keys) and username/password login
(POSTs to `/api/auth/login` and caches the returned JWT).

**API key mode** (Ridgeline default when `auth:` is omitted). Create
the key in the Umami UI, store it, reference it from the config:

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

**Login mode** (opt in with `auth: login`). Store the username and
password with `ridgeline creds put`, then declare `auth: login` plus
`username_ref` and `password_ref`:

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
# starting myapp/hn (hackernews)...
# myapp/hn: 50 extracted, 50 persisted, 1 states saved
# done: 50 extracted, 50 persisted

./ridgeline sync --config ridgeline.yaml
# loaded ridgeline.yaml
# state: ./ridgeline.db
# starting myapp/hn (hackernews)...
# myapp/hn: 0 extracted, 0 persisted, 1 states saved      # cursor sees no new items yet
# done: 0 extracted, 0 persisted
```

Each sync persists a `created_at_i` high-water mark per stream into
the SQLite state store, so re-runs only fetch records strictly newer
than the last one seen.

### Pulling Google Search Console data

The `gsc` connector reads daily Search Analytics rows for a configured
property via the webmasters/v3 API. Auth is OAuth 2.0. The connector
exchanges a long-lived refresh token for short-lived access tokens at
sync time and caches the access token in the per-connector state map
so a typical hourly run makes one token call per hour.

There are two ways to get the refresh token into the credential store.

**Browser flow (PKCE).** Create a desktop-app OAuth client in Google
Cloud Console, then run:

```sh
# Read the secret from a file (keeps it out of shell history):
./ridgeline creds oauth gsc \
  --config ridgeline.yaml \
  --client-id "1234567890-xxxxx.apps.googleusercontent.com" \
  --client-secret-file ~/Downloads/client_secret.json \
  --name gsc

# Or pipe it from stdin:
cat ~/Downloads/client_secret.json | ./ridgeline creds oauth gsc \
  --config ridgeline.yaml \
  --client-id "1234567890-xxxxx.apps.googleusercontent.com" \
  --client-secret-stdin \
  --name gsc

# --client-secret VALUE also works but writes the secret into shell history.
```

A local HTTP listener is bound on a random port and an authorization
URL is printed. Open it in a browser, sign in with the Google account
that owns the Search Console property, grant the read-only scope, and
the callback completes the PKCE exchange. Three credentials are
stored: `gsc_client_id`, `gsc_client_secret`, `gsc_refresh_token`.
The command prints the exact `*_ref` lines to paste into
`ridgeline.yaml` under the connector's `config:` block.

**Bring-your-own refresh token.** If a browser flow is not available
(for example on a headless server without port forwarding), obtain a
refresh token out of band with the Google OAuth Playground, selecting
the `https://www.googleapis.com/auth/webmasters.readonly` scope, and
seed the same three credential keys directly:

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

### Pulling Plausible Analytics data

The `plausible` connector reads daily aggregate metrics from
[Plausible Analytics](https://plausible.io) (cloud or self-hosted) via
the stats API. Each record covers one calendar day with `visitors`,
`pageviews`, `bounce_rate`, and `visit_duration` columns stored as typed
Parquet fields. Syncs are incremental: the cursor is the last date
successfully fetched.

Create an API token in your Plausible dashboard under Settings -> API
Tokens, then store it:

```sh
echo "plau_..." | ./ridgeline creds put --config ridgeline.yaml plausible_token
```

```yaml
version: 1
state_path: ./ridgeline.db
products:
  myapp:
    connectors:
      - name: stats
        type: plausible
        config:
          site_id: example.com          # domain as registered in Plausible
          api_token_ref: plausible_token
          # base_url: https://plausible.io   # omit for cloud; set for self-hosted
          # lookback_days: 30                # initial backfill window (default 30)
        streams: [daily]
        sink:
          type: parquet
          options:
            dir: ./plausible-out
```

The `daily` stream fetches from `/api/v1/stats/timeseries` with
`interval=date`. Each sync requests from `(last_date + 1)` through
yesterday so today's incomplete data is never written. The legacy name
`timeseries` is accepted as a stream alias for existing configs.

### Pulling GitHub repository traffic

The `github` connector reads daily views and clones for a GitHub
repository via the REST API. Traffic data requires push access to the
repository. Each record covers one calendar day with `count` and
`uniques` columns stored as typed Parquet fields. The `views` and
`clones` streams each maintain an independent incremental cursor.

Create a personal access token (PAT) with `repo` scope (or `public_repo`
for public repositories), then store it:

```sh
echo "github_pat_..." | ./ridgeline creds put --config ridgeline.yaml github_token
```

```yaml
version: 1
state_path: ./ridgeline.db
products:
  myrepo:
    connectors:
      - name: traffic
        type: github
        config:
          owner: acme
          repo: widgets
          api_token_ref: github_token
        streams: [views, clones]
        sink:
          type: parquet
          options:
            dir: ./github-out
```

The GitHub traffic API always returns the last 14 days of data. Each
sync filters server-returned rows through the per-stream cursor so only
new days are written to the sink.

### Pulling PostHog events

The `posthog` connector reads individual analytics events from
[PostHog](https://posthog.com) (cloud or self-hosted) via a Personal
API Key. Each event lands as a row with typed `timestamp`, `event`, and
`distinct_id` columns plus a `data_json` payload for all other properties.

Create a Personal API Key in PostHog under Settings -> Personal API keys,
then store it:

```
echo "phx_..." | ./ridgeline creds put --config ridgeline.yaml posthog_key
```

Add the connector to your config:

```yaml
version: 1
state_path: ./ridgeline.db
products:
  myapp:
    connectors:
      - name: analytics
        type: posthog
        config:
          project_id: "12345"          # numeric project ID from PostHog
          api_key_ref: posthog_key
          # base_url: https://app.posthog.com  # omit for cloud; set for self-hosted
          # lookback_days: 30                  # initial backfill window
        streams: [events]
        sink:
          type: parquet
          options:
            dir: ./posthog-out
```

Query events after a sync:

```
./ridgeline query "SELECT event, COUNT(*) AS n FROM read_parquet('posthog-out/*/*.parquet') GROUP BY event ORDER BY n DESC LIMIT 10"
```

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
        streams: [metrics]
        sink:
          type: parquet
          options:
            dir: ./py-out
```

```sh
./ridgeline sync --config ridgeline.yaml
# loaded ridgeline.yaml
# state: ./ridgeline.db
# starting myapp/pydemo (external)...
# myapp/pydemo: 7 extracted, 7 persisted, 1 states saved
# done: 7 extracted, 7 persisted

ridgeline memory metrics --config ridgeline.yaml
# METRIC                      UNIT  DIRECTION        AGG  LAST VALUE  LAST SEEN
# myapp.metrics.error_rate    %     lower_is_better  avg  1.30        2026-08-15T00:00:00Z
# myapp.metrics.p99_latency   ms    lower_is_better  avg  134.00      2026-08-15T00:00:00Z
# myapp.metrics.requests            higher_is_better sum  1070.00     2026-08-15T00:00:00Z
```

The runner sends one `extract` command on the child's stdin (with the
configured streams and the persisted incremental state) and reads
RECORD, STATE, LOG, SCHEMA, ERROR, and DONE messages back. Anything
the child writes to stderr is surfaced as a warn-level log.

To participate in Business Memory (baselines, anomaly detection, `ridgeline explain`),
the child should emit a `SCHEMA` message with `kind: metric` and per-column semantic
annotations before its first `RECORD`. See [`docs/protocol.md`](docs/protocol.md) for
the full field reference. Without a `SCHEMA` message the stream is stored as
`unstructured` and will not appear in `ridgeline memory metrics`.

#### RECORD field reference

| Field       | Type                         | Required | Behavior when absent or invalid                          |
|-------------|------------------------------|----------|----------------------------------------------------------|
| `type`      | string (`"RECORD"`)          | yes      | missing type aborts the sync                             |
| `stream`    | string                       | yes      | RECORD for a stream not in `streams:` is warned and skipped |
| `timestamp` | RFC 3339 string or number    | yes      | missing, null, or unparseable value is warned and skipped; a number is interpreted as Unix epoch seconds (integer or float, e.g. `1710495000`) |
| `data`      | JSON object                  | yes      | absent or null is warned and skipped                     |

Records skipped for any of the above reasons are counted in `records_skipped: N` in the sync summary.

Each external connector runs under a per-connector timeout (default 5 minutes;
configurable via `timeout: 10m` in the connector's `config:` block). On expiry
the child is killed and, with `--continue-on-error`, the remaining connectors
still run. Cancelling the parent context (SIGTERM to `ridgeline sync`) also
kills the child.

### Writing Parquet or JSON-lines

Both the `parquet` and `jsonl` sinks write the same logical columns, so
you can swap `type: parquet` for `type: jsonl` (or vice versa) on any
sink block. DuckDB uses a different reader for each format, so a saved
query keeps its column names but must switch table functions:
`read_parquet('./pq-out/*/*.parquet')` for the parquet sink,
`read_json_auto('./out/*/*.jsonl')` for the jsonl sink.

```yaml
sink:
  type: parquet   # or: type: jsonl
  options:
    dir: ./pq-out
```

Every output file has at minimum these three columns:

| Column      | Type (parquet)  | Type (jsonl)   | Meaning                                    |
|-------------|-----------------|----------------|--------------------------------------------|
| `stream`    | UTF8            | string         | Stream name (also encoded in the filename) |
| `timestamp` | INT64           | number         | Record timestamp, unix microseconds, UTC   |
| `data_json` | UTF8            | string         | Record body encoded as JSON                |

Connectors that declare a stream Schema (currently `gsc` and `umami`)
get typed columns for the declared fields alongside the `data_json`
payload, so a `gsc` sync writes real `clicks` and `impressions`
columns rather than burying them in JSON. Connectors without a
declared schema fall back to the three-column layout above, keeping
the sink usable for every source without a per-stream declaration.

### Enriching records

An enricher is a transform step that runs after extraction and before
the sink writes each batch of records. Add an `enrichers:` list under
any connector to opt that stream into enrichment:

```yaml
version: 1
state_path: ./ridgeline.db
products:
  myapp:
    connectors:
      - name: events
        type: umami
        config:
          base_url: https://analytics.example.com
          website_id: 00000000-0000-0000-0000-000000000000
          api_key_ref: umami_main
        streams: [events]
        enrichers:
          - type: url_host
            config:
              url_field: url        # field to read the URL from (default: url)
              host_field: host      # field to write the hostname into (default: host)
        sink:
          type: parquet
          options:
            dir: ./pq-out
```

With this config a `host` field is added to each record whose `url`
field is present and contains a parseable hostname, so you can group
by domain in DuckDB without parsing URLs in SQL. Records whose URL is
missing, empty, or hostless (relative paths, opaque strings) pass
through unchanged and have no `host` key. The enricher runs after every
connector batch before the sink writes it; an enricher error aborts
the sync for that connector.

Built-in enrichers:

| Type           | What it adds                                                                | Config keys                                                                        |
|----------------|-----------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `url_host`     | `host` - the hostname extracted from a URL field                            | `url_field` (default `url`), `host_field` (default `host`)                        |
| `ts_normalize` | rewrites a timestamp field to UTC RFC 3339 (sub-second precision preserved) | `ts_field` (default `timestamp`), `out_field` (default: same as `ts_field`)       |

Accepted input formats for `ts_normalize`:

- RFC 3339 with optional sub-seconds: `2006-01-02T15:04:05Z`, `2006-01-02T15:04:05.123Z`
- RFC 3339 with timezone offset: `2006-01-02T15:04:05+02:00`
- Datetime without timezone (treated as UTC): `2006-01-02T15:04:05`, `2006-01-02 15:04:05`
- Date only: `2006-01-02`
- Unix epoch as int or int64: values up to 1e10 are seconds, larger are milliseconds
- Unix epoch as float64: seconds with optional sub-second fraction (up to microsecond precision); values up to 1e10 are seconds, larger are milliseconds
- Numeric epoch encoded as a string: `"1710495000"`, `"1710495000000"`, or `"1710495000.123"` (float strings carry sub-second precision)

Records whose `ts_field` is absent, holds an unsupported type, or cannot be parsed pass through
unchanged. A warning is logged for each skipped value.

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

Pass the entire SQL statement as a single quoted argument.

#### Working with timestamps

The `timestamp` column is stored as a BIGINT of Unix microseconds (UTC).
To convert it to a native DuckDB timestamp for time-window queries, use
`to_timestamp`:

```sql
-- Convert to timestamp for readable display
SELECT to_timestamp(timestamp / 1000000.0) AS ts, stream
FROM read_parquet('./pq-out/*/*.parquet')
ORDER BY ts;

-- Group by day (UTC)
SELECT date_trunc('day', to_timestamp(timestamp / 1000000.0)) AS day,
       count(*) AS n
FROM read_parquet('./pq-out/*/*.parquet')
GROUP BY day ORDER BY day;

-- Filter to a time range
SELECT *
FROM read_parquet('./pq-out/*/*.parquet')
WHERE timestamp BETWEEN
  epoch_us(TIMESTAMPTZ '2024-01-01') AND
  epoch_us(TIMESTAMPTZ '2024-02-01');
```

`to_timestamp` returns a TIMESTAMPTZ (UTC). All standard DuckDB
date/time functions (`date_trunc`, `date_diff`, `strftime`, etc.) work
on the result.

The default mode is read-only. The gate uses a closed allow-list:

- Only the following statement kinds are accepted: `SELECT` (including
  CTEs introduced by `WITH`), `EXPLAIN` (without `ANALYZE`), `DESCRIBE`,
  `SHOW`, and `PRAGMA`. Every other statement -- including `CREATE`,
  `COPY`, `INSERT`, `DELETE`, `USE`, `BEGIN`, `COMMIT`, and any
  unrecognized verb -- is rejected before DuckDB executes anything.
  `EXPLAIN ANALYZE` is rejected in both the keyword form
  (`EXPLAIN ANALYZE ...`) and the parenthesized-options form
  (`EXPLAIN (ANALYZE) ...`) because it executes the wrapped statement.
- Each statement in a semicolon-separated batch is checked individually;
  a leading `SELECT` does not license a trailing write.
- Network reads (HTTP, S3, GCS) are blocked by disabling DuckDB's
  `httpfs` extension. Local filesystem reads (`read_parquet`,
  `read_csv_auto`, `read_json_auto`, `sqlite_scan`) remain unrestricted.

Pass `--write` to allow modifications (INSERT, DELETE, COPY, ATTACH, DDL)
and to lift the network restriction.

pandas, pyarrow, and the external `duckdb` CLI read the same files
with no translation layer, so `ridgeline query` is one convenient
option rather than the only option.

Run the test suite:

```sh
go test ./...
```

## Concepts

### Semantic stream metadata

Every stream Ridgeline syncs carries a `SemanticKind` that classifies what
its records represent in business terms:

| Kind | Meaning |
|---|---|
| `metric` | Every record is a measurement that can be aggregated over time (page views per day, clicks per hour). |
| `event` | Every record is a discrete occurrence with a timestamp (a click, a sign-up, a deploy). Events are counted, not summed. |
| `dimension` | Every record is a reference entity (a user, a product) that other streams join against. |
| `unstructured` | Records have no standardized quantitative interpretation (raw log lines, free-form text). |

Metric columns carry additional annotations:

| Field | Values |
|---|---|
| `direction` | `higher_is_better`, `lower_is_better`, or `neutral` |
| `aggregation` | `sum`, `avg`, `last`, or `count` |
| `unit` | Optional human-readable label (`%`, `seconds`, `USD`, ...) |

These tags are the raw material for the Business Memory layer: anomaly
detection, baseline computation, and the `explain` reasoning primitive
all use directionality to frame deviations as good-news or bad-news.

To inspect what Ridgeline knows about a data source:

```
ridgeline schema plausible
ridgeline schema plausible.daily
ridgeline schema github.views
```

Example output:

```
connector:  plausible
stream:     daily
kind:       metric
description: Daily visitors, pageviews, bounce rate, and visit duration for the configured site.
columns:
  date            timestamp, key
  visitors        int, higher_is_better, sum
  pageviews       int, higher_is_better, sum
  bounce_rate     float, lower_is_better, avg, unit=%
  visit_duration  float, neutral, avg, unit=seconds
```

Each sync also writes a `_ridgeline_semantics.json` file alongside the
data files, so downstream tooling can read the tags without importing
the Go package.

## Business Memory

Ridgeline maintains a persistent catalog of everything it has observed
across sync runs. The catalog survives sink wipes -- it is the durable
understanding, not the raw data files.

### Streams catalog

After each sync, the catalog records which connector + stream was seen,
what kind of data it carries, when it was first observed, and how many
rows have accumulated over all time.

```
ridgeline memory streams --config ridgeline.yaml
```

```
CONNECTOR   STREAM    KIND     FIRST SEEN            LAST SEEN             ROWS (LIFETIME)
----------  --------  -------  --------------------  --------------------  ---------------
plausible   daily     metric   2026-08-01T12:00:00Z  2026-08-08T12:00:00Z  56
posthog     events    event    2026-08-01T12:00:00Z  2026-08-08T12:00:00Z  14302
```

`FIRST SEEN` is set on first observation and never overwritten, so
"how long has Ridgeline been watching this stream?" is always answerable.

### Metrics catalog

Metric-typed streams have columns annotated with unit, directionality,
and aggregation hints. The catalog tracks the last observed value for
each metric column.

```
ridgeline memory metrics --config ridgeline.yaml
```

```
METRIC                            UNIT     DIRECTION          AGGREGATION  LAST VALUE  LAST SEEN
--------------------------------  -------  -----------------  -----------  ----------  --------------------
plausible.daily.bounce_rate       %        lower_is_better    avg          38.2        2026-08-08T12:00:00Z
plausible.daily.pageviews         users    higher_is_better   sum          4821        2026-08-08T12:00:00Z
plausible.daily.visit_duration    seconds  neutral            avg          142         2026-08-08T12:00:00Z
plausible.daily.visitors          users    higher_is_better   sum          1234        2026-08-08T12:00:00Z
```

The `direction` column records whether higher or lower values are
preferable for this metric -- the raw material for the upcoming anomaly
detection and `explain` reasoning primitives.

### Baselines

For each metric column, Ridgeline maintains rolling-window statistics
computed from every observed `last_value` since tracking began. Three
windows are computed on each sync: 7-day, 30-day, and 90-day.

```
ridgeline memory baselines --config ridgeline.yaml plausible.daily.visitors
```

```
Metric: plausible.daily.visitors
30d sparkline: ▂▃▃▄▅▄▆▇▆▅▆▇▇▆▄▅▆▇▇▇▇▇▆▇▇▇▇▇▇▇

WINDOW    SAMPLES  MEAN      STDDEV    MIN       MAX       COMPUTED
--------  -------  --------  --------  --------  --------  --------------------
7d        7        1187      43.2      1102      1234      2026-08-08T12:00:00Z
30d       30       1143      61.8      987       1234      2026-08-08T12:00:00Z
90d       62       1098      88.4      832       1234      2026-08-08T12:00:00Z
```

The sparkline is an ASCII rendering of the last 30 days of observed
values using Unicode block elements. No external dependencies are
required.

Baselines are the raw material for anomaly detection: when a new value
falls outside `k * stddev` from the window mean, Ridgeline can flag it
as a surprise -- good or bad depending on the declared directionality of
the metric.

To recompute all baselines from recorded history (useful after importing
historical data or changing window configuration):

```
ridgeline memory recompute --config ridgeline.yaml
ridgeline memory recompute --config ridgeline.yaml --since 30d
```

The `--since` flag restricts recomputation to metrics that received new
observations within that window.

### Anomalies

After each sync, Ridgeline checks every metric with an established baseline
against the newly observed value. When a value deviates by more than `k`
standard deviations from the rolling window mean, an anomaly event is
written to the catalog. The event records:

- the metric, the observed value, the baseline mean, and the sigma deviation
- the window that triggered the flag (7d, 30d, or 90d)
- a directional interpretation: `surprise-good`, `surprise-bad`, or
  `surprise-neutral`, derived from the metric's declared directionality

```
ridgeline memory events --config ridgeline.yaml --since 7d
```

```
TIME                  METRIC                         WINDOW  OBSERVED    MEAN        DEVIATION  DIRECTION
-------------------   ----------------------------   ------  ----------  ----------  ---------  ----------------
2026-08-09T12:00:00Z  plausible.daily.visitors       7d      724         1187        -4.32σ     surprise-bad
2026-08-07T12:00:00Z  plausible.daily.bounce_rate    30d     61.4        38.2        +5.18σ     surprise-bad
```

Two knobs control detection sensitivity:

- `anomaly_k` (default `2.5`): the standard deviation multiplier. A value of
  `2.5` flags observations more than 2.5 standard deviations from the mean.
  Lower values produce more events; higher values produce fewer.
- `min_samples` (default `14`): the minimum number of historical observations
  required before a metric is eligible for detection. This prevents false
  positives during the first two weeks of tracking.

Both knobs are configurable globally and per-metric in `ridgeline.yaml`:

```yaml
memory:
  anomaly_k: 2.5
  min_samples: 14
  metric_overrides:
    "plausible.daily.bounce_rate":
      anomaly_k: 3.0
```

Events are never deleted; they accumulate as a historical record of
surprising business moments. Use `--since 0` to list all events ever recorded.

### Reasoning: explain a metric

`ridgeline explain` turns the memory catalog into a plain-text narrative.
It assembles the metric's current value, its standing relative to the rolling
baseline, the change from the prior period of the same length, and any anomalies
detected during sync:

```
ridgeline explain plausible.daily.visitors --config ridgeline.yaml --since 7d
```

```
plausible.daily.visitors -- last 7d

Current value: 724 visitors (as of 2026-08-09).
The 30d baseline is 1187 +/- 105 visitors (higher is better); current is -4.4 sigma from the mean.
Compared to the prior 7d (mean 1151, 7 samples), this period is -37.1%.
1 anomaly detected in the last 7d:
  2026-08-09: 724 observed (-4.3 sigma from 30d baseline) -- surprise-bad

Summary: visitors is below baseline (watch), with one surprise-bad spike on 2026-08-09.
```

Add `--json` to get the same content as a structured object for agent consumption:

```
ridgeline explain plausible.daily.visitors --config ridgeline.yaml --since 7d --json
```

```json
{
  "metric_fq": "plausible.daily.visitors",
  "since": "7d",
  "current_value": 724,
  "current_at": "2026-08-09T12:00:00Z",
  "direction": "higher_is_better",
  "unit": "visitors",
  "baseline": { "window_days": 30, "mean": 1187, "stddev": 105, "sample_count": 30 },
  "window_mean": 952,
  "window_samples": 7,
  "prior_mean": 1151,
  "prior_samples": 7,
  "anomalies": [
    {
      "at": "2026-08-09T12:00:00Z",
      "observed_value": 724,
      "baseline_mean": 1187,
      "stddev_from_mean": -4.3,
      "window_days": 30,
      "direction": "surprise-bad"
    }
  ],
  "correlated_events": [],
  "confidence": 0.33,
  "summary": "visitors is below baseline (watch), with one surprise-bad spike on 2026-08-09 (low confidence: 30-day baseline, n=30)."
}
```

The output is templated -- no LLM required. It can be piped directly into an
agent prompt or used as the answer to "what happened to my visitors this week?"
The `explain` command works for any metric in the catalog regardless of which
connector produced it.

When the timeline includes correlated events (deploys, commits, notes from
other connectors), `explain` surfaces them alongside the anomaly list so you
can see causality at a glance:

```
Correlated events in window:
  2026-08-09 [deploy]: shipped v2.1 -- routing refactor
  2026-08-09 [commit]: refactor: replace custom router with stdlib mux
```

### Reasoning: compare two metrics

`ridgeline compare` produces a pairwise narrative that walks both metrics
through the same baseline/anomaly pipeline `explain` uses, then composes a
comparative verdict -- whether they moved together, diverged, and which anomalies
or correlated events the two windows share:

```
ridgeline compare plausible.daily.visitors plausible.daily.pageviews \
  --config ridgeline.yaml --since 7d
```

```
Comparing visitors vs pageviews -- last 7d

visitors: current 724 visitors (-37.1% vs prior), -4.4 sigma from 30d baseline.
pageviews: current 2890 pageviews (-31.2% vs prior), -3.9 sigma from 30d baseline.

Verdict: both regressed.
visitors: 1 anomaly(s) -- surprise-bad
pageviews: 1 anomaly(s) -- surprise-bad
1 shared event(s) in window:
  2026-08-09 [deploy]: shipped v2.1 -- routing refactor

Summary: visitors and pageviews both regressed.
```

Add `--json` to get the same content as a structured object with per-metric
`explain` sub-objects, a top-level `verdict`, `diverged` flag, and
`shared_events` array suitable for agent consumption.

### Reasoning: period-over-period comparison

Use `--against RECENT,PRIOR` to compare one metric against a prior window of
a different length:

```
ridgeline compare plausible.daily.visitors --against 7d,14d \
  --config ridgeline.yaml
```

```
visitors -- last 7d vs prior 14d

Recent 7d: mean 952 visitors (7 sample(s)).
Prior 14d: mean 1151 visitors (14 sample(s)).
Change: -17.3% vs prior period.
Verdict: regressed.
The 30d baseline is 1187 +/- 105 visitors (higher is better); recent mean is -2.2 sigma.
1 anomaly(s) in recent window:
  2026-08-09: 724 observed (-4.3 sigma from 30d baseline) -- surprise-bad

Summary: visitors regressed (-17.3% vs prior 14d).
```

Both forms work with `--json` and produce structured output for agent consumption.

### Reasoning: investigate a metric

`ridgeline investigate` produces a cross-source causal narrative: it runs the
same baseline and anomaly analysis as `explain`, then correlates any detected
anomalies with non-metric events (deploys, commits, manual notes) by temporal
proximity, and computes Pearson correlation against sibling metrics in the same
window.

```
ridgeline investigate plausible.daily.visitors --config ridgeline.yaml --since 14d
```

```
Investigating visitors -- last 14d

1 anomaly(s) detected:
  2026-08-13: 312 (-69% vs baseline 1014, 5.2 stddev, surprise-bad)

Correlated events (within 48h before anomaly):
  2026-08-13 09:14 [deploy]: shipped v0.2.0-rc1 (12.2h before anomaly at 2026-08-13)
  2026-08-12 22:30 [commit]: Remove caching layer (22.5h before anomaly at 2026-08-13)

Sibling metric correlation:
  plausible.daily.pageviews: r=0.94 (moved together, 14 shared days)
  plausible.daily.bounce_rate: r=-0.81 (moved inversely, 14 shared days)

```

Use `--json` for structured output:

```
ridgeline investigate plausible.daily.visitors --config ridgeline.yaml --since 14d --json
```

The JSON response includes `explain` (the full explain payload), `causal_candidates`
(each with `event_at`, `kind`, `description`, `anomaly_at`, `proximity_hours`), and
`sibling_correlations` (each with `metric_fq`, `r`, `samples`). This shape is designed
for agent consumption: an AI assistant can read the causal candidates and phrase a
hypothesis ("the caching layer removal appears correlated with the visitor drop").

### Reasoning: summarize all tracked metrics

`ridgeline summarize` answers "what happened this week?" across your whole
Business Memory catalog. It ranks every tracked metric by directionality-adjusted
deviation from its baseline -- surprise-bad events (a metric that should be high but
is low) rank above surprise-good events of the same magnitude -- and prints the most
actionable ones grouped by connector.

```
ridgeline summarize --config ridgeline.yaml --since 7d
```

```
Business Memory: 6 metric(s) across 2 connector(s) -- last 7d

[plausible]
  plausible.daily.visitors: 724 visitors (-4.3 sigma from 30d baseline, surprise-bad)
  plausible.daily.pageviews: 1891 pageviews (-3.1 sigma from 30d baseline, surprise-bad)
  plausible.daily.bounce_rate: 62.4% (within baseline range)

[github]
  github.commits.total: 18 commits (+1.2 sigma from 30d baseline, above average)

```

Use `--top N` to show more or fewer metrics (default 5). Use `--json` for
structured output where each entry includes the full explain payload:

```
ridgeline summarize --config ridgeline.yaml --since 7d --top 3 --json
```

```json
{
  "since": "7d",
  "total_metrics": 6,
  "total_connectors": 2,
  "top_metrics": [
    {
      "metric_fq": "plausible.daily.visitors",
      "connector": "plausible",
      "score": 4.31,
      "confidence": 0.92,
      "explain": { ... }
    },
    ...
  ]
}
```

`score` is the directionality-adjusted z-score (positive = surprise-bad,
negative = surprise-good). An agent can sort or filter by score to focus on
the metrics that need attention.

### Confidence scoring

Every reasoning primitive attaches a numeric confidence score to its output.
The score is in `[0, 1]` and reflects how much evidence backs a claim.

**Score sources:**

| Claim type | Evidence | Formula |
|---|---|---|
| Baseline claim | Sample count behind the baseline | `min(1.0, n/90)` -- saturates at 90 samples |
| Anomaly claim | Absolute z-score of the event | `min(1.0, \|z\|/3.0)` -- saturates at 3 sigma |
| Causal candidate | Hours between event and anomaly | `1 - hours/48` -- linear decay over 48h |
| Sibling correlation | Pearson r and sample count | `\|r\| * min(1.0, n/30)` -- requires 5+ shared days |

**Text output** includes a tag on the summary line:

```
visitors is below baseline (watch), with one downward spike on 2026-08-13
(low confidence: 7-day baseline, n=7).
```

**JSON output** carries a `confidence` field (float) on every result object:

```json
{
  "metric_fq": "plausible.daily.visitors",
  "confidence": 0.92,
  "summary": "visitors is above baseline (positive) (high confidence: 90-day baseline, n=83)."
}
```

**How to read scores:**
- `>= 0.75` -- high confidence; claim is backed by substantial data.
- `0.40 - 0.75` -- medium confidence; directionally reliable but thin data.
- `< 0.40` -- low confidence; treat as a signal to collect more data, not a firm finding.

A metric that was first synced yesterday will show low confidence until its
baseline window fills. An anomaly at 4 sigma shows high anomaly confidence even
with a short baseline. Agents can gate on `confidence >= 0.75` to suppress
low-evidence findings from automated alerts.

### Cross-connector event timeline

The Business Memory timeline accumulates events from multiple sources. Two
ways to add non-metric events:

**Manual notes** -- record what you did:

```
ridgeline memory note --config ridgeline.yaml --kind deploy --description "shipped v2.1"
ridgeline memory note --config ridgeline.yaml --kind rollback --description "reverted v2.1" --at 2026-08-10
```

Accepted `--at` formats: RFC3339 (`2026-08-10T14:00:00Z`) or `YYYY-MM-DD`.
`--kind` is free-form; common values: `deploy`, `release`, `rollback`, `incident`, `migration`.

**Git connector** -- reads commits from a local repository automatically on
each sync. Configure it in `ridgeline.yaml` under the usual
`products.<name>.connectors` block:

```yaml
version: 1
state_path: ./ridgeline.db
products:
  myapp:
    connectors:
      - name: myapp-git
        type: git
        streams: [commits]
        config:
          path: /home/user/code/myapp
        sink:
          type: parquet
          options:
            dir: data/git
```

On sync, every new commit is written to the parquet sink (queryable via
`ridgeline query`) and also inserted into the Business Memory timeline as a
`commit` event. Subsequent syncs are incremental -- only commits newer than
the last-seen hash are processed.

All events (anomalies, deploys, commits, notes) appear together in
`ridgeline memory events`:

```
ridgeline memory events --config ridgeline.yaml --since 7d
```

```
TIME                  KIND     DETAIL
-------------------   ------   -------------------------------------------------
2026-08-09T14:23:00Z  deploy   shipped v2.1 -- routing refactor
2026-08-09T13:01:52Z  commit   refactor: replace custom router with stdlib mux
2026-08-09T12:00:00Z  anomaly  plausible.daily.visitors: 724 (-4.32σ, 30d) -- surprise-bad
```

## MCP server

Ridgeline exposes its Business Memory to AI agents via a
[Model Context Protocol](https://spec.modelcontextprotocol.io) server. Once
configured, an agent can call five tools against your real synced data without
any custom glue code.

### Connecting to Claude Desktop

Add an entry in `claude_desktop_config.json` (usually at
`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "ridgeline": {
      "command": "ridgeline",
      "args": ["mcp", "--config", "/path/to/ridgeline.yaml"]
    }
  }
}
```

Restart Claude Desktop. Ridgeline now appears as a connected tool source.

### Tools

**`list_metrics`** -- returns all metrics in the Business Memory catalog as a
JSON array. Each element includes `fq_name`, `unit`, `direction`,
`aggregation`, and `last_value`.

**`explain(metric_fq, since)`** -- returns a structured JSON narrative for the
metric covering current value, baseline comparison, prior-period trend,
anomalies, and correlated events. `since` defaults to `7d`; accepts `Nd` or
Go duration strings (`24h`, `168h`).

**`investigate(metric_fq, since)`** -- returns a cross-source causal narrative
for the metric: anomalies in the window, events that fall within a temporal
proximity window of each anomaly (candidate causes), and sibling metrics
ranked by Pearson correlation over the same window. `since` defaults to `14d`
and accepts the same forms as `explain`.

**`compare(metric_a, metric_b, since)`** -- runs `explain` on both metrics
over the same window and returns a side-by-side JSON result with per-metric
baselines, anomalies, shared correlated events, a directional verdict
(`both-improved`, `diverged`, `both-regressed`, or `unchanged`), and a
confidence score. `since` defaults to `7d`.

**`summarize(since, top)`** -- walks all tracked metrics, ranks them by
directionality-weighted deviation from baseline (surprise-bad events surface
first), and returns the top-`top` results as a JSON array. Each entry includes
the full `explain` payload and a `score`. Use this to answer "what should I
focus on this week?" `since` defaults to `7d`; `top` defaults to 5.

### Example agent transcripts

**Diagnosing a drop:**

```
User: Why did my signups drop this week?

Claude: [calls explain(metric_fq="myapp.daily.signups", since="7d")]
        [calls investigate(metric_fq="myapp.daily.signups", since="14d")]

explain result:
{
  "metric_fq": "myapp.daily.signups",
  "since": "7d",
  "current_value": 38,
  "direction": "higher_is_better",
  "baseline": {"window_days": 30, "mean": 91.4, "stddev": 14.2},
  "anomalies": [{"at": "2026-08-12T00:00:00Z", "stddev_from_mean": -3.76, "direction": "surprise-bad"}],
  "correlated_events": [{"at": "2026-08-12", "kind": "deploy", "description": "migrated auth to new provider"}],
  "confidence": 0.87,
  "summary": "signups is below baseline (watch), with one surprise-bad spike on 2026-08-12."
}
```

**Weekly focus question:**

```
User: What should I focus on this week?

Claude: [calls summarize(since="7d", top=5)]

Result:
{
  "since": "7d",
  "total_metrics": 12,
  "total_connectors": 3,
  "top_metrics": [
    {"metric_fq": "myapp.daily.signups", "connector": "myapp", "score": 3.76, "confidence": 0.87, "explain": {...}},
    ...
  ]
}
```

**Comparing two metrics:**

```
User: Did pageviews and signups move together last month?

Claude: [calls compare(metric_a="myapp.daily.pageviews", metric_b="myapp.daily.signups", since="30d")]

Result:
{
  "since": "30d",
  "verdict": "diverged",
  "diverged": true,
  "confidence": 0.72,
  "summary": "pageviews improved 12% while signups regressed 24%; metrics diverged.",
  "metric_a": {...},
  "metric_b": {...}
}
```

### Running manually

```
ridgeline mcp --config ridgeline.yaml
```

The server reads JSON-RPC messages from stdin and writes responses to stdout.
All diagnostic output goes to stderr so it does not corrupt the transport.

## What exists today

| Package                     | Status                                                                   |
|-----------------------------|--------------------------------------------------------------------------|
| `connectors`                | `Connector` interface, types, message variants, init-time registry.      |
| `connectors/testsrc`        | Synthetic source used by `sync --dry-run`.                               |
| `connectors/hackernews`     | Incremental Algolia-backed Hacker News search (stories, comments).       |
| `connectors/umami`          | Incremental Umami events feed; API-key or login (username/password) auth.|
| `connectors/gsc`            | Google Search Console daily Search Analytics; OAuth 2.0 refresh token.   |
| `connectors/plausible`      | Plausible Analytics daily timeseries (visitors, pageviews, bounce rate). |
| `connectors/github`         | GitHub repository traffic: daily views and clones (PAT auth).           |
| `connectors/posthog`        | PostHog individual events; typed timestamp, event, distinct_id columns.  |
| `connectors/git`            | Reads local git commits into the Business Memory event timeline (incremental, by commit hash). |
| `connectors/external`       | Runs any executable that speaks the JSON-lines protocol as a connector.  |
| `sinks`                     | `Sink` interface, `SinkConfig` accessors, init-time registry.            |
| `sinks/jsonl`               | JSON-lines file sink. Registers manifest partitions on Close.            |
| `sinks/parquet`             | Apache Parquet file sink with a `{stream, timestamp, data_json}` schema. |
| `enrichers`                 | `Enricher` interface, `EnrichConfig` accessors, init-time registry.      |
| `enrichers/urlhost`         | Built-in `url_host` enricher: extracts hostname from a URL field.        |
| `enrichers/tsnormalize`     | Built-in `ts_normalize` enricher: normalizes timestamps to UTC RFC 3339. |
| `protocol`                  | JSON-lines `Encoder`/`Decoder` for external plugins.                     |
| `pipeline`                  | ETL lifecycle: Connector -> batch -> Sink -> Flush -> StateStore.Save.   |
| `manifest`                  | Atomic partition index written alongside sink output.                    |
| `state/sqlite`              | Durable `StateStore` on pure-Go SQLite (modernc.org/sqlite).             |
| `creds`                     | AES-256-GCM credential store, shares the SQLite database.                |
| `config`                    | YAML loader for ridgeline.yaml (products, connectors, sinks).            |
| `memory`                    | Business Memory catalog: streams, metrics, baselines, anomaly events, and the `explain` / `compare` / `investigate` / `summarize` reasoning primitives. |
| `query`                     | In-process DuckDB runner. Backs the `ridgeline query` CLI.               |
| `cmd/ridgeline`             | Binary. `version`, `sync`, `serve`, `status`, `query`, `creds`, `tui`, `schema`, `memory`, `explain`, `compare`, `investigate`, `summarize`, `mcp`. |

The wire format that lets external plugins be written in any language
is specified in [docs/protocol.md](docs/protocol.md).

## What is coming

See [ROADMAP.md](ROADMAP.md) for known gaps and planned work.

## Install

**macOS** - install via Homebrew:

```
brew install xydac/tap/ridgeline
```

**Linux (x86-64)** - download a pre-built binary from the
[releases page](https://github.com/xydac/ridgeline/releases), unpack,
and place the binary in your PATH:

```
tar -xzf ridgeline_VERSION_linux_amd64.tar.gz
sudo mv ridgeline /usr/local/bin/
```

Pre-built binaries are published for linux-amd64, darwin-amd64, and
darwin-arm64. Linux arm64 users must build from source (see below).

**Build from source** - requires Go 1.25+ and a C compiler (DuckDB is
linked via cgo, so `CGO_ENABLED=1` and a working C toolchain are
required):

```
# Debian/Ubuntu: sudo apt install build-essential
# macOS:         xcode-select --install
git clone https://github.com/xydac/ridgeline
cd ridgeline
go build ./cmd/ridgeline
```

Releases are produced by [goreleaser](https://goreleaser.com) on every
`v*` tag. macOS users get a Homebrew cask via
[xydac/homebrew-tap](https://github.com/xydac/homebrew-tap); Linux
x86-64 users install from the tar.gz archive on the releases page.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [CLA.md](CLA.md).

## License

MIT. See [LICENSE](LICENSE).
