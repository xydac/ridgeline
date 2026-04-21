# Ridgeline

Self-hosted intelligence platform for indie developers.

Extract from anywhere. Enrich with AI. Query with SQL. Alert on what
matters. One binary. Pluggable connectors in any language. DuckDB-powered.

> **Status: early bootstrap.** The ETL core is wired end-to-end with
> an in-process test source and a JSON-lines sink. The first real
> connector and a Parquet sink are next. See [ROADMAP.md](ROADMAP.md).
> Built in public.

## Try it now

Clone, build, and run the dry-run pipeline. It emits synthetic
records through the JSON-lines sink and writes a manifest you can
inspect.

```sh
git clone https://github.com/xydac/ridgeline.git
cd ridgeline
go build -o ridgeline ./cmd/ridgeline
./ridgeline sync --dry-run --out ./out --records 3
# wrote 6 records across 2 streams into ./out
#   pages: 3 records
#   events: 3 records
# manifest: out/manifest.json

cat ./out/manifest.json
# {
#   "version": 1,
#   "updated_at": "...",
#   "partitions": [
#     {"stream": "pages",  "path": "<run>/pages.jsonl",  "rows": 3, ...},
#     {"stream": "events", "path": "<run>/events.jsonl", "rows": 3, ...}
#   ]
# }

head -1 ./out/*/pages.jsonl
# {"data":{"id":"pages-0","index":0,"stream":"pages"},"stream":"pages","timestamp":"..."}
```

Run the test suite:

```sh
go test ./...
```

## What exists today

| Package                     | Status                                                                   |
|-----------------------------|--------------------------------------------------------------------------|
| `connectors`                | `Connector` interface, types, message variants, init-time registry.      |
| `connectors/testsrc`        | Synthetic source used by `sync --dry-run`.                               |
| `sinks`                     | `Sink` interface, `SinkConfig` accessors, init-time registry.            |
| `sinks/jsonl`               | JSON-lines file sink. Registers manifest partitions on Close.            |
| `enrichers`                 | `Enricher` interface, `EnrichConfig` accessors, init-time registry.      |
| `protocol`                  | JSON-lines `Encoder`/`Decoder` for external plugins.                     |
| `pipeline`                  | ETL lifecycle: Connector -> batch -> Sink -> Flush -> StateStore.Save.   |
| `manifest`                  | Atomic partition index written alongside sink output.                    |
| `cmd/ridgeline`             | Binary. `ridgeline version`, `ridgeline sync --dry-run`.                 |

The wire format that lets external plugins be written in any language
is specified in [docs/protocol.md](docs/protocol.md).

## What is coming

See [ROADMAP.md](ROADMAP.md). Next up: a SQLite state + credential
store, then the first real native connectors (GSC, Umami, Hacker News),
then the Parquet sink and DuckDB integration.

## Install

A `brew install` release ships once the first real connector is wired
up end-to-end.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [CLA.md](CLA.md).

## License

MIT. See [LICENSE](LICENSE).
