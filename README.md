# Ridgeline

Self-hosted intelligence platform for indie developers.

Extract from anywhere. Enrich with AI. Query with SQL. Alert on what
matters. One binary. Pluggable connectors in any language. DuckDB-powered.

> **Status: early bootstrap.** The interfaces below are stable enough
> to start writing connectors and sinks against, but the orchestrator
> binary is still a placeholder. See [ROADMAP.md](ROADMAP.md) for
> what is built and what is next. Built in public.

## Try it now

The binary builds and prints its version. That is the only end-to-end
behavior available today.

```sh
git clone https://github.com/xydac/ridgeline.git
cd ridgeline
go build -o ridgeline ./cmd/ridgeline
./ridgeline version
# 0.0.0-dev
```

Run the test suite:

```sh
go test ./...
```

## What exists today

| Package      | Status                                                              |
|--------------|---------------------------------------------------------------------|
| `connectors` | `Connector` interface, types, message variants, init-time registry. |
| `sinks`      | `Sink` interface, `SinkConfig` accessors, init-time registry.       |
| `enrichers`  | `Enricher` interface, `EnrichConfig` accessors, init-time registry. |
| `protocol`   | JSON-lines `Encoder`/`Decoder` for external plugins.                |
| `cmd/ridgeline` | Placeholder binary, prints version.                              |

The wire format that lets external plugins be written in any language
is specified in [docs/protocol.md](docs/protocol.md).

## What is coming

See [ROADMAP.md](ROADMAP.md). The next cycles add the ETL lifecycle,
a Parquet sink, DuckDB integration, and the first three native
connectors.

## Install

A `brew install` release ships once the first connector is wired up
end-to-end.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [CLA.md](CLA.md).

## License

MIT. See [LICENSE](LICENSE).
