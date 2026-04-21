# Ridgeline JSON-Lines Protocol

This document specifies the wire format Ridgeline uses to talk to
external connectors, enrichers, and sinks. The protocol is
deliberately simple: any language that can read and write UTF-8
to stdin/stdout can implement a Ridgeline plugin.

The Go reference implementation lives in package
[`protocol`](../protocol).

## Transport

- The orchestrator launches the external process and connects pipes:
  - `stdin`  -> commands from orchestrator
  - `stdout` -> structured messages from plugin
  - `stderr` -> unstructured logs (forwarded to the orchestrator UI verbatim)
- Each message is **one line of UTF-8 JSON terminated by `\n`**.
- There is no length prefix or framing other than the newline.
- Lines longer than 1 MiB are rejected by the default decoder. Plugins
  must split large payloads across multiple `RECORD` messages.

## Message envelope

Every message has a required `"type"` field. The orchestrator rejects
any line that lacks one.

Command tags (orchestrator -> plugin) are **lowercase**.
Output tags (plugin -> orchestrator) are **UPPERCASE**.

### Commands (stdin)

| Type        | Purpose                                         | Other fields |
|-------------|-------------------------------------------------|--------------|
| `spec`      | Ask the plugin for its `Spec`.                  | none         |
| `validate`  | Ask the plugin to test its credentials.         | `config`     |
| `discover`  | Ask the plugin which streams are available.     | `config`     |
| `extract`   | Ask the plugin to start emitting records.       | `config`, `streams`, `state` |
| `enrich`    | Hand the plugin a batch of records to augment.  | `config`, `records` |

### Outputs (stdout)

| Type      | Purpose                                          | Other fields |
|-----------|--------------------------------------------------|--------------|
| `SPEC`    | Reply to `spec`.                                 | `spec`       |
| `RECORD`  | One data row.                                    | `stream`, `timestamp`, `data` |
| `STATE`   | Incremental checkpoint, durable resume point.    | `state`      |
| `LOG`     | Structured log entry.                            | `level`, `message` |
| `SCHEMA`  | Schema declaration or change for a stream.       | `stream`, `schema` |
| `DONE`    | Plugin finished cleanly. Closes the session.     | none         |
| `ERROR`   | Fatal error. Plugin exits after sending.         | `error`      |

## Examples

### `spec` round trip

```
-> {"type":"spec"}
<- {"type":"SPEC","spec":{"name":"acme","version":"0.1.0","auth_type":"api_key","streams":[{"name":"events","sync_modes":["incremental"],"default_cron":"0 */1 * * *"}]}}
```

### `extract` round trip

```
-> {"type":"extract","config":{"keywords":["widgets","gizmos"]},"streams":[{"name":"events","mode":"incremental"}],"state":{"cursor":"abc"}}
<- {"type":"SCHEMA","stream":"events","schema":{"columns":[{"name":"id","type":"string","key":true},{"name":"text","type":"string"}]}}
<- {"type":"RECORD","stream":"events","timestamp":"2026-04-20T00:00:00Z","data":{"id":"1","text":"hello"}}
<- {"type":"RECORD","stream":"events","timestamp":"2026-04-20T00:00:01Z","data":{"id":"2","text":"world"}}
<- {"type":"STATE","state":{"cursor":"def"}}
<- {"type":"DONE"}
```

### `enrich` round trip

```
-> {"type":"enrich","config":{"model":"sentiment"},"records":[{"stream":"events","timestamp":"2026-04-20T00:00:00Z","data":{"text":"hello"}}]}
<- {"type":"RECORD","stream":"events","timestamp":"2026-04-20T00:00:00Z","data":{"text":"hello","sentiment":0.8}}
<- {"type":"DONE"}
```

## Semantics

- **Ordering**: within one stream, the orchestrator preserves the order
  of `RECORD` messages. Across streams, ordering is not guaranteed.
- **Checkpointing**: a `STATE` message means "every record before this
  line is safe to commit". The orchestrator only persists state after
  the corresponding records have been durably written.
- **Schema changes**: a plugin may emit a `SCHEMA` message at any time.
  The orchestrator treats it as the schema for all subsequent `RECORD`
  messages on that stream until the next `SCHEMA` for the same stream.
- **Errors**: a plugin reports unrecoverable errors via `ERROR` and
  exits with a non-zero status. Recoverable problems should be logged
  via `LOG` at level `warn` or `error` and the plugin should keep going.
- **Forward compatibility**: the default decoder ignores unknown JSON
  fields. New optional fields can be added without breaking existing
  plugins.

## Stability

This protocol is at version **0.1**. Breaking changes between 0.x
versions are possible until 1.0. The orchestrator and SDK packages
are versioned together and will remain compatible across patch
releases.
