# External connector example: Python

This directory holds `myconnector.py`, a tiny Python script that speaks
the Ridgeline JSON-lines protocol on stdin and stdout. It is fully
self-contained: no dependencies beyond the Python 3 standard library.

## What it does

On receiving an `extract` command on stdin, the script emits three
RECORD messages on each requested stream and then a STATE checkpoint.
The records are synthetic; the point is to show the wire shape.

## Run it with ridgeline

Create a `ridgeline.yaml` next to the script:

```yaml
version: 1
state_path: ./ridgeline.db
key_path: ./ridgeline.key
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

Then:

```sh
./ridgeline sync --config ridgeline.yaml
# loaded ridgeline.yaml
# state: ./ridgeline.db
# myapp/pydemo: 3 records, 1 states saved
# done: 3 records total
```

The three records land as JSON lines under `./py-out/`. Re-running with
the same state reuses the cursor: the script reads `state.since` from
its stdin command and starts numbering from there, so the second run
emits records 4 through 6 with the cursor advancing on every run.

## Writing your own

The protocol is documented in
[`protocol/doc.go`](../../protocol/doc.go). A connector only has to:

1. Read one line of JSON from stdin (an `extract` command).
2. Write one RECORD per data row to stdout, one JSON object per line.
3. Write a STATE line with an updated cursor.
4. Exit zero.

Errors should be written as ERROR messages on stdout or as plain text
on stderr (which ridgeline surfaces as warn-level logs).
