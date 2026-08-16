#!/usr/bin/env python3
"""Tiny external connector that speaks the Ridgeline JSON-lines protocol.

On stdin the script expects a single "extract" command:
    {"type":"extract","streams":[{"name":"metrics","mode":"incremental"}],
     "state":{"since":3}}

On stdout it writes one JSON object per line:
    {"type":"SCHEMA","stream":"metrics","schema":{...}}
    {"type":"RECORD","stream":"metrics","timestamp":"...","data":{...}}
    {"type":"STATE","state":{"since":6}}
    {"type":"DONE"}

The SCHEMA message is optional but recommended for metric streams: it lets
Ridgeline's Business Memory catalog know the stream's kind and the semantic
meaning of each column (unit, direction, aggregation). Without a SCHEMA
message the stream is treated as "unstructured" and will not appear in
'ridgeline memory metrics', baselines, or anomaly detection.

The records are synthetic so the script has no external dependencies
beyond the Python 3 standard library, and the example runs identically
on every machine.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, timedelta, timezone, datetime


# Schema declaration for the "metrics" stream.
# kind: "metric" tells Ridgeline this stream contains quantitative
# measurements that should be tracked in Business Memory.
METRICS_SCHEMA = {
    "kind": "metric",
    "columns": [
        {"name": "date", "type": "timestamp", "key": True},
        {
            "name": "requests",
            "type": "int",
            "direction": "higher_is_better",
            "aggregation": "sum",
        },
        {
            "name": "error_rate",
            "type": "float",
            "unit": "%",
            "direction": "lower_is_better",
            "aggregation": "avg",
        },
        {
            "name": "p99_latency_ms",
            "type": "float",
            "unit": "ms",
            "direction": "lower_is_better",
            "aggregation": "avg",
        },
    ],
}


def main() -> int:
    cmd = read_command()
    streams = cmd.get("streams") or [{"name": "metrics"}]
    state = cmd.get("state") or {}
    since_day = int(state.get("since_day") or 0)

    last_day = since_day
    for stream in streams:
        name = stream["name"]

        # Announce the schema before emitting records so Ridgeline can
        # populate Business Memory with the correct kind and semantics.
        if name == "metrics":
            emit({"type": "SCHEMA", "stream": name, "schema": METRICS_SCHEMA})

        # Emit 7 days of synthetic daily observations starting after since_day.
        days_to_emit = 7
        today = date.today()
        start = today - timedelta(days=days_to_emit - 1)
        for offset in range(days_to_emit):
            day = start + timedelta(days=offset)
            day_index = since_day + offset + 1
            ts = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).isoformat()
            emit({
                "type": "RECORD",
                "stream": name,
                "timestamp": ts,
                "data": {
                    "date": ts,
                    "requests": 1000 + day_index * 10,
                    "error_rate": max(0.0, 2.0 - day_index * 0.1),
                    "p99_latency_ms": 120.0 + day_index * 2,
                },
            })
            last_day = day_index

    emit({"type": "STATE", "state": {"since_day": last_day}})
    emit({"type": "DONE"})
    return 0


def read_command() -> dict:
    line = sys.stdin.readline()
    if not line:
        return {}
    try:
        return json.loads(line)
    except json.JSONDecodeError as exc:
        emit({"type": "ERROR", "error": f"malformed extract command: {exc}"})
        sys.exit(1)


def emit(message: dict) -> None:
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    raise SystemExit(main())
