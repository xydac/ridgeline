#!/usr/bin/env python3
"""Tiny external connector that speaks the Ridgeline JSON-lines protocol.

On stdin the script expects a single "extract" command:
    {"type":"extract","streams":[{"name":"events","mode":"incremental"}],
     "state":{"since":3}}

On stdout it writes one JSON object per line:
    {"type":"RECORD","stream":"events","timestamp":"...","data":{...}}
    {"type":"STATE","state":{"since":6}}

The records are synthetic so the script has no external dependencies
beyond the Python 3 standard library, and the example runs identically
on every machine.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone


def main() -> int:
    cmd = read_command()
    streams = cmd.get("streams") or [{"name": "events"}]
    state = cmd.get("state") or {}
    since = int(state.get("since") or 0)

    records_per_stream = 3
    last_seen = since
    for stream in streams:
        name = stream["name"]
        for i in range(since + 1, since + records_per_stream + 1):
            emit({
                "type": "RECORD",
                "stream": name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {
                    "id": f"{name}-{i}",
                    "n": i,
                    "produced_at": time.time(),
                },
            })
            last_seen = i

    emit({"type": "STATE", "state": {"since": last_seen}})
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
