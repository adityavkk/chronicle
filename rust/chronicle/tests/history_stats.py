#!/usr/bin/env python3
"""Describe measured history latency/rate; this is not a safety checker."""
import argparse
from collections import Counter
import gzip
import json
import math


def quantiles(values):
    """Nearest-rank percentiles, including slow/error attempts when supplied."""
    values = sorted(values)
    return {name: values[math.ceil(q * len(values)) - 1] if values else None
            for name, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}


def summarize(histories):
    counts = Counter()
    attempts, logical, starts, ends = [], [], [], []
    success_gaps = {"append": [], "strict_read": []}
    read_outcomes = Counter()
    acknowledged = 0
    for events in histories:
        previous = {}
        # Concurrent emitters can acquire the output lock out of timestamp order.
        for event in sorted(events, key=lambda event: event["time_ns"]):
            function = event.get("f")
            category = "append" if function in ("append", "append-retry") else "strict_read" if function == "read" else None
            kind = event.get("type")
            if category == "strict_read" and kind in ("ok", "fail", "unknown"):
                read_outcomes[kind] += 1
            if category is not None and kind == "ok":
                if category in previous:
                    success_gaps[category].append((event["time_ns"] - previous[category]) / 1_000_000)
                previous[category] = event["time_ns"]
        # IDs are local to a history. A retry retains the original invocation.
        invoked, succeeded = {}, set()
        for event in events:
            if event.get("f") not in ("append", "append-retry"):
                continue
            kind, identity = event["type"], event["id"]
            if kind == "invoke":
                invoked.setdefault(identity, event["time_ns"])
                starts.append(event["time_ns"])
            elif kind in ("ok", "fail", "unknown"):
                counts[kind] += 1
                attempts.append(event["value"]["latency_ms"])
                ends.append(event["time_ns"])
                if kind == "ok" and identity not in succeeded:
                    acknowledged += 1
                    succeeded.add(identity)
                    logical.append((event["time_ns"] - invoked[identity]) / 1_000_000)
    seconds = (max(ends) - min(starts)) / 1_000_000_000 if starts and ends else None
    return {
        "acknowledged_logical_appends": acknowledged,
        "attempt_outcomes": {kind: counts[kind] for kind in ("ok", "fail", "unknown")},
        "append_window_s": seconds,
        "acknowledged_appends_per_s": acknowledged / seconds if seconds and seconds > 0 else None,
        "attempt_latency_ms_including_errors": quantiles(attempts),
        "acknowledged_logical_latency_ms_including_retries": quantiles(logical),
        "strict_read_attempt_outcomes": {kind: read_outcomes[kind] for kind in ("ok", "fail", "unknown")},
        "per_stream_success_gap_ms": {kind: quantiles(values) for kind, values in success_gaps.items()},
        "success_gap_scope": "between successive successful completions, separately per stream; includes client pacing and retries; not a continuous outage bound",
        "scope": "client-observed closed-loop run; not server capacity or a safety verdict",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("histories", nargs="+", help="schema-1 histories from the same host monotonic clock")
    args = parser.parse_args()
    histories = []
    for path in args.histories:
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as source:
            histories.append([json.loads(line) for line in source if line.strip()])
    print(json.dumps(summarize(histories), indent=2, sort_keys=True))
