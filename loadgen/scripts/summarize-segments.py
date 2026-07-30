#!/usr/bin/env python3
"""Build the checked-in issue-6 summary from raw dsload artifacts."""

import argparse
import datetime as dt
import json
import math
import re
from pathlib import Path


PROM_SAMPLE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{[^}]*\})?\s+"
    r"(?P<value>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)$"
)


def prometheus(path: Path) -> dict[str, float]:
    samples: dict[str, float] = {}
    if not path.exists():
        return samples
    for line in path.read_text().splitlines():
        match = PROM_SAMPLE.match(line)
        if match:
            samples[match.group("name")] = float(match.group("value"))
    return samples


def redis_script_microseconds(path: Path) -> int | None:
    if not path.exists():
        return None
    total = 0
    found = False
    for line in path.read_text().splitlines():
        if not line.startswith("cmdstat_eval"):
            continue
        for item in line.partition(":")[2].split(","):
            key, separator, value = item.partition("=")
            if separator and key == "usec":
                total += int(value)
                found = True
    return total if found else 0


def delta(before: dict[str, float], after: dict[str, float], name: str) -> float:
    return max(0.0, after.get(name, 0.0) - before.get(name, 0.0))


def resource_summary(samples: list[dict], name: str) -> dict:
    selected = sorted((s for s in samples if s["name"] == name), key=lambda s: s["sec"])
    measured = [s for s in selected if s.get("phase") == "measurement"]
    if measured:
        selected = measured
    if not selected:
        return {}
    first, last = selected[0], selected[-1]
    elapsed = last["sec"] - first["sec"]
    cpu = None
    if elapsed > 0 and last["cpu_seconds"] >= first["cpu_seconds"]:
        cpu = (last["cpu_seconds"] - first["cpu_seconds"]) / elapsed * 100

    def counter(field: str) -> int | None:
        if not any(field in sample for sample in selected):
            return None
        return max(0, int(last.get(field, 0)) - int(first.get(field, 0)))

    return {
        "rss_max_bytes": max(s.get("rss_bytes", 0) for s in selected),
        "cpu_mean_percent": cpu,
        "open_files_max": max(s.get("open_files", 0) for s in selected),
        "connections_max": max(s.get("connections", 0) for s in selected),
        "network_read_bytes": counter("network_read_bytes"),
        "network_write_bytes": counter("network_write_bytes"),
        "operations": counter("operations"),
        "script_microseconds": counter("script_microseconds"),
    }


def error_count(counters: dict[str, int], operation: str | None = None) -> int:
    prefix = "err:" if operation is None else f"err:{operation}:"
    return sum(value for key, value in counters.items() if key.startswith(prefix))


def ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def finite(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def summarize(result_path: Path) -> dict:
    raw = json.loads(result_path.read_text())
    artifact = result_path.parent
    before = prometheus(artifact / "metrics-before.prom")
    after = prometheus(artifact / "metrics-after.prom")
    started = dt.datetime.fromisoformat(raw["measure_start"])
    ended = dt.datetime.fromisoformat(raw["measure_end"])
    seconds = (ended - started).total_seconds()
    counters = raw.get("counters", {})
    catchup_bytes = counters.get("catchup_bytes", 0)
    catchup_ok = counters.get("catchup_ok", 0)
    errors = error_count(counters)
    catchup_errors = error_count(counters, "catchup")
    append_errors = error_count(counters, "append")
    catchup_drops = counters.get("catchup_dropped", 0)
    append_drops = counters.get("appends_dropped", 0)
    catchup_attempts = catchup_ok + catchup_errors + catchup_drops
    append_ok = counters.get("appends_ok", 0)
    append_attempts = append_ok + append_errors + append_drops
    cache_hits = delta(before, after, "chronicle_segment_cache_hits_total")
    cache_misses = delta(before, after, "chronicle_segment_cache_misses_total")
    origin_reads = delta(before, after, "chronicle_segment_origin_reads_total")
    allocated = delta(before, after, "go_memstats_alloc_bytes_total")
    append_metric = raw.get("metrics", {}).get("append", {})
    catchup_metric = raw.get("metrics", {}).get("catchup_total", {})
    ttfb_metric = raw.get("metrics", {}).get("catchup_ttfb", {})
    redis_resource = resource_summary(raw.get("resources", []), "redis")
    script_before = redis_script_microseconds(
        artifact / "redis-commandstats-before.txt"
    )
    script_after = redis_script_microseconds(
        artifact / "redis-commandstats-after.txt"
    )
    if script_before is not None and script_after is not None:
        redis_resource["script_microseconds"] = max(0, script_after - script_before)
        redis_resource["script_scope"] = "setup_warmup_measurement_teardown"
    return {
        "mode": raw["label"].removeprefix("chronicle-"),
        "scenario": raw["scenario"]["Name"],
        "readers": raw["scenario"]["Catchup"]["Readers"],
        "mixed": raw["scenario"]["Writers"]["PerStream"] > 0,
        "measurement_seconds": seconds,
        "catchup_mib_per_second": catchup_bytes / (1 << 20) / seconds,
        "catchup_reads_per_second": catchup_ok / seconds,
        "catchup_p50_ms": catchup_metric.get("p50_ms"),
        "catchup_p95_ms": catchup_metric.get("p95_ms"),
        "catchup_p99_ms": catchup_metric.get("p99_ms"),
        "ttfb_p50_ms": ttfb_metric.get("p50_ms"),
        "ttfb_p95_ms": ttfb_metric.get("p95_ms"),
        "ttfb_p99_ms": ttfb_metric.get("p99_ms"),
        "catchup_completion_percent": ratio(catchup_ok * 100, catchup_attempts),
        "append_completion_percent": ratio(append_ok * 100, append_attempts),
        "errors": errors,
        "catchup_errors": catchup_errors,
        "append_errors": append_errors,
        "catchup_drops": catchup_drops,
        "append_drops": append_drops,
        "append_per_second": append_ok / seconds,
        "append_p95_ms": append_metric.get("p95_ms"),
        "append_p99_ms": append_metric.get("p99_ms"),
        "chronicle": resource_summary(raw.get("resources", []), "chronicle"),
        "redis": redis_resource,
        "go_allocated_bytes": allocated,
        "allocated_bytes_per_returned_byte": ratio(allocated, catchup_bytes),
        "segment_seals": delta(before, after, "chronicle_segment_seals_total"),
        "segment_seal_seconds_total": delta(
            before, after, "chronicle_segment_seal_seconds_total"
        ),
        "segment_seal_seconds_max": after.get("chronicle_segment_seal_seconds_max", 0),
        "segment_reads": delta(before, after, "chronicle_segment_reads_total"),
        "primary_fallbacks": delta(
            before, after, "chronicle_segment_primary_fallbacks_total"
        ),
        "checksum_failures": delta(
            before, after, "chronicle_segment_checksum_failures_total"
        ),
        "segment_bytes_served": delta(
            before, after, "chronicle_segment_bytes_served_total"
        ),
        "backend_bytes_read": delta(
            before, after, "chronicle_segment_backend_bytes_read_total"
        ),
        "backend_bytes_written": delta(
            before, after, "chronicle_segment_backend_bytes_written_total"
        ),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "cache_hit_percent": ratio(cache_hits * 100, cache_hits + cache_misses),
        "cache_bytes": after.get("chronicle_segment_cache_bytes", 0),
        "cache_evictions": delta(
            before, after, "chronicle_segment_cache_evictions_total"
        ),
        "origin_data_index_pairs": origin_reads,
        "object_requests_estimated": origin_reads * 2,
        "object_bytes": delta(
            before, after, "chronicle_segment_origin_bytes_total"
        ),
        "redis_chunk_reads": delta(
            before, after, "chronicle_segment_redis_reads_total"
        ),
        "redis_chunk_writes": delta(
            before, after, "chronicle_segment_redis_writes_total"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results_root", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    rows = [
        summarize(path)
        for path in sorted(args.results_root.glob("chronicle-*/segment-*/results.json"))
    ]
    payload = {
        "schema": "chronicle-immutable-segments-local-v1",
        "note": (
            "Working-session raw files on one local host. Some issue-6-local "
            "cells were overwritten by later rejected diagnostics, so this is "
            "not an accepted cross-cell comparison. Filesystem object emulator "
            "results and systems with different hardware or durability are not "
            "directly comparable."
        ),
        "results": rows,
    }
    encoded = json.dumps(payload, indent=2, sort_keys=True, default=finite) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    else:
        print(encoded, end="")


if __name__ == "__main__":
    main()
