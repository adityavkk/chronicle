#!/usr/bin/env python3
"""Aggregate per-pod Chronicle SUT samples into the upstream five-column CSV."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


HEADER = ("ts_ms", "rss_bytes", "cpu_ticks", "write_bytes", "pod_ws_bytes")


def read_samples(path: Path) -> dict[int, tuple[int, int, int, int]]:
    buckets: dict[int, tuple[int, int, int, int]] = {}
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != HEADER:
            raise ValueError(f"{path}: unexpected samples header")
        for row in reader:
            ts_ms = int(row["ts_ms"])
            bucket = ts_ms // 1000
            buckets[bucket] = (
                int(row["rss_bytes"]),
                int(row["cpu_ticks"]),
                int(row["write_bytes"]),
                int(row["pod_ws_bytes"]),
            )
    if not buckets:
        raise ValueError(f"{path}: no sample rows")
    return buckets


def aggregate(input_dir: Path, output: Path) -> int:
    paths = sorted(input_dir.glob("*.csv"))
    if not paths:
        raise ValueError(f"{input_dir}: no per-pod sample files")
    samples = [read_samples(path) for path in paths]
    common = set(samples[0])
    for rows in samples[1:]:
        common.intersection_update(rows)
    if not common:
        raise ValueError("per-pod samples have no common one-second bucket")

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(HEADER)
        for bucket in sorted(common):
            rows = [sample[bucket] for sample in samples]
            writer.writerow(
                (
                    bucket * 1000,
                    sum(row[0] for row in rows),
                    sum(row[1] for row in rows),
                    max(row[2] for row in rows),
                    sum(row[3] for row in rows),
                )
            )
    return len(common)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    aggregate(args.input_dir, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
