#!/usr/bin/env python3
"""compare.py — cross-SUT comparison tables from dsload results.

Usage: scripts/compare.py <results-root> [sut ...]

Reads <root>/<sut>/<scenario>/results.json for every SUT given (default:
all directories under the root) and prints, per scenario, a markdown
table of the golden signals side by side.
"""

import json
import sys
from pathlib import Path

SCENARIOS = [
    "append-light",
    "append-steady",
    "append-sweep",
    "token-sessions",
    "producer-sessions",
    "sessions-light",
    "fanout",
    "catchup",
    "mixed",
]

METRICS = [
    ("append", "Append"),
    ("delivery_sse", "Delivery SSE"),
    ("delivery_long_poll", "Delivery LP"),
    ("catchup_ttfb", "Catch-up TTFB"),
    ("catchup_total", "Catch-up read"),
]


def load(root: Path, suts):
    out = {}
    for sut in suts:
        for sc in SCENARIOS:
            p = root / sut / sc / "results.json"
            if p.exists():
                out[(sut, sc)] = json.loads(p.read_text())
    return out


def fmt(v, nd=2):
    return f"{v:.{nd}f}" if isinstance(v, (int, float)) else "—"


def errors(counters):
    return sum(v for k, v in counters.items() if k.startswith("err:"))


def cpu_stats(res, name):
    samples = sorted(
        ((s["sec"], s["cpu_seconds"]) for s in res.get("resources", []) if s["name"] == name)
    )
    if len(samples) < 2:
        return None
    deltas = []
    for (s0, c0), (s1, c1) in zip(samples, samples[1:]):
        if s1 > s0 and c1 >= c0:
            deltas.append((c1 - c0) / (s1 - s0) * 100)
    return (sum(deltas) / len(deltas), max(deltas)) if deltas else None


def rss_max(res, name):
    vals = [s["rss_bytes"] for s in res.get("resources", []) if s["name"] == name]
    return max(vals) / (1 << 20) if vals else None


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "results")
    suts = sys.argv[2:] or sorted(d.name for d in root.iterdir() if d.is_dir())
    data = load(root, suts)

    for sc in SCENARIOS:
        rows = [(sut, data[(sut, sc)]) for sut in suts if (sut, sc) in data]
        if not rows:
            continue
        print(f"\n### {sc}\n")
        # throughput + errors
        print("| SUT | window s | appends/s | msgs del'd/s | catchup MiB/s | errors | drops |")
        print("|---|---:|---:|---:|---:|---:|---:|")
        for sut, r in rows:
            c = r["counters"]
            t0, t1 = r["measure_start"], r["measure_end"]
            import datetime as dt

            secs = (
                dt.datetime.fromisoformat(t1) - dt.datetime.fromisoformat(t0)
            ).total_seconds()
            delivered = c.get("msgs_sse", 0) + c.get("msgs_long_poll", 0)
            print(
                f"| {sut} | {secs:.0f} | {c.get('appends_ok',0)/secs:.0f} "
                f"| {delivered/secs:.0f} | {c.get('catchup_bytes',0)/(1<<20)/secs:.1f} "
                f"| {errors(c)} | {c.get('appends_dropped',0)+c.get('catchup_dropped',0)} |"
            )
        # latency per metric
        for key, label in METRICS:
            if not any(key in r["metrics"] for _, r in rows):
                continue
            print(f"\n**{label} latency (ms)**\n")
            print("| SUT | count | p50 | p90 | p99 | p99.9 | max |")
            print("|---|---:|---:|---:|---:|---:|---:|")
            for sut, r in rows:
                q = r["metrics"].get(key)
                if not q:
                    print(f"| {sut} | — | — | — | — | — | — |")
                    continue
                print(
                    f"| {sut} | {q['count']} | {fmt(q['p50_ms'])} | {fmt(q['p90_ms'])} "
                    f"| {fmt(q['p99_ms'])} | {fmt(q['p999_ms'])} | {fmt(q['max_ms'])} |"
                )
        # resources
        print("\n**Resources**\n")
        print("| SUT | server CPU mean/max | server RSS max | redis CPU mean/max | redis mem max | loadgen CPU mean |")
        print("|---|---:|---:|---:|---:|---:|")
        for sut, r in rows:
            server = "caddy" if sut.startswith("caddy") else "chronicle"
            sc_cpu = cpu_stats(r, server)
            lg_cpu = cpu_stats(r, "loadgen")
            rd_cpu = cpu_stats(r, "redis")
            srss = rss_max(r, server)
            rmem = rss_max(r, "redis")
            cell = lambda s: s if s else "—"
            print(
                "| {} | {} | {} | {} | {} | {} |".format(
                    sut,
                    cell(f"{sc_cpu[0]:.0f}%/{sc_cpu[1]:.0f}%" if sc_cpu else None),
                    cell(f"{srss:.0f} MiB" if srss else None),
                    cell(f"{rd_cpu[0]:.0f}%/{rd_cpu[1]:.0f}%" if rd_cpu else None),
                    cell(f"{rmem:.0f} MiB" if rmem else None),
                    cell(f"{lg_cpu[0]:.0f}%" if lg_cpu else None),
                )
            )


if __name__ == "__main__":
    main()
