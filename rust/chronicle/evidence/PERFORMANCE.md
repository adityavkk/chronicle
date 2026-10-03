# Representative release-mode comparison, not a capacity claim

`benchmark-64020/` retains a closed-loop run on the real local k3d cluster with
the release binary from source `60d0bae` (`chronicle-raft:producer-reply`). Both
cases use durable-majority writes, three voters per group, SQLite WAL/FULL,
strict reads, the same private NodePort ingress, eight concurrent producers,
four readers and 96-byte records. Each producer submits 256 sequential appends.
The second case distributes two producers and one reader to each of four streams,
one per virtual shard. The first puts all clients on one stream in shard 1.

| Case | Acknowledged appends | Append window | Ack/s | Logical p50 | Logical p99 |
|---|---:|---:|---:|---:|---:|
| One hot stream | 2,048 | 13.106 s | 156.27 | 47.78 ms | 171.63 ms |
| Four streams / four shards | 2,048 | 7.825 s | 261.72 | 17.87 ms | 73.67 ms |

Neither case had a failed or unknown append attempt. The hot-stream history and
all four spread histories independently received Porcupine **Ok**; smoke checks
also found all acknowledged records in final strict reads. These are safety
observations for these histories, not a durability proof or fault experiment.

`history_stats.py` derives the window from first append invocation through last
append completion using the driver's monotonic clock, excluding setup and offline
checking. Logical latency includes waiting and retries until first success; the
report separately includes all attempt latencies, including errors. Quantiles
use nearest rank. Unit tests distinguish error latency, retry waiting, reused IDs
across independent histories, and empty successful samples. Histories are losslessly
compressed after checking; recomputing statistics from gzip matched the raw files.

## Resource observations are coarse, not per-case peaks

Kubernetes metrics retained before/after the runs report approximately 1.80 CPU
cores and 234 MiB memory across all five Chronicle containers during the sampled
load window. Individual container CPU samples span different 11.6–17.5 s windows.
The after-hot and after-many captures contain identical timestamps and values:
metrics-server had not refreshed. They therefore **cannot compare case resource
usage or establish peak CPU/RSS**. They also exclude clients, Kubernetes, Docker
and the Victoria stack. The raw timestamped samples remain, rather than assigning
the same stale sample to two supposedly independent measurements.

## Limits and reproduction

This is one short hot-then-spread run, not repeated/randomized steady state.
The cluster already contained earlier conformance/fault-test data; streams were
fresh, but storage was not reset. The client uses Python urllib and a new HTTP
connection per operation; client overhead, full-prefix reads, forwarding, telemetry
and shared-host scheduling are included. All five pods and local-path PVCs live
on one orb host, not independent disks/AZs. These numbers are neither a server
capacity estimate nor an equal-semantics comparison against Electric or Go.
They establish no SLO and do not validate resource-informed balancing. One stream
remains leader ordered; four-shard results do not remove that ordering bottleneck.

From `rust/chronicle`, with a fresh numeric seed and new output directory:

```sh
GO=/path/to/pinned/go bash ops/benchmark.sh "$PRIVATE_CHRONICLE_URL" evidence/benchmark-NEW NEW_SEED
python3 tests/history_stats.py evidence/benchmark-NEW/hot.jsonl.gz
python3 tests/history_stats.py evidence/benchmark-NEW/many-[1-4].jsonl.gz
```

The script reuses `tests/history.py`, runs the existing Go Porcupine adapter,
retains every outcome, and fails if any workload/check fails. It adds no alternate
safety checker. Kubernetes resource queries use the guarded local k3d context.
To independently recheck an archive, decompress it into a temporary file and
pass that file to `go run ../../jepsen/checker -rust-history FILE`.
