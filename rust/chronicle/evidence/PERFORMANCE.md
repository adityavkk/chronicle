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

## Qualified alpha36 candidate, seed 85401

`benchmark-upgrade-85401/` repeats the same client counts, record size, operation
count and CP/strict semantics on release image `chronicle-raft:leadership4`.
The mount-aware path mapping places the spread case on four distinct shards.
This cluster contains the earlier pressure/conformance data and now has a 1536 MiB
per-pod limit. Consequently these figures are **not a controlled version comparison**
against the older table above.

| Case | Acknowledged appends | Ack/s | Logical p50 | Logical p99 |
|---|---:|---:|---:|---:|
| One hot stream | 2,048 | 99.51 | 71.27 ms | 348.45 ms |
| Four streams / four shards | 2,048 | 113.09 | 72.36 ms | 115.12 ms |

All five histories passed Porcupine and retention checks. Neither case had failed
or unknown append/read attempts. Raw timestamped resource snapshots are retained:
after-hot samples total about 1.91 CPU cores and 1,437 MiB across five processes;
after-many samples total about 0.57 cores and 1,521 MiB. Their per-container windows
are heterogeneous (11.9–19.6 seconds), not synchronized with case boundaries or
peak measurements. They cannot establish steady-state utilization, saturation,
or memory safety. Clients, Kubernetes and the Victoria stack are excluded.

Reproduce against the candidate's private NodePort using a fresh output/seed:

```sh
CLUSTER=chronicle-upgrade MOUNTED_TENANT=conformance-mounted GO=/path/to/pinned/go \
  bash ops/benchmark.sh "$PRIVATE_CANDIDATE_URL" /tmp/benchmark-NEW NEW_SEED
```

The default script behavior and baseline-cluster guard remain unchanged. Both
cases are short, ordered hot-then-spread, closed-loop samples, not invented SLOs
or confirmation of speculative 100k/s targets.
