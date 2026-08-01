# shared-hub-sse-soak

Thirty-minute active SSE stability run with 20 streams, reconnect cycling, and a separately throttled client.

- **Target**: `http://127.0.0.1:4439` (stream root `/v1/stream/`)
- **Run**: 2026-07-31 12:21:14 → 12:51:14 (measured window 1800.0s after 30s warmup)
- **Host**: m-H6R03HYGYY, darwin/arm64, 14 CPUs, go1.26.4

## Workload

- Streams: 20 (`shared-hub/soak-*`, application/json)
- Writers: 20 (1/stream) @ 1/s each, 256B messages, batch 1, producer none
- Tailers: 2 SSE + 0 long-poll per stream (40 total), from `now`

## Throughput (measured window)

| Signal | Total | Per second |
|---|---:|---:|
| Appends (requests OK) | 36000 | 20.0/s |
| Messages appended | 36000 | 20.0/s |
| Bytes appended | 8.79 MiB | 0.00 MiB/s |
| Messages delivered (SSE) | 72040 | 40.0/s |
| Bytes delivered (SSE) | 17.59 MiB | 0.01 MiB/s |
| SSE reconnects | 69879 | 38.8/s |

## Latency (ms)

| Metric | Count | Min | Mean | p50 | p90 | p95 | p99 | p99.9 | Max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Append (scheduled→response) | 36000 | 2.91 | 19.06 | 6.15 | 48.67 | 97.47 | 230.66 | 426.50 | 545.28 |
| Delivery, SSE (write→receipt) | 72040 | 4.90 | 22.63 | 8.96 | 56.13 | 108.54 | 232.32 | 438.01 | 552.45 |

## Errors

None observed in the measured window.

## Resources (sampled 1s; CPU% from cumulative deltas)

| Process | RSS mean | RSS max | CPU mean | CPU max |
|---|---:|---:|---:|---:|
| chronicle | 48.2 MiB | 56.5 MiB | 3% | 7% |
| loadgen | 76.6 MiB | 111.5 MiB | 2% | 4% |
| redis | 10.7 MiB | 18.6 MiB | 1% | 8% |
