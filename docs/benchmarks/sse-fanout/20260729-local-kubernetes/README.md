# Per-client SSE fanout baseline

This is the minimal retained evidence subset for the per-client fanout baseline.
The topology was local Kubernetes, not the paid GCP campaign, so its throughput
and latency results are not directly comparable with the sealed cloud baseline.

## Run identity

- ds-bench source commit:
  `93a1a066a511ad2ce5114dc429afb1fd0f6d99bf`
- ds-bench image:
  `sha256:c4e0f455462d168354c8452c424667b8e0a1bb4a7477609eb4c1f005aafcf719`
- Chronicle source build context:
  `54e4892280ab66630bf2398164b1ba7fa3e52d78cd1ed28263883795e0859ff6`
  across 109 files
- Chronicle image:
  `sha256:a2595bf4d5c9ffdb467b437ce0f71c5a1a0907a8a11df0b15a2a4277280f8767`
- Topology: one Chronicle replica with 2 vCPU and 4 GiB, plus one Redis
  primary with 2 vCPU and 12 GiB
- Durability: Redis AOF `appendfsync always`
- Workloads:
  `benchmarks/ds-bench/sse-hub-validation.json`

The original result root contained unrelated campaign artifacts and a stale
top-level provenance file. Neither is retained here. This directory contains
only the two baseline cells, including client results, sampled Redis counters,
RSS samples, runtime metrics, and CPU, allocation, heap, goroutine, block, and
mutex profiles.

## Raw results

The one-stream cell used 1,000 clients for 20 seconds. It delivered 992,320
events at 49,616 events/s, with 7.123 ms p50, 19.519 ms p99, 58.815 ms p999,
and zero client errors. Peak sampled Chronicle RSS was 184,287,232 bytes, or
175.75 MiB.

The 100-stream cell used 2,048 clients for 15 seconds. It delivered 1,173,067
events at 78,204.47 events/s, with 40.159 ms p50, 71.167 ms p99, 104.191 ms
p999, and zero client errors. Peak sampled Chronicle RSS was 529,838,080 bytes,
or 505.29 MiB.

The sampled Redis CSVs do not contain the exact measurement-start snapshots
needed to reproduce previously reported live-window deltas. The auditable
full-cell terminal counters are:

| Cell | First `ZRANGEBYLEX` | Last `ZRANGEBYLEX` | First `PUBLISH` | Last `PUBLISH` | Full-cell ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 stream, 1,000 clients | 6 | 2,326 | 0 | 1,325 | 1.7509 |
| 100 streams, 2,048 clients | 0 | 88,902 | 0 | 86,937 | 1.0226 |

The one-stream full-cell ratio includes setup and one catch-up read per client,
so it is not the steady-live mechanism gate. The exact branch-local guard in
`exact-counter-benchmark.txt` excludes setup and catch-up with explicit
before-and-after Redis counters. It measured 100 `ZRANGEBYLEX` calls for 100
publishes with 1,000 connected clients, or 1.000 reads per publish.

The post-run runtime metrics snapshots show zero active SSE clients and hubs.
They therefore do not establish a slow-client or reconnect-storm RSS ceiling.
Those bounds are enforced structurally by the byte-bounded replay ring,
one-slot watcher wakeups, connection write deadlines, and lifecycle tests, but
a dedicated active-fault memory run remains future validation.
