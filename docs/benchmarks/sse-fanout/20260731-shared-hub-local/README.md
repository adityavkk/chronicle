# Shared-hub SSE fanout evidence

This archive supports the local validation of the shared-hub fanout path. It
contains the small set of raw results needed to check the notification
topology, durable read amplification, delivery rate, latency, frame encoding,
and active fault memory claims.

## Provenance

| Item | Value |
| --- | --- |
| Final implementation commit | `0171e01dc99f13da70264b092f33c9ab5c090144` |
| Rebased integration base | `2f703d4a3dc034e646f2bd1cf06b21dc904a7939` |
| Delivery-workload source commit | `7640f9878214215a9a8efc20f87d3018b1aa0289` |
| Original branch base | `b2a5d04bfcc0ce5274915b2a147c2fee8a6c6035` |
| Delivery-workload Chronicle image | Recorded digest prefix `sha256:d4cd1389`; task image removed after capture |
| Delivery-workload Chronicle binary | Recorded SHA-256 prefix `c5e13e`; task binary removed after capture |
| ds-bench source | `93a1a066a511ad2ce5114dc429afb1fd0f6d99bf` with adapter digest `1440fc1d0496` |
| ds-bench image | `sha256:c4e0f455462d168354c8452c424667b8e0a1bb4a7477609eb4c1f005aafcf719` |
| Redis image | `sha256:e8eb6f2980c06c6a25c08f62cb2e00dc7d2fead9aa492cfdd8b54a42109ae0f2` |
| Host | Apple M4 Pro, 14 logical CPUs, Darwin arm64, Go 1.26.4 |
| Cluster | Local Kind on the task-owned 14 CPU, 24 GiB Colima profile |
| SUT split | Chronicle 2 CPU and 4 GiB; Redis 2 CPU and 12 GiB; one replica and one Redis primary |

The Kubernetes delivery workloads used the named delivery-workload source and
image. After integration with ADR-0007, the final implementation was rebased
and its exact Redis read counts, frame path, focused race tests, complete test
suite, conformance suite, lint, specification checks, and Jepsen suite were run
again. Documentation and evidence files were still uncommitted when the image
was created. No paid cloud resources were used.

## Mandatory delivery workloads

Each result directory was moved away before the next run. The three repetitions
therefore contain fresh client JSON and fresh HDR state. The gate uses the
component median across all three repetitions. A lower latency is not treated as
a regression.

| Workload | Per-client baseline | Shared-hub median | Change | Gate |
| --- | ---: | ---: | ---: | --- |
| 1 stream, 1,000 clients, delivery rate | 49,616/s | 49,099.15/s | -1.04% | pass, within 5% |
| 1 stream, 1,000 clients, p99 | 19.519 ms | 8.591 ms | -55.99% | pass |
| 1 stream, completion | 99.232% | 98.198% | -1.034 points | pass, at least 98% |
| 100 streams, 2,048 clients, delivery rate | 78,204.47/s | 99,701.80/s | +27.49% | pass |
| 100 streams, 2,048 clients, p99 | 71.167 ms | 51.903 ms | -27.07% | pass |
| 100 streams, completion | 76.371% | 97.365% | +20.994 points | reported |

Every repetition recorded zero backpressure and zero other client errors. The
one-stream median delivered 981,983 of 1,000,000 offered events. The 100-stream
median delivered 1,495,527 of 1,536,000 offered events.

The raw client records are under `client/blog/` and `client/fleet/`. Their RSS
samples show median peak Chronicle RSS of 134.25 MiB for the one-stream workload
and 396.89 MiB for the 100-stream workload.

## Notification and durable read topology

The exact amplification benchmark opened 1,000 clients on one stream, waited
for catch-up to settle, and then measured 100 appends:

| Counter | Before | After | Delta | Per publish |
| --- | ---: | ---: | ---: | ---: |
| Redis `PUBLISH` | 3,480 | 3,580 | 100 | 1.000 |
| Redis `ZRANGEBYLEX` | 5,602 | 5,702 | 100 | 1.000 |
| Instrumented durable `ReadPage` calls | n/a | n/a | 100 | 1.000 |

This passes the maximum of 1.2 durable pages per publish. The legacy
`Store.Read` counter remained zero. The standalone integration tests separately
assert one logical registration and one physical Pub/Sub connection for one
active stream, then 100 logical registrations over one physical connection for
100 active streams. The 100-stream Redis sampler also recorded 100 subscribed
channels with one `pubsub_client`.

## Frame path decision

The real HTTP benchmark ran 50,000 client updates five times per candidate.
Median results were:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Previous map and two writes | 14,228 | 905 | 15 |
| Typed reusable buffer and two writes | 13,550 | 216 | 5 |
| Typed reusable combined write | 13,519 | 216 | 5 |

Production uses the typed reusable two-write path. It reduced retained
allocation bytes per delivered update by 76.1 percent and improved time by 4.8
percent. This passes the issue rule because allocated bytes improved by more
than 10 percent and CPU time did not regress.

## Redis key profile correction

The first clean three-run 100-stream median narrowly missed its p99 gate at
75.647 ms. Its CPU profile attributed 3.39 percent of samples to repeated path
escaping, including 1.17 CPU-seconds rebuilding `strings.Replacer`. Reusing one
immutable replacer and escaping once per script key set reduced that focused
profile to 0.52 percent. `strings.Replacer.build` disappeared from the profile.
The final median then reached 99,701.80 deliveries/s at 51.903 ms p99.

The raw before and after CPU profiles and their focused text reports are under
`profiles/`. The key bytes and Redis hash slot did not change.

## Thirty-minute active fault run

The measured window ran from 12:21:14 through 12:51:14 EDT. Twenty writers
appended one 256 byte message per second to 20 streams. Forty regular SSE
clients reconnected continuously. A separate throttled client repeatedly
blocked and reconnected against its own active stream.

| Signal | Result |
| --- | ---: |
| Successful appends | 36,000 |
| SSE deliveries | 72,040 |
| Protocol reconnects | 69,879 |
| Load generator errors | 0 |
| Chronicle RSS, mean | 48.2 MiB |
| Chronicle RSS, peak | 56.5 MiB |
| Heap allocated, sampled peak | 20.1 MiB |
| Heap in use, sampled peak | 27.7 MiB |
| Goroutines, sampled peak | 157 |
| File descriptors, sampled peak | 143 |
| Logical registrations, steady | 20 to 21 |
| Physical notification connections, active | 1 |
| Ring raw bytes, peak | 0 |
| Ring wire bytes, peak | 2,888,556 |
| Ring index bytes, peak | 250,272 |
| Ring total bytes, peak | 3,138,828 |
| Lag disconnects | 0 |
| Write timeouts at measured-window end | 244 |

The final five-minute samples kept file descriptors between 141 and 142 outside
one transient sample, kept the physical connection at one, and repeatedly
drained ring retention to zero. Heap allocation ended below its value at the
start of that window. Goroutines returned to 134 to 137 between reconnect
waves. The write-timeout total is a cumulative event counter. It increased only
because the fault driver kept reconnecting the throttled client.

After all clients left, the exported client, hub, logical registration,
physical connection, raw, wire, index, and total ring gauges were exactly zero.
The cleanup goroutine profile contained eight goroutines.

The independent 10-second sampler lost its middle interval when a temporary
file creation hit the host disk reserve. The load generator's 1,497 Chronicle
RSS samples cover the complete measured window. The independent sampler covers
the first and final five-minute windows and supplies heap, goroutine, descriptor,
topology, and ring component values. Both sample parts and the active and
cleanup metric snapshots are retained under `soak/`.

## Evidence boundary

This archive retains the resolved client results, active RSS and runtime
samples, Redis counters, relevant profiles, configuration, and metric snapshots.
It excludes the 32 MiB general load-generator corpus, stale resumed HDR data,
failed overload experiments, generated caches, local binaries, and unrelated
benchmark results. `evidence-checksums.txt` seals every retained file except the
checksum file itself.

The downside remains per-client socket work. Chronicle shares durable reads and
data framing, but each client still needs a control event, deadline, flush, and
kernel socket buffer.
