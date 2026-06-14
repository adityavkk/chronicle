# dsload — a load generator for Durable Streams servers

`dsload` drives realistic, declaratively-configured load against any
[Durable Streams](https://github.com/durable-streams/durable-streams)
protocol server and produces benchmark-grade results: HDR-histogram
latencies, per-second throughput series, error accounting, and SUT
resource usage — as JSON plus a rendered markdown summary.

It exists to benchmark [chronicle](../) (Redis 8 backend) against the
reference Caddy plugin, but it speaks only the wire protocol and works
against any conformant server.

## Quick start

```bash
go build -o bin/dsload ./cmd/dsload

# against any running server
bin/dsload run -scenario scenarios/smoke.yaml -label my-server \
  -base-url http://localhost:4437

# validate a scenario without running it
bin/dsload validate -scenario scenarios/token-sessions.yaml
```

Results land in `results/<label>/<scenario>/{results.json,summary.md}`;
the summary is also printed to stdout.

## Scenarios

A scenario is one YAML file (see [scenarios/](scenarios/)):

```yaml
name: token-sessions
duration: 60s          # measured window
warmup: 5s             # workload runs, nothing recorded

streams:
  count: 50            # stream population
  prefix: bench/sess   # streams: bench/sess-0000 …
  content_type: application/json   # default
  prefill:             # optional: seed messages before measurement
    messages: 5000
    message_bytes: 256

writers:               # the append workload
  per_stream: 1
  rate: 30/s           # per writer; "a/s..b/s" ramps linearly
  message_bytes: 120
  batch: 1             # messages per POST
  producer: none       # or "idempotent" (Producer-Id/Epoch/Seq)

tailers:               # the live-read population
  sse_per_stream: 2
  long_poll_per_stream: 0
  from: now            # or "start" (-1)
  connect_ramp: 3s     # stagger connection arrival

catchup:               # cold full-stream reads (offset=-1)
  rate: 10/s

limits:                # overload degrades into recorded drops, not pile-up
  max_in_flight_appends: 1024
  max_in_flight_catchup: 256
  request_timeout: 10s
```

The bundled scenarios model the protocol's documented use cases:

| Scenario | Models | Key signal |
| --- | --- | --- |
| `token-sessions` | 50 AI sessions @ 30 tok/s, 2 SSE followers each | delivery latency |
| `producer-sessions` | same, with idempotent producers | producer-path cost |
| `fanout` | 1 stream → 400 SSE + 100 long-poll followers | wakeup/fan-out latency |
| `append-sweep` | aggregate writes ramping 200→2,000/s | write-path knee |
| `append-steady` | flat 1,000 writes/s | append latency under load |
| `catchup` | full-stream replays of ~1.25 MiB streams @ 40/s | TTFB + read MB/s |
| `mixed` | 100 sessions: writes + SSE + long-poll + refreshes | everything at once |
| `smoke` | 10-second everything-works check | — |

## Methodology

**Open-loop appends.** Writers send on a fixed schedule computed from
the configured rate (`pace` package, closed-form), never waiting for
the previous response. Append latency is measured from the *scheduled*
send time, so client-side queueing under SUT stalls is included — the
standard defense against coordinated omission. If in-flight appends
exceed `limits.max_in_flight_appends`, sends are *dropped and counted*
rather than queued: sustained drops mean the SUT (or the client) cannot
sustain the offered rate at that concurrency.

**Write-to-receipt delivery latency.** Every message embeds the
writer's send timestamp. Each SSE/long-poll tailer records
`receipt − sent` per message — each message is judged against its own
clock, so a stalled tailer accrues the whole backlog delay and fast
percentiles can't mask slow consumers. (Same method as ElectricSQL's
fan-out benchmarks; generator and SUT share a host clock here.)

**Tailers are protocol-faithful clients.** SSE tailers follow control
events, reconnect with the last `streamNextOffset` when the server
cycles connections, echo cursors, and stop on `streamClosed`.
Long-pollers loop `200 → next offset / 204 → retry` with cursors.

**HDR histograms, merged.** Each worker records into its own
HdrHistogram (1 µs–120 s, 3 s.f.); merging is lossless, percentiles up
to p99.9 are reported, and full percentile curves are archived in
`results.json` (`hdr_curves`, hdrhistogram.github.io-compatible).

**Warmup gating.** The workload runs through `warmup` with recording
disabled, then a measurement window of `duration` opens. Writers stop
at window close; tailers get a 2 s drain grace so the tail of
deliveries is observed rather than truncated.

**Resource sampling.** With `-sample-pid name=pid` (any process) and
`-sample-redis name=host:port` (Redis INFO over TCP), dsload samples
RSS and cumulative CPU once per second — including itself, so "was the
generator the bottleneck?" is answerable from the results file. CPU% is
derived from cumulative deltas, not instantaneous readings.

**Why not vegeta/fortio/k6?** Their unit of work is
request→complete-response, which cannot express per-event latency on a
connection that never ends (SSE) or a population of long-pollers. The
genuinely reusable pieces — open-loop pacing and HDR recording — are
small and are implemented here directly (`pace`, `stats`).
Prometheus-style fixed-bucket histograms are also deliberately avoided
for the *authoritative* numbers; JSON + HDR curves are the source of
truth.

## Design

Pure core, imperative shell:

```
scenario/   YAML schema, defaults, validation        (pure)
pace/       open-loop arrival schedules, closed form (pure)
payload/    message build/parse, timestamp embedding (pure)
ssewire/    incremental SSE event parser             (pure)
stats/      HDR recording, merging, summaries        (pure aggregation)
report/     Result → markdown                        (pure)
dsclient/   thin wire client: no hidden retries      (shell)
run/        orchestration: workers, sampler, output  (shell)
cmd/dsload/ CLI                                      (shell)
```

The pure packages have no I/O and no clocks (timestamps are inputs) and
carry the unit tests; the shell stays thin enough to read.
