# Async subscription fan-out results

These measurements compare the requested pinned baseline,
`4c78ea9daef78278a73f1637dd41e3fbfca0d2cf`, with this change on the same
Apple M4 Pro and local repository Redis. The benchmark source is identical on
both revisions. Each row runs ten serial iterations.

Every iteration performs a real durable source-stream append. It then waits for
the complete old synchronous path or explicitly drains the new asynchronous
worker. It verifies that all S subscriptions are in the expected new wake
generation, acknowledges them to the appended offset, and only then starts the
next iteration. `wakes/op` was exactly S in every row. No owed wake was missing.

P is the number of linked streams per subscription. One link receives the
append. The other links force the multi-link tail-evaluation path.

## Append response path

`append` starts before the durable store append and ends when the HTTP path can
write its response. `hook` starts when the store append returns. Throughput is
the inverse of the sampled append p50 for this serial benchmark.

| S | P | baseline append p50 / p99 | async append p50 / p99 | async hook p50 / p99 | baseline / async requests per second |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 4.881 / 5.177 ms | 1.034 / 1.126 ms | 0.542 / 0.917 µs | 204.9 / 967.2 |
| 1 | 4 | 6.543 / 6.670 ms | 1.223 / 1.282 ms | 1.083 / 1.500 µs | 152.8 / 818.0 |
| 4 | 1 | 16.251 / 17.529 ms | 1.168 / 1.309 ms | 1.125 / 1.458 µs | 61.53 / 856.2 |
| 4 | 4 | 20.185 / 20.753 ms | 1.144 / 1.283 ms | 1.125 / 1.791 µs | 49.54 / 874.2 |
| 64 | 1 | 214.106 / 291.935 ms | 1.024 / 1.140 ms | 1.250 / 1.583 µs | 4.671 / 976.4 |
| 64 | 4 | 306.932 / 363.863 ms | 1.046 / 1.305 ms | 1.458 / 1.708 µs | 3.258 / 956.0 |
| 256 | 1 | 840.134 / 895.868 ms | 1.035 / 1.254 ms | 1.125 / 1.792 µs | 1.190 / 966.4 |
| 256 | 4 | 1217.907 / 1426.549 ms | 1.053 / 1.301 ms | 1.250 / 1.750 µs | 0.8211 / 949.5 |
| 1000 | 1 | 3233.716 / 3356.583 ms | 1.060 / 1.221 ms | 1.666 / 2.166 µs | 0.3092 / 943.2 |
| 1000 | 4 | 4870.577 / 5053.845 ms | 1.030 / 1.235 ms | 1.625 / 1.834 µs | 0.2053 / 971.0 |

The new hook remains independent of subscriber and link cardinality. The roughly
one millisecond async append result is dominated by the durable Redis append,
not the subscription handoff.

## Complete fan-out work

Completion begins when the durable append returns and ends after every
subscription has been evaluated and every pull-wake event has been persisted
and stamped. Redis command counts are server-wide command-stat deltas, so the
old and new matrices ran serially. Memory is Go benchmark allocated bytes per
complete operation.

| S | P | baseline completion p50 / p99 | async completion p50 / p99 | baseline / async Redis commands | baseline / async allocated bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 3.900 / 4.076 ms | 4.087 / 4.514 ms | 40 / 40 | 101326 / 102875 |
| 1 | 4 | 5.355 / 5.712 ms | 4.756 / 4.992 ms | 43 / 43 | 123510 / 124987 |
| 4 | 1 | 15.095 / 16.070 ms | 11.590 / 12.891 ms | 121 / 118 | 273085 / 254111 |
| 4 | 4 | 19.153 / 19.773 ms | 11.475 / 11.967 ms | 133 / 121 | 361938 / 278031 |
| 64 | 1 | 212.970 / 290.802 ms | 133.700 / 140.708 ms | 1737 / 1674 | 3704740 / 3285172 |
| 64 | 4 | 305.900 / 362.880 ms | 142.528 / 201.020 ms | 1929 / 1677 | 5127729 / 3337177 |
| 256 | 1 | 839.243 / 894.855 ms | 560.992 / 614.059 ms | 6833 / 6578 | 14670456 / 12973093 |
| 256 | 4 | 1216.898 / 1425.528 ms | 583.335 / 634.219 ms | 7602 / 6581 | 20362994 / 13120523 |
| 1000 | 1 | 3232.675 / 3355.545 ms | 2189.865 / 2242.725 ms | 26261 / 25261 | 57047571 / 50396676 |
| 1000 | 4 | 4869.509 / 5052.848 ms | 2269.114 / 2310.377 ms | 29261 / 25264 | 79278732 / 50927785 |

The command reduction is the linked-tail batching effect. One-link rows remove
S minus one repeated tail commands. Four-link rows also avoid rereading the
three shared linked tails for every subscriber. Subscription hydration is sent
as one pipeline, so fewer network round trips also reduce completion latency
even though Redis still accounts for each pipelined command.

The benchmark deliberately keeps one stream dirty at a time. Queue depth is
therefore at most one, queue capacity is 1024, and overflow, enqueue coalescing,
and duplicate fan-out work are all zero. Dirty recovery delay is the async
completion interval in the table. Dedicated saturation and duplicate tests
exercise nonzero queue, overflow, coalescing, and recovery paths.

## CPU and resident memory diagnostic

A separate one-iteration S=1000, P=1 run used `/usr/bin/time -l`. These are
whole-command diagnostics, including benchmark setup and acknowledgement, so
they are not used for a speed claim.

| revision | real | user CPU | system CPU | maximum RSS | benchmark allocated bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| pinned baseline | 7.62 s | 0.60 s | 1.27 s | 242565120 | 57057952 |
| async fan-out | 5.81 s | 0.62 s | 1.18 s | 237535232 | 50410592 |

## Exact commands

```bash
env GOCACHE=/private/tmp/chronicle-go-cache BENCH_REDIS_URL=redis://localhost:6379/13 \
  go test -run '^$' -bench '^BenchmarkSubscriptionFanout$' -benchtime=10x -count=1 -benchmem

env GOCACHE=/private/tmp/chronicle-go-cache BENCH_REDIS_URL=redis://localhost:6379/12 \
  go test -run '^$' -bench '^BenchmarkSubscriptionFanout$' -benchtime=10x -count=1 -benchmem

env GOCACHE=/private/tmp/chronicle-go-cache BENCH_REDIS_URL=redis://localhost:6379/13 \
  /usr/bin/time -l go test -run '^$' -bench '^BenchmarkSubscriptionFanout/S1000_P1$' \
  -benchtime=1x -count=1 -benchmem

env GOCACHE=/private/tmp/chronicle-go-cache BENCH_REDIS_URL=redis://localhost:6379/12 \
  /usr/bin/time -l go test -run '^$' -bench '^BenchmarkSubscriptionFanout/S1000_P1$' \
  -benchtime=1x -count=1 -benchmem
```

## Downside

Fan-out work is no longer part of append latency, but it still consumes Redis,
CPU, and memory after the response. A sustained arrival rate above worker
capacity increases dirty age and can force a full recovery sweep. The bounded
queue and recovery metrics make that pressure visible; they do not remove it.

## Acceptance validation

All commands below ran from the issue worktree against the final source. No
global `REDIS_URL` was present for the full Go test, so the webhook and stream
packages retained their separate repository database defaults.

| Gate | Command | Result |
| --- | --- | --- |
| Pure and short tests | `make test-unit` | PASS, race detector enabled |
| All Go packages | `env -u REDIS_URL go test ./...` | PASS |
| Redis integration | `make test` | PASS, race detector enabled |
| Static analysis | `make lint` | PASS, 0 issues |
| Spec provenance | `make spec-check` | PASS, suite 0.3.5 and pinned upstream unchanged |
| Protocol conformance | `make conformance` | PASS, exactly 332 of 332 |
| Async queue and lifecycle soak | `go test -race -count=20 ./webhook -run '^(TestDirty|TestOnStreamAppend|TestManagerDirty|TestConcurrentDirty|TestDeletedStreamHint|TestManagerStop|TestStopRacingStart|TestLostDirtyHint|TestFailpointDirty)'` | PASS |
| Redis reconnect soak | `go test -race -count=10 ./webhook -run '^TestRedisDisconnectReconnectRecoversLostDirtyHint$'` | PASS |
| HTTP boundary soak | `go test -race -count=30 . -run '^(TestHTTPAppendReturnsWhileSubscriptionTailReadIsBlocked|TestAppendDuplicateDoesNotWake|TestCreateWithInitialDataNotifiesAppendOnce|TestAppendWithClose)$'` | PASS |

The documented Jepsen lifecycle was run exactly with `jepsen/up.sh`,
`jepsen/run.sh`, and `jepsen/down.sh`. Baseline, origin restart, Redis restart,
paged catch-up, and SSE resume all passed. The first SSE attempt stopped before
fault injection because the local Kubernetes API proxy returned HTTP 502 while
scraping a pod metric. A fresh single-scenario run passed and injected eleven
verified faults, including a lost and duplicated Pub/Sub hint, one origin kill,
all-origin death, Redis death, and Pub/Sub client death. Every one of eight SSE
clients observed its 40 messages exactly once and resumed to the closed tail.

The final `redis-restart` slice reported zero nemesis actions because its 320
append workload finished before the scheduled kill. That slice therefore proves
only the no-fault final-state check for that run. Redis replacement was positively
verified in the final paged catch-up and SSE runs. Paged catch-up matched all 136
Redis-oracle frames after both Redis and all Chronicle origins were replaced.

The relevant opt-in hardening scenarios also passed:

- `pull-wake-arm-crash`: 320 appends, all origins killed after the final append,
  and 8 of 8 subscription cursors recovered to tail.
- `expired-lease-takeover`: generation advanced from 1 to 2 and the deposed
  worker's late acknowledgement returned `409 FENCED`.
- `glob-create-crash`: 320 appends and 13 origin-kill actions, including eight
  all-origin kills, with 8 of 8 streams at tail.
- `ownership-exclusivity`: 169 live Redis operations across four ownership
  slots were linearizable under concurrent claims and GC pauses.

The ownership checker now records a failed claim as an indeterminate write.
This follows the existing data-plane checker rule: a Redis write whose reply is
lost may have committed. The pure Porcupine model admits both states and later
observations constrain the result. Dropping that call had produced a false
counterexample when its retry correctly returned `RENEWED`.

The durability scenarios prove final owed work is recovered from durable
cursors and tails across the injected process and Redis faults. The SSE
scenario additionally proves exact durable message coverage and resume for its
connections. The ownership scenario proves the slot claim and epoch register is
linearizable. These runs do not prove exactly-once webhook delivery, a
cross-slot transaction, or every possible failure schedule. The generation,
wake ID, acknowledgement, and owner fences plus the maintained recovery sweep
remain the correctness argument outside the sampled histories.
