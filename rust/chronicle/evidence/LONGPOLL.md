# Strict long polling: local k3d evidence

Normal and fault-feature release images were built from local source revision
`72101bf`. `longpoll-fault-images.json` and `longpoll-normal-images.json` retain
the actual running image digests, readiness and fault environment. The normal
image was restored after the tests; the temporary direct-leader forward was stopped.
All replicas share one physical host: this is not an AZ or power-loss experiment.

## Contract and bounded checks

GET with `live=long-poll` requires an offset and strict consistency. `offset=now`
and future numeric offsets resolve against the initial captured tail. The request
keeps that offset and incarnation across waits. Notifications only suggest another
observation; successful replies require a strict barrier and captured frontier.
The five-second deadline ends waiting, not the obligation to perform that barrier.
Recreation fails the existing request rather than binding it to the replacement.
SSE and stale live reads return 400; this is not full protocol conformance.

`LiveRead.tla` preceded implementation: 678 reachable states, with retained negative
mutations catching skipped timeout bytes and incarnation rebinding. This is bounded
model checking, not mechanized Rust refinement. `make check` passed Rust formatting,
Clippy, tests, storage-fault/VFS checks and rustdoc. Python discovery subsequently
passed all 12 tests. Independent review found cancellation accounting and imprecise
deadline evidence; both were fixed and the follow-up review closed those findings.
An in-flight detached fault task retains both admission guards after cancellation.

## Real HTTP observations

`longpoll-http-history.jsonl` and its result cover empty timeout, closed EOF,
stored-byte JSON offsets, cursor validation, expiration and concurrent arrival.
With all 32 live slots held, the next live request received 429, while an append
succeeded and woke all 32 waiters. Live admission is separate from the 128 total
request slots. This checks bounded backpressure, not a throughput SLO.

`tests/long_poll_gate.py` owns fault timing. Its named server gate is reached only
in the deadline branch, before the subsequent strict observation. Each scenario
has a retained `longpoll-gate-*-history.jsonl` and successful result:

| Change while gated | Observed pending response |
|---|---|
| Append `after` | 200, exact body `after`, next stored-byte offset 5 |
| Delete/recreate with `NEW` | 409; later strict read returns `NEW` |
| Close empty stream | 204 with EOF |
| Isolate leader from quorum | 503, `not enough for a quorum`, after 7.351 seconds; strict read recovers after heal |

The first quorum attempt failed **before injection** because the absent-chain
`iptables -S CHRONICLE_FAULT` probe returned an incompatible-chain error. Its
history is preserved as `longpoll-gate-quorum-history.jsonl`; the cleanup diagnosis
records completed 204, resumed marker, absent arm and all 32 live slots free.
The harness now requires a successful, stderr-free full filter listing without
the owned chain before injecting. `quorum2` is the successful fault run. All
recovery attempts remain in the history; the harness retries only the read.

These schema-3 observations are HTTP contract checks, **not general linearizability
evidence** and not input to the schema-1/2 Porcupine adapter. Separately, after
restoring the normal image, `longpoll-restored-history.jsonl` failed because the
port-forward still referenced the replaced pod sandbox. It had no successful read
and establishes no retention result. After the supervised forward restarted and
a status request succeeded, `longpoll-restored2-history.jsonl` passed the prefix
smoke checker and the existing Go Porcupine adapter; see its retained results.

## Observed telemetry and reproduction

`longpoll-victoria-logs.jsonl`, `longpoll-victoria-trace.json` and
`longpoll-victoria-metrics.json` retain the forced-sampled timeout observation:
two spans and correlated completion events, leader `wait_us=5001113`, forwarder
`forward_us=5003524`, and four live-slot metric series. The isolated leader's 503
appears in both retained pod stdout and VictoriaLogs. Its error body completed
(222 yielded bytes), while its request outcome remains unknown. Delivery completion
does not imply a successful operation or proven client receipt. Export error counters
were zero in this observation; it is not a telemetry-outage test.

Run `python3 tests/long_poll.py --url "$LOCAL_FORWARD" --path "$UNUSED_PREFIX"
--output "$NEW_HISTORY"` against a normal local cluster. For deadline gates, build
with `--features storage-faults`, explicitly enable the fault environment in the
disposable cluster, and use `tests/long_poll_gate.py --url "$DIRECT_LEADER_FORWARD"
--path "$UNUSED_PREFIX" --output "$NEW_HISTORY" --fault-root "$FAULT_ROOT"
--scenario append` (or `recreate`, `close`, `quorum`). The harness checks direct
leadership and refuses existing gate controls. Restore the normal image afterwards;
never enable these unauthenticated test controls in a shared deployment.
