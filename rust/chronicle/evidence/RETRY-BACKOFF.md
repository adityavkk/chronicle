# Unavailable-peer retry classification

The leader-drain image's retained startup logs exposed a fast DNS failure loop:
about 20,000 DNS-related log lines on one replica in five seconds. Log lines are
not request counts. Inspection of pinned OpenRaft 0.9.25 explains the mechanism:
`Network` can immediately reschedule pending replication; `Unreachable` activates
the default 500 ms per-worker backoff. The adapter had classified every reqwest
failure as `Network`.

Transport, HTTP-status and response-decoding failures now use `Unreachable`.
Decoded remote Raft errors remain `RemoteError`; snapshot mismatch therefore
retains the library's restart-from-zero behavior. No consensus algorithm, custom
timer or mutation retry was added. Unknown remote outcomes remain unknown.
Read-quorum probes and vote RPCs have independent caller timeouts, not this
replication-worker rate limit. Snapshot chunk retries use the pinned library's
separate bounded-attempt policy; its backoff sleep can delay cancellation by
roughly 500 ms.

The two-state TLA policy and negative immediate-retry mutation were checked and
committed before code. They are not a scheduler proof. The stronger code bridge
is a real OpenRaft/SQLite loopback test: reject a learner with HTTP 503, then
malformed replies, restore its real Raft handler, and await applied catch-up.

* Correct implementation: six failed attempts, minimum spacing **503 ms**.
* HTTP-error mutation to `Network`: 12 attempts, minimum **867 μs**; the test
  rejects the burst (`retry-backoff/negative-http-network.txt`).
* Separate send-error mutation fails the connection-refused classification
  assertion (`negative-send-network.txt`). A fresh no-proxy HTTP client prevents
  the test from accidentally reusing a still-closing pooled connection.
* `make check` passed; the reviewed test and clippy passed after the fresh-client
  correction. Oracle found no production blocker and suggested that correction.

## Updated real k3d release

`retry-backoff/source.txt` pins source, image and binary identity. All five pods
were stopped for the binary upgrade, preserving PVCs, then returned Ready.

The first post-upgrade history used pod deletion with its 30-second grace period.
All 600 appends were retained and Porcupine returned `Ok`, but this does **not**
establish that appends overlapped process death. The result remains retained as
`leader-kill*`; it is not counted as the explicit SIGKILL qualification below.

The second run used `tests/kill_leader.py` as the single history fault driver.
It observed the actual runtime leader of the selected shard and signalled that
container through containerd. Retained events verify exit 137, unchanged pod UID,
changed container ID and an increased restart count on the same PVC:

* **800 acknowledged appends retained; Porcupine `Ok`.**
* 106 acknowledgements before the kill-issued event, 694 afterward. This proves
  the workload continued beyond the recorded fault, unlike the grace-period run.
* Ten unknown append attempts, all retried successfully; zero fail/no-effect
  appends. Logical p99 including retries was 49.6 ms, maximum 1.04 s.
* Replacement readiness was observed about 2.21 s after kill-issued. This includes
  polling overhead and is not an exact process or election-duration measurement.

These are throttled representative workloads, not capacity or equal-semantics
benchmark claims. A process SIGKILL leaves host storage intact; it is not power
loss or proof about independent disks/AZs. The live restart tests establish
continued functionality of this release, not a deterministic DNS-outage rate
comparison; the controlled loopback regression provides the retry-rate evidence.
Histories/logs are gzip-compressed; decompress histories for the Go checker.

Reproduce the explicit fault with a new path, seed and output files:

```sh
python3 tests/history.py run --url "$PRIVATE_URL" --tenant jepsen --path "$STREAM_PATH" \
  --seed "$SEED" --output "$HISTORY" --producers 2 --readers 1 \
  --operations 400 --append-interval .1 --read-interval .2 \
  --timeout 10 --retries 50 --retry-interval .2 \
  --nemesis leader-kill --nemesis-delay 5 --hook-timeout 180 \
  --hook "leader-kill:start=python3 tests/kill_leader.py --tenant jepsen --path '$STREAM_PATH' --output '$EVENTS'"
```
