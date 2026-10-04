# Retained-leader removal and restoration on k3d

The prior withheld-volume run exposed a real cleanup deadlock. All five groups
had applied uniform voters `[1,2,3]`, but retained nonvoter 4 remained Leader.
`volume-loss-64040/retirement-timeout-state.json` preserves that failure. In
OpenRaft 0.9.25 this is intentional `retain=true` behavior, not incomplete commit:
the controller must explicitly remove the demoted leader's node record. Waiting
for runtime Learner before removing that record cannot make progress.

The fix was specified and TLC-checked before implementation. The five-state
`LeaderRetirement` model passes safety and eventual step-down under stated
quorum/fairness assumptions; its wait-for-Learner mutation produces a retained
temporal counterexample. It does not model Raft itself or cross-group routing.
Self-removal requires applied uniform demotion at or beyond the replicated
placement boundary, the existing movement lock, fresh completed generation and
core-admitted vote fence. Final retirement still requires runtime Learner.

The real-Raft regression uses four loopback nodes and five SQLite groups. It
removes control first, observes Learner with a stale self-leader hint, introduces
later data placements, verifies their completion on the natural control
successor while local control remains stale, then verifies final retirement.
`make check` and `make formal` passed (`leader-retirement/{check,formal}.txt`).
Oracle found a residual stale-hint completion/discovery path; both it and the
equivalent ingress/admin checks were fixed. Follow-up review found no blocker
for this mechanism. The regression explicitly drives controller ticks/cleanup;
the live run below exercises autonomous scheduling.

## Actual live result, including interrupted clients

`leader-retirement/source.txt` pins the release binary/image built from source
`da30751a51b1d670692e6fc5e624bc9273323e86`. All five pods were stopped together
for the binary upgrade, preserving PVCs. `post-upgrade.json` explicitly labels
that recovery: restarting the original stuck leader is **not** evidence that
the new self-removal path works.

The subsequent `tests/leader_drain.py` schedule ran with no process restart:

1. Enable verified spare 4 and await automatic placement.
2. Identify actual control leader 3, which also leads data groups, and drain it.
3. Await verified retirement, then restore node 3's eligibility and drain 4.
4. Await all five placements `[1,2,3]` and verified retirement of 4.

Every pod UID, container ID and restart count remained unchanged through that
cycle (`leader-retirement/events.jsonl`). Leader retirement took about 24.6 s
after the drain response; restoration took about 16.2 s after draining spare 4.
No native campaigns or consensus upgrade were used. This is graceful membership
removal followed by natural election, **not directed or zero-disruption transfer**.

Eight producers and four strict readers ran across four shards with 96-byte
records and 200 ms per-producer pauses. The intended workload was 512 appends
per producer, but two producers exhausted their 11-attempt retry budgets:

* **3,402 acknowledged logical appends retained; all four Porcupine checks `Ok`.**
* 106 unknown append attempts, zero guaranteed-failure appends. Strict reads also
  received transient 503s. Observed maximum gaps between successful reads were
  3.35/3.35/2.74/3.66 s by shard; these include the client's polling interval.
* Logical append latency including successful retries: p99 37.2 ms, max 8.32 s.
  The 119.85 s append window yielded 28.4 acknowledged appends/s. This throttled
  fault run measures neither capacity nor a user SLO.
* Original failed/unknown attempts are retained unchanged. Separate extended
  histories retry the two unresolved tuples after restoration; both return 200,
  and both extended histories receive Porcupine `Ok`. They are not discarded
  failures or fresh producer identities.

The original volume-loss histories were also read again after restoration,
upgrade and this cycle: all **3,072 records** still match their earlier checked
strict reads (`original-volume-history-after-restoration.json`). This does not
convert the earlier withheld-volume simulation into physical power-loss proof.

The committed histories are gzip-compressed without content changes; decompress
before running `go run ../../jepsen/checker -rust-history FILE`. The two recovery
histories contain the original workload followed by explicit retry/read events.
`stats.json` describes the original workload only. `resources-after.txt` is a
single post-run sample, not peak resource usage. Per-pod logs are retained: they
also expose a separate startup DNS retry burst, outside this drain workload.

## Reproduction and limits

Use the existing private k3d setup, eligible seeds 1/2/3, verified drained spare
4, and the normal release image. Run four concurrent `history.py run` processes
using distinct paths covering all shards, two producers/one reader each, and
the arguments in each history's run record. One history owns the schedule:

```sh
--nemesis node-join --nemesis-delay 5 --hook-timeout 360 \
--hook 'node-join:start=exec python3 tests/leader_drain.py --output NEW_EVENTS.jsonl'
```

The hook includes join, leader drain and restoration; no second fault driver
runs concurrently. It refuses existing output, does not retry mutations, and
does not clean up blindly after a failure. Inspect retained state before reuse.
This single-host k3d run is not independent-AZ/disk evidence, broad scheduling
coverage, resource-informed leadership balancing or general durability proof.
The two exhausted producers remain an availability qualification, even though
eventual restoration and their later recovery succeeded.
