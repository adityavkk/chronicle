# Membership overlap exposed stale-ingress routing

On the real five-pod `chronicle-upgrade` cluster, seed 85361 observed automatic
attempt 9 for shard 4, source vote `(5331, 2)`, target 3. Registration admitted
node 4 in target 3's simulated zone at control index 628; draining node 3 committed
at index 629. A subsequent strict control read still showed attempt 9 `Planned`.
All five groups then completed replacement to voters `[1,2,4]`, and the retirement
API confirmed node 3 could stop. This is placement/leadership-admission overlap,
not a queued transfer interrupted midway. Later attempt 10 successfully consumed
the same source vote for target 4 under the new membership. The high-water rule
therefore implies attempt 9 did not consume it; no direct closing-event capture
for attempt 9 is claimed.

The original run **failed** its smoke check: all 7,200 appends were acknowledged,
but shard 3's final read received 503, leaving three records unobserved by its
recorded reads. A direct probe found complete data through four pods and persistent
503 through nonvoting node 5. Its captured control database has no applied node
registry, and its configured seeds omit newly admitted node 4. Discovery tried
only those locally known addresses, never following a surviving follower's hint.
This is an availability defect, not evidence that acknowledged data disappeared.

Separate extended histories add actual subsequent strict reads. All 7,200 records
were observed, all four smoke checks passed, and all four extended histories
returned Porcupine `Ok`. The failed originals are preserved alongside extensions
in `leadership/attempts/drain-85361.tar.gz`. It would be incorrect to relabel the
original driver exit as a pass. Its 457 unknown append attempts and 1.283 s maximum
logical latency include the routing defect; they do not measure migration alone.

## Fix and independent review

Before implementation, the routing refinement was documented and the full formal
checks rerun. They establish unchanged modeled safety assumptions, not discovery
availability. The new real four-node/five-group regression fails before the fix
with `503 no leader; retry`, then passes after it. It gives a retired ingress only
a surviving follower's address, explicitly excluding the leader. Full `make check`
passes, including storage/VFS faults, Rust tests, Clippy, docs and 39 Python tests.

Discovery now follows a peer's reported leader address from membership and probes
that destination. The recipient still enforces ReadIndex/client_write. The existing
three-second deadline, a 32-iteration queue limit, and one visit per identity bound
the search. Mutations are not replayed. Oracle found no scoped blocker, while
noting that the budget is not general discovery completeness and a stale
self-reported leader may still cause a retry.

Reviewed image: `chronicle-raft:leadership4`, exact ID in `retired-routing/image.txt`.
On its original volumes, **20 direct strict GETs**—four shards through each of five
pods—returned exactly 1,800 unique expected records each. Node 5 had no shard-4
leader hint, while node 4 actually led shard 4. This exercises the missing-registry
case over real HTTP rather than only the discovery helper. Full unchanged
conformance also passed: **326 passed, 6 upstream-default skips, zero failures**,
76.56 seconds. Raw probe, formal, negative/positive regression, check and protocol
outputs are in `retired-routing/`.

A sampled read through node 5 is present in VictoriaLogs and VictoriaTraces:
`retired-ingress-85401`, trace `00000000000000000000000000085401`. Both ingress
and authority completion events report 172,800 delivered bytes and the same
frontier, with a parent/child span link. The authority event includes its ReadIndex
timing; the ingress event does not pretend its local applied index is authority.
The metrics query returns 25 node/group submission series. These post-rollout
counters do not establish the earlier attempt-9 submission count.

## Retained unsuccessful coverage attempts

`leadership/attempts/` retains seeds 85281 (missed pre-claim phase), 85301 and 85341
(no new plan in the observation window), and 85321 (pressure setup hit the logical
quota before starting writers). Their failed outcomes were not suppressed.
Pressure streams from completed experiments were read/verified and explicitly
deleted to reclaim disposable capacity; `pressure-cleanup.jsonl` records this.
The originals of seeds 85281, 85301 and 85341 each have four Porcupine `Ok` results,
which say nothing about the missed fault interleaving. Earlier OOM and missed-window
cases remain in [SNAPSHOT-MEMORY.md](SNAPSHOT-MEMORY.md).

## Post-fix restoration passed without replacing any owner

`tests/restore_upgrade.py` restored voters `[1,2,3]` and verified node 4's retirement
while two producers and one strict reader per shard were active. Seed 85421 ran
500 appends per producer: all **4,000 acknowledged records** were retained; all
four smoke checks and all four Porcupine histories passed. Four append attempts
were unknown and retried; logical p99 was 70.28 ms, maximum 660.45 ms. These paced
client latencies include restoration, not a continuous outage bound. All pod UIDs,
container IDs and restart counts stayed identical. Full events and histories are
in `retired-routing/restore-85421.tar.gz`.

The durable Rust regression was then extended to five actual HTTP/Raft processes.
The nonvoting fifth ingress has neither node 4 in its seeds nor any applied control
registry. With shard 1 led by node 4, create, producer append and strict GET traverse
the real stream handlers. A second append commits before a test-only HTTP layer
discards its successful reply; the client receives 503, the authority sees exactly
two POSTs total, and a later strict GET returns `start-a-b`. Thus proxy discovery
does not replay the ambiguous mutation. The first extended-fixture run exposed
bootstrap membership not yet committed; the fixture now performs ReadIndex before
adding learners. Both the failed setup and passing full check are retained.
