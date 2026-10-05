# Directed leadership policy — implementation checkpoint, live gate outstanding

The candidate remains isolated from the stable OpenRaft 0.9 deployment. Directed
balancing is explicit opt-in (`CHRONICLE_LEADERSHIP_BALANCE=1`) and defaults off.
Native experimental campaigns also remain off. No baseline promotion or general
availability/convergence claim follows from these checks.

Formal checks preceded implementation: 6,340 TLC states, four detected negative
mutations, a retained unmutated stale queued-trigger witness, and Lean proofs of
one-shot high-water claims. See [`formal/LEADERSHIP.md`](../formal/LEADERSHIP.md).
The durable ledger is constant-size per fixed shard, survives SQLite reopen and
snapshot installation, and denies duplicate claims even after lost responses.
Repair admission does not wait for this optimization's cooldown or expiry.

Executed checks:

* `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 make check`: fmt, Clippy, Rust,
  upstream storage, VFS faults, fail-stop tests, docs and 37 Python tests passed.
* Five ledger tests cover same/older full votes, same-term different-node order,
  cooldown/expiry boundaries, stale close IDs, repair preemption, completion
  evidence, SQLite reopen and installed snapshots.
* Weighted tests reach a stable 32/144/32 assignment for one indivisible heavy
  group; all-shard vote/membership changes reset the window, apply progress does
  not, and default-off produces no elective leadership command. This is pure
  chooser evidence, not live convergence or a CPU-load prediction.
* Four-node/five-group HTTP/Raft regressions pass: a retired control replica with
  a stale local hint discovers the control leader and submits once; a committed
  claim whose response is discarded submits zero times; a claim followed by
  target draining before response submits zero times. Fresh concurrent executor
  invocations do not reconstruct permission. These tests use real SQLite and
  HTTP; they do not simulate process power loss. Separate ledger tests cover
  store reopen/snapshot, not a restarted executor process in these scenarios.

Oracle found the stale control-hint routing bug. The executor now performs the
same bounded read-only discovery as ingress before its single claim POST. The
regression removes the source from control replication while keeping it a data
leader; it preserves the stale hint rather than clearing it to make the test pass.

The first executor run failed a **test accounting assumption**: it equated a
submission with one total transfer RPC. Inspection of pinned upstream source and
the corrected per-group/recipient trace established three autonomous control
removal deliveries plus two data-group deliveries from one explicit submission.
The application-boundary counter now independently asserts one submission and
the recipient counters assert `[0, 1, 1, 0]`; both fault cases assert zero for each.
The failed run is retained, not discarded. The old retirement test's description
of exclusively natural elections was corrected: alpha36's step-down watcher can
initiate a transfer after complete leader removal.

## Live candidate qualification

The isolated `chronicle-upgrade` k3d cluster ran `chronicle-raft:leadership1`
([image identity](leadership/image.txt), [release binary](leadership/binary.sha256)).
All five restored PVC owners were stopped before replacing their binaries; this
does not qualify mixed-version operation. The stable cluster was unchanged.

`tests/leadership_balance.py`, seed 85201, wrote a 4 MiB pressure stream and ran
two producers plus one strict reader per data shard. All **14,400 acknowledged
appends** were retained and all four full histories returned **Porcupine Ok**.
No append attempt failed or had an unknown outcome. One strict read returned
503; 1,924 strict reads succeeded. The chooser observed weights 16/28/29/26/46
and moved group 2 from node 3 to node 1. Its pairwise squared-load score fell
from 83² + 0² = 6,889 to 54² + 29² = 3,757. This is the implemented advisory
weight score, not measured CPU redistribution.

The same full votes/memberships remained stable for **127.7 seconds while all
four workloads were running**. There was exactly one application transfer
submission through workload completion; no pod replacement/restart hid churn.
This is finite observed convergence, not a convergence theorem for changing load.
The delivered driver additionally rejects a pre-existing completed attempt and
checks the final vote/attempt after workers finish; those two guards were added
after this run started. This run began with an empty ledger. Its retained
submission metrics and pod metadata independently establish the narrower claims
above. Full histories, observations and statistics: [live-85201](leadership/live-85201/).

The paced run measured 29.67 acknowledged appends/s and logical p99 49.43 ms,
maximum 378.08 ms. Success-to-success gaps reached 493.39 ms for appends and
2,008.57 ms for strict reads. Gaps include client pacing (250 ms per producer,
one-second reader interval), errors and ingress routing; they are **not continuous
outage bounds or a capacity/SLO claim**.

`tests/leadership_unavailable.py`, seed 85221, observed a new group-3 plan targeting
node 1, then externally paused its actual k3d agent container for 34 seconds.
The source repeatedly failed target admission; attempt 2 expired without a
claim. Its shard is absent from the durable consumed-vote ledger and no submission
for it appears in the source log. After healing, a new-vote group-4 plan completed
normally. Thus this run exercises **unavailability before claim**, not the later
queued-trigger race. The latter remains an explicit model counterexample and
the separate real-Raft dead-target test in [UPGRADE.md](UPGRADE.md).

All **7,200 acknowledged appends** survived that pause; all four histories were
Porcupine Ok. There were 76 unknown append attempts, 15 unknown strict reads and
no rejected appends. Retries recovered all logical appends; maximum logical
latency was 13.53 seconds, p99 515.76 ms. Longest observed success gaps were
5.81 seconds for appends and 9.51 seconds for reads. These include service routing
to the paused node, ordinary elections and client cadence. Membership stayed
unchanged. Pausing one container is neither independent-AZ loss nor power loss.
Evidence: [unavailable-85221](leadership/unavailable-85221/), node movement logs.

The **unchanged pinned conformance suite** on this exact image, overlapping the
first workload, passed **326 tests / zero failures / six upstream-default skips**
in 77.98 seconds. Full JSON and text reports are retained here. The statistics
extension passed all 38 Python tests, including errors not ending success gaps,
separate stream histories, and out-of-order concurrent event emission.

A separate candidate Victoria/OTel stack ingested real requests. Retained
[telemetry](leadership/telemetry/) correlates the same trace ID across completion
events and VictoriaTraces, and records all 25 node/group submission counters.
Grafana's request-rate, p99 and applied-index panels were rendered and inspected.
Its early empty time range reflects the later collector deployment, not missing
server traffic fabricated with probes. This is local disposable observability;
the manifest's emptyDir volumes are not production telemetry retention.

Remaining: live membership/leadership overlap qualification and independent
follow-up review before promotion. Policy remains **default-off**. Claim timeouts
remain unknown; expiry cannot cancel queued commands, and a lost claim response
can spend the optimization until a newer source vote.
