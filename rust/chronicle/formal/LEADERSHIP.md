# Weighted leadership attempts: contract before implementation

Use the pinned upstream directed-transfer API; never enable native campaigns as
ordinary balancing. Keep canonical stream safety in Raft. This policy authorizes
an optional optimization, not writes or membership changes.

## One-shot durable authorization

The control group stores one attempt: non-reused ID, shard, placement generation,
committed source vote (term **and node**), uniform membership log ID, target,
creation time, phase, and claim time. Planning does not increment placement
generation or make placement incomplete. A separate claim command consumes the
attempt. Only its original successful response permits the source executor to
submit one trigger. Every repeated claim denies permission, even when the first
claim committed but its response was lost. Do not recover a permit by reading
`Claimed`, replaying cached success, or restarting the controller.

Persist a consumed-vote high-water mark per fixed shard. A new attempt ID cannot
rearm the same or an older committed source vote, even after expiry, snapshot or
restart. This bounds state without retaining an unbounded attempt history. A
lost claim response can waste the optimization until a newer leadership vote;
that is preferable to repeated write-disrupting submissions. The global
60-second claim cooldown also survives cleanup. A wall-clock cooldown is not a
real-time bound under unbounded clock jumps/skew; monotonic local observation
windows remain separate. Raft safety does not depend on either clock.

Phases are Planned → Claimed → ObservedTarget or ClosedUnknown; an unclaimed
expiry is ClosedUnused. Closing is conditional on the same attempt ID. A claim
or trigger timeout is never classified as known no-effect. Expiry does **not**
cancel a queued Raft trigger. Replica repair preempts elective policy and never
waits for its expiry/cooldown. Healthy replica and leadership optimizations do
not knowingly overlap.

Before claim and again before submission, check the exact source vote, placement
generation, non-draining distinct target voter, matching applied/effective
uniform membership, fresh target health/catch-up and bounded actor queue. Use
the local movement mutex to serialize local membership work. This is best-effort
fresh admission: upstream has no conditional trigger API. A queued command can
execute after a later vote/membership change. The model must retain that witness,
not pretend application expiry fences the consensus event loop.

Completion requires a strict barrier **on the target itself**, a successor vote,
and matching applied/effective uniform membership. A redirected probe or merely
`Leader` metrics is insufficient. Completion records observation, not causation
or lasting leadership. The unavailable-target path suppresses source heartbeats
and relies on another voter election, not a source rollback timer. Keep
automatic elections enabled. In-process 200/800/1600 ms testing recovered a strict
read/write in 2.552s; the negative elections-disabled case did not recover in4s.
Those are measurements, not an outage bound or product SLO.

## Resource preference and verification boundary

Reuse the existing 30-second complete observation window, per-shard charged-byte
and actor-service weights, queue veto and 10% squared-load improvement threshold.
For leadership, charge a shard's weight to its **current leader only**. Extend
window continuity to all observed shard votes and membership identities, not
only the control term and placement generations. Missing/stale samples, actor
restarts, membership changes and completed/abandoned attempts reset the window.
Choose deterministically among positive gains; no count-only fallback when
observations are absent. A transfer does not move bytes; this is a weighted
leadership-demand proxy, not a CPU/disk-load prediction. One hot stream remains
ordered by one leader.

The application model separates claim persistence, reply delivery, caller loss,
enqueue and upstream handling. It assumes serialized durable control apply and
safe Raft membership/read barriers; it does not re-prove Raft. Negative controls
must catch duplicate permits, lost persisted claim state, new-ID budget bypass
and cooldown bypass. An unmutated model must also exhibit stale queued execution
as a documented API limitation. Lean should prove the pure high-water claim rule
for unbounded values. Rust replay/snapshot tests and real fault histories are the
unmechanized model-to-code bridge.

Before automatic enablement: test competing controllers, every claim/reply/crash
boundary, snapshot/reopen, stale views and same-term different-node identities,
membership overlap, target loss, and stable weighted convergence. Measure live
strict-read/write outage and acknowledged retention under load. No fairness
assumption guarantees an optional attempt after a lost claim response; liveness
claims require eventual connectivity/elections and fresh eligible observations.

## Executed checks and intended code mapping

`PATH=$HOME/.elan/bin:$PATH make -C formal check` passed before implementation.
`Leadership.cfg` explored 6,340 distinct states. Four mutations failed their
named invariants; `LeadershipStaleExecution.cfg` retained an **unmutated** queued
trigger/vote-change counterexample. Lean checked successful-claim persistence,
retry/older-vote denial and strictly increasing consumed marks without proof
holes. Full output and counterexamples are in `evidence/leadership-check.txt`
and `evidence/negative/`. The final temporal-error line in the full output is an
expected existing negative control, checked by exit status and exact message.

Implementation mapping, to keep checked as code is added:

| Model | Intended owner |
| --- | --- |
| Plan, Claim, Close; highWater, lastClaim | `leadership::Operation` and `Ledger`, applied through `model::State` |
| Durable control apply / Crash | `storage` actor SQLite transaction; explicit reopen and snapshot metadata |
| Deliver / volatile permits | original successful claim `client_write` response only; never a ledger read |
| Submit | source controller, movement mutex, fresh admission rechecks, one native trigger call |
| queued / Handle | pinned upstream event loop; no application vote-CAS or cancellation claim |
| vote / generation | full committed OpenRaft vote / replicated placement generation |

This model isolates authorization. Its arbitrary Close permits both expiry and
completion; it does not verify completion probes, weighted convergence or Raft
itself. Those remain separate implementation-test and live-qualification gates.
Lean's natural-number vote rank abstracts the order of committed `(term, node)`
identities; the Rust representation/refinement is not mechanized.

The submission budget covers application-triggered optimization only. The pinned
Raft step-down watcher can separately initiate transfers when a leader is fully
removed from effective/committed membership. A single trigger broadcasts to all
other effective voters. `chronicle_leadership_submissions_total` counts calls at
the application boundary, not those autonomous removal transfers or RPC fanout.
See `UPGRADE.md`; the HTTP regression retains per-group/recipient deliveries.
