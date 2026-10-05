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

Remaining: live loaded k3d convergence/no-churn, stale/membership-overlap and
unavailable-target histories with read/write outage measurement; telemetry and
conformance on the exact new image; independent follow-up review. Claim timeouts
remain unknown, expiry cannot cancel queued upstream commands, and optional
movement is not guaranteed after a lost claim response until a newer source vote.
