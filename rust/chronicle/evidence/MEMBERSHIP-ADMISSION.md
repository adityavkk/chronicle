# Membership admission and completion fences

The previous retirement implementation checked leadership before calling the
membership API. That leaves a gap: a paused caller can resume after the same node
loses and regains leadership and propose an obsolete target in the new term.
Separately, a cancelled membership future can leave an outstanding joint entry
while the applied voters still match a successor target. A strict read barrier
does not rule out that uncommitted entry.

The formal model was committed before the implementation. Its positive check
visits 28 states; removing either vote admission or the completion barrier
violates `CompletedAuthority`. This assumes Raft's serialized admission and
durable commitment, not a new proof of Raft or mechanized Rust refinement.

## Implementation and assumptions

OpenRaft remains version 0.9.25. The checksum-pinned published crate is vendored
with exact upstream licenses and a four-file patch. The core checks the complete
expected Vote synchronously before membership derivation/append. Both API phases
retain the originating vote. Existing unfenced APIs remain unchanged but the
controller uses the fenced API for learner preparation, promotion, pruning and
reattachment. Catch-up covers the committed AddNodes entry as well as the read
boundary. A rejected fence causes intent rereading, not a refreshed-vote retry.

Incomplete or unknown-history placements always await a target-membership
operation, even when applied voters already match. Its response identifies the
actual applied uniform membership entry and supplies the retirement lower bound.
Matching membership alone cannot complete these intents. Already completed,
known-history placements retain their cheap matching-state check.

There is one serial timeout-owned controller per process and no additional
membership writers. Dropping its future prevents unsent phases; already queued
work remains live and precedes the next membership barrier. This fences admission,
not revocation: an accepted old operation may settle after a newer intent, but
cannot reassert itself after the successor completes under these assumptions.
Do not add detached membership jobs or another writer without revisiting them.

## Executed checks

* `make check`: passed, including six dependency admission tests, SQLite VFS
  faults, the existing state-machine/storage suites, lint/docs and 23 Python tests.
  See `membership-check.txt`.
* `make formal`: passed Lean, bounded TLC checks and negative mutations.
  See `membership-formal.txt` and `formal/evidence/negative/`.
* The new controller regression runs two real OpenRaft instances with SQLite
  and HTTP. It drops data-bearing AppendEntries while allowing empty heartbeats,
  cancels the joint-change waiter, observes a successful strict barrier and
  matching applied voters, and requires completion to return `InProgress`.
  After healing, it verifies completion and the exact stored membership log ID.
* Temporarily restoring the matching-prefix shortcut makes that regression fail
  at `unwrap_err`: the false success returns the old applied boundary. The
  retained failure is `membership-barrier-negative.txt`; the mutation is reverted.
* `membership-vendor-verification.txt` verifies the archive checksum, all retained
  published files, changed-file inventory, source provenance and license hashes.
* Oracle follow-up found no blocker in these two races under the ownership
  assumptions above. Its requested exact-boundary assertion and routine
  dependency-test invocation are included. This is not a review of all remaining
  protocol, placement or performance acceptance criteria.

These local regressions are not k3d fault histories or physical durability tests.
Pending-intent supersession and resource-informed balancing remain outstanding;
experimental native leadership campaigns remain default-off.
