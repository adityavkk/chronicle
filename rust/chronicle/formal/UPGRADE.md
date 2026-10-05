# Isolated OpenRaft upgrade qualification

The working cluster remains on 0.9.25. The candidate is exactly
0.10.0-alpha.36, upstream revision
`0acd6b8d547ad4468f66708b05bc03baaf04c7c8`; registry archive SHA256
`858ce506e827306c40820757d94bcf086b5ad54f7c8b9c47578362e4c048e842`.
This is a prerelease from a mature project, not a stable release. No rolling
upgrade, mixed-version wire compatibility, or rollback is assumed.

## Adapter contract, before changing dependencies

`UpgradeStorage` models only the adapter boundaries changed by this upgrade.
Raft supplies the immutable committed prefix and valid truncation requests;
SQLite FULL transactions supply atomic durable persistence, conditional on the
filesystem/device honoring synchronization. Neither is proved by this model.
Indices start at **zero**; -1 represents `None`, not a valid log index.

| Model | Required code mapping |
|---|---|
| Persist then Notify | SQLite log transaction commits before `IOFlushed::io_completed(Ok(()))`; actor retains ownership after caller cancellation. |
| Commit then Apply then Reply | Apply command, membership and last-applied in one transaction; only then invoke each optional `ApplyResponder::send`. Missing responders never skip application. |
| Truncate(b) | `truncate_after(Some(b))` deletes `idx > b.index`, whereas `None` deletes all logs; never reuse the old inclusive predicate. |
| Install(i) | Snapshot state, membership, last-applied and current-snapshot envelope change in one transaction before success; all derive from the same verified snapshot. |
| Crash | Abandoned callers do not erase durable work. Recovery uses committed SQLite state, not cached responses. |

Separate negative configurations move the flush callback or apply responder
early, retain the old inclusive truncation predicate, or split snapshot metadata
from data. Existing ownership, producer/lifecycle, membership and read models
remain applicable; this small adapter model does not replace them. Lean proves
that exclusive truncation preserves every committed entry at or below its
boundary, including index zero. Rust/VFS tests must establish the model-to-code
mapping; it is not a mechanized refinement.

## Membership and transfer constraints

Use `Precondition::CommittedLeaderId` and `LastMembershipLogId` together at
membership admission. The latter compares **effective**, not necessarily
committed, membership. Uniform admission retains the leader condition and
compares against the newly applied joint membership. Preserve the advanced
leader-ID representation containing term and node ID; inspect serialized fixture
compatibility explicitly. A canceled operation can leave joint consensus and
must still be reconciled. Conditions are admission fences, not retroactive
commit-time predicates. Existing strict completion/retirement barriers remain.

Directed transfer must implement the upstream network RPC. Trigger success is
submission, not election completion. Source vote, voter eligibility and the
target's flushed log boundary are checked by upstream; the controller must
observe a successor under a strict barrier before declaring success. Keep native
campaign balancing default-off. Do not enable a replacement policy until its
staleness, membership overlap, unavailable-target, repeated-attempt and outage
tests pass. The initiating trigger itself has no expected-vote CAS.

## Promotion gates

Keep the immutable stopped-process 0.9 PVC/WAL captures and original binary.
Exercise only copies on a separate network, never duplicate live Raft identities.
Deserialize and recover all original groups, check acknowledged records and
producer results, run upstream storage tests plus our cancellation/VFS/fail-stop
tests, and verify bounded snapshot transport. Qualify transfer under real k3d
load with complete histories, exact image identity, conformance and measured
outages. Review before replacing the stable baseline. Captures contain WAL and
are not claimed to be clean shutdowns; SIGKILL is not a power-loss experiment.
