# Cancellation retains local Raft admission

Pinned OpenRaft 0.9.25 enqueues `client_write` into an unbounded core API queue;
dropping its response receiver does not retract the command. Previously the
eight-second HTTP timeout released capacity while that waiter could remain
pending without quorum. The strict-read barrier has the same submission/wait
split. This was a source-level diagnosis, not an observed cluster OOM.

`formal/tla/Admission.tla` was written and checked before implementation:
37 distinct states passed; `BadAdmission` catches release-on-timeout in the
four-state Admit → Submit → Finish trace. The model addresses local waiter
ownership, not all internal Raft allocations or durable log retention.

Source `30c1d5cd3fae8e5df10b4d97e2c6ecf9ec46a01b` retains request guards in
detached completion tasks for writes, strict barriers and read-triggered expiry.
HTTP timeout still returns an unknown outcome. A pending waiter may occupy its
slot indefinitely without progress; it cannot repeatedly free admission for new
proposals. Trusted administrative endpoints are outside this public admission
contract. Forwarding metadata jobs use a separately bounded actor queue.

## Executed checks

* `make check` passed; full output is `admission-check.txt`, including locked
  clippy/tests, storage-fault feature checks, VFS cases, rustdoc and 21 Python tests.
* The actual single-node Raft regression holds a SQLite writer transaction,
  cancels the caller, checks no new admission, releases storage, then observes
  frontier 27/incarnation 1. A second case uses production read-triggered expiry:
  both general/live slots remain held, then return after the tombstone applies.
* `make formal` passed the entire Lean/TLC suite and expected negative controls.
* The normal release image was rolled to all four real k3d pods; exact image
  identities are in `admission-images.json`. Header regressions passed again.
  Four producers appended 40 records with concurrent reads; the retained history
  passed the independent Go Porcupine adapter (`admission-porcupine.txt`: `Ok`).

Oracle review found the original strict-barrier gap; the fix and expiry test
were included before a follow-up review found no blocker within this scope.
The 100-ms unit deadline is not synchronized to Raft dequeue and does not test
HTTP disconnect, quorum loss, or crash recovery. In particular it does not
independently falsify reverting only the barrier guard; that stalled-barrier
regression remains useful additional coverage. The k3d run above is ordinary
post-rollout compatibility/history evidence, not a cancellation fault test or
general memory-bound proof. Full protocol conformance remains failing.
