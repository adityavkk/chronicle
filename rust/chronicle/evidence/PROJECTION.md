# Committed file projection validation

Specification commit `5f751d6` preceded implementation commit `5ae4d38`.
`formal/evidence/projection-check.txt` retains the complete formal run: 3,168
projection states and two negative controls, alongside the earlier model/proofs.
This is bounded model checking plus reviewed correspondence, not Rust refinement.

`projection-check.txt` records passing fmt, clippy, feature-gated storage tests,
SQLite VFS errors, OpenRaft storage contracts, unit/integration tests and rustdoc.
`projection-pvc-tests.txt` repeats all seven projection integration tests with
`TMPDIR` on the actual running k3d PVC. Each test creates its own disposable
directory; it never opens the service's databases. Temporary test files were removed.

Independent review found two issues before rollout: metadata could survive a
same-incarnation snapshot replacement, and cancelled Tokio filesystem reads could
outlive admission. A process-local snapshot token and admission owned by explicit
blocking read jobs resolve these; follow-up review closed both findings. Tests
exercise identical-byte replacement and cancellation behind a blocked worker.

All four pods run `chronicle-raft:5ae4d38`; `projection-images.json` retains their
image IDs, and `projection-pvc-files.txt` shows real materialized files. The HTTP
checks and histories used port-forward 18084 to the **drained node 4**, forcing
forwarding to current voters rather than accidentally testing only local reads.

* `projection-http-k3d.json`: seven binary/JSON GET, HEAD, length/frontier,
  empty-tail and JSON-boundary checks passed.
* `projection-lifecycle.jsonl`: 21 operations, Porcupine `Ok`.
* `projection-load.jsonl`: 400 acknowledged appends, 61 strict reads,
  retention/prefix smoke checks passed and Porcupine `Ok`.

The last run used four producers, two readers and 96-byte records, with no nemesis.
From first append invocation to last append response: 2.008277703 seconds,
199.18 appends/s; nearest-rank append p99: 39.809 ms. This short release-mode smoke
workload on a single nested-container host is **not a capacity benchmark**, an
equal-semantics baseline comparison, or an independent-disk/AZ durability test.

Projection files are disposable, not fsynced authorities. Published inodes assume
ordinary filesystem reliability; silent same-length corruption is not checked by
a cache digest. Restart reconstructs them from SQLite. No `sendfile`/zero-copy claim
is made: the Axum path uses bounded 256 KiB reads and explicit unexpected-EOF errors.
