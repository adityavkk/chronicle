# Same-volume overlap failure and regression

The original `leader-kill.jsonl` fails retention: 760 unique acknowledgements,
450 records in the final strict read, 310 acknowledged records absent. Do not
delete or replace it with the passing `leader-kill-locked.jsonl` (661 records).
Missing from a strict read does not mean every physical copy disappeared.

Independent oracle follow-up reconstructed the archived WALs in memory with
checksum/salt validation; SQLite integrity checks succeeded. Node IDs 1/2 had
applied indices 762/761 and 760/759 records. Restarted node 3 had applied index
477 and 450 records. Twenty-five retained `(term, leader, index)` identities had
different commands across replicas. At `(1,3,453)`, nodes 1/2 had producer
`history-20261004-1` sequence 112; node 3 had sequence 194. History line 1474
acknowledges sequence 112 at index 453. Runtime logs in `failed-restart/` show
replacement start 03:41:28.353872 and old exit 03:41:29.754620, a 1.40s overlap.
This substantiates overlapping identity/stale cache overwrite; it does not
reconstruct every intermediate Raft transition.

The raw database/WAL archive is preserved privately by the maintainer, outside
the published repository and its branch history. `failed-restart/archive.sha256`
records its checksum. The synthetic histories, runtime logs, diagnosis and
regression outputs remain public; reproducing the archived-WAL analysis requires
the private capture, not just this checkout.

## Regression executed on the actual PVC

The library test executable built from these sources was copied into the running
pod and invoked with `TMPDIR=/data`, the actual local-path PVC mount:

```sh
KUBECONFIG=/tmp/chronicle-kubeconfig kubectl -n chronicle cp \
  target/debug/deps/chronicle_raft-32c47d1df47cfe19 chronicle-0:/tmp/ownership-tests
KUBECONFIG=/tmp/chronicle-kubeconfig kubectl -n chronicle exec chronicle-0 -- \
  env TMPDIR=/data /tmp/ownership-tests --exact \
  storage::tests::process_lock_survives_overlap_and_releases_on_sigkill --nocapture
```

`ownership-pvc-test.txt` records success. Assertions check rejected contender,
unchanged DB/WAL bytes and lock inode, first owner durably advancing vote 9→10,
SIGKILL releasing the lock, and replacement recovering vote 10. The production
binary was also launched as a contender in that pod: `ownership-live-contender.txt`
records the lock error and identical device/inode before and after. It never
reaches the listener/bootstrap route because storage opens first.

`ownership-tlc.txt` checks 20 states under exclusive ownership;
`ownership-counterexample.txt` retains the negative model's actual lost-ack trace.
This retrospective model does not prove Raft or fsync.

The guarantee is confined to cooperating binaries using the same persistent
lock inode on the tested filesystem. A cloned volume, an older non-locking
binary, hostile lock-file replacement, and physical disk/power loss are not
covered. SIGKILL is process failure, not power failure. New admission guards
and missing-store rejection are tested separately; do not attribute them to
the earlier passing workload, which predates those changes.
