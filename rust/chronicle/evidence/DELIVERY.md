# Response delivery fault on real k3d

The normal and `storage-faults` images were built from source revision
`72a93960906af393e703d5fa38bdb9f14b49ccee`. The experiment used the existing
four-pod local cluster and its actual PVCs, through drained node 4's port-forward
so both ingress forwarding and the current data leader participated.

`tests/delivery_history.py` creates a fresh stream, commits one 96-byte record,
retries it, and warms its disposable Electric-format projection. It finds that
file by the fixture digest, pauses the blocking file reader before reading,
truncates only the cache, and releases the reader. It requires an unknown read
and a subsequent strict read containing the exact committed record. No database
or authoritative log is modified. Run in isolation: the test gate is shared by
file-response readers on that pod.

```sh
python3 tests/delivery_history.py --url http://127.0.0.1:18084 \
  --seed 2026100399 --path delivery-2026100399 \
  --output evidence/delivery-truncate.jsonl
/tmp/go/bin/go run ../../jepsen/checker \
  -rust-history evidence/delivery-truncate.jsonl -rust-history-timeout 30s
# Ok
```

Use a new path, seed and output for another run. The script refuses an unexpected
cluster context, absent fault environment markers, or pre-existing gate controls.
On failure it preserves the release marker so a detached reader cannot become
stuck; restore the normal image before cleaning those controls. On success it
waits for `.resumed` before removing them. This is not a production fault endpoint:
the gate code is absent without the compile-time feature.

The live run used `chronicle-raft:delivery-faults` with both
`CHRONICLE_STORAGE_FAULTS=1` and `CHRONICLE_FAULT_DIR=/data/delivery-gates` set in
one StatefulSet patch. After the experiment the StatefulSet was rolled back to
`chronicle-raft:delivery` and both environment variables removed. Image identities
are retained in `delivery-fault-images.json` and `delivery-normal-images.json`.

`delivery-victoria-logs.jsonl` has two events per correlated request, one for the
forwarder and one for the leader:

| Request | Status | Delivery | Outcome | Observed / expected bytes |
|---|---:|---|---|---:|
| duplicate | 204 | complete | ok | 0 / 0 |
| truncated read | 200 | body_error | unknown | 0 / 96 |
| recovered read | 200 | complete | ok | 96 / 96 |

`delivery-victoria-trace.json` retains the six correlated spans, including error
status on the truncated responses. `delivery-victoria-metrics.json` records one
body error each on the forwarder and leader; this is two server observations of
one failed client read, not two independent failures.

`make check` passed (fmt, normal and feature clippy/tests, SQLite VFS tests,
rustdoc and nine Python tests). Unit regressions cover partial-body errors,
cancelled handlers/bodies, delayed completion, length mismatches, bodyless
statuses, privacy and exporter outage. Oracle follow-up closed the forwarded-204
false-cancellation defect and found no blocker in this scope.

These observations establish server-side delivery classification and cache
reconstruction, not client receipt, general protocol conformance, physical-disk
durability or power-loss behavior. Pre-handler extractor/admission failures are
outside this scoped observer. The linearizability history is small and tests no
concurrent mutation; the retained larger histories cover different schedules.
