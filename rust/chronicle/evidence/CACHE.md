# Conditional-read verification

Source `5904598`; image `chronicle-raft:cache`, ID
`sha256:5216989dfed17f2c9d104e2444e02264cd084428606ff92deedcc8b388b54366`;
release binary SHA256
`89bc5c1e9d4f09b439eae0f86f1034d1a5410a7f6a8bd99da3b2995e6d4664ca`.
All five k3d pods stopped before upgrade, PVCs preserved.

`make check` passed: fmt, clippy, Rust tests (including storage/VFS faults and
OpenRaft suite), docs, and 33 Python tests. `CacheView` checked 56 reachable
states; sampling a later tag triggered the retained `ValidatorBound` negative
counterexample. Independent Oracle review found no correctness blocker.

`tests/cache_http.py` passed against the cluster: ordinary/weak/list/wildcard
revalidation, changed append/recreation tags, missing stream, beyond-tail and
invalid JSON range rejection before 304, expiry and sliding-TTL renewal on 304.
The schema-3 history `cache-http.jsonl` is not Porcupine-certified. This test does
not inject quorum loss during conditional reads or force capture/append races.

Full unchanged pinned suite: **242 passed, 84 failed, 6 upstream-default skips**
in `conformance-cache.{json,txt}`. All cache tests now pass. Five SSE and 79 fork
failures remain. Cache-Control remains no-store: ETags enable explicit client
revalidation, not shared-cache freshness. No end-to-end proof claim follows.
