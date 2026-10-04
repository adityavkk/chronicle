# SSE compatibility verification

Source `ea20abd`; image `chronicle-raft:sse-compat`, ID
`sha256:1ec3f102f8a0569f4b189dfa2059cac2be09675904d995a70a8284c707f590eb`;
release binary SHA256
`16f0ad7b7546da766677cba788f0a40b67b9a8adcacb96cbf6006ace17952f42`.
Stop-all upgrade of five real k3d pods preserved PVCs.

`make check` passed, including arbitrary chunk/property framing tests, UTF-8,
leading-space preservation and source-error termination. The existing SSE model
passed 2,462 reachable states before implementation. Batching follows its
data-completion-before-control ordering; it does not guarantee network-atomic
delivery. Buffers flush at 16 KiB plus at most one bounded source chunk.

Full unchanged upstream suite: **247 passed, 79 failed, 6 upstream-default skips**
(`conformance-sse-compat.{json,txt}`). All five previously failing SSE cases now
pass. The upstream injection helper uses a substring stop condition that can
match payload text before a complete event; small-event batching avoids that
observed failure in this run but does not repair the helper or claim arbitrary
transport segmentation is safe for that helper. No upstream code was changed.

The independent `tests/sse.py` harness also passed (`sse-compat-http.{jsonl,txt}`),
with event-aware parsing, leading spaces, CR/LF injection, JSON/base64, live
append/closure, heartbeat/lifetime and incarnation-change interruption checks.
This run used load-balanced ingress, not verified nonleader ingress, and is
schema-3 HTTP evidence, not a history-based linearizability proof.

All remaining full-suite failures concern forks. Prior barrier-timeout failures
remain retained availability evidence; these passing cases do not erase them.
