# Implicit lifecycle admission verification

Source `308f18b`; image `chronicle-raft:recreation`, ID
`sha256:6dae3e4446335737bbd3221843d75f79b8fd1606e924db04041af71a36562272`;
release binary SHA256
`8bc1781bdedd7643a703a581f314bdf3fd16ca88d412afb6950c291b4afa1dd0`.
All five pods stopped before upgrade; PVCs preserved.

Initial live check after Kubernetes Ready returned 503 `no leader; retry`.
`recreation-http.jsonl` retains it: pod readiness is not leader readiness.
The later independent run `recreation-http-ready.jsonl` passed deletion/TTL
recreation, producer reset and explicit stale create/append/delete checks.
These schema-3 HTTP observations are not Porcupine-certified histories.

Full pinned suite: **239 passed, 87 failed, 6 upstream-default skips** in
`conformance-recreation.{json,txt}`. Recreation and offset-now now pass;
fork failures increased from 78 to 79 and remain unresolved. Upstream source and
assertions are unchanged. The runner's default test timeout changed from 5s to
30s, above the intentional 5s server long-poll wait and suite's 20s long-poll
configuration; this is a harness deadline correction, not a server latency fix.

Formal admission mapping/checks preceded implementation. `make check` passed
before deployment. Independent Oracle review found no admission/conditional-read
correctness blockers; sequential live checks do not establish all concurrent
admission interleavings. Explicit original incarnation remains necessary to
distinguish cross-lifetime retries from new implicit operations.
