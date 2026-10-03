# Strict SSE: bounded publication and real k3d boundaries

This is HTTP contract and fault-boundary evidence, not general linearizability,
full protocol conformance, independent-disk testing or an end-to-end proof.
The cluster uses actual k3d pods and local-path PVCs on one physical host.

## Contract and verification

`SseRead.tla` preceded implementation. TLC explored 2,462 distinct states;
negative mutations detect an uncovered control offset and incarnation rebinding.
Raft commitment and captured projection ranges are assumptions supplied by the
other models, not re-proved by this model. Rust refinement is not mechanized.

`src/sse.rs` keeps the initial incarnation and resolves `now`/future offsets once.
Every subsequent observation uses a strict barrier, including heartbeats. The
encoder finishes a captured range before publishing its stored-byte control
offset. `upToDate` refers to that observation, not freshness after a concurrent
write. Errors after headers abort the body without advancing the control offset.
The 60-second lifetime starts at reader construction, after initial metadata
acquisition. It assumes HTTP consumer progress and is not a forced socket teardown
guarantee when the transport stops polling. Idle lifetime expiry emits no cursor.

UTF-8 and base64 are encoded in bounded chunks without a producer queue. The
Electric adaptation intentionally emits `data: `, preserving a leading payload
space that an SSE parser would strip from upstream's `data:` form. JSON offsets
count stored framing bytes, not emitted brackets or SSE fields. Forwarding uses
a 65-second body timeout rather than the ordinary client's 10-second timeout.

`sse-reviewed-check.txt` records successful `make check`: normal/fault/VFS Clippy,
tests, formatting, rustdoc, and 20 Python tests. `sse-reviewed-formal.txt` records
successful Lean/TLC checks and the negative-control command. Retained model
counterexamples remain under `formal/evidence/negative/`.

Independent review found three implementation/oracle issues and one broader
admission gap: leading-space loss, projection-open outside the lifetime, callback
errors mistaken for SSE aborts, and cancelled metadata jobs escaping admission.
All were fixed. Metadata and projection-open jobs now own admission inside the
actor closure. A deterministic regression blocks the actor, queues metadata,
cancels its caller, and checks both permits remain held until FIFO completion.
The follow-up review found those fixes sufficient; it did not independently run
the live tests or establish general service correctness.

## Executed observations

| Scenario | Observed result |
|---|---|
| Normal forwarded HTTP | Text/JSON/base64, leading spaces, offsets, append/close, recreation abort, heartbeat and clean 60-second lifetime passed |
| Truncate captured projection before file read | Both nodes ended the 200 body with an error; no complete data/control event escaped; strict recovery rebuilt correct bytes |
| Append while captured range is gated | Original data/control preceded new data/control; the first cursor did not skip newly appended bytes |
| 32 forwarded idle streams | Both nodes showed 0 live / 96 general slots; the 33rd live request got 429; an append succeeded and closed all readers; slots recovered to 32 / 128 |
| Isolate direct leader from quorum | Only the initial control escaped; the body aborted after 16.209 seconds; a strict read succeeded after healing |
| Initial projection-open blocked | Clean 503 after 60.008 seconds; 31 live / 127 general slots remained while actor was blocked, then 32 / 128 after release |
| Later projection-open blocked | Incomplete 200 body after 61.008 seconds; only the previous control escaped; the same held/released slot counts were observed |

The final two rows use the tightened harness and corrected source in
`sse-reviewed-{open-timeout,later-open-timeout}-history.jsonl`. Each asserts a
58–70-second worker-measured jitter window; the initial 503 must be fully consumed
without an error. The 75-second watchdog is not the application-deadline oracle.
The harness owns all gate/partition timing and recovery. These are schema-3
observations, not input to the schema-1/2 Porcupine adapter.

The earlier `sse-open-timeout-result.txt` records a failed **preflight** after the
quorum experiment changed the leader. No deadline gate was injected in that
attempt. A direct-leader forward was repointed before the successful `open-timeout2`
run. Both earlier successful deadline histories are retained, not replaced by the
post-review repeats. Normal cancellation observations do not independently prove
blocked-body cancellation accounting; that boundary has a deterministic Rust test.

After restoring the corrected normal image, `sse-reviewed-http-history.jsonl`
repeated the forwarded HTTP suite successfully, and `sse-reviewed-longpoll-*`
passed the long-poll regression. A separate two-producer, 40-append history
(`sse-reviewed-restored-history.jsonl`) passed the prefix smoke checker and the
Go Porcupine adapter (`sse-reviewed-restored-porcupine.txt`: `Ok`). This does not
extend the schema-3 SSE observations into a general linearizability result.

## Source identities and telemetry limits

`sse-normal-images.json` records the normal run at source `115cf76`;
`sse-fault-images.json` records fault scenarios at `30f3a87`. The post-review fault
and restored normal identities are in `sse-reviewed-{fault,restored}-images.json`,
source `0cb7657d1449d6908c3adbf8ebaa6e2edcae3a7c`. The restored image has no
storage-fault feature or fault environment. Image digests and pod readiness are
retained rather than inferred from a tag alone.

`sse-lifetime-victoria-{logs.jsonl,trace.json}` contains two correlated completion
events/spans for a forwarded connection lasting about 60 seconds, not a proxy
timeout. `sse-truncate-victoria-{logs.jsonl,trace.json}` records `body_error` and
`unknown` on both nodes despite HTTP status 200. `bytes_out` counts frames yielded
to the HTTP server, not proven client receipt. SSE phase timings and sampled Raft
indices currently describe initial request setup, not every later observation;
connection duration and delivery outcome cover the body. Deeper trace linkage is
still unfinished.

Reproduce against a disposable cluster using `tests/sse.py` and
`tests/sse_faults.py --help`. Normal tests require an unused path and optional
`--require-forwarded`; fault tests require an explicitly enabled fault build and
matching `/data/...` gate directory. Quorum/open-deadline scenarios require direct
leader forwarding; truncation/append/admission scenarios require nonleader ingress.
Never infer leader identity from a previous fault run. Restore the normal image
and remove fault environment after the tests.
