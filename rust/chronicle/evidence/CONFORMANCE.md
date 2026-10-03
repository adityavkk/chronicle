# Protocol compatibility baseline — failing

The full, unmodified core suite `@durable-streams/server-conformance-tests@0.3.5`
ran against the real k3d service at source
`0cb7657d1449d6908c3adbf8ebaa6e2edcae3a7c`, normal release image. This is the same
running source/image recorded in `sse-reviewed-restored-images.json`. No test
was changed to conceal a failure. Six optional Chronicle subscription tests are
disabled by upstream's default, not by a new skip list.

| Run | Passed | Failed | Skipped | Qualification |
|---|---:|---:|---:|---|
| `conformance-baseline-20261003` | 150 | 176 | 6 | Includes forwarding transport failures |
| `conformance-diagnostic-20261003` | 150 | 176 | 6 | Failure causes logged; `ECONNREFUSED` during tunnel restart |
| `conformance-private-20261003` | 180 | 146 | 6 | Private NodePort; no `fetch failed` test results |

Each row has retained `.json` and `.txt` reports. The private run still includes
expected abort diagnostics from test timeouts/cancellation. It is not a clean
conformance pass, and the 146 failures are not all interchangeable server defects.
`conformance-portforward-failure.txt` captures the broken-pipe failure and new
forwarding process; Chronicle pods had no restart. The changed transport removed
the tunnel outage but did not change the server binary. An isolated upstream SSE
test also passed (`conformance-sse-probe.txt`), disproving the initial inference
that all 29 SSE cases were protocol failures.

## Actual missing behavior and contract differences

The suite exposes missing forks, `Stream-Seq` enforcement, sliding TTL/absolute
expiry, ETags, producer response metadata, status mappings and security headers.
Some currently recognized protocol headers are silently ignored; do not treat a
successful response to such a request as implementation of that feature. This is
an unfinished compatibility/safety boundary, not an accepted production behavior.

Same-URL recreation tests also conflict with this implementation's explicit
incarnation fence: an omitted incarnation stays 1 rather than silently rebinding
an old mutation to a replacement stream. That policy is documented, but its
legacy-client compatibility is not established. No test failure was waived on
that basis. Fork source-path assumptions must likewise be checked against the
tenant URL prefix when implementing forks.

The reference protocol inspected during design is
`durable-streams/durable-streams` revision
`461b40267aabd644558f9b19dbb9507dd5f691cf`; Electric release 0.1.5 is a separate
implementation pin, not a protocol version. Suite 0.3.5 is also the existing Go
repository's conformance pin. The research handoff did not declare this suite a
complete acceptance contract or exempt forks. These results quantify gaps; they
do not justify calling the smaller implementation conformant.

## SSE: distinguish framing from a stopping-condition artifact

The private run passed 24 of 29 upstream `SSE Mode` cases. Five failed:

* One requires `Cache-Control` to contain `no-cache`; the server sends `no-store`.
* One compares raw `data:line1` spelling, while the server emits `data: line1`.
  The separator is valid SSE and protects payload-leading spaces from parser
  stripping. Reverting to unconditional `data:` would lose those payload bytes.
* Three injection cases stop before the actual control event. The pinned helper
  searches for `event: control` anywhere in accumulated bytes, then stops after
  the next blank-line boundary. That string intentionally occurs inside these
  data payloads. With separate data/control chunks, the data-event terminator
  satisfies its stopping condition; the genuine control has not been consumed.

`tests/sse.py` now independently parses complete events before stopping. Live
CRLF, CR-only and JSON fixtures preserve their literal attack strings as one
data event, followed by one real control with the correct stored-byte frontier
and no injected field. `sse-parser-boundary2-history.jsonl` and its result retain
the successful run on the unchanged server. This does not rewrite or turn green
the upstream failed tests; it supports a narrower diagnosis.

The first parser-boundary run failed its **own fixture model**: it reserialized
JSON compactly when estimating the wire length, whereas Electric framing retains
lexical bytes inside each value. The observed frontier was 70, not the model's
69. Its failed history/result are retained as `sse-parser-boundary-*`. The model
now extracts raw JSON spans with Python's independent decoder. A regression
checks whitespace, escaped Unicode, decimal spelling, nested arrays and exponent
spelling; all 21 Python tests pass. Expected frontiers are derived from submitted
bytes, never from response metadata.

See `tests/conformance/README.md` for reproducible installation and private-cluster
execution. The default manifests remain ClusterIP-only; the temporary local test
NodePort must not be exposed publicly and should be removed after testing.

## Fail-closed header boundary after the baseline

Source `30a541ec75f35b49f58e7d08b9dd3a8f5c8312c5` rejects PUT fork and
absolute-expiry headers and POST `Stream-Seq` with 501 before admission or body
extraction. `header-guard-images.json` identifies all four updated k3d pods;
`header-guard-history.jsonl` and `header-guard-result.json` retain the passing
HTTP regression. Rejected creates leave no object, rejected ordered appends
change neither bytes nor producer sequence, and unrelated headers still work.
This removes false success for those missing features; it does not implement
them or turn the failing conformance baseline into a pass.

## Committed create response mapping

Source `c9b9b4026b99ca3cf7e09fc7b385f67b50c025b4` maps idempotent PUT to 200
and includes the validated content type for creation and retry. It does not
change the replicated transition. `make check` passed. On actual k3d, the
unmodified suite's three selected PUT header/idempotency/config-conflict tests
passed (`create-response-conformance.txt`); the other 329 were not executed by
that targeted command. This is not a new full-suite baseline.

The first post-rollout regression stopped on a GET returning 503 `no leader;
retry`, after successful create/retry/conflict cases. Its history is retained
as `create-response-history.jsonl`, and its empty result file records that the
runner never reached success. After further cluster observation, a new path
passed all cases (`create-response-converged-*`). No automatic retry hides the
original failure. The failure occurred shortly after rollout and vote timeouts
were observed, but its precise cause is not established.

`create-response-images.json` was another empty capture caused by the incorrect
`app=chronicle` selector. `create-response-images-verified.json` uses the actual
`app=chronicle-raft` selector and asserts four results; all four have the same
image digest. The analogous admission-stage empty capture is also disclosed in
`ADMISSION.md`; it must not be cited as live image-identity evidence.

An additional observation remains open: drained node 4 is still running and its
local groups retain obsolete joint membership and repeatedly become candidates,
while strict control reports completed placements with voters 1/2/3 and node 4
draining (`drained-node-candidate-status.json`, `create-response-control.json`).
These are ordinary Raft elections, not the default-off balancing experiment.
Their availability/resource impact has not been qualified; do not infer useful
leadership convergence or completed physical node removal from placement alone.

## Refreshed full baseline after fail-stop deployment

`conformance-fail-stop-64004.json` and `.txt` retain an unmodified full-suite run
against all five `chronicle-raft:fail-stop` pods (source
`6393655db6d9f1c164e696dcd25c006b74f3b261`). It reports **164 passed, 162 failed,
6 skipped**. This is not a compatibility pass. Unsupported headers now reject
instead of silently succeeding, so these counts are not directly comparable to
the earlier silently-ignored-header baseline. Ordinary POST/close-only success
status mapping is a concrete next fix; TTL, forks, sequence ordering, metadata,
incarnation compatibility and the documented SSE-helper issues remain separate.

## Committed ordinary append and close-only status

Source `f96a9b37d37c590117b64ba8f76c2f414c3e3333` changes only successful HTTP
status mapping: ordinary append/empty close-only are 204; fresh producer data is
200; duplicates remain 204. The committed transition and retained outcomes are
unchanged. The mapping was recorded in `formal/README.md` first. `make check`
passed (`post-status-check.txt`). The new HTTP matrix failed on the old server's
200 (`post-status-before-*`) and passed on the new image (`post-status-after-*`),
checking exact bytes/frontiers, both producer modes and producer duplicates.
Five uniform-image pods are recorded in `post-status-pods.json`.

The full unmodified rerun `conformance-post-status-64007` reports **169 passed,
157 failed, 6 skipped**. Five tests changed from failure to pass and none from
pass to failure: POST response headers, matching content type, and three
property tests for concurrent-reader byte consistency, random operations and
read-your-writes. This remains a failing conformance baseline. Successful
close-only status does not implement its missing closure response headers or
non-producer repeated-close semantics.

## Closure metadata captured during apply

Source `f4764d7` captures closure and error frontiers during replicated apply,
including retained old-sequence success after closure. It does not reconstruct
closure from changed retry flags or a later read. Empty producer retries reach
dedup lookup before fresh-empty validation. Missing `empty_body` in historical
commands preserves replay semantics. POST JSON `[]` remains rejected; the first
draft fixture mistakenly expected acceptance and was corrected against the
unchanged pinned encoder and upstream test, not by weakening either.

`make check` and `make formal` passed (`write-reply-final-check.txt`,
`write-reply-formal.txt`). The closure publication model precedes implementation;
both request-echo and later-state negative controls fail as intended. This is a
bounded boundary model, not mechanized refinement. Snapshot/reopen regressions
check retained results and closure. `write-reply-before-*` retains the old-image
failure; `write-reply-after-*` passes the expanded HTTP matrix on the new image.
`write-reply-pods.json` records all five same-image pods after a stop-all/start-all
upgrade preserving PVCs.

The unchanged full suite `conformance-write-reply-64011` reports **184 passed,
142 failed, 6 skipped**: 15 closure tests improve, with no new failures compared
with the preceding full run. Producer response metadata still prevents two
producer-close tests from passing. Forks, TTL, sequence ordering and the other
documented gaps remain; this is not full conformance or a new durability proof.

## Browser response headers

Source `794a9e9` adds the Go server's `nosniff` and cross-origin resource-policy
headers outside stream admission and extraction. It does not consume the body,
change status, or grant authorization. `browser-headers-check.txt` records passing
checks, including an unfinished streaming body retaining admission and 204/413/
429/501 responses. Independent scoped review found no blocker in this wrapper or
the closure change; its wording correction distinguishes HTTP admission from
Raft proposal.

`browser-headers-after-*` passes the live matrix; `browser-headers-pods.json`
records five uniform-image pods on retained PVCs. The unchanged full suite
`conformance-browser-headers-64013` reports **190 passed, 136 failed, 6 skipped**:
six header tests improve, with no new failures. All compatibility and durability
limitations above still apply. The listener remains trusted-private only.

## Producer position captured during apply

Source `60d0bae` adds captured epoch/highest-sequence response metadata without
changing commands or snapshots. A retained retry still returns its original
frontier; its sequence header instead reports the highest accepted sequence at
retry apply. Fenced epochs return 403; an existing producer's invalid epoch
upgrade returns 400; ordinary gaps remain 409 with expected/received metadata.
No rejected tuple is consumed. Snapshot/reopen and property tests distinguish
these positions, including after closure and after a subsequent epoch change.

Formal-first source `64ddd1a` extends `WriteReply` to 504 bounded states, with
two producer-publication negative controls. Its first draft mixed a string
sentinel and integers, causing TLC evaluation error 75; the retained draft
error is not a successful check or intended counterexample. The corrected
`make formal` passed. `producer-reply-check.txt` and the state-machine rerun
record passing Rust checks. Independent scoped review found no blocker.

`producer-reply-before-*` retains the missing-header failure on the prior image;
`producer-reply-after-*` passes the extended live HTTP matrix. All five image
identities are retained in `producer-reply-pods.json`. The unmodified full suite
`conformance-producer-reply-64016` reports **200 passed, 126 failed, 6 skipped**:
ten producer/closure tests improve, with no new failures. Full compatibility
remains unfinished, including producer input validation outside this change.

## Stream-wide ordering committed with application state

Source `5c31d0e` implements optional per-incarnation `Stream-Seq`, compared by
bytes, not numerically. Empty and absent differ; absent leaves the token intact.
Fences/cached-success lookup retain precedence, and conflicts consume neither
sequence nor epoch. Payload, producer state and token share the existing durable
apply transaction and snapshot. Metadata bounds include the token. Missing
fields in legacy commands/snapshots default absent; mixed versions and downgrade
after token-bearing writes are unsupported.

Formal-first `2532b19` includes Lean byte-order/transitivity/preservation proofs
and a 98-state bounded TLC check. The first model accidentally excluded
duplicates through an unparenthesized conjunction; its negative control failed
to find a counterexample. That failed run is retained as `stream-order-formal-draft`
and `stream-order-draft-missed-duplicate`, not counted as success. After fixing
the model, both numeric-order and duplicate-update mutations produce actual
counterexamples. All `make formal` checks passed before implementation.

`stream-order-check-reviewed.txt` records passing fmt/clippy/tests/docs, including
snapshot/install/reopen, legacy replay, rejected epoch changes, capacity and
rank-model property tests. Independent review found first-value-only header
parsing could ignore a malformed second field. The implemented fix rejects
repeated fields; a follow-up review closed that blocker. Live raw HTTP tests use
separate header fields, verify rejection leaves the tuple available, and exercise
all four shards through three pinned-pod ingresses. Stable before/after status
classifies direct/forwarded paths; it is not per-request leadership evidence.
These schema-3 histories are contract fixtures, not Porcupine histories.

All five retained-PVC pods run `chronicle-raft:stream-order`; image/source identity
is in `stream-order-image-identity.txt` and `stream-order-pods.json`. The extended
HTTP matrix passed. The unchanged full suite `conformance-stream-order` reports
**213 passed, 113 failed, 6 skipped**: thirteen improvements, no new failures
relative to `conformance-producer-reply-64016`. Full compatibility remains failing.
