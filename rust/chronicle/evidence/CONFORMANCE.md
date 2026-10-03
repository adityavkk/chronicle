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
