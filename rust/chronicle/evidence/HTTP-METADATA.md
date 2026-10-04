# HTTP metadata compatibility

The refinement contract was recorded in `formal/README.md` before implementation.
No claim that the existing TLA/Lean model proves HTTP parsing is made. `make check`
passed, including legacy/new framing projection through snapshot/install/reopen,
media-type equivalence, stored Create reply metadata and framing-conflict tests.

Oracle review identified two bugs, fixed before deployment: a matching PUT with
different parameters must return the stored Content-Type, captured at apply,
and an explicitly empty POST Content-Type must not satisfy the required header.
The supplemental live test checks both, as well as JSON classification/ranges,
opaque `application/jsonp` bytes and Location authority.

New configs persist framing explicitly. Legacy configs without the field retain
the old prefix-based interpretation. This deliberately does not reinterpret or
repair old misclassified payloads. Stop-all upgrades only; mixed versions and
downgrades are unsupported. Location handling covers ordinary HTTP origin-form
requests, not TLS/public-origin discovery or general absolute-form forwarding.

## Actual k3d verification

Source `6508b4d`, image `chronicle-raft:metadata`, image ID
`sha256:6579814f0c6f136c7b1aae629a11195f35783909894000f58c67b7a22dafd0c8`;
release binary SHA256
`8a8dc979265954fbcadbeeffa9d987457486d2568f5a31f2f2de29be71bceeb4`.
Stopped all five pods, preserved PVCs, upgraded and waited for all five Ready.

`tests/http_metadata.py` **passed through node 4**, verified nonleader for all
five groups in `metadata-nonleader-status.json`. The private NodePort authority
differs from every leader's internal address, so the Location assertion exercises
forwarding rather than only a direct-leader response. Schema-3 observations are
in `metadata-http.jsonl`. The temporary service was deleted after the check.

The unchanged full upstream suite returned **236 passed / 90 failed / 6 upstream
default skips** (`conformance-metadata.{json,txt}`). All eight HTTP metadata
failures passed; the existing recreation/offset/cache/SSE/fork failures remain.
The previously failed randomized concurrent-read property passed this run, but
its earlier barrier-timeout cause is not fixed or excused by a passing rerun.
