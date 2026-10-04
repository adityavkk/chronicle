# HTTP metadata compatibility

The refinement contract was recorded in `formal/README.md` before implementation.
No claim that the existing TLA/Lean model proves HTTP parsing is made. `make check`
passed, including legacy/new framing projection through snapshot/install/reopen,
media-type equivalence, stored Create reply metadata and framing-conflict tests.

Oracle review identified two bugs, fixed before deployment: a matching PUT with
different parameters must return the stored Content-Type, captured at apply,
and an explicitly empty POST Content-Type must not satisfy the required header.
The supplemental live test checks both, as well as JSON classification/ranges,
opaque `application/jsonp` bytes and Location authority. Live results belong
below after execution; the full-suite ledger is not changed by local tests.

New configs persist framing explicitly. Legacy configs without the field retain
the old prefix-based interpretation. This deliberately does not reinterpret or
repair old misclassified payloads. Stop-all upgrades only; mixed versions and
downgrades are unsupported. Location handling covers ordinary HTTP origin-form
requests, not TLS/public-origin discovery or general absolute-form forwarding.
