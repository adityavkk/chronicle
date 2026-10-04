# Replicated expiry compatibility

The formal expiry model and Lean theorems were committed before implementation.
`make formal` passed again after implementation: bounded TLC positives, expected
negative counterexamples (including removal of the expiry fence), and Lean build.
This does not prove wall-clock accuracy, storage hardware or Rust refinement.

`make check` passed with six expiry-specific tests, including actual SQLite
snapshot/install/reopen, a legacy-format snapshot with a fixed deadline,
timestamp-less log replay, renewal persisted despite append rejection, stale
incarnation/access fences, rollback and arithmetic/parser boundaries. An initial
full check stopped because the separate VFS workspace lockfile needed the new
direct `time` dependency; both lockfiles were updated without version changes,
then the full check passed.

Independent Oracle review found two issues, fixed before deployment: retain both
admission permits during pending live-read renewal, and reject non-RFC3339 date
separators accepted by the time library. Regressions cover both.

Policy: initial strict GET and post-fence append attempts renew; HEAD, explicitly
stale reads, live continuations and idempotent PUT do not. Later-rejected appends
can persist renewal, so such TTL histories require an application-result model,
not a no-effect failure interpretation. Absolute deadlines and legacy fixed
deadlines never slide. Mixed-version operation/downgrade is unsupported.

## Actual k3d run

Source: `c0db066`; image `chronicle-raft:expiry`, ID
`sha256:d71672625782e15b0d89a7e1c9a994f78ee0686dd703f517cbe69d8f9be4efc2`.
Release binary SHA256:
`78e4eceb1cea67b178ce43b09f76a04d453bf5016918b4d42e4ba5466db1bc92`.
All five pods were stopped before the upgrade, preserving existing PVCs, then
all five reached Ready. No mixed-version interval or new genesis was used.

`tests/expiry_http.py` passed against an active replica's temporary private
NodePort: strict GET/error renewal; HEAD/stale GET/PUT nonrenewal; long-poll
initial renewal without continuation renewal. Observations are in
`expiry-http-active-ready.jsonl`. Earlier failed attempts are retained:
`expiry-http.jsonl` assumed a load-balanced stale read reached an active replica
(a retired replica legitimately returned 404), and `expiry-http-active.jsonl`
ran before the new NodePort accepted connections. The successful run waited for
health first. The temporary service was removed afterward.

The unchanged full upstream suite, through the original load-balanced endpoint,
returned **227 passed / 99 failed / 6 upstream-default skips**. Raw reports:
`conformance-expiry.{json,txt}`. Fifteen previous expiry failures passed; implicit
recreation is still unresolved. No compatibility claim beyond these results.

A new random concurrent-read failure is preserved with seed **-1131163861**,
path **15** and its byte-array counterexample in the raw report. It interpreted
222 bytes as stream bytes against a 28-byte upper bound without checking HTTP
status. Correlated server events show 222-byte **503** replies at the relevant
time, from ~200ms leadership-barrier timeouts, not successful data responses
(`expiry-read-errors.jsonl`). This is an availability failure; these observations
do not establish payload corruption or a general durability result. It remains
in the failure ledger rather than being waived or hidden by a passing rerun.
