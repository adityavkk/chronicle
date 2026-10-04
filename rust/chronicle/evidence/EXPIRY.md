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

Live selection checks and unchanged full conformance must be recorded below
after deployment. Until then the full-suite ledger remains unchanged.
