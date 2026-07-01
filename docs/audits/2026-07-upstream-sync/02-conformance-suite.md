# Audit — Server conformance test suite (`@durable-streams/server-conformance-tests`)

**Audit date:** 2026-07-01
**Upstream suite HEAD / latest published:** `0.3.5`
**Chronicle pin:** `0.3.5` — [`test/conformance/package.json`](../../../test/conformance/package.json)

## Bottom line

**Chronicle is on the current suite and runs a strict superset of the reference
harness.** It pins `0.3.5` (the latest published version and upstream HEAD),
gates it in CI, and — unlike the reference caddy-plugin's own harness — runs it
with `subscriptions: true`, exercising the reserved `__ds` conformance block the
reference skips. No newer suite version exists to adopt. Findings are hygiene /
robustness improvements.

### Method

- Upstream `packages/server-conformance-tests/package.json@main` → `version: 0.3.5`;
  matches the top of the upstream `CHANGELOG.md`.
- Inventoried the compiled suite (`src/index.ts@main`, 11,466 lines): **48
  `describe` groups, 318 `it`/`test` cases**, including the recently-added groups
  (see below).
- Cross-checked Chronicle actually implements the behaviors the newest suite
  versions assert (SSE close-race, sub-offset forks, fork content-type inherit,
  sliding TTL, subscriptions).

## What the current suite covers (and Chronicle's status)

| Suite area (upstream `CHANGELOG`) | Version | Chronicle |
|---|---|---|
| SSE close-race: final append delivered before `streamClosed` control | 0.3.5 (#376) | Implemented — [`handler_sse.go:157-188`](../../../handler_sse.go) (drain-before-close branch + explanatory comment) |
| Fork content-type mismatch must not leak a source refcount | 0.3.5 (#376) | Implemented — content-type checked before ref taken in Go **and** Lua (see Caddy-divergence report FORK section) |
| Reserved subscription APIs (`__ds`) | 0.3.2 (#361) | Implemented + **enabled in the harness** (`subscriptions: true`) |
| Fork PUT inherits source content-type when header omitted | 0.3.1 (#342) | Implemented |
| TTL sliding-window renewal (reset on read + write) | 0.3.0 (#321) | Implemented — sliding-TTL in Lua + Go (`store/redis/store.go`, `store.Read` refreshes TTL) |
| Stream forking (refcount, soft-delete, cascade GC) | 0.3.0 (#312) | Implemented |
| Idempotent producers | 0.1.7 (#140) | Implemented |
| Browser-security headers / CRLF-injection / status-code standardization | 0.1.6 | Implemented |

## Findings

### CONF-1 — Chronicle exercises the subscription conformance block that the reference does not

- **Severity:** informational (Chronicle ahead of reference)
- **Chronicle:** [`test/conformance/conformance.test.ts:14`](../../../test/conformance/conformance.test.ts)
  → `runConformanceTests({ baseUrl, subscriptions: true })`.
- **Reference:** `packages/caddy-plugin/test/conformance.test.ts` →
  `runConformanceTests(config)` with **no** `subscriptions` flag, so the
  `describe.runIf(options.subscriptions)("Reserved subscription APIs")` block
  (upstream `src/index.ts` ~line 11090) is **skipped** in the reference's own CI.
- **Why it matters:** Chronicle is the stricter of the two implementations here.
  No action for Chronicle; recorded so the audit trail is complete and as a
  candidate to upstream (enable the block in the reference harness).

### CONF-2 — Suite/spec version pin is not consolidated or CI-drift-checked

- **Severity:** hygiene / governance
- **Chronicle:** the certified suite version (`0.3.5`) lives in
  `test/conformance/package.json`; the spec SHA lives in `docs/spec/README.md`;
  the pass-count is not recorded anywhere machine-checkable. There is **no
  `SPEC_VERSION.md`**, despite [`docs/research/06-ecosystem.md`](../../research/06-ecosystem.md)
  §1 explicitly recommending one (and citing the Rust server's precedent of
  pinning both SHA + suite version).
- **Why it matters:** "what spec + suite version is Chronicle certified against,
  and at what pass count" is the single most-asked interop question, and it is
  currently spread across two files with no drift alarm.
- **Remediation:** add `SPEC_VERSION.md` at repo root recording (a) the vendored
  `PROTOCOL.md` upstream SHA, (b) the exact conformance suite version, (c) the
  certified pass count ("N/N at 0.3.5"). Optionally a CI step that fails if
  `package.json`'s pinned version and `SPEC_VERSION.md` disagree.

### CONF-3 — Conformance is gated on the default (memory-ish) path only; subscription hardening isn't conformance-covered

- **Severity:** improvement (coverage)
- **Chronicle:** CI runs the suite against a single live server with default
  config ([`.github/workflows/ci.yml:192-227`](../../../.github/workflows/ci.yml)).
  The suite is black-box and cannot see Chronicle's crash-hardening (fencing,
  leases, recovery sweep) — that is validated separately by Jepsen and the
  differential/property tests, **not** by the conformance run.
- **Why it matters:** this is expected (the suite only asserts wire behavior), but
  it means "conformance is green" does **not** imply the subscription control
  plane survives the four crash windows. That coverage boundary should be explicit
  so a green suite is not over-read.
- **Remediation:** documentation only — note in the conformance runbook that the
  suite validates protocol semantics, while durability/fencing invariants are
  owned by `jepsen/` and the differential suites. (No code change.)

## Primary sources

- Chronicle: `test/conformance/conformance.test.ts`, `test/conformance/package.json`, `.github/workflows/ci.yml`, `handler_sse.go`
- Upstream: `packages/server-conformance-tests/{package.json,CHANGELOG.md,src/index.ts}` @ `82f9963`; `packages/caddy-plugin/test/conformance.test.ts`
- Prior art: `docs/research/06-ecosystem.md` §1
