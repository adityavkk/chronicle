# Upstream-sync audit — 2026-07-01

A point-in-time audit of Chronicle against the reference Durable Streams
implementation, its conformance suite, and the protocol spec.

- **Reference / spec:** `durable-streams/durable-streams` @
  `82f9963ae0b489566352393be9b4796c788c99c2` — the upstream `main` HEAD at audit
  time, which is also the commit Chronicle already pins
  ([`docs/spec/README.md`](../../spec/README.md)).
- **Conformance suite:** `@durable-streams/server-conformance-tests@0.3.5` — the
  latest published version, which Chronicle already pins
  ([`test/conformance/package.json`](../../../test/conformance/package.json)).

## Headline

**Chronicle is current and faithful.** There is no spec drift and no missed
conformance version. The handler, storage-contract, offset/producer,
content-type, expiry, fork, and `__ds` subscription layers are near-line-for-line
ports, and the recent upstream fixes (fork content-type-before-refCount + SSE
close-race from PR #376; auto-claim producer serialization from `92c0821`) are all
already adopted. In several areas (SSRF, spec-conformant backoff, idempotency
hashing, key rotation, `__ds`-reserved-when-disabled) Chronicle is *ahead* of the
reference.

The audit found **one actionable behavioral defect** (DIV-1) and **a handful of
improvements/hygiene items**; everything else is faithful, cosmetic, or already
tracked with armed CI tests.

## Reports

| # | Category | Report |
|---|---|---|
| 01 | Caddy reference divergence | [`01-caddy-divergence.md`](01-caddy-divergence.md) |
| 02 | Conformance test suite | [`02-conformance-suite.md`](02-conformance-suite.md) |
| 03 | Durable Streams specification | [`03-protocol-spec.md`](03-protocol-spec.md) |
| 04 | Other improvements from the reference | [`04-other-improvements.md`](04-other-improvements.md) |

## Actionable items → GitHub

Filed under the epic **"Upstream-sync audit follow-ups"** as stories:

| Finding | Report | Severity | Story |
|---|---|---|---|
| DIV-1 duplicate-append wakes subscribers | 01 | defect | S1 |
| IMP-1 callback-token refresh | 04 | improvement | S2 |
| IMP-2 `StreamRootURL` slash normalization | 04 | robustness | S3 |
| DIV-3/4 + IMP-4 control-plane 4xx alignment | 01/04 | low | S4 |
| SPEC-1 vendored `PROTOCOL.md` local edit | 03 | hygiene | S5 |
| CONF-2/SPEC-2 `SPEC_VERSION.md` | 02/03 | governance | S6 |

Already-tracked / not re-filed: LB-1 offset width
([#46](https://github.com/adityavkk/chronicle/issues/46),
[#27](https://github.com/adityavkk/chronicle/issues/27), ADR-0003), LB-3 ReadSeq
([#48](https://github.com/adityavkk/chronicle/issues/48),
[#32](https://github.com/adityavkk/chronicle/issues/32)). Informational-only:
CONF-1 (Chronicle runs the subscription conformance block the reference skips),
CONF-3 (conformance ≠ durability coverage), DIV-2 (`__ds` reserved when disabled),
DIV-6 (cosmetic fork-offset disjunct).

## How this was produced

Reference source was fetched from `raw.githubusercontent.com` at `main`
(== `82f9963`) and diffed subsystem-by-subsystem against Chronicle. Storage
mechanism (Redis vs memory/file), logging, and Caddy-module-vs-stdlib wiring were
excluded from scoring by design — only externally-observable protocol behavior and
genuine reference improvements were counted.
