# Audit — Durable Streams protocol specification (vendored spec vs upstream HEAD)

**Audit date:** 2026-07-01
**Upstream repo:** [`durable-streams/durable-streams`](https://github.com/durable-streams/durable-streams)
**Upstream `main` HEAD at audit time:** `82f9963ae0b489566352393be9b4796c788c99c2` (2026-06-03)
**Chronicle vendored-spec pin:** `82f9963…` (recorded in [`docs/spec/README.md`](../../spec/README.md), "fetched 2026-06-10")

## Bottom line

**No spec drift.** Upstream `main` HEAD *is* the commit Chronicle already pins.
Both vendored documents are byte-for-byte identical to upstream HEAD, with a
single deliberate local addition (see finding SPEC-1). There is nothing in the
protocol text to port. The findings below are governance/hygiene items, not
compliance gaps.

### Method

- `docs/spec/PROTOCOL.md` diffed against the same file at both upstream `main`
  and the pinned SHA `82f9963`. Both diffs are empty except one added line.
- `docs/spec/IMPLEMENTATION_TESTING.md` diffed against upstream `main` — **identical**.
- HEAD confirmed to equal the pin two independent ways: (a) `PROTOCOL.md@main`
  == `PROTOCOL.md@82f9963`; (b) `packages/server-conformance-tests/package.json@main`
  reports `0.3.5`, matching the top entry of the upstream `CHANGELOG.md`.

## Findings

### SPEC-1 — Chronicle's vendored `PROTOCOL.md` carries a local, non-upstream clarification (INV-DIFF-03)

- **Severity:** hygiene / provenance
- **Chronicle:** [`docs/spec/PROTOCOL.md:317`](../../spec/PROTOCOL.md) — an added
  bullet **"Lex-safe client precondition (INV-DIFF-03)"** explaining that
  `Stream-Seq` is compared byte-wise, so clients MUST choose lexicographically
  monotonic values (fixed-width zero-padded decimals / ULIDs), and that an
  unpadded decimal counter is unsafe (`"10" < "9"` byte-wise).
- **Upstream:** this bullet does **not** exist at `82f9963` — the vendored file is
  therefore *not* a pristine copy.
- **Why it matters:** the file is presented as "Vendored specs … copied from
  upstream." A local edit inside a vendored artifact silently breaks the "diff to
  re-sync" workflow the repo relies on (`README.md`: "when upstream changes, diff
  those files and port") — a future re-vendor will show a phantom conflict on
  line 317.
- **Note — this is a genuinely good clarification.** It is the client-side dual of
  the offset-width footgun the project already tracks (LB-1 / LB-2, ADR-0003).
- **Remediation (pick one):**
  1. **Upstream it.** Open a PR against `durable-streams/durable-streams` adding
     INV-DIFF-03 to the canonical `PROTOCOL.md` (§ on `Stream-Seq` / conditional
     append). Once merged and re-vendored, the local delta disappears.
  2. **Relocate it.** Move the clarification out of the vendored file into a
     Chronicle-authored companion note (e.g. `docs/spec/CHRONICLE-NOTES.md` or an
     ADR), keeping `docs/spec/PROTOCOL.md` a pristine mirror.

### SPEC-2 — Vendored-spec provenance is unversioned and the "fetched" date is drifting

- **Severity:** hygiene / governance
- **Chronicle:** [`docs/spec/README.md:3`](../../spec/README.md) records the commit
  SHA and a hand-written "fetched 2026-06-10" date, but nothing re-verifies it.
- **Why it matters:** the pin happens to still be HEAD today (2026-07-01), but the
  repo has no automated signal for *when it stops being HEAD*. The provenance
  lives in prose in one file; the conformance-suite version lives in a different
  file (`test/conformance/package.json`). See the companion conformance report
  (CONF-2) for the consolidated-`SPEC_VERSION.md` recommendation already written
  up in [`docs/research/06-ecosystem.md`](../../research/06-ecosystem.md) §1.
- **Remediation:** adopt the `SPEC_VERSION.md` proposal from research/06 (single
  file recording spec SHA + conformance version), and/or a small CI check that
  fetches upstream HEAD and warns when the pin falls behind.

## Primary sources

- Vendored: `docs/spec/PROTOCOL.md`, `docs/spec/IMPLEMENTATION_TESTING.md`, `docs/spec/README.md`
- Upstream: `PROTOCOL.md`, `IMPLEMENTATION_TESTING.md` @ `82f9963`
- Prior art: `docs/research/06-ecosystem.md` §1 (pin-by-SHA recommendation), `docs/adr/0003-offset-string-width-migration-lb1.md` (the offset/`Stream-Seq` lex-order failure class INV-DIFF-03 belongs to)
