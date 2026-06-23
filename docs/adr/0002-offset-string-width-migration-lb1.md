# ADR-0002: Offset.String() `%016d` minimum-width inverts lexicographic order at a field ≥ 10^16 (LB-1) — migration decision

- **Status:** Proposed
- **Date:** 2026-06-22
- **Deciders:** @adityavkk
- **Surfaced by:** [#27](https://github.com/adityavkk/chronicle/issues/27) (LB-1 unguarded Compare-vs-strcmp rapid property + checked-in counterexample fixture)
- **Epic:** [#25](https://github.com/adityavkk/chronicle/issues/25) · **Track:** `property-based-testing`
- **Discharges (the DECISION half of):** `INV-OFF-02`
- **Tracking issue:** _to be filed_ — this ADR is the in-repo record of the finding; file a GitHub issue referencing it so the wire/persisted-format change is scheduled and reviewed on its own, never bundled into a verification pass.

## Context

This ADR is the tracked **migration-decision finding** spun out of #27 so the
wire/persisted-format change is never bundled into verification work. #27
SURFACEs and DOCUMENTs the latent bug LB-1; it deliberately does **not** change
the format. This ADR holds the decision.

### The bug (LB-1)

`store.Offset.String()` ([`store/offset.go`](../../store/offset.go)) renders the
two `uint64` fields as `fmt.Sprintf("%016d_%016d", ReadSeq, ByteOffset)`.
`%016d` is a **minimum** field width, not a maximum: it zero-pads to 16 digits
but does not cap. Max `uint64` is `18446744073709551615` (20 digits), so the
instant a field reaches `10^16` the rendered field grows past 16 chars and the
zero-padding stops keeping every string the same length.

| numeric value | `String()` rendering | length |
|---|---|---|
| `9999999999999999` (10^16 − 1) | `9999999999999999` | 16 |
| `10000000000000000` (10^16) | `10000000000000000` | 17 |

Bytewise, `"10000000000000000"` sorts **before** `"9999999999999999"`
(`'1' < '9'` at position 0), so the larger numeric offset sorts *earlier* as a
string. `Offset.Compare` is correct (it compares the numeric fields), so the Go
core and any string-order consumer disagree in the `>= 10^16` region.

**Where it bites:** Redis stores stream frames as ZSET members keyed by the
offset string (`encodeFrame` in [`store/redis/keys.go`](../../store/redis/keys.go),
which also hardcodes `offsetStrLen = 33`), and the read path range-scans them
with `ZRANGEBYLEX` ([`store/redis/scripts/read.lua`](../../store/redis/scripts/read.lua)).
Read correctness rests on lexicographic member order equalling numeric frame
order. Past a `10^16`-byte `ByteOffset` (~10 PB on one stream) the ZSET ordering
inverts at the boundary and reads return frames out of order — silent
data-plane corruption, not a crash. The `MemoryStore` oracle never reproduces it
because it reads by comparing the numeric `ByteOffset` field, never the string.

Surfaced and pinned in machine-checked form by #27:

- [`store/offset_property_test.go`](../../store/offset_property_test.go) —
  guarded property `TestOffsetCompareMatchesStrcmpGuarded` passes on the safe
  domain (`< 10^16`), runs every CI build.
- [`store/offset_property_unguarded_test.go`](../../store/offset_property_unguarded_test.go) —
  unguarded property `TestOffsetCompareMatchesStrcmpUnguarded` fails at the
  boundary (build-tag `offset_lb1_surface`-quarantined so default CI stays
  green).
- [`store/offset_width_lb1_test.go`](../../store/offset_width_lb1_test.go) —
  committed counterexample fixture `TestOffsetWidthCounterexampleLB1` asserting
  the known divergence (e.g. `{0, 9999999999999999}` vs `{0, 10000000000000000}`:
  numeric `Compare` says `<`, string compare says `>`).

### Decision drivers

- **Read-path correctness** — lexicographic ZSET member order MUST equal numeric
  frame order for `ZRANGEBYLEX` reads to be sound.
- **Persisted/wire-format stability** — the offset string is both a persisted
  ZSET member key and the wire `Stream-Next-Offset` value; changing it is a
  migration, not a refactor.
- **Practicality** — ~10 PB on a single stream is far beyond any realistic
  workload, which makes a runtime guard a defensible alternative to a true
  format change.

## Decision

**Not yet decided.** This ADR records the two viable resolutions for a separate,
deliberately-scheduled change. Until then the safe domain (`< 10^16`) is pinned
as a passing CI invariant and the unsafe domain is a known, fixtured divergence.

### Option 1 — Widen the rendered field to 20 digits (`%020d_%020d`)

Fully covers the `uint64` domain (max is 20 digits). This is a **persisted-format
migration**: existing ZSET members rendered at width 16 stop sorting correctly
against new width-20 members; `offsetStrLen` (and `framePrefixLn`,
`decodeFrame`, `lexLowerBound`/`lexUpperBoundInclusive` in `store/redis/keys.go`,
plus the Lua read bounds) all change; and the wire `Stream-Next-Offset` value
changes. Requires a migration/compat story for streams created under the old
width (e.g. version the member encoding, or re-key on read).

### Option 2 — Enforce a runtime invariant `ByteOffset < 10^16` (and `ReadSeq < 10^16`)

Keeps the format byte-for-byte identical (`offsetStrLen` stays 33, no Lua/ZSET
encoding touched) and the safe-domain property holds by construction; rejects or
caps offsets that would cross the boundary. Given ~10 PB per stream is
unreachable in practice, this may be acceptable as a guard rather than a true
fix.

### Cross-link: LB-2

The sibling **LB-2** (`Stream-Seq` lexicographic comparison, one layer up — see
`TestIntegrationStreamSeqLexRegression` in
[`store/redis/integration_test.go`](../../store/redis/integration_test.go) and
the `Stream-Seq` handling) shares the same **"string order must match intended
order"** failure class. Whichever option is chosen here should note whether it
generalizes to `Stream-Seq`.

## Consequences

- **Until decided:** `Offset.String()` is unchanged (`%016d_%016d`),
  `offsetStrLen` stays `33`, and no `.lua` is touched. CI documents the safe
  domain as a live invariant and the unsafe domain as a fixtured known
  divergence.
- Whichever option is chosen, the #27 counterexample fixture
  (`offset_width_lb1_test.go`) MUST be updated intentionally as part of that
  change, and the matching machine-checked safe-domain proof belongs to the Lean
  pure-core proofs issue (the proof half of `INV-OFF-02`).
- The change MUST be a deliberate, separately-reviewed PR, not part of a
  verification pass.
