/-!
# Cursor progression (typed transcription)

Faithful typed transcription of `GenerateCursor` / `GenerateResponseCursor` from
`protocol/cursor.go` (Chronicle repo, this worktree). The Go functions take a
`time.Time` clock and format/parse decimal-string cursors; the verification
content is the *integer interval arithmetic*, so this transcription lifts the
clock to an injected `nowMs : Int` (Unix milliseconds) and works on the parsed
integer interval directly. String formatting/parsing is a decimal bijection on
the realised domain and is factored out, exactly as `02-proof-assistants-lean.md`
prescribes ("`omega`/`linarith` on the floor-division").

## int / time correspondence

Go uses `int64` Unix-milli timestamps and `int64` interval numbers; modeled as
Lean `Int` (the floor division and the `clientInterval + jitter` only compare and
add over the realised domain, never relying on `int64` wraparound). The constants
match `protocol/cursor.go`: `CursorIntervalSeconds = 20`, so `intervalMs = 20000`;
`jitterIntervals = max(1, 3600/20) = 180` (the reference middle-of-range jitter).
-/

namespace Chronicle.Cursor

/-- Cursor interval width in milliseconds: `CursorIntervalSeconds * 1000 = 20000`. -/
def intervalMs : Int := 20000

/-- The fixed jitter advance, in intervals:
`jitterSeconds = 1 + (3600-1)/2 = 1800`; `1800 / 20 = 90 > 1`, so the Go code takes
`jitterIntervals = jitterSeconds / CursorIntervalSeconds`. NOTE the Go integer
division: `(3600-1)/2 = 1799` (truncated), `1 + 1799 = 1800`, `1800/20 = 90`.
The issue text's "180" predates the `(max-min)/2` change; the source computes 90.
We transcribe the source. -/
def jitterIntervals : Int := 90

/-- `generateCursor nowMs epochMs` is the interval number at `nowMs`:
`floor((nowMs - epochMs) / intervalMs)`. Mirror of `GenerateCursor`, with the
clock injected as Unix-millis. Lean `Int` division is floor division (matching the
mathematical floor; for the realised `nowMs ≥ epochMs` domain it coincides with
Go's truncated division, and the pre-epoch negative-interval edge is handled by
`Int` floor, which `INV-CUR-02` covers explicitly). -/
def generateCursor (nowMs epochMs : Int) : Int :=
  (nowMs - epochMs) / intervalMs

/-- `generateResponseCursor` over the parsed integer interval. `clientInterval` is
`none` for an empty/invalid client cursor (Go's `clientCursor == ""` or a parse
error), else `some i`. Mirror of `GenerateResponseCursor`:
- no/invalid client cursor ⇒ current interval;
- client behind current ⇒ current interval;
- client at/ahead ⇒ `clientInterval + jitterIntervals`. -/
def generateResponseCursor (clientInterval : Option Int) (nowMs epochMs : Int) : Int :=
  let cur := generateCursor nowMs epochMs
  match clientInterval with
  | none => cur
  | some ci =>
    if ci < cur then cur
    else ci + jitterIntervals

end Chronicle.Cursor
