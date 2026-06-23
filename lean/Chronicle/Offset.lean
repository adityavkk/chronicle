/-!
# Stream offsets (typed transcription)

Faithful typed transcription of `Offset`, `Compare`, and `Offset.Add`
from `store/offset.go` (Chronicle repo, this worktree). This is the
P0.5 skeleton: total Lean functions mirroring the Go. No theorems are
stated here; the `LinearOrder` / total-order proof for `compare`
(INV-OFF-01) lands in the P1.1 pure-core-proofs issue.

## uint64 correspondence

Go `Offset.ReadSeq` and `Offset.ByteOffset` are Go `uint64`. They are
modeled here as Lean `UInt64` (NOT `Nat`), because Go `uint64` addition
**wraps on overflow** and `Offset.Add` is plain `+`. `UInt64`
arithmetic in Lean wraps mod 2^64 exactly as Go does, so `add`
reproduces the overflow behaviour; modeling the fields as `Nat` would
saturate-by-growth instead of wrapping and would diverge the moment a
proof or oracle touches the top of the domain.
-/

namespace Chronicle.Offset

/-- A position within a stream.

Mirror of `Offset` in `store/offset.go`: a pair of Go `uint64` modeled
as `UInt64`. -/
structure Offset where
  readSeq    : UInt64  -- ReadSeq: for future log rotation support
  byteOffset : UInt64  -- ByteOffset: bytes of actual data (not framing)
  deriving Repr, DecidableEq, Inhabited

/-- `compare a b` is the lexicographic comparison on `(readSeq, byteOffset)`,
returning `-1` if `a < b`, `0` if `a == b`, `1` if `a > b`.

Branch-for-branch transcription of `Compare(a, b Offset) int` in
`store/offset.go`: compare `readSeq` first, then `byteOffset`, returning
`{-1, 0, 1}`. -/
def compare (a b : Offset) : Int :=
  if a.readSeq < b.readSeq then
    -1
  else if a.readSeq > b.readSeq then
    1
  else if a.byteOffset < b.byteOffset then
    -1
  else if a.byteOffset > b.byteOffset then
    1
  else
    0

/-- `add o bytes` raises `byteOffset` by `bytes` and leaves `readSeq`
untouched.

Transcription of `(o Offset) Add(bytes uint64) Offset` in
`store/offset.go`. The addition is `UInt64` and **wraps on overflow**,
mirroring Go `uint64` `+`. -/
def add (o : Offset) (bytes : UInt64) : Offset :=
  { readSeq := o.readSeq
  , byteOffset := o.byteOffset + bytes }

end Chronicle.Offset
