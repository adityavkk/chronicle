/-!
# Cursor-monotonicity predicate (typed transcription)

Faithful typed transcription of `offsetGreater` from `webhook/state.go`
(Chronicle repo, this worktree). This is the P0.5 skeleton: a total
Lean predicate mirroring the Go branch-for-branch. No theorems are
stated here; the strict-total-order proof (INV-CURSOR-01) lands in the
P1.1 pure-core-proofs issue.

## string correspondence

Go offsets here are opaque, lexicographically-sortable Go `string`s.
Go's `>` on `string` is a **bytewise** (unsigned-byte) lexicographic
comparison. Offset strings in the protocol are ASCII (zero-padded
digits and `_`, plus the sentinels `"-1"` and `""`), so a comparison
over the UTF-8 bytes is the faithful model of Go `>`. `byteCompareGt`
below compares the two strings' UTF-8 byte sequences, exactly matching
Go's semantics rather than relying on Lean's codepoint-level `String`
ordering. -/

namespace Chronicle.OffsetGreater

/-- Bytewise lexicographic strict-greater on two byte lists: unsigned-byte
comparison, the shorter prefix being the lesser. This models Go's `>` on
`string` (a bytewise compare). -/
def byteListGt : List UInt8 → List UInt8 → Bool
  | [], [] => false
  | [], _ :: _ => false       -- a is a strict prefix of b ⇒ a < b
  | _ :: _, [] => true        -- b is a strict prefix of a ⇒ a > b
  | x :: xs, y :: ys =>
    if x > y then true
    else if x < y then false
    else byteListGt xs ys

/-- `byteCompareGt a b` reports whether `a > b` under Go's bytewise
string ordering, comparing the UTF-8 bytes of each string. -/
def byteCompareGt (a b : String) : Bool :=
  byteListGt a.toUTF8.toList b.toUTF8.toList

/-- `offsetGreater a b` reports `a > b` for opaque,
lexicographically-sortable offset strings, treating the protocol's `"-1"`
and `""` beginning sentinels as less than any real offset.

Branch-for-branch transcription of `offsetGreater(a, b string) bool` in
`webhook/state.go`:

* equal ⇒ `false`;
* `b ∈ {"-1", ""}` ⇒ `a ∉ {"-1", ""}`;
* `a ∈ {"-1", ""}` ⇒ `false`;
* otherwise bytewise `a > b`. -/
def offsetGreater (a b : String) : Bool :=
  if a == b then
    false
  else if b == "-1" || b == "" then
    a != "-1" && a != ""
  else if a == "-1" || a == "" then
    false
  else
    byteCompareGt a b

end Chronicle.OffsetGreater
