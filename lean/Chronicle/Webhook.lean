import Chronicle.OffsetGreater

/-!
# Webhook cursor reducer (typed transcription)

Faithful typed transcription of `MergeAcks` from `webhook/state.go` (Chronicle
repo, this worktree), on top of the `offsetGreater` predicate already transcribed
in `Chronicle/OffsetGreater.lean`. `MergeAcks` advances each link's cursor
**forward-only**: an ack moves a link iff its offset is strictly `offsetGreater`
than the stored one.

## correspondence

Go `StreamLink` carries `Path`, `LinkType`, `AckedOffset` (string); only `Path`
and `AckedOffset` participate in the merge, so the model keeps those two. `Ack`
carries `Stream` (the path) and `Offset`. The Go code builds a `byPath` map of
the *last* ack per path, then for each link advances if `offsetGreater(off, cur)`.
The model uses an association-list lookup (`ackFor`) returning the last matching
ack's offset, reproducing "last write wins per path" within one `MergeAcks` call.
-/

namespace Chronicle.Webhook

open Chronicle.OffsetGreater

/-- One stream's durable cursor within a subscription (the merge-relevant fields
of Go `StreamLink`). -/
structure StreamLink where
  path        : String
  ackedOffset : String
  deriving Repr, DecidableEq, Inhabited

/-- A single offset acknowledgment (Go `Ack`: stream path + offset). -/
structure Ack where
  stream : String
  offset : String
  deriving Repr, DecidableEq, Inhabited

/-- Last ack offset for `path` in `acks` (last-write-wins, matching the Go
`byPath` map build which overwrites earlier entries). `none` if no ack targets
`path`. -/
def ackFor (acks : List Ack) (path : String) : Option String :=
  (acks.filter (fun a => a.stream == path)).reverse.head?.map (·.offset)

/-- Apply one round of acks to one link: advance forward-only iff the ack offset
is strictly `offsetGreater` than the stored cursor. Mirror of the per-link body
of `MergeAcks`. -/
def mergeLink (acks : List Ack) (l : StreamLink) : StreamLink :=
  match ackFor acks l.path with
  | some off => if offsetGreater off l.ackedOffset then { l with ackedOffset := off } else l
  | none => l

/-- `mergeAcks links acks` advances each link's cursor forward-only. Mirror of
`MergeAcks(links, acks)` in `webhook/state.go`. -/
def mergeAcks (links : List StreamLink) (acks : List Ack) : List StreamLink :=
  links.map (mergeLink acks)

end Chronicle.Webhook
