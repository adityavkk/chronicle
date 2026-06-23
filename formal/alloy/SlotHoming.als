/*
 * SlotHoming.als -- INV-JEP-T5-01 (issue #40, Alloy relational model).
 *
 * No cross-subscriber leakage under slot-homing: for every stream path p, the
 * implementation's bitmap-gated S-slot scatter-gather subscriber set EQUALS the
 * independent reference set (every subscriber linked to p), AND equals a
 * brute-force union over ALL S per-slot fan-out shards -- Foreign == {},
 * Missing == {}, BruteDiffer == {} (jepsen/checker/check_slot.go
 * computeSlotLeakage). A wake for p reaches EXACTLY p's subscribers.
 *
 * The mechanism (webhook/keys.go + redis_store.go):
 *   - each subscriber s homes to ONE slot  home(s) = fnv32a(baseSubID) % S
 *     (slotOf, keys.go:78). Modeled as a total function Sub -> Slot.
 *   - a link of s to p does:  SADD streamSubs(home(s), p) s     (indexStream)
 *     so the per-slot shard streamSubs(h, p) holds exactly the linked subs that
 *     home to h.
 *   - and SETBIT occupied(p) home(s) 1     (the occupied-slots bitmap). The bit
 *     is set on EVERY link and NEVER cleared on deindex (keys.go:168), so the
 *     bitmap is always a SUPERSET of the truly-occupied slots.
 *   - OnStreamAppend SCATTER-GATHERS: union the shards streamSubs(h, p) over the
 *     bitmap-marked slots h only (it skips unmarked slots for efficiency).
 *
 * The leak-freedom rests on the bitmap being a superset of the occupied slots:
 * if a truly-occupied slot were ever UNMARKED, the scatter-gather would MISS its
 * subscribers (a Missing leak). We model a possibly-STALE/torn bitmap (any
 * superset of the occupied set, OR -- to find the bug -- an arbitrary set) and
 * check that AS LONG AS the bitmap covers the occupied slots, scatter == ref ==
 * brute, for ALL homing functions and link topologies up to a scope.
 */
module SlotHoming

sig Sub  {}
sig Path {}
sig Slot {}

/*
 * home: each subscriber homes to exactly ONE slot (slotOf is total and
 * deterministic). `home in Sub -> one Slot` makes it a total function.
 */
one sig World {
  home  : Sub -> one Slot,
  links : Sub -> Path,          // the canonical: which subs are linked to which paths
  bitmap: Path -> Slot          // the per-path occupied-slots bitmap (marked slots)
}

// The subscribers linked to path p (the independent REFERENCE set).
fun reference[p: Path]: set Sub { World.links.p }

// The slots TRULY occupied for p: the homes of p's linked subscribers.
fun occupiedSlots[p: Path]: set Slot { World.home[reference[p]] }

// The per-slot fan-out shard streamSubs(h, p): the linked subs of p homing to h.
fun shard[h: Slot, p: Path]: set Sub { reference[p] & World.home.h }

// BRUTE-FORCE gather: union the shards over ALL S slots (ignores the bitmap).
fun bruteGather[p: Path]: set Sub { { s: reference[p] | World.home[s] in Slot } }

// SCATTER-GATHER as the implementation does it: union the shards over the
// BITMAP-MARKED slots only.
fun scatterGather[p: Path]: set Sub {
  { s: Sub | some h: World.bitmap[p] | s in shard[h, p] }
}

/*
 * BitmapCoversOccupied: the modeled correctness precondition the implementation
 * maintains -- the bitmap is a SUPERSET of the truly-occupied slots (set on every
 * link, never cleared). A stale bit (marked but now-empty slot) is allowed; a
 * MISSING bit (occupied but unmarked) is what would cause a leak.
 */
pred BitmapCoversOccupied {
  all p: Path | occupiedSlots[p] in World.bitmap[p]
}

// ---- headline assertions (check => holds for ALL topologies in scope) ----

// (1) No leakage: when the bitmap covers the occupied slots, the scatter-gather
// set EQUALS the independent reference set for every path -- Foreign == {} and
// Missing == {}.
assert NoLeakageWhenCovered {
  BitmapCoversOccupied =>
     (all p: Path | scatterGather[p] = reference[p])
}

// (2) Scatter equals the brute-force union over all S slots (BruteDiffer == {}):
// the bitmap gating never drops a subscriber a full all-slots scan would find.
assert ScatterEqualsBrute {
  BitmapCoversOccupied =>
     (all p: Path | scatterGather[p] = bruteGather[p])
}

// (3) Reference always equals brute (sanity: the brute gather is a faithful
// reference, independent of the bitmap) -- holds unconditionally.
assert ReferenceEqualsBrute {
  all p: Path | reference[p] = bruteGather[p]
}

check NoLeakageWhenCovered for 6
check ScatterEqualsBrute   for 6
check ReferenceEqualsBrute for 6

/*
 * THE NEGATIVE / DIAGNOSTIC DIRECTION: if the bitmap does NOT cover the occupied
 * slots (a missing bit -- the bug the never-clear-on-deindex discipline prevents),
 * a Missing leak CAN occur. We `run` for a witness that a missing bitmap bit
 * causes scatterGather to MISS a real subscriber (scatter strictly smaller than
 * reference). SAT here confirms the precondition in (1)/(2) is LOAD-BEARING --
 * the leak-freedom genuinely depends on the bitmap-covers-occupied discipline,
 * so the assertions are not vacuously true.
 */
pred MissingBitCausesLeak {
  not BitmapCoversOccupied
  some p: Path | scatterGather[p] != reference[p]
}
run MissingBitCausesLeak for 6

// Non-vacuity witness: a genuine multi-slot fan-out exists (subscribers to one
// path homing to DIFFERENT slots, all correctly gathered). SAT => the scatter
// path is exercised over a real scatter (not a degenerate single-slot case).
pred MultiSlotFanoutWitness {
  BitmapCoversOccupied
  some p: Path | #occupiedSlots[p] > 1        // p's subs span >1 slot
  all p: Path | scatterGather[p] = reference[p]
}
run MultiSlotFanoutWitness for 6
