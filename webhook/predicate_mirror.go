package webhook

// predicate_mirror.go exports the control-plane fence/slot predicates that are
// MAINTAINED IN THREE INDEPENDENT COPIES (this Go core, the live Lua in
// scripts/common.lua, and the Jepsen checker's reference mirror) so the
// cross-package triple-mirror differential (issue #33) can drive them.
//
// These are thin, allocation-free wrappers over the unexported pure-core funcs
// (slotOf/baseSubID in keys.go; offsetGreater in state.go). FenceDecision is
// ALREADY exported in state.go for exactly this reason ("exists for unit tests
// and must be changed together with fence.lua"); SlotOf/BaseSubID/OffsetGreater
// follow that same established pattern — they are the surface the
// predicate_differential_test.go property and jepsen/checker's slot mirror pin
// against the live Lua and the checker's own independent copy. Exporting them
// does NOT collapse the copies into one shared helper: the whole point of the
// differential is that the Lua and the checker mirrors stay hand-written and
// independent, and a translation bug between any two is what the property
// catches.

// SubSlots (S) is the number of {__ds:h} keyspace slots a subscription homes
// into — the modulus of the FNV-1a/32 home-slot math. Exposed so the slot mirror
// can assert it matches the checker's dsSubSlots.
const SubSlots = subSlots

// SlotOf is the keyspace slot a subscription is homed into: h = fnv32a(baseSubID)
// % SubSlots (FNV-1a/32, DELIBERATELY NOT Redis CRC16). It is the Go-core copy the
// checker's dsSlotOf mirrors; see slotOf in keys.go.
func SlotOf(id string) int { return slotOf(id) }

// BaseSubID strips a "<id>:g:<n>" claim-granularity shard suffix to the base
// subscription id (only when the suffix is ":g:<digits>"). It is the Go-core copy
// the checker's dsSlotOf suffix-strip / allDigits mirror; see baseSubID in keys.go.
func BaseSubID(id string) string { return baseSubID(id) }

// SlotTag is the {__ds:h} hash tag a subscription id is homed under. Exposed so
// the slot mirror can route the PRODUCED tag through live go-redis CRC16 and the
// checker's table-free crc16/clusterSlot and assert they resolve to one slot.
func SlotTag(id string) string { return slotTag(id) }

// OffsetGreater reports a > b for opaque, lexicographically-sortable offsets,
// treating the "-1"/"" beginning sentinel as less than any real offset. It is the
// Go-core copy the live Lua offset_greater and the checker's offsetGreater mirror;
// see offsetGreater in state.go.
func OffsetGreater(a, b string) bool { return offsetGreater(a, b) }
