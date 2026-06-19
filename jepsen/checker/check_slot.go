package main

import (
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
)

// check_slot.go is the PURE core of T5 — no cross-subscriber leakage under
// slot-homing (07 line 46). It carries two independent things the live driver
// (scenario_slot.go) leans on:
//
//  1. A SELF-CONTAINED MIRROR of the webhook package's {__ds:h} key schema (the
//     keys.go funcs are unexported). Mirroring it here — the same discipline
//     nemesis.go uses for the schedule ZSET — makes the differential genuinely
//     INDEPENDENT: if the harness's mirror of "which slot a sub homes to" ever
//     disagrees with the implementation's scatter-gather, the differential FAILS.
//  2. The CRC16/cluster-slot oracle: two keys share a Redis Cluster slot iff their
//     {hash tag} CRC16s agree. This is the cluster's authority on single-slot, used
//     to assert every key for one sub resolves to ONE slot and that a mis-tagged sub
//     lands in a DIFFERENT slot (CROSSSLOT is DETECTED, not silent).
//
// The differential itself (slotLeakage) is pure: it compares the implementation's
// scatter-gather subscriber set against the independent reference set the harness
// built, plus a brute-force union over all S slots — flagging foreign wakes
// (returned-but-not-linked) and missed subscribers (linked-but-not-returned).

// dsSubSlots mirrors webhook.subSlots — S, the number of {__ds:h} keyspace slots.
const dsSubSlots = 256

// dsSlotOf mirrors webhook.slotOf: h = fnv32a(baseSubID) % S (FNV-1a/32, NOT CRC16),
// stripping a "<id>:g:<n>" claim-granularity suffix so a g-shard homes to its sub's
// slot. The harness recomputes the home slot independently of the implementation.
func dsSlotOf(id string) int {
	base := id
	if i := strings.LastIndex(id, ":g:"); i >= 0 {
		if suf := id[i+3:]; suf != "" && allDigits(suf) {
			base = id[:i]
		}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(base))
	return int(h.Sum32() % uint32(dsSubSlots))
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

func dsSlotTag(h int) string                 { return "{__ds:" + strconv.Itoa(h) + "}" }
func dsSubKey(id string) string              { return "ds:" + dsSlotTag(dsSlotOf(id)) + ":sub:" + id }
func dsLinksKey(id string) string            { return "ds:" + dsSlotTag(dsSlotOf(id)) + ":sub:" + id + ":links" }
func dsLeaseKey(h int) string                { return "ds:" + dsSlotTag(h) + ":sched:lease" }
func dsDueKey(h int) string                  { return "ds:" + dsSlotTag(h) + ":due" }
func dsStreamSubsKey(h int, p string) string { return "ds:" + dsSlotTag(h) + ":stream:" + p }
func dsStreamSlotsKey(p string) string       { return "ds:{__ds-occ}:streamslots:" + p }

// crc16 is the CCITT/XMODEM CRC16 Redis Cluster uses to map a key (or its hash tag)
// to a slot — table-free, so the harness depends on no internal go-redis package.
func crc16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc ^= uint16(s[i]) << 8
		for b := 0; b < 8; b++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// clusterSlot is the Redis Cluster slot a key routes to: CRC16 of the hash tag (the
// non-empty span between the first '{' and the next '}'), else the whole key, mod
// 16384. Two keys are single-slot iff this agrees — the precondition every multi-key
// atomic Lua script depends on.
func clusterSlot(key string) int {
	if i := strings.IndexByte(key, '{'); i >= 0 {
		if j := strings.IndexByte(key[i+1:], '}'); j > 0 {
			key = key[i+1 : i+1+j]
		}
	}
	return int(crc16(key) % 16384)
}

// subKeysOneSlot reports whether every key one subscription touches — its
// config/runtime hash, links hash, and per-slot lease/due schedule — resolves to ONE
// Redis cluster slot (the T5 static precondition for the atomic scripts).
func subKeysOneSlot(id string) (slot int, ok bool) {
	h := dsSlotOf(id)
	keys := []string{dsSubKey(id), dsLinksKey(id), dsLeaseKey(h), dsDueKey(h)}
	slot = clusterSlot(keys[0])
	for _, k := range keys[1:] {
		if clusterSlot(k) != slot {
			return slot, false
		}
	}
	return slot, true
}

// slotLeakage is the T5 differential verdict: given the independent reference set
// (the subscribers the harness linked to a path), the implementation's scatter-gather
// result, and a brute-force union over all S per-slot fan-out shards, it returns the
// foreign ids (returned by the impl but never linked — a cross-subscriber leak) and
// the missing ids (linked but not returned — a dropped subscriber). T5 PASSES iff
// both are empty AND the scatter-gather set equals the brute-force union (the bitmap
// missed no occupied slot). Pure.
type slotLeakage struct {
	Foreign     []string // returned by scatter-gather but not in the reference (foreign wake)
	Missing     []string // in the reference but not returned (dropped subscriber)
	BruteDiffer []string // ids the brute-force all-S union has but scatter-gather missed
}

func (v slotLeakage) clean() bool {
	return len(v.Foreign) == 0 && len(v.Missing) == 0 && len(v.BruteDiffer) == 0
}

func computeSlotLeakage(reference, scatter, brute []string) slotLeakage {
	refSet := toSet(reference)
	scatterSet := toSet(scatter)
	bruteSet := toSet(brute)
	var v slotLeakage
	for id := range scatterSet {
		if _, ok := refSet[id]; !ok {
			v.Foreign = append(v.Foreign, id)
		}
	}
	for id := range refSet {
		if _, ok := scatterSet[id]; !ok {
			v.Missing = append(v.Missing, id)
		}
	}
	for id := range bruteSet {
		if _, ok := scatterSet[id]; !ok {
			v.BruteDiffer = append(v.BruteDiffer, id)
		}
	}
	sort.Strings(v.Foreign)
	sort.Strings(v.Missing)
	sort.Strings(v.BruteDiffer)
	return v
}

func toSet(xs []string) map[string]struct{} {
	m := make(map[string]struct{}, len(xs))
	for _, x := range xs {
		m[x] = struct{}{}
	}
	return m
}
