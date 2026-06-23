package main

// export_test.go is the TEST-ONLY accessor that exposes the checker's
// independent mirror copies of the control-plane fence/slot/ordering predicates
// to the triple-mirror differential (predicate_mirror_test.go, issue #33).
//
// The checker is `package main` and cannot be imported, so its mirrors are
// reached only from within this package's own test binary. Re-exporting them
// here — rather than re-implementing the predicates in the test — keeps the
// checker's copy the single independent reference the differential pins, so a
// translation bug between the checker copy and the Go core / live Lua is what the
// property catches (preserving the independent-copy property).
//
// These are the SAME funcs the live scenario drivers use (check_slot.go's
// dsSlotOf/crc16/clusterSlot/allDigits, model_fence.go's checkerFenced,
// check_cursor.go's offsetGreater), so the differential exercises the shipped
// checker mirror, not a duplicate.

// CheckerDsSlotOf is the checker's FNV-1a/32 home-slot mirror (with the
// :g:<digits> suffix strip).
func CheckerDsSlotOf(id string) int { return dsSlotOf(id) }

// CheckerAllDigits mirrors the checker's allDigits used by the suffix strip.
func CheckerAllDigits(s string) bool { return allDigits(s) }

// CheckerSubSlots is the checker's S (the FNV modulus).
const CheckerSubSlots = dsSubSlots

// CheckerClusterSlot is the checker's table-free CRC16 cluster-slot oracle.
func CheckerClusterSlot(key string) int { return clusterSlot(key) }

// CheckerCRC16 is the checker's table-free CCITT/XMODEM CRC16.
func CheckerCRC16(s string) uint16 { return crc16(s) }

// CheckerSubKeysOneSlot reports whether every key one subscription touches
// resolves to ONE cluster slot (the T5 static precondition).
func CheckerSubKeysOneSlot(id string) (slot int, ok bool) { return subKeysOneSlot(id) }

// CheckerFenced is the checker's independent fence-predicate copy.
func CheckerFenced(curGen, reqGen, tokenGen int64, curWake, reqWake string) bool {
	return checkerFenced(curGen, reqGen, tokenGen, curWake, reqWake)
}

// CheckerOffsetGreater is the checker's offset-order mirror (the order the cursor
// monotonicity checker assumes).
func CheckerOffsetGreater(a, b string) bool { return offsetGreater(a, b) }

// CheckerStaleGenInert reports whether a status is in the checker's inert
// (non-granting) stale-gen vocabulary {FENCED,BUSY,STALE,NOSUB} — the fence-aware
// allowed-status classification (check_stalegen.go).
func CheckerStaleGenInert(status string) bool { return staleGenInert[status] }
