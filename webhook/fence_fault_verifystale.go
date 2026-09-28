//go:build fence_fault_verifystale

package webhook

import (
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Fault-injection control (#192, WRITE-FENCING.md §9.1). This build disables
// exactly one mechanism: check_write_fence.lua's token-identity predicate, the
// block that compares the token's incarnation, generation, wake, and holder
// with the live claim's. The liveness half (dispatch shape, phase, holder
// flag, lease) is untouched, so the pre-check answers OK for any of the
// server's unexpired write tokens for the subscription while a claim is live,
// whichever claim the token names.
//
// The claim/verify route has no rung behind the pre-check, so it alone goes
// stale: a deposed or predecessor-incarnation token verifies 200. A fenced
// append is still refused by the stream-slot marker (reason marker or sealed
// in place of precheck). Under `-tags fence_fault_verifystale` the extension
// conformance test
//
//	WF-29 verify evaluates a write token claim without a write
//
// (test/conformance-ext/write-fencing.test.ts) MUST fail at its recreated-
// subscription assertion. The tag is off by default: the shipped build never
// compiles this file, and the script loads unmodified from scripts/*.lua.
//
// The fault is cut out of the real script (the pattern of store/redis's
// fence_fault_nobind and fence_fault_noseal): the identity block is textually
// removed, and a script where the block no longer matches panics at init, so
// the fault can never drift away from the shipped predicate silently.
func init() {
	// The identity block of scripts/check_write_fence.lua, verbatim.
	const identity = `if (a_incarnation ~= '' and a_incarnation ~= cfg_inc)
  or gen ~= a_generation or wake ~= a_wake_id
  or a_holder == '' or a_holder ~= claim_holder then
  return { 'FENCED' }
end`
	prelude, err := scriptFS.ReadFile("scripts/common.lua")
	if err != nil {
		panic(fmt.Sprintf("webhook: embedded common.lua missing: %v", err))
	}
	body, err := scriptFS.ReadFile("scripts/check_write_fence.lua")
	if err != nil {
		panic(fmt.Sprintf("webhook: embedded script check_write_fence.lua missing: %v", err))
	}
	faulted := strings.Replace(string(body), identity,
		"-- fence_fault_verifystale: the token-identity predicate is disabled; liveness kept.", 1)
	if faulted == string(body) {
		panic("fence_fault_verifystale: identity block not found in check_write_fence.lua — realign the fault with the script")
	}
	writeFenceScript.script = redis.NewScript(string(prelude) + "\n" + faulted)
}
