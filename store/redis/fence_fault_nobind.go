//go:build fence_fault_nobind

package redis

import (
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Fault-injection control (#183, design §H.3). This build disables exactly one
// mechanism: fence_bind's producer-binding HSET. An accepted fenced-class
// write still records wfLastOff, but wfbind:<producer_id> is never written, so
// a later open-class write naming that producer id is not refused as bound.
//
// Under `-tags fence_fault_nobind` the extension conformance test
//
//	WF-16 an open write naming a bound producer id is 409 bound
//
// (test/conformance-ext/write-fencing.test.ts) MUST fail. The tag is off by
// default: the shipped build never compiles this file, and the scripts load
// unmodified from scripts/*.lua.
//
// The fault is cut out of common.lua's real fence_bind (the same pattern as
// fence_fault_noseal): the binding write is textually removed from the shared
// prelude, and a prelude where the site no longer matches panics at init, so
// the fault can never drift away from the shipped function silently.
func init() {
	// The producer-binding half of fence_bind in scripts/common.lua, verbatim.
	const bindWrite = `  hset[#hset + 1] = 'wfbind:' .. producer_id
  hset[#hset + 1] = f_gen`
	appendScript = loadScriptNoBind("append.lua", bindWrite)
	closeScript = loadScriptNoBind("close.lua", bindWrite)
}

// loadScriptNoBind is loadScript with the producer-binding write stripped from
// the shared prelude the script body calls into.
func loadScriptNoBind(name, bindWrite string) *redis.Script {
	prelude, err := scriptFS.ReadFile("scripts/common.lua")
	if err != nil {
		panic(fmt.Sprintf("chronicle redis: embedded common.lua missing: %v", err))
	}
	faulted := strings.Replace(string(prelude), bindWrite,
		"  -- fence_fault_nobind: the producer-binding HSET is disabled; wfLastOff kept.", 1)
	if faulted == string(prelude) {
		panic("fence_fault_nobind: binding write site not found in common.lua — realign the fault with fence_bind")
	}
	body, err := scriptFS.ReadFile("scripts/" + name)
	if err != nil {
		panic(fmt.Sprintf("chronicle redis: embedded script %s missing: %v", name, err))
	}
	return redis.NewScript(faulted + "\n" + string(body))
}
