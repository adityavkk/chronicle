//go:build fence_fault_nobind

package redis

import (
	"fmt"

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
func init() {
	const shim = `
-- fence_fault_nobind: the producer-binding HSET is disabled; wfLastOff kept.
local function fence_bind(hset, m, has_fence, producer_id, f_gen, off)
  if not (has_fence and m.wf == '1') then return end
  hset[#hset + 1] = 'wfLastOff'
  hset[#hset + 1] = off
end
`
	appendScript = loadScriptShimmed("append.lua", shim)
	closeScript = loadScriptShimmed("close.lua", shim)
}

// loadScriptShimmed is loadScript with a fault shim spliced between the shared
// prelude and the script body: the shim's local declarations lexically shadow
// the prelude's for everything the body calls.
func loadScriptShimmed(name, shim string) *redis.Script {
	prelude, err := scriptFS.ReadFile("scripts/common.lua")
	if err != nil {
		panic(fmt.Sprintf("chronicle redis: embedded common.lua missing: %v", err))
	}
	body, err := scriptFS.ReadFile("scripts/" + name)
	if err != nil {
		panic(fmt.Sprintf("chronicle redis: embedded script %s missing: %v", name, err))
	}
	return redis.NewScript(string(prelude) + "\n" + shim + "\n" + string(body))
}
