//go:build fence_fault_noseal

package redis

import (
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Fault-injection control (#183, design §H.3). This build disables exactly one
// mechanism: seal_append_fence.lua's durable seal record. done/release still
// tombstone the claim marker, but no wfseal:<auth> / wfSealGen / wfSealOff is
// ever written — the "seal" is then only the marker's revocation, which
// PEXPIRE reaps.
//
// Under `-tags fence_fault_noseal` the extension conformance test
//
//	WF-19 done closes the generation: the seal is recorded and the old token refused
//
// (test/conformance-ext/write-fencing.test.ts) MUST fail at its HEAD
// Write-Fence-Sealed-Generation assertion. The tag is off by default: the
// shipped build never compiles this file, and the script loads unmodified.
func init() {
	prelude, err := scriptFS.ReadFile("scripts/common.lua")
	if err != nil {
		panic(fmt.Sprintf("chronicle redis: embedded common.lua missing: %v", err))
	}
	body, err := scriptFS.ReadFile("scripts/seal_append_fence.lua")
	if err != nil {
		panic(fmt.Sprintf("chronicle redis: embedded script seal_append_fence.lua missing: %v", err))
	}
	const sealWrite = `redis.call('HSET', KEYS[1],
  'wfseal:' .. auth, generation .. ':' .. wake_id .. ':' .. off,
  'wfSealGen', generation,
  'wfSealOff', off)`
	faulted := strings.Replace(string(body), sealWrite, "-- fence_fault_noseal: the seal record write is disabled", 1)
	if faulted == string(body) {
		panic("fence_fault_noseal: seal write site not found in seal_append_fence.lua — realign the fault with the script")
	}
	sealAppendFenceScript = redis.NewScript(string(prelude) + "\n" + faulted)
}
