//go:build fence_fault_nopair

package chronicle

// Fault-injection control (#183, design §H.3). This build disables exactly one
// mechanism: writeError's terminal gap pair (Producer-Expected-Seq ==
// Producer-Received-Seq == the request's seq) on data-plane 409 FENCED
// rejections. The envelope, Producer-Epoch echo and status are untouched.
//
// Without the pair, the pinned @durable-streams/client@0.2.6
// IdempotentProducer parses Producer-Expected-Seq as 0, treats every fenced
// 409 at seq >= 1 as a resolvable predecessor gap, and re-sends without
// bound. Under `-tags fence_fault_nopair` the extension conformance test
//
//	pinned IdempotentProducer observes WRITE_FAILED within one batch after a takeover
//
// (test/conformance-ext/client.test.ts) MUST fail: the producer never throws
// the terminal SequenceGapError — the retry loop runs until the 5 s deadline
// or the write token's own TTL ends it with a credential error. The tag is
// off by default: the shipped build never compiles this file, and
// fenceFaultNoPair stays false.
func init() { fenceFaultNoPair = true }
