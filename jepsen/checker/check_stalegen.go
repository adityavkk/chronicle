package main

import "fmt"

// check_stalegen.go is the PURE CORE of the no-stale-generation-effect safety
// test (T4 in docs/specs/horizontal-scale/research/07): an op carrying a
// generation other than the subscription's then-current one must be INERT — it
// returns a non-granting status and leaves the durable record byte-identical.
// This is the durable-effect complement to T1: T1 proves the fence rejects a
// stale OK (no two holders), T4 proves a rejected op also wrote nothing (a ghost
// worker replaying a stale token cannot corrupt state). It is a direct effect
// checker over recorded observations, cheaper than full linearizability (a
// single stale-gen mutation is a finite counterexample on its own), so — like
// check_cursor.go — it is a pure function over a slice with no I/O.
//
// T4 runs against TODAY's code: the (generation, wake_id) fence and the
// forward-only cursor already make every stale-gen op a no-op (ack.lua/
// release.lua fence first; arm_wake.lua/claim.lua BUSY when not idle). The
// scenario driver (scenario_stalegen.go) records the before/after snapshot
// around an op known to carry a superseded generation and feeds it here.

// statusStale is part of T4's accepted vocabulary (07) for a status that
// explicitly reports a stale token; chronicle folds this into FENCED today, but
// the checker accepts it so the contract does not pin an internal code.
const statusStale = "STALE"

// staleGenObservation is one recorded operation that presented reqGen while the
// subscription's current generation was curGen, plus the observed status and the
// durable snapshot captured immediately before and after the op. before/after
// are opaque, byte-comparable serializations of the subscription's durable state
// (its sub hash + links); the checker only ever compares them for equality.
type staleGenObservation struct {
	sub    string
	op     string // "ack" | "release" | "arm" | "claim" — for the witness message
	reqGen int64  // the generation the op carried
	curGen int64  // the subscription's current generation when the op was applied
	status string
	before string // durable snapshot before the op
	after  string // durable snapshot after the op
	atNs   int64
}

// staleGenViolation records a stale-generation op that was not inert: either it
// returned a granting status, or it mutated the durable snapshot.
type staleGenViolation struct {
	obs    staleGenObservation
	reason string
}

func (v staleGenViolation) String() string {
	return fmt.Sprintf("stale-gen %s on sub=%s (reqGen=%d curGen=%d, status=%s): %s (at +%dms)",
		v.obs.op, v.obs.sub, v.obs.reqGen, v.obs.curGen, v.obs.status, v.reason, v.obs.atNs/1e6)
}

// staleGenInert is the set of statuses a stale-generation op may legally return
// — each grants nothing and (T4's second clause) must mutate nothing.
var staleGenInert = map[string]bool{
	statusFenced: true, // ack.lua / release.lua fence
	statusBusy:   true, // arm_wake.lua / claim.lua refuse a non-idle / held sub
	statusStale:  true, // explicit stale report (folded into FENCED today)
	statusNoSub:  true, // the subscription was deleted out from under the op
}

// CheckNoStaleGenEffect asserts T4 over the observations: for every op whose
// reqGen differs from the then-current curGen, the status must be one of
// {FENCED,BUSY,STALE,NOSUB} AND the durable snapshot must be byte-identical
// before and after. It returns every violation found; an empty result means the
// property held. Current-generation ops (reqGen == curGen) are skipped — they may
// legitimately mutate.
func CheckNoStaleGenEffect(obs []staleGenObservation) []staleGenViolation {
	var violations []staleGenViolation
	for _, o := range obs {
		if o.reqGen == o.curGen {
			continue
		}
		if !staleGenInert[o.status] {
			violations = append(violations, staleGenViolation{
				obs:    o,
				reason: "accepted with a granting status (want FENCED/BUSY/STALE/NOSUB)",
			})
			continue
		}
		if o.before != o.after {
			violations = append(violations, staleGenViolation{
				obs:    o,
				reason: "mutated the durable snapshot (before != after)",
			})
		}
	}
	return violations
}
