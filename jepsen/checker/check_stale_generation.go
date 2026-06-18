package main

import (
	"bytes"
	"fmt"
)

// check_stale_generation.go is the PURE CORE scaffold for T4 (no
// stale-generation effect): an operation carrying an old generation must return
// a fenced/no-authority status and leave the durable subscription snapshot
// byte-identical. The live scenario records the snapshots from GET
// /__ds/subscriptions/<id>; future mechanism slices can feed lower-level Redis
// snapshots into the same checker.

const statusStale = "STALE"

type staleGenerationObservation struct {
	scope      string
	op         string
	requestGen int64
	currentGen int64
	status     string
	before     []byte
	after      []byte
}

type staleGenerationViolation struct {
	scope  string
	op     string
	reason string
}

func (v staleGenerationViolation) String() string {
	return fmt.Sprintf("%s %s: %s", v.scope, v.op, v.reason)
}

func CheckStaleGenerationNoop(obs []staleGenerationObservation) []staleGenerationViolation {
	var violations []staleGenerationViolation
	for _, o := range obs {
		if o.requestGen == o.currentGen {
			continue
		}
		if !staleNoAuthorityStatus(o.status) {
			violations = append(violations, staleGenerationViolation{
				scope: o.scope, op: o.op,
				reason: fmt.Sprintf("stale generation returned %s", o.status),
			})
			continue
		}
		if !bytes.Equal(o.before, o.after) {
			violations = append(violations, staleGenerationViolation{
				scope: o.scope, op: o.op,
				reason: "stale generation mutated durable snapshot",
			})
		}
	}
	return violations
}

func staleNoAuthorityStatus(status string) bool {
	switch status {
	case statusFenced, statusBusy, statusStale, statusNoSub:
		return true
	default:
		return false
	}
}
