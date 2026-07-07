package main

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDurableExternalizeVerdict_PassWhenExternalizedFenceSurvives(t *testing.T) {
	w := durableExternalizedWake{subID: "s1", generation: 7, primaryOffsetAtExternalize: 1000, promotedOffset: 1000, survivingGeneration: 7}
	r := durableExternalizeVerdict(nil, http.StatusConflict, "FENCED", 0, false, time.Second, 4, []durableExternalizedWake{w})
	if !r.pass || r.verdict != durableExternalizePass {
		t.Fatalf("surviving externalized fence must pass, got %+v", r)
	}
	if !durableExternalizeAssertingGateOK(r) {
		t.Fatalf("clean run with an externalized wake must satisfy the gate")
	}
	if !strings.Contains(r.String(), "Tier-B acknowledged losses: 0") {
		t.Fatalf("verdict must report zero acknowledged losses, got:\n%s", r.String())
	}
}

func TestDurableExternalizeVerdict_FailsTierBAcknowledgedOffsetLoss(t *testing.T) {
	w := durableExternalizedWake{subID: "s1", generation: 7, primaryOffsetAtExternalize: 1000, promotedOffset: 900, survivingGeneration: 7, reestablishedWithinBudget: true}
	r := durableExternalizeVerdict(nil, http.StatusConflict, "FENCED", 100, true, time.Second, 4, []durableExternalizedWake{w})
	if r.pass {
		t.Fatalf("Tier-B externalized offset loss must fail even if L1 recovery later succeeds: %+v", r)
	}
	if len(r.lostExternalized) != 1 || r.reestablishedLostWithin != 1 {
		t.Fatalf("lost externalized accounting = %d/%d, want 1/1", len(r.lostExternalized), r.reestablishedLostWithin)
	}
	if !strings.Contains(r.String(), "awaitDurable externalized a fence that promotion lost") {
		t.Fatalf("verdict must name the false durability acknowledgement, got:\n%s", r.String())
	}
}

func TestDurableExternalizeVerdict_FailsTierBAcknowledgedGenerationLoss(t *testing.T) {
	w := durableExternalizedWake{subID: "s1", generation: 7, primaryOffsetAtExternalize: 1000, promotedOffset: 1000, survivingGeneration: 6}
	r := durableExternalizeVerdict(nil, http.StatusConflict, "FENCED", 0, true, time.Second, 4, []durableExternalizedWake{w})
	if r.pass || len(r.lostExternalized) != 1 {
		t.Fatalf("surviving generation below externalized generation must fail, got %+v", r)
	}
}

func TestDurableExternalizeVerdict_PositiveRPOWithoutExternalizedLossPasses(t *testing.T) {
	w := durableExternalizedWake{subID: "s1", generation: 7, primaryOffsetAtExternalize: 1000, promotedOffset: 1200, survivingGeneration: 7}
	r := durableExternalizeVerdict(nil, http.StatusConflict, "FENCED", 4096, false, time.Second, 4, []durableExternalizedWake{w})
	if !r.pass {
		t.Fatalf("positive RPO without Tier-B externalized loss should pass existing at-least-once framing, got %+v", r)
	}
}

func TestDurableExternalizeVerdict_KeepsExistingFailoverClauses(t *testing.T) {
	w := durableExternalizedWake{subID: "s1", generation: 7, primaryOffsetAtExternalize: 1000, promotedOffset: 1000, survivingGeneration: 7}
	gaps := []deliveryGap{{path: "events/de-1", want: "0000000000000040_0", got: "0000000000000031_0", msgs: 40}}
	r := durableExternalizeVerdict(gaps, http.StatusConflict, "FENCED", 0, false, time.Second, 4, []durableExternalizedWake{w})
	if r.pass || r.base.pass {
		t.Fatalf("L1 gap must still fail before Tier-B clause, got %+v", r)
	}

	r = durableExternalizeVerdict(nil, http.StatusOK, "", 0, false, time.Second, 4, []durableExternalizedWake{w})
	if r.pass || r.base.deposedFenced {
		t.Fatalf("unfenced deposed ack must still fail, got %+v", r)
	}
}
