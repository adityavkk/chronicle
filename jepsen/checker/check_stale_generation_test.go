package main

import "testing"

func TestCheckStaleGenerationNoop_AllowsNoAuthorityNoop(t *testing.T) {
	obs := []staleGenerationObservation{
		{
			scope: "sub:s", op: "ack", requestGen: 1, currentGen: 2,
			status: statusFenced, before: []byte(`{"status":"active"}`), after: []byte(`{"status":"active"}`),
		},
		{
			scope: "sub:s", op: "record", requestGen: 1, currentGen: 2,
			status: statusBusy, before: []byte(`{"status":"active"}`), after: []byte(`{"status":"active"}`),
		},
		{
			scope: "sub:s", op: "release", requestGen: 1, currentGen: 2,
			status: statusStale, before: []byte(`{"status":"active"}`), after: []byte(`{"status":"active"}`),
		},
		{
			scope: "sub:s", op: "ack", requestGen: 1, currentGen: 2,
			status: statusNoSub, before: []byte(`missing`), after: []byte(`missing`),
		},
	}
	if got := CheckStaleGenerationNoop(obs); len(got) != 0 {
		t.Fatalf("expected no violations, got %v", got)
	}
}

func TestCheckStaleGenerationNoop_CurrentGenerationIgnored(t *testing.T) {
	obs := []staleGenerationObservation{{
		scope: "sub:s", op: "ack", requestGen: 2, currentGen: 2,
		status: statusOK, before: []byte(`before`), after: []byte(`after`),
	}}
	if got := CheckStaleGenerationNoop(obs); len(got) != 0 {
		t.Fatalf("current-generation op should not be checked as stale, got %v", got)
	}
}

func TestCheckStaleGenerationNoop_RejectsAuthoritativeStatus(t *testing.T) {
	obs := []staleGenerationObservation{{
		scope: "sub:s", op: "ack", requestGen: 1, currentGen: 2,
		status: statusOK, before: []byte(`{"phase":"live"}`), after: []byte(`{"phase":"live"}`),
	}}
	got := CheckStaleGenerationNoop(obs)
	if len(got) != 1 || got[0].reason != "stale generation returned OK" {
		t.Fatalf("expected authoritative-status violation, got %v", got)
	}
}

func TestCheckStaleGenerationNoop_RejectsSnapshotMutation(t *testing.T) {
	obs := []staleGenerationObservation{{
		scope: "sub:s", op: "release", requestGen: 1, currentGen: 2,
		status: statusFenced, before: []byte(`{"phase":"live"}`), after: []byte(`{"phase":"idle"}`),
	}}
	got := CheckStaleGenerationNoop(obs)
	if len(got) != 1 || got[0].reason != "stale generation mutated durable snapshot" {
		t.Fatalf("expected snapshot-mutation violation, got %v", got)
	}
}
