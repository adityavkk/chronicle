package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

// scenario_stalegen.go is the IMPERATIVE SHELL for T4 (no stale-generation
// effect, docs/specs/horizontal-scale/research/07): an op carrying a superseded
// generation must be inert — FENCED and leaving the durable cursor unchanged. It
// runs against today's code (the fence already enforces it) and feeds the
// before/after snapshot to the pure CheckNoStaleGenEffect (check_stalegen.go).
//
// The shape generalizes runExpiredLeaseTakeover from "assert A's late ack is
// 409 FENCED" to "assert A's late ack is FENCED AND advanced no cursor", with a
// positive control: the SAME offset ack that is fenced for the deposed worker A
// succeeds for the live worker B, so the no-op is meaningful rather than vacuous.

// ackEntry mirrors the wire Ack (webhook/wire.go) so the harness can attempt a
// cursor advance under a chosen token.
type ackEntry struct {
	Stream string `json:"stream"`
	Offset string `json:"offset"`
}

// runStaleGenNoop drives an expired-lease takeover, then has the deposed worker
// attempt to advance the cursor with its now-stale token, and asserts the attempt
// is fenced and inert.
func runStaleGenNoop(c config) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	subID := fmt.Sprintf("jepsen-t4-%d", time.Now().UnixNano())
	const leaseTTLMs = 1500
	stream := "events/t4-0"

	// Create the subscription FIRST, then append — so the glob links the stream at
	// the beginning (acked < tail, real pending work the control ack can advance).
	// Appending before creating would backfill the link at the current tail, leaving
	// nothing to advance and making the test vacuous.
	if err := createPullWakeSubscription(c.base, subID, "events/*", "events/t4-wake", leaseTTLMs); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)
	tail, err := appendStream(c.base, stream, 6)
	if err != nil {
		return fmt.Errorf("seed stream: %w", err)
	}
	fmt.Printf("created pull-wake subscription %s (lease_ttl_ms=%d), tail=%s\n", subID, leaseTTLMs, short(tail))

	// Worker A claims, then GC-pauses past the lease so B can take over.
	a, err := claim(c.base, subID, "worker-A")
	if err != nil {
		return fmt.Errorf("worker A claim: %w", err)
	}
	fmt.Printf("worker A claimed: generation=%d\n", a.Generation)
	sleep(gcPause(time.Duration(leaseTTLMs) * time.Millisecond))

	b, err := claim(c.base, subID, "worker-B")
	if err != nil {
		return fmt.Errorf("worker B claim (takeover): %w", err)
	}
	if b.Generation == a.Generation {
		return fmt.Errorf("fence did not rotate on takeover: B reused generation %d", a.Generation)
	}
	fmt.Printf("worker B claimed (takeover): generation=%d (current)\n", b.Generation)

	// Snapshot the durable cursor before the stale-gen attempt.
	before, err := cursorSnapshot(c.base, subID)
	if err != nil {
		return fmt.Errorf("snapshot before: %w", err)
	}

	// The deposed worker A (stale generation) tries to advance the cursor to the
	// tail with a heartbeat ack (done=false, so the only attempted effect is the
	// cursor advance). It MUST be fenced.
	acks := []ackEntry{{Stream: stream, Offset: tail}}
	aStatus, aCode, err := callbackWithAcks(c.base, subID, a.Token, a.WakeID, a.Generation, acks, false)
	if err != nil {
		return fmt.Errorf("worker A stale ack: %w", err)
	}
	after, err := cursorSnapshot(c.base, subID)
	if err != nil {
		return fmt.Errorf("snapshot after: %w", err)
	}

	// Positive control: the same offset ack under B's CURRENT token must succeed
	// and advance the cursor — proving the cursor was advanceable, so A's no-op is
	// meaningful, not vacuous.
	bStatus, bCode, err := callbackWithAcks(c.base, subID, b.Token, b.WakeID, b.Generation, acks, true)
	if err != nil {
		return fmt.Errorf("worker B control ack: %w", err)
	}
	control, err := cursorSnapshot(c.base, subID)
	if err != nil {
		return fmt.Errorf("snapshot control: %w", err)
	}

	obs := []staleGenObservation{
		{sub: subID, op: "ack", reqGen: a.Generation, curGen: b.Generation,
			status: ackStatusOf(aStatus, aCode), before: before, after: after},
	}
	violations := CheckNoStaleGenEffect(obs)

	fmt.Println("---- result ----")
	fmt.Printf("scenario:           %s\n", c.scenario)
	fmt.Printf("A generation:       %d (stale)\n", a.Generation)
	fmt.Printf("B generation:       %d (current)\n", b.Generation)
	fmt.Printf("A stale-ack status: %d %s\n", aStatus, aCode)
	fmt.Printf("cursor before:      %s\n", before)
	fmt.Printf("cursor after stale: %s\n", after)
	fmt.Printf("cursor after B ack: %s (control: %d %s)\n", control, bStatus, bCode)

	if aStatus != http.StatusConflict || aCode != "FENCED" {
		return fmt.Errorf("worker A's stale ack returned %d %q, want 409 FENCED", aStatus, aCode)
	}
	if before != after {
		return fmt.Errorf("stale-gen ack mutated the durable cursor: %s -> %s", before, after)
	}
	if bStatus != http.StatusOK {
		return fmt.Errorf("control ack under the current generation returned %d %q, want 200 (the cursor must be advanceable)", bStatus, bCode)
	}
	if control == before {
		return fmt.Errorf("control ack did not advance the cursor (%s) — the test is vacuous", control)
	}
	if len(violations) > 0 {
		for _, v := range violations {
			fmt.Printf("  VIOLATION %s\n", v)
		}
		return fmt.Errorf("T4 violated: a stale-generation op had a durable effect")
	}
	fmt.Println("PASS: T4 — the stale-generation ack was FENCED and advanced no cursor; the same ack succeeded under the current generation")
	return nil
}

// cursorSnapshot reads the subscription and returns a canonical, byte-comparable
// serialization of its durable cursors (sorted "path=acked_offset;"). This is the
// durable state T4 asserts a stale-gen op must not change; the API does not expose
// generation/phase, but the cursor is the durable EFFECT the property is about.
func cursorSnapshot(base, subID string) (string, error) {
	v, err := getSubscription(base, subID)
	if err != nil {
		return "", err
	}
	pairs := make([]string, 0, len(v.Streams))
	for _, s := range v.Streams {
		pairs = append(pairs, s.Path+"="+s.AckedOffset)
	}
	sort.Strings(pairs)
	out := ""
	for _, p := range pairs {
		out += p + ";"
	}
	return out, nil
}

// callbackWithAcks POSTs an ack carrying explicit offset acks plus a done flag
// under a chosen bearer token and fence, returning the HTTP status and (on a 4xx
// envelope) the error code without retrying — the caller asserts on the exact
// status. Unlike ackPullWake it sends offsets, so a stale token's attempt to
// advance the cursor is observable.
func callbackWithAcks(base, id, token, wakeID string, generation int64, acks []ackEntry, done bool) (status int, code string, err error) {
	body, _ := json.Marshal(struct {
		WakeID     string     `json:"wake_id"`
		Generation int64      `json:"generation"`
		Acks       []ackEntry `json:"acks"`
		Done       *bool      `json:"done"`
	}{WakeID: wakeID, Generation: generation, Acks: acks, Done: &done})
	url := fmt.Sprintf("%s/v1/stream/__ds/subscriptions/%s/ack", base, id)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	status = resp.StatusCode
	if status >= 400 {
		var env errEnvelope
		if json.NewDecoder(resp.Body).Decode(&env) == nil {
			code = env.Error.Code
		}
	}
	return status, code, nil
}
