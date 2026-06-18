package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// scenario_lease.go is the IMPERATIVE SHELL for T1 (single-holder lease,
// docs/specs/horizontal-scale/research/07): it drives a live chronicle pull-wake
// subscription with N contending workers and an in-process GC-pause nemesis,
// records every claim/ack into a porcupine history, and checks it against the
// pure leaseModel (model_fence.go). On a violation it writes a counterexample
// timeline with porcupine.VisualizePath.
//
// The gcPause nemesis is the highest-ROI fault for T1 and needs no infrastructure
// (07's recommendation): a worker that has claimed deliberately stalls past
// lease_ttl_ms before acking, so a peer takes over (rotating the fence) and the
// stalled worker's later ack arrives with a now-stale token. This is Kleppmann's
// deposed-but-resumed process — exactly the case the fence exists to make safe —
// generalized from the single hand-built runExpiredLeaseTakeover sequence to a
// model-checked concurrent history.

// runSingleHolderLinz drives the workers, then checks the recorded history for
// linearizability against the lease-fence model.
func runSingleHolderLinz(c config) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}

	subID := fmt.Sprintf("jepsen-linz-%d", time.Now().UnixNano())
	const leaseTTLMs = int64(1000)

	// A pull-wake subscription: claim.lua grants a lease whenever no unexpired
	// holder exists (it checks only the BUSY guard, never pending work), so the
	// workers contend on the fence directly — no stream appends are needed to
	// manufacture contention. Each grant from idle/live rotates the generation, so
	// the history is a clean stream of fence handoffs.
	if err := createPullWakeSubscription(c.base, subID, "events/*", "events/__linz_wake__", leaseTTLMs); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)
	fmt.Printf("created pull-wake subscription %s (lease_ttl_ms=%d); %d workers for %dms\n",
		subID, leaseTTLMs, c.workers, c.workloadMs)

	rec := newRecorder()
	deadline := time.Now().Add(time.Duration(c.workloadMs) * time.Millisecond)
	var wg sync.WaitGroup
	for w := 0; w < c.workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			leaseWorker(c.base, subID, id, leaseTTLMs, deadline, rec)
		}(w)
	}
	wg.Wait()

	history := rec.history()
	claims, grants := countClaims(history)
	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("workers:           %d\n", c.workers)
	fmt.Printf("operations:        %d (%d claims, %d granted)\n", len(history), claims, grants)

	// CheckOperationsVerbose returns Ok | Illegal | Unknown (Unknown only on the
	// timeout, which 07's gap #1 warns about for over-concurrent histories).
	result, info := porcupine.CheckOperationsVerbose(leaseModel(), history, 20*time.Second)
	switch result {
	case porcupine.Ok:
		fmt.Println("linearizable:      yes")
		fmt.Println("PASS: the lease fence held the single-holder invariant under concurrency + GC pauses")
		return nil
	case porcupine.Illegal:
		const path = "linz-counterexample.html"
		if err := porcupine.VisualizePath(leaseModel(), info, path); err != nil {
			fmt.Printf("counterexample:    FAILED to render (%v)\n", err)
		} else {
			fmt.Printf("counterexample:    %s\n", path)
		}
		return fmt.Errorf("NOT linearizable: two workers held a fence-valid token for one subscription — the single-holder invariant was violated")
	default: // porcupine.Unknown
		return fmt.Errorf("linearizability UNKNOWN: the history was too concurrent to decide within the timeout — reduce -workers or -workload-ms (07's gap #1)")
	}
}

// leaseWorker claims, optionally GC-pauses, then acks(done) in a loop until the
// deadline, recording every claim and ack into the shared history.
func leaseWorker(base, subID string, id int, leaseTTLMs int64, deadline time.Time, rec *recorder) {
	worker := fmt.Sprintf("w-%d", id)
	// Per-worker backoff so the losers of a race don't thunder back in lockstep.
	backoff := time.Duration(20+id*11) * time.Millisecond
	grants := 0

	for time.Now().Before(deadline) {
		callNs := rec.now()
		status, res, code, err := claimOnce(base, subID, worker)
		if err != nil {
			sleep(backoff) // network error: the op never completed — record nothing
			continue
		}
		out := fenceOutput{status: claimStatusOf(status, code)}
		if status == http.StatusOK {
			out.gen, out.wake = res.Generation, res.WakeID
		}
		rec.record(id, fenceInput{sub: subID, op: opClaim, worker: worker}, out, callNs)
		if status != http.StatusOK {
			sleep(backoff) // BUSY / NOSUB: let the holder finish, then retry
			continue
		}
		grants++

		// gcPause nemesis: on roughly every third grant, stall past the lease so a
		// peer takes over (rotating the fence) and this ack races in with a stale
		// token.
		if grants%3 == 0 {
			sleep(gcPauseDuration(leaseTTLMs))
		}

		callNs = rec.now()
		astatus, acode, aerr := ackPullWake(base, subID, res.Token, res.WakeID, res.Generation)
		if aerr != nil {
			continue // network error on the ack: do not record a phantom outcome
		}
		rec.record(id, fenceInput{
			sub: subID, op: opAck, worker: worker,
			reqGen: res.Generation, reqWake: res.WakeID, tokenGen: res.Generation, done: true,
		}, fenceOutput{status: ackStatusOf(astatus, acode)}, callNs)
	}
}

func gcPauseDuration(leaseTTLMs int64) time.Duration {
	return time.Duration(leaseTTLMs)*time.Millisecond + 250*time.Millisecond
}

// claimOnce POSTs a single pull-wake claim WITHOUT retrying, returning the raw
// HTTP status, the decoded body on 200, and the error code on a 4xx envelope.
// Unlike claim() in main.go it surfaces BUSY as data rather than an error,
// because the linearizability history must record every outcome verbatim.
func claimOnce(base, id, worker string) (status int, res claimResult, code string, err error) {
	body, _ := json.Marshal(ClaimBody{Worker: worker})
	url := fmt.Sprintf("%s/v1/stream/__ds/subscriptions/%s/claim", base, id)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, res, "", err
	}
	defer resp.Body.Close()
	status = resp.StatusCode
	if status == http.StatusOK {
		err = json.NewDecoder(resp.Body).Decode(&res)
		return status, res, "", err
	}
	var env errEnvelope
	if json.NewDecoder(resp.Body).Decode(&env) == nil {
		code = env.Error.Code
	}
	return status, res, code, nil
}

// claimStatusOf maps an HTTP claim response to the model's status vocabulary. An
// unrecognized code is recorded verbatim; the model treats any non-CLAIMED claim
// as a no-op, so an unexpected status can never mask a real violation.
func claimStatusOf(status int, code string) string {
	switch {
	case status == http.StatusOK:
		return statusClaimed
	case code == "ALREADY_CLAIMED":
		return statusBusy
	case code == "NOT_FOUND", code == "NOSUB":
		return statusNoSub
	default:
		return code
	}
}

// ackStatusOf maps an HTTP ack response to the model's status vocabulary. The ack
// endpoint (routes.go handleAckLike) returns only 200, 409 FENCED, 401
// TOKEN_INVALID, or 400 — notably it folds a gone subscription into 409 FENCED
// (not NOSUB) and rejects a bad bearer with 401 TOKEN_INVALID. Both 401 and any
// other non-OK, non-FENCED reply leave the fence untouched, so they fall through
// to the model's default no-op branch (model_fence.go stepAckOrRelease) — there
// is no NOSUB ack to map.
func ackStatusOf(status int, code string) string {
	switch {
	case status == http.StatusOK:
		return statusOK
	case code == "FENCED":
		return statusFenced
	default:
		return code // TOKEN_INVALID / INVALID_REQUEST: a pre-fence no-op
	}
}

// countClaims tallies claim attempts and grants for the result summary.
func countClaims(history []porcupine.Operation) (claims, grants int) {
	for _, o := range history {
		if o.Input.(fenceInput).op != opClaim {
			continue
		}
		claims++
		if o.Output.(fenceOutput).status == statusClaimed {
			grants++
		}
	}
	return claims, grants
}
