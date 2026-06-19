package main

import (
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// history.go is the recorder seam between the imperative shell (the scenario
// drivers) and the pure model (model_fence.go). It brackets each client operation
// into a porcupine.Operation with a monotonic timestamp read from the DRIVER HOST
// clock — never a (possibly skewed) cluster node — so the [Call, Return]
// real-time intervals porcupine reasons over stay sound even while a clock-skew
// nemesis runs (07's gap #5).

// recorder accumulates a porcupine history from concurrent workers.
type recorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation
}

func newRecorder() *recorder { return &recorder{start: time.Now()} }

// now returns nanoseconds since the recorder started, from the host's monotonic
// clock (time.Since reads the monotonic component, immune to wall-clock skew).
func (r *recorder) now() int64 { return int64(time.Since(r.start)) }

// record appends one completed operation. callNs is captured by the caller
// immediately before issuing the request; the return timestamp is taken here,
// after the response. Safe for concurrent callers.
func (r *recorder) record(clientID int, in fenceInput, out fenceOutput, callNs int64) {
	ret := r.now()
	r.mu.Lock()
	r.ops = append(r.ops, porcupine.Operation{
		ClientId: clientID,
		Input:    in,
		Output:   out,
		Call:     callNs,
		Return:   ret,
	})
	r.mu.Unlock()
}

// recordOp is the model-agnostic form of record: it brackets an operation whose
// input/output are any porcupine-model values (e.g. the shardInput/shardOutput of
// the ownership-exclusivity T3 driver), not just the fence model's types. callNs
// is captured by the caller before the request; the return stamp is taken here.
func (r *recorder) recordOp(clientID int, in, out interface{}, callNs int64) {
	ret := r.now()
	r.mu.Lock()
	r.ops = append(r.ops, porcupine.Operation{
		ClientId: clientID,
		Input:    in,
		Output:   out,
		Call:     callNs,
		Return:   ret,
	})
	r.mu.Unlock()
}

// history returns a copy of the recorded operations, safe to hand to porcupine
// while workers may still be running.
func (r *recorder) history() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]porcupine.Operation, len(r.ops))
	copy(out, r.ops)
	return out
}
