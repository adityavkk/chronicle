//go:build subtrace

// Package webhook trace seam (issue #39, CCF "smart casual verification").
//
// TracingStore is a Store decorator that records a JSONL trace of every fence
// linearization point — each Lua commit that the SubscriptionFence TLA+ model
// (#37) treats as one atomic action — so the running engine's real executions
// can be replayed against the spec under TLC in constrained/DFS mode (the
// S ∩ T ≠ ∅ check, research/01 Finding 1).
//
// It is BEHIND THE subtrace BUILD TAG: normal `go build`/`go test` never compile
// this file, so production and the existing test suite are byte-for-byte
// unaffected. The decorator is opt-in even within a subtrace build — a nil
// recorder (the zero value) records nothing — mirroring the Metrics seam's
// discipline (metrics.go: a nil/Nop recorder is a no-op).
//
// Linearization points recorded (one JSONL line each), exactly the granting +
// non-granting outcomes the spec's per-action IsEvent predicates constrain:
//
//	arm    arm_wake.lua         ARMED | BUSY | NOSUB | FENCED
//	claim  claim.lua            CLAIMED | BUSY | NOSUB
//	ack    ack.lua              OK | FENCED | NOSUB   (done flag in args)
//	release release.lua         OK | FENCED | NOSUB
//	expire expire_lease.lua     EXPIRED | ACTIVE | NOSUB | FENCED
//	stamp  record_wake_sent.lua OK | STALE | NOSUB    (the arm->emit split, T2)
//
// Each record carries the durable PRE and POST subscription state (read via the
// wrapped store's Get at the two instants around the mutating call) plus the
// request args (req_gen, req_wake, token_gen, done, worker, offsets). PRE/POST
// are the {generation, wake_id, phase, lease_until_ns, cursor-ish} the model's
// `sub` record tracks; the request args are what the model needs to reconstruct
// which worker's token is ack-acceptable. The Lua reply status is the action
// discriminator (granting vs no-op).
//
// Grain-of-atomicity (research/01 Pitfall "Grain-of-atomicity mismatch"): every
// recorded op is exactly one single-slot Lua commit, so one trace line = one
// spec action. The one non-atomic split the spec composes — arm_wake (ARMED)
// then the Go writeWakeEvent + record_wake_sent (stamp) — surfaces as TWO trace
// lines (arm then stamp), which Trace.tla composes via WakeAppend;WakeStamp.
package webhook

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// TraceRecord is one fence linearization point. Field order/JSON tags are the
// contract the Go->TLA converter (formal/tla/trace/) reads; keep it in sync with
// Trace.tla's record reconstruction.
type TraceRecord struct {
	Seq       int        `json:"seq"`       // monotone line counter (the model's trace index)
	Sub       string     `json:"sub"`       // subscription id (a Subs element after canonicalization)
	Op        string     `json:"op"`        // arm|claim|ack|release|expire|stamp
	LuaStatus string     `json:"luaStatus"` // the Lua reply discriminator
	Args      TraceArgs  `json:"args"`      // request args at the linearization point
	PreState  TraceState `json:"preState"`  // durable sub state immediately before the commit
	PostState TraceState `json:"postState"` // durable sub state immediately after the commit
}

// TraceState is the durable subscription state the model's `sub` record tracks.
// A subscription that does not exist (NOSUB pre/post) is reported with Exists
// false and zero fields.
type TraceState struct {
	Exists       bool   `json:"exists"`
	Phase        string `json:"phase"`
	Generation   int64  `json:"generation"`
	WakeID       string `json:"wakeId"`
	LeaseUntilNs int64  `json:"leaseUntilNs"`
	Holder       bool   `json:"holder"`
	HolderWorker string `json:"holderWorker"`
	WakeSentNs   int64  `json:"wakeSentNs"`
	Dispatch     string `json:"dispatch"` // "webhook" | "pull-wake"
}

// TraceArgs is the request side of a linearization point: the (req_gen, req_wake,
// token_gen) the fence checks, plus the worker and done flag where they apply.
type TraceArgs struct {
	Worker   string `json:"worker,omitempty"`
	ReqGen   int64  `json:"reqGen"`
	ReqWake  string `json:"reqWake,omitempty"`
	TokenGen int64  `json:"tokenGen"`
	Done     bool   `json:"done,omitempty"`
	ArmLease bool   `json:"armLease,omitempty"`
	WakeID   string `json:"wakeId,omitempty"` // the wake_id minted/presented by this op
}

// TraceRecorder appends TraceRecords as JSONL. Safe for concurrent use: the
// engine drives ack/claim/arm from many goroutines.
type TraceRecorder struct {
	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
	seq int
}

// NewTraceRecorder opens path for append and returns a recorder. A close func is
// returned for the driver to flush and release the file.
func NewTraceRecorder(path string) (*TraceRecorder, func() error, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, nil, err
	}
	r := &TraceRecorder{f: f, enc: json.NewEncoder(f)}
	return r, f.Close, nil
}

func (r *TraceRecorder) emit(rec TraceRecord) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	rec.Seq = r.seq
	_ = r.enc.Encode(rec) // best-effort; a trace IO error must not perturb the engine
}

// TracingStore wraps a Store and records a fence trace at each linearization
// point. It implements Store by embedding the inner store, so only the six
// fence-mutating methods are overridden; everything else passes through.
type TracingStore struct {
	Store
	rec *TraceRecorder
}

// NewTracingStore wraps inner so its fence ops are traced to rec. A nil rec makes
// it a transparent pass-through (records nothing).
func NewTracingStore(inner Store, rec *TraceRecorder) *TracingStore {
	return &TracingStore{Store: inner, rec: rec}
}

var _ Store = (*TracingStore)(nil)

// snap reads the durable subscription state for the trace. A read error or a
// missing sub yields Exists=false; the trace is best-effort and never fails the op.
func (t *TracingStore) snap(id string) TraceState {
	sub, ok, err := t.Store.Get(id)
	if err != nil || !ok {
		return TraceState{Exists: false}
	}
	return TraceState{
		Exists:       true,
		Phase:        string(sub.Phase),
		Generation:   sub.Generation,
		WakeID:       sub.WakeID,
		LeaseUntilNs: sub.LeaseUntilNs,
		Holder:       sub.Holder,
		HolderWorker: sub.HolderWorker,
		WakeSentNs:   sub.WakeEventSentNs,
		Dispatch:     string(sub.Config.Type),
	}
}

// ArmWake records arm_wake.lua.
func (t *TracingStore) ArmWake(id string, now time.Time, leaseTTLMs int64, armLease bool, wakeID string, owner ...OwnerScope) (ArmResult, error) {
	pre := t.snap(id)
	res, err := t.Store.ArmWake(id, now, leaseTTLMs, armLease, wakeID, owner...)
	if err != nil {
		return res, err
	}
	post := t.snap(id)
	t.rec.emit(TraceRecord{
		Sub: id, Op: "arm", LuaStatus: armStatus(res),
		Args:     TraceArgs{ArmLease: armLease, WakeID: wakeID},
		PreState: pre, PostState: post,
	})
	return res, nil
}

func armStatus(r ArmResult) string {
	switch {
	case r.Armed:
		return "ARMED"
	case r.Busy:
		return "BUSY"
	case r.NoSub:
		return "NOSUB"
	case r.Fenced:
		return "FENCED"
	default:
		return "UNKNOWN"
	}
}

// Claim records claim.lua.
func (t *TracingStore) Claim(id, worker, wakeID string, now time.Time, leaseTTLMs int64) (ClaimResult, error) {
	pre := t.snap(id)
	res, err := t.Store.Claim(id, worker, wakeID, now, leaseTTLMs)
	if err != nil {
		return res, err
	}
	post := t.snap(id)
	t.rec.emit(TraceRecord{
		Sub: id, Op: "claim", LuaStatus: claimStatus(res),
		Args:     TraceArgs{Worker: worker, WakeID: wakeID, ReqGen: res.Generation},
		PreState: pre, PostState: post,
	})
	return res, nil
}

func claimStatus(r ClaimResult) string {
	switch {
	case r.Claimed:
		return "CLAIMED"
	case r.Busy:
		return "BUSY"
	case r.NoSub:
		return "NOSUB"
	default:
		return "UNKNOWN"
	}
}

// Ack records ack.lua.
func (t *TracingStore) Ack(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64, owner ...OwnerScope) (string, error) {
	pre := t.snap(id)
	status, err := t.Store.Ack(id, reqGeneration, reqWakeID, tokenGeneration, done, acks, now, leaseTTLMs, owner...)
	if err != nil {
		return status, err
	}
	post := t.snap(id)
	t.rec.emit(TraceRecord{
		Sub: id, Op: "ack", LuaStatus: status,
		Args:     TraceArgs{ReqGen: reqGeneration, ReqWake: reqWakeID, TokenGen: tokenGeneration, Done: done},
		PreState: pre, PostState: post,
	})
	return status, nil
}

// Release records release.lua.
func (t *TracingStore) Release(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, owner ...OwnerScope) (string, error) {
	pre := t.snap(id)
	status, err := t.Store.Release(id, reqGeneration, reqWakeID, tokenGeneration, owner...)
	if err != nil {
		return status, err
	}
	post := t.snap(id)
	t.rec.emit(TraceRecord{
		Sub: id, Op: "release", LuaStatus: status,
		Args:     TraceArgs{ReqGen: reqGeneration, ReqWake: reqWakeID, TokenGen: tokenGeneration},
		PreState: pre, PostState: post,
	})
	return status, nil
}

// ExpireLease records expire_lease.lua (a server step, no client op).
func (t *TracingStore) ExpireLease(id string, now time.Time, owner ...OwnerScope) (string, error) {
	pre := t.snap(id)
	status, err := t.Store.ExpireLease(id, now, owner...)
	if err != nil {
		return status, err
	}
	post := t.snap(id)
	t.rec.emit(TraceRecord{
		Sub: id, Op: "expire", LuaStatus: status,
		PreState: pre, PostState: post,
	})
	return status, nil
}

// RecordWakeEventSent records record_wake_sent.lua (the stamp half of the
// non-atomic arm->emit split, window T2). The wrapped method returns only an
// error, so the status is reconstructed from pre/post (the model only needs to
// know whether the stamp took effect or was STALE).
func (t *TracingStore) RecordWakeEventSent(id string, generation int64, wakeID string, now time.Time) error {
	pre := t.snap(id)
	err := t.Store.RecordWakeEventSent(id, generation, wakeID, now)
	if err != nil {
		return err
	}
	post := t.snap(id)
	status := "STALE"
	if !post.Exists {
		status = "NOSUB"
	} else if post.WakeSentNs != 0 && post.WakeSentNs != pre.WakeSentNs {
		status = "OK"
	} else if pre.Exists && pre.Generation == generation && pre.WakeID == wakeID {
		// generation/wake matched at pre — the stamp was applied (or already set).
		status = "OK"
	}
	t.rec.emit(TraceRecord{
		Sub: id, Op: "stamp", LuaStatus: status,
		Args:     TraceArgs{ReqGen: generation, WakeID: wakeID},
		PreState: pre, PostState: post,
	})
	return nil
}
