package store

import (
	"encoding/base64"
	"strconv"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// FenceReason names why EvaluateWriteFence refused a mutation; FenceNone
// accepts. The values are the `reason` vocabulary of a data-plane 409 FENCED
// rejection and of the Lua evaluate_write_fence reply, so they never change.
type FenceReason string

const (
	// FenceNone accepts the mutation.
	FenceNone FenceReason = ""
	// FenceSealed refuses a fenced-class write on a fenced stream whose claim
	// generation is at or below its authority's seal (its done already landed).
	FenceSealed FenceReason = "sealed"
	// FenceMarker refuses a fenced-class write whose stream-slot marker is
	// absent, revoked, names another claim, has lapsed, or belongs to an earlier
	// stream incarnation: the claim fence #169 shipped, unchanged.
	FenceMarker FenceReason = "marker"
	// FenceProducerRequired refuses a fenced-class write on a fenced stream that
	// carries no producer headers (the in-slot backstop of the handler's 400).
	FenceProducerRequired FenceReason = "producer_required"
	// FenceEpoch refuses a fenced-class write on a fenced stream whose
	// Producer-Epoch is not the claim generation.
	FenceEpoch FenceReason = "epoch"
	// FenceBound refuses an open-class write on a fenced stream that names a
	// producer id an accepted fenced write has bound to the fence.
	FenceBound FenceReason = "bound"
)

// Marker states written by the grant, revoke, and seal scripts.
const (
	// WriteFenceMarkerLive is the state of an installed, unrevoked claim marker.
	WriteFenceMarkerLive = "live"
	// WriteFenceMarkerRevoked is the tombstone state left by a revoke or seal.
	WriteFenceMarkerRevoked = "revoked"
)

// WriteFenceMarker is one stream-slot claim marker as the fence rung reads it.
// Present is false when the authority has no marker on the stream.
type WriteFenceMarker struct {
	Present           bool
	State             string // WriteFenceMarkerLive or WriteFenceMarkerRevoked
	Generation        int64
	WakeID            string
	Holder            string
	LeaseUntilNs      int64
	StreamIncarnation string
}

// WriteFenceSeal is one authority's seal on a stream: the highest generation
// it has closed and the definite last fenced-class offset recorded with it.
type WriteFenceSeal struct {
	Present    bool
	Generation int64
	WakeID     string
	Offset     Offset
}

// WriteFenceInput is everything the fence rung decides on. Fence is nil for an
// open-class write; Marker and Seal belong to Fence's authority and are ignored
// when Fence is nil, exactly as BoundGeneration is ignored when it is not.
type WriteFenceInput struct {
	StreamFenced      bool
	StreamIncarnation string
	Fence             *auth.AppendFence // nil = open class
	Marker            WriteFenceMarker  // the marker of Fence's authority
	Seal              WriteFenceSeal    // the seal of Fence's authority
	NowNs             int64
	HasProducer       bool
	ProducerEpoch     int64
	BoundGeneration   int64 // wfbind:<producer_id>; 0 = unbound
}

// WriteFenceOutcome is the fence decision plus what a rejection may disclose:
// the current generation (the marker's if present, else the seal's, else the
// bound producer's, else 0) and the holder of a live, unexpired marker.
type WriteFenceOutcome struct {
	Reason     FenceReason
	Generation int64
	Holder     string
}

// EvaluateWriteFence is the pure write-fence rung of the append ladder (#183),
// mirrored atomically by evaluate_write_fence in store/redis/scripts/common.lua
// and bound to it by TestWriteFenceDifferential. Rules, first hit wins:
//
//  1. fenced class, fenced stream, sealed authority, generation <= seal → sealed
//  2. fenced class, marker not the live, unexpired, same-incarnation match → marker
//  3. fenced class, fenced stream, no producer headers → producer_required
//  4. fenced class, fenced stream, Producer-Epoch != generation → epoch
//  5. open class, fenced stream, producer headers, bound producer id → bound
//
// On an unfenced stream only rule 2 can fire, so streams that never opt in
// keep exactly the claim fence they have today. The seal precedes the marker so
// a holder whose done landed learns "sealed", not "marker".
func EvaluateWriteFence(in WriteFenceInput) WriteFenceOutcome {
	if in.Fence == nil {
		out := WriteFenceOutcome{Generation: in.BoundGeneration}
		if in.StreamFenced && in.HasProducer && in.BoundGeneration > 0 {
			out.Reason = FenceBound
		}
		return out
	}
	m, f := in.Marker, in.Fence
	var out WriteFenceOutcome
	switch {
	case m.Present:
		out.Generation = m.Generation
	case in.Seal.Present:
		out.Generation = in.Seal.Generation
	}
	leaseLive := m.Present && m.LeaseUntilNs > in.NowNs
	if m.Present && m.State == WriteFenceMarkerLive && leaseLive {
		out.Holder = m.Holder
	}
	switch {
	case in.StreamFenced && in.Seal.Present && f.Generation <= in.Seal.Generation:
		out.Reason = FenceSealed
	case !m.Present ||
		m.State != WriteFenceMarkerLive ||
		m.Generation != f.Generation ||
		m.WakeID != f.WakeID ||
		m.Holder != f.Holder ||
		!leaseLive ||
		m.StreamIncarnation != in.StreamIncarnation:
		out.Reason = FenceMarker
	case in.StreamFenced && !in.HasProducer:
		out.Reason = FenceProducerRequired
	case in.StreamFenced && in.ProducerEpoch != f.Generation:
		out.Reason = FenceEpoch
	}
	return out
}

// FenceAuthority is the per-authority identity shared by a claim's stream-slot
// marker key suffix and its seal field in the stream meta hash:
// base64url(subscriptionID + "\x00" + incarnation) + ":" + shard. The
// incarnation is part of it so a recreated subscription starts with an empty
// seal namespace and can never revive an old claim. Mirrored by fence_auth in
// store/redis/scripts/common.lua, which recovers it from the marker key.
func FenceAuthority(f auth.AppendFence) string {
	identity := f.SubscriptionID + "\x00" + f.SubscriptionIncarnation
	return base64.RawURLEncoding.EncodeToString([]byte(identity)) + ":" + strconv.Itoa(f.Shard)
}
