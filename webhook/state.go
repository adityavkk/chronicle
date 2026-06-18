package webhook

import (
	"math"
	"time"
)

// TailFunc returns the current tail offset of a stream and whether the stream
// exists. It is the pure-core seam over the store (Caddy's getTailOffset),
// injected so HasPendingWork and Snapshot stay testable without Redis.
type TailFunc func(path string) (offset string, ok bool)

// Snapshot computes the per-stream wake snapshot for a subscription's links:
// each link's current tail and whether tail > acked_offset. Offsets are
// lexicographically comparable (PROTOCOL §8), so the comparison is a string
// compare. A stream that no longer exists is reported with its acked offset as
// the tail and has_pending=false. Returns the snapshot and whether any link has
// pending work.
func Snapshot(links []StreamLink, tailOf TailFunc) ([]StreamSnapshot, bool) {
	out := make([]StreamSnapshot, 0, len(links))
	anyPending := false
	for _, l := range links {
		tail, ok := tailOf(l.Path)
		if !ok {
			tail = l.AckedOffset
		}
		pending := offsetGreater(tail, l.AckedOffset)
		if pending {
			anyPending = true
		}
		out = append(out, StreamSnapshot{
			Path:        l.Path,
			LinkType:    l.LinkType,
			AckedOffset: l.AckedOffset,
			TailOffset:  tail,
			HasPending:  pending,
		})
	}
	return out, anyPending
}

// HasPendingWork reports whether any linked stream has a tail beyond its acked
// offset (PROTOCOL §7).
func HasPendingWork(links []StreamLink, tailOf TailFunc) bool {
	_, pending := Snapshot(links, tailOf)
	return pending
}

// HasPendingWorkFrom is the batched-read form of HasPendingWork: it checks links
// against a pre-fetched map of stream tails rather than a per-link TailFunc, so
// the recovery sweep can read every linked tail in one batch instead of one
// round trip per link. A path absent from tails is treated as a missing stream
// (no pending work), matching TailFunc's not-ok case. Short-circuits on the
// first pending link.
func HasPendingWorkFrom(links []StreamLink, tails map[string]string) bool {
	for _, l := range links {
		tail, ok := tails[l.Path]
		if !ok {
			continue
		}
		if offsetGreater(tail, l.AckedOffset) {
			return true
		}
	}
	return false
}

// offsetGreater reports a > b for opaque, lexicographically-sortable offsets,
// treating the protocol's "-1" beginning sentinel as less than any real offset.
func offsetGreater(a, b string) bool {
	if a == b {
		return false
	}
	if b == "-1" || b == "" {
		return a != "-1" && a != ""
	}
	if a == "-1" || a == "" {
		return false
	}
	return a > b
}

// RetryDelay is the webhook backoff for the nth attempt (1-based), PROTOCOL
// §7.1: exponential from 1 s, capped at 60 s, with up to 20% added jitter. The
// jitter fraction is injected (0 ≤ jitter < 1) so the function is deterministic.
func RetryDelay(attempt int, jitter float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := float64(minRetryDelay) * math.Pow(2, float64(attempt-1))
	if base > float64(maxRetryDelay) {
		base = float64(maxRetryDelay)
	}
	if jitter < 0 {
		jitter = 0
	} else if jitter >= 1 {
		jitter = math.Nextafter(1, 0)
	}
	return time.Duration(base * (1 + retryJitter*jitter))
}

// LeaseDeadlineNs returns the lease expiry, in Unix nanoseconds, for a lease
// armed at now with the given TTL (PROTOCOL §7.3).
func LeaseDeadlineNs(now time.Time, leaseTTLMs int64) int64 {
	return now.UnixNano() + ClampLeaseTTL(leaseTTLMs)*int64(time.Millisecond)
}

// LeaseExpired reports whether a lease deadline has passed at now.
func LeaseExpired(leaseUntilNs int64, now time.Time) bool {
	return leaseUntilNs != 0 && now.UnixNano() >= leaseUntilNs
}

// FenceDecision is the pure mirror of fence.lua: it returns "" when a callback,
// ack, or release may proceed, or an error code (ErrCodeFenced) when the request
// is stale. A request is fenced unless the token generation, request generation,
// and request wake_id all match current subscription state (PROTOCOL §7.3). The
// authoritative, atomic check runs in Lua; this exists for unit tests and must
// be changed together with fence.lua.
func FenceDecision(cur Subscription, reqGeneration int64, reqWakeID string, tokenGeneration int64) string {
	if tokenGeneration != cur.Generation ||
		reqGeneration != cur.Generation ||
		reqWakeID == "" || reqWakeID != cur.WakeID {
		return ErrCodeFenced
	}
	return ""
}

// ClaimDecision is the pure mirror of claim.lua's conflict check: a pull-wake
// claim is rejected with ALREADY_CLAIMED while another worker holds an unexpired
// lease (PROTOCOL §7.2). It returns ("", "") when the claim may proceed, or the
// error code and current holder when it is busy. On a grantable claim, see
// ClaimRotatesFence for whether the generation is rotated.
func ClaimDecision(cur Subscription, now time.Time) (code, holder string) {
	if cur.Phase == PhaseLive && cur.Holder && !LeaseExpired(cur.LeaseUntilNs, now) {
		return ErrCodeAlreadyClaimed, cur.HolderWorker
	}
	return "", ""
}

// ClaimRotatesFence reports whether a grantable claim mints a fresh
// generation/wake_id rather than reusing the in-flight one. The wake is reused
// only for the normal first claim of an already-issued pull-wake event (phase
// waking with a wake set); every other grantable case — idle, a cleared wake,
// or taking over an EXPIRED live lease — rotates the fence so the deposed holder
// is fenced out (its old-generation token can no longer ack). Mirror of
// claim.lua; change the two together.
func ClaimRotatesFence(phase Phase, wakeID string) bool {
	return phase != PhaseWaking || wakeID == ""
}

// DueAction is the pure decision for one due-set entry the dueWorker drains. The
// due-set (ds:{__ds}:due) is a "needs a wake" outbox, not a deadline queue: a
// mark means "this subscription may be owed a wake". A sum type, not a bool pair,
// so every drained mark resolves to exactly one of three reconciliations.
type DueAction int

const (
	// DueClear removes a stale mark: the subscription is gone, or it is idle with
	// its cursor caught up to every linked tail, so no wake is owed.
	DueClear DueAction = iota
	// DueFire issues a wake: the subscription is idle with pending work.
	DueFire
	// DueSkip leaves the mark untouched: a wake is already in flight (the
	// subscription is waking/live), so a re-fire would only coalesce (arm_wake
	// returns BUSY). The mark clears on the eventual done-ack or release.
	DueSkip
)

// DecideDue reconciles a due-set mark against a subscription's live phase and
// pending-work state (PROTOCOL §7). It is the pure core of the dueWorker's drain.
//
// Clearing a no-longer-owed mark is load-bearing, not housekeeping: claim_due
// re-scores due members forward and never ZREMs them (at-least-once by
// construction), and expire_lease re-owes unconditionally because the single-slot
// script cannot read a stream's tail to test pending work. So the dueWorker is
// the one place a mark is reconciled against pending state — without DueClear a
// caught-up or deleted subscription's mark would churn the due-set forever and
// its cardinality would never return to ~0 at quiescence.
func DecideDue(exists bool, phase Phase, hasPending bool) DueAction {
	if !exists {
		return DueClear
	}
	if phase != PhaseIdle {
		return DueSkip
	}
	if !hasPending {
		return DueClear
	}
	return DueFire
}

// LeaseReconcile is the pure decision for one subscription in the failover-aware
// eager reconcile (reconcileLeases, issue #13): whether its lease-schedule entry
// must be re-derived from the durable sub hash. A sealed sum type, not a bool, so
// the stranded case is named and the reconcile decision stays exhaustive and
// total — mirroring DecideDue.
type LeaseReconcile int

const (
	// LeaseIntact needs no repair: the subscription is idle (it holds no lease), or
	// it is mid-wake (live/waking) and still present in the lease ZSET, so the lease
	// worker can already see it.
	LeaseIntact LeaseReconcile = iota
	// LeaseStranded marks a subscription that is mid-wake (live/waking) with a lease
	// deadline but ABSENT from the lease ZSET: a failover dropped its schedule tail
	// (the L3 dropLeaseTail fault). The lease worker is blind to it — its ZADD is
	// gone — so the lapsed lease never expires and the subscription can never return
	// to idle to be re-fired. reconcileLeases re-ZADDs the entry from the durable
	// hash so the lease worker sees the lapse on its next tick.
	LeaseStranded
)

// DecideLeaseReconcile reconciles one subscription's presence in the lease
// schedule against its durable phase (PROTOCOL §7.3). It is the pure core of the
// failover-aware eager reconcile, the analogue of DecideDue for the lease ZSET.
//
// Recovering a stranded lease tail is load-bearing once the recovery floor is
// raised off 2 s (issue #13): the lease worker only ever sees a subscription via
// the lease ZSET, so a live/waking subscription whose entry a failover dropped is
// invisible to it and, absent this re-derivation, would wait for the coarse floor
// (seconds-to-minutes) rather than the fast lease tick. An idle subscription holds
// no lease, so it is never stranded here — its recovery is the due-set's job
// (DecideDue), not the lease schedule's.
func DecideLeaseReconcile(phase Phase, leaseUntilNs int64, inLeaseZSet bool) LeaseReconcile {
	if (phase == PhaseLive || phase == PhaseWaking) && leaseUntilNs > 0 && !inLeaseZSet {
		return LeaseStranded
	}
	return LeaseIntact
}

// MergeAcks applies acks to links, advancing each matching link's cursor
// forward-only (an ack that would move a cursor backward is ignored; offsets are
// last-processed-inclusive, PROTOCOL §7). Returns the updated links. Pure: the
// authoritative advance also runs forward-only in Lua.
func MergeAcks(links []StreamLink, acks []Ack) []StreamLink {
	byPath := make(map[string]string, len(acks))
	for _, a := range acks {
		byPath[a.Stream] = a.Offset
	}
	out := make([]StreamLink, len(links))
	copy(out, links)
	for i := range out {
		if off, ok := byPath[out[i].Path]; ok && offsetGreater(off, out[i].AckedOffset) {
			out[i].AckedOffset = off
		}
	}
	return out
}
