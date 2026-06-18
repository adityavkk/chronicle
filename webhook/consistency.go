package webhook

import (
	"fmt"
	"strings"
)

// defaultWaitTimeoutMs bounds the server-side WAIT/WAITAOF block on the
// fence-minting write. Kept well under go-redis' default 3s read timeout so the
// raw Do() call returns the achieved-ack count on a slow replica instead of the
// client tripping its own i/o deadline.
const defaultWaitTimeoutMs = 1000

// ConsistencyTier is the per-deployment (and, as a config-surface extension,
// per-subscription) durability + freshness setting for the fence-minting writes
// (doc 05 "Tunable consistency", 05:588-605). It is a SEALED SUM TYPE
// (TierA|TierB|TierC), never a bool or a free string, so an out-of-set tier is
// unrepresentable and the WAIT-vs-no-WAIT decision is a total function.
//
// CRITICAL (doc 06 correction #3, 06:114-122): the tier is a DURABILITY +
// read-freshness knob, NOT a consistency/linearizability knob. WAIT/WAITAOF buy
// replica-acked / fsync durability only. The single strong guarantee — the one
// that holds exclusivity across a region failover and immunizes correctness
// against cross-region clock skew — stays the monotonic (gen,wake_id) fence. NO
// code path may read a WAIT count to infer ordering or who holds the lease.
type ConsistencyTier int

const (
	// TierA — at-least-once, fast (the default). No WAIT; the wake is issued on the
	// local primary and, if a failover loses it, re-fired post-failover and deduped
	// by the fence. Best latency; RPO = full async replication lag.
	TierA ConsistencyTier = iota
	// TierB — durable wake. WAITAOF on the fence-minting write (the arm_wake / claim
	// generation HINCRBY) BEFORE dispatch, with the client checking the returned
	// pair. RPO ~ the AOF fsync interval. Costs one write round-trip per arm and
	// needs AOF + an in-region replica. The ONLY tier that touches the hot path.
	TierB
	// TierC — read-your-writes. Carries a freshness token (a D1-Sessions-bookmark-
	// style monotonic generation) so a replica read blocks until it has applied
	// that generation, else falls back to the primary. A read-path concern: its
	// fence-minting WRITE behaves like TierA (no extra WAIT). The token plumbing is
	// designed here and STUBBED (see FreshnessToken) — out of hot-path scope for #16.
	TierC
)

// String renders the tier as its config letter ("A"/"B"/"C").
func (t ConsistencyTier) String() string {
	switch t {
	case TierA:
		return "A"
	case TierB:
		return "B"
	case TierC:
		return "C"
	default:
		return "unknown"
	}
}

// ConsistencyError is the typed parse failure for an unknown tier (the package's
// wrapped-error discipline rather than a bare errors.New).
type ConsistencyError struct{ value string }

func (e *ConsistencyError) Error() string {
	return fmt.Sprintf("webhook: unknown consistency tier %q (want A, B, or C)", e.value)
}

// ParseConsistencyTier parses a config string into the sealed tier (parse, don't
// validate): an empty string defaults to TierA, "a"/"b"/"c" (any case, with an
// optional "tier-"/"tier_"/"tier" prefix) select the tier, and anything else is a
// typed error so a misconfigured deployment fails fast rather than silently
// running the wrong durability path.
func ParseConsistencyTier(s string) (ConsistencyTier, error) {
	v := strings.ToLower(strings.TrimSpace(s))
	for _, p := range []string{"tier-", "tier_", "tier"} {
		v = strings.TrimPrefix(v, p)
	}
	switch v {
	case "", "a":
		return TierA, nil
	case "b":
		return TierB, nil
	case "c":
		return TierC, nil
	default:
		return TierA, &ConsistencyError{value: s}
	}
}

// DurabilityPlan is the pure description of the durability barrier a tier imposes
// on a fence-minting write — what WAIT/WAITAOF to issue and how to read its reply.
// It carries NO exclusivity meaning (correction #3): it is a recipe for a
// durability gate and nothing else.
type DurabilityPlan struct {
	// Wait is false for tiers that never block the write (A and C). When false the
	// store issues no WAIT/WAITAOF at all — the Tier A "issues no WAIT" path.
	Wait bool
	// UseAOF selects WAITAOF (local + replica AOF fsync, Tier B) over plain WAIT
	// (replica memory ack). Tier B is WAITAOF.
	UseAOF bool
	// NumLocal / NumReplicas are the acks REQUIRED for the write to count as
	// durable. WAITAOF's canonical Tier B value is (1,1): one local AOF fsync + one
	// replica AOF fsync. In Redis Software HA numreplicas is always 1, so 1 is the
	// realistic per-shard ceiling (06:70). The single-Redis local rig sets
	// NumReplicas=0 (local fsync only) since it has no replica.
	NumLocal    int
	NumReplicas int
	// TimeoutMs bounds the server-side block. On timeout WAIT/WAITAOF return the
	// acks ACHIEVED so far (which may be short) rather than erroring — the client
	// MUST inspect the count, which is exactly what InterpretWaitAOF/InterpretWait do.
	TimeoutMs int
}

// DurabilityFor maps a tier to its write-path durability plan (pure, total). Only
// Tier B imposes a barrier; A and C leave the write on the local primary. The
// caller supplies the deployment's replica requirement and timeout so the same
// Tier B plan adapts to the single-Redis local rig (numReplicas 0 — local AOF
// fsync only) and the STANDARD_HA substrate (numReplicas 1) with no code change.
func DurabilityFor(tier ConsistencyTier, numReplicas, timeoutMs int) DurabilityPlan {
	if tier != TierB {
		return DurabilityPlan{} // Wait:false — A and C issue no WAIT on the write
	}
	if numReplicas < 0 {
		numReplicas = 0
	}
	if timeoutMs <= 0 {
		timeoutMs = defaultWaitTimeoutMs
	}
	return DurabilityPlan{Wait: true, UseAOF: true, NumLocal: 1, NumReplicas: numReplicas, TimeoutMs: timeoutMs}
}

// DurabilityShortError is the surfaced (NEVER swallowed) result of a WAIT/WAITAOF
// reply that fell short of the plan's required acks: the fence-minting write
// reached the primary but its durability could not be proven within TimeoutMs, so
// a failover could lose it. It is a DURABILITY verdict ONLY — it says nothing
// about who holds the lease; the (gen,wake_id) fence already decided that. It
// deliberately carries only ack counts, never a holder/generation, so it cannot
// be laundered into an exclusivity decision.
type DurabilityShortError struct {
	WantLocal, GotLocal       int
	WantReplicas, GotReplicas int
	UseAOF                    bool
}

func (e *DurabilityShortError) Error() string {
	cmd := "WAIT"
	if e.UseAOF {
		cmd = "WAITAOF"
	}
	return fmt.Sprintf(
		"webhook: %s durability short: local %d/%d, replicas %d/%d (write reached primary, durability unproven — fence still governs exclusivity)",
		cmd, e.GotLocal, e.WantLocal, e.GotReplicas, e.WantReplicas,
	)
}

// InterpretWaitAOF reduces a WAITAOF reply (the [numlocal, numreplicas] pair) to a
// single DURABILITY verdict against the plan: nil when both ack counts met the
// requirement, else a *DurabilityShortError. It is a pure total function and its
// ONLY output is durability — it returns no count, no holder, no ordering, so no
// caller can launder a WAIT count into an exclusivity decision (correction #3). A
// short reply is an error, never swallowed and never read as "I hold the lease".
// An over-ack (more replicas than required) is still just "durable" — a larger
// count conveys no stronger guarantee than the fence already provides.
func InterpretWaitAOF(plan DurabilityPlan, gotLocal, gotReplicas int) error {
	if !plan.Wait {
		return nil
	}
	if gotLocal < plan.NumLocal || gotReplicas < plan.NumReplicas {
		return &DurabilityShortError{
			WantLocal: plan.NumLocal, GotLocal: gotLocal,
			WantReplicas: plan.NumReplicas, GotReplicas: gotReplicas,
			UseAOF: true,
		}
	}
	return nil
}

// InterpretWait reduces a plain WAIT reply (a single replica-ack count) to a
// durability verdict. Like InterpretWaitAOF its only output is durability; a
// short count is a surfaced error, an over-ack is still just "durable".
func InterpretWait(plan DurabilityPlan, gotReplicas int) error {
	if !plan.Wait {
		return nil
	}
	if gotReplicas < plan.NumReplicas {
		return &DurabilityShortError{
			WantReplicas: plan.NumReplicas, GotReplicas: gotReplicas,
			UseAOF: false,
		}
	}
	return nil
}

// FreshnessToken is the Tier C read-your-writes bookmark (doc 05 Tier C; doc 06
// correction #3 "WAIT-on-writer != fresh reader", 06:128-131): a strictly-
// monotonic generation a reader carries until a replica has applied at least that
// generation (the D1 Sessions-API bookmark / Cosmos session-token model). It is
// DESIGNED here and STUBBED — Tier C's config surface is real, but the per-read
// token plumbing (replica-lag check + primary fallback) is out of #16's hot-path
// scope. NewFreshnessToken / Stale capture the contract for the follow-on slice.
type FreshnessToken struct {
	// Generation is the fence generation the write minted; a read is "fresh" for
	// this token only once the replica it reads has applied at least Generation.
	Generation int64
}

// NewFreshnessToken builds the bookmark a Tier C write hands back to the reader.
func NewFreshnessToken(generation int64) FreshnessToken {
	return FreshnessToken{Generation: generation}
}

// Stale reports whether a replica that has applied appliedGeneration is too stale
// to satisfy this token, so the Tier C read path must fall back to the primary.
// Pure; the stub the plumbing will call once the read path carries the token.
func (t FreshnessToken) Stale(appliedGeneration int64) bool {
	return appliedGeneration < t.Generation
}
