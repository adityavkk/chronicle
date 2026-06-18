package webhook

import "time"

// Store is the durable persistence contract for the subscription control plane
// — the parallel of store.Store for the __ds layer. It owns all subscription
// state: configuration, per-stream cursors, generation, lease, retry schedule,
// the fan-out index, and the signing/token key material. Every method that
// mutates wake/lease/generation state is a single atomic Redis operation so
// correctness never depends on the caller serializing.
//
// The RedisStore implementation persists everything so the PROTOCOL §6–7
// "MUST survive a restart" requirements hold; an in-memory implementation could
// satisfy the same interface for tests, deliberately failing those requirements.
type Store interface {
	// CreateOrConfirm creates a subscription, or returns Matched when an
	// identical configuration already exists, or Conflict on a same-id
	// different-config collision (PROTOCOL §6.2). links seeds explicit streams
	// at their current tail.
	CreateOrConfirm(id string, cfg Config, links []StreamLink, now time.Time) (CreateStatus, error)

	// Get returns a subscription with its Links hydrated, and whether it exists.
	Get(id string) (Subscription, bool, error)

	// GetMany hydrates many subscriptions in one pipelined batch, omitting any
	// that no longer exist (order is not significant). It is the batched form of
	// Get for the recovery sweep, which reads every subscription per tick.
	GetMany(ids []string) ([]Subscription, error)

	// Delete tombstones a subscription and removes its fan-out index entries.
	Delete(id string) error

	// List returns all subscription ids (for the recovery sweep).
	List() ([]string, error)

	// Link links a stream to a subscription at offset if absent; an explicit
	// link upgrades an existing glob link. Maintains the fan-out index.
	Link(id, path string, linkType LinkType, offset string) error

	// Unlink removes an explicit link; if stillGlob the link is kept as a glob
	// link (cursor preserved), else removed and de-indexed.
	Unlink(id, path string, stillGlob bool) error

	// StreamSubscribers returns the subscription ids linked to a stream.
	StreamSubscribers(path string) ([]string, error)

	// ReconcileIndexes rebuilds the per-stream fan-out index from the canonical
	// links, re-adding any membership a crash dropped between the link write and
	// the index update. It only mirrors links and never invents membership.
	ReconcileIndexes() error

	// ArmWake issues a new wake generation if the subscription is idle; armLease
	// arms the lease at issue (webhook) versus deferring it to claim (pull-wake).
	ArmWake(id string, now time.Time, leaseTTLMs int64, armLease bool, wakeID string) (ArmResult, error)

	// Claim is the pull-wake compare-and-set claim (PROTOCOL §7.2).
	Claim(id, worker, wakeID string, now time.Time, leaseTTLMs int64) (ClaimResult, error)

	// Ack fences then applies acks forward-only; done releases the lease, else it
	// extends the lease as a heartbeat (PROTOCOL §7.1, §7.2).
	Ack(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64) (string, error)

	// Release fences then releases the lease without acking (PROTOCOL §7.2).
	Release(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64) (string, error)

	// ExpireLease clears an expired lease, returning the subscription to idle.
	ExpireLease(id string, now time.Time) (string, error)

	// LeasedIDs returns the members currently in the lease schedule ZSET — the set
	// the lease worker can see. The failover-aware eager reconcile diffs the durable
	// subscription set against this to find a live/waking sub whose lease tail a
	// failover dropped (absent here, but live/waking in its hash).
	LeasedIDs() ([]string, error)

	// RestoreLease re-derives a stranded subscription's dropped schedule entries
	// from its durable sub hash (issue #13): it re-ZADDs the lease entry at the
	// hash's lease_until_ns while the sub is still live/waking, and re-owes the due
	// mark when owed (pending work, which the caller computes — the single-slot
	// script cannot read a stream tail). Conditioned on the live/waking phase so a
	// sub a concurrent release/ack idled is left untouched (no stale schedule entry
	// leaked back for claim_due to churn). Returns RESTORED, INTACT, or NOSUB.
	RestoreLease(id string, owed bool, now time.Time) (string, error)

	// DueLeases / DueRetries take due schedule members by re-scoring them forward
	// to a visibility window (never removing them), so a crashed worker's item
	// recurs (docs/research/07 §6.1).
	DueLeases(now time.Time, limit int, visibility time.Duration) ([]string, error)
	DueRetries(now time.Time, limit int, visibility time.Duration) ([]string, error)

	// ClaimDue takes due members of the "needs a wake" due-set outbox, re-scoring
	// them forward like DueLeases/DueRetries (Move 2). The dueWorker drains it in
	// O(owed) instead of re-evaluating every subscription.
	ClaimDue(now time.Time, limit int, visibility time.Duration) ([]string, error)

	// ClearDue removes a subscription's due-set mark when it is no longer owed (the
	// dueWorker's reconcile). claim_due never removes, so this is how a caught-up or
	// deleted subscription leaves the due-set and its cardinality returns to ~0.
	ClearDue(id string) error

	// ScheduleRetry records a webhook failure and persists next_attempt; returns
	// the new retry count.
	ScheduleRetry(id string, now, nextAttempt time.Time) (int, error)

	// RecordSuccess clears webhook failure bookkeeping after an accepted delivery.
	RecordSuccess(id string) error

	// RecordWakeEventSent stamps that the current pull-wake event was durably
	// appended to the wake stream, fenced on (generation, wakeID). A no-op when
	// the wake has been superseded.
	RecordWakeEventSent(id string, generation int64, wakeID string, now time.Time) error

	// LoadSigningKey adopts or installs the persisted active webhook signing key.
	LoadSigningKey(now time.Time) (SigningKey, error)

	// SigningKeys returns all persisted signing keys for the JWKS endpoint.
	SigningKeys() ([]SigningKey, error)

	// LoadTokenKey adopts or installs the persisted HMAC token key.
	LoadTokenKey() ([]byte, error)
}

// CreateStatus is the outcome of CreateOrConfirm.
type CreateStatus int

// Create outcomes, mapped to HTTP status by the routes layer.
const (
	CreateCreated  CreateStatus = iota // 201
	CreateMatched                      // 200 idempotent
	CreateConflict                     // 409 different config
)

// ArmResult is the outcome of ArmWake.
type ArmResult struct {
	Armed      bool // a new wake was issued
	Busy       bool // a wake was already in flight or a lease was held
	NoSub      bool // the subscription no longer exists
	Generation int64
	WakeID     string
}

// ClaimResult is the outcome of Claim.
type ClaimResult struct {
	Claimed    bool
	Busy       bool // another worker holds an unexpired lease
	NoSub      bool
	Generation int64
	WakeID     string
	Holder     string
}
