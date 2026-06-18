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

	// Claim is the pull-wake compare-and-set claim for one claim shard
	// (PROTOCOL §7.2 plus Chronicle's shard extension).
	Claim(id string, mode ClaimMode, shard ClaimShard, worker, wakeID string, now time.Time, leaseTTLMs int64) (ClaimResult, error)

	// Ack fences then applies acks forward-only; done releases the lease, else it
	// extends the lease as a heartbeat (PROTOCOL §7.1, §7.2).
	Ack(id string, mode ClaimMode, shard ClaimShard, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64) (string, error)

	// Release fences then releases the lease without acking (PROTOCOL §7.2).
	Release(id string, mode ClaimMode, shard ClaimShard, reqGeneration int64, reqWakeID string, tokenGeneration int64) (string, error)

	// ExpireLease clears an expired shard lease, returning that claim shard to
	// idle. pending records that the caller saw durable pending work and lets the
	// script atomically re-owe the subscription in the due set.
	ExpireLease(ref LeaseRef, now time.Time, pending bool) (string, error)

	// ReconcileLeaseSchedule mirrors a live/waking durable lease back into the
	// volatile schedules after failover drops a ZSET tail. It never changes the
	// subscription HASH fence or phase; pending re-derives due-set membership from
	// durable cursor state.
	ReconcileLeaseSchedule(ref LeaseRef, now time.Time, pending bool) (LeaseReconcileResult, error)

	// DueLeases / DueRetries / DueWakes take due schedule members by re-scoring
	// them forward to a visibility window (never removing them), so a crashed
	// worker's item recurs (docs/research/07 §6.1).
	DueLeases(now time.Time, limit int, visibility time.Duration) ([]LeaseRef, error)
	DueRetries(now time.Time, limit int, visibility time.Duration) ([]string, error)
	DueWakes(now time.Time, limit int, visibility time.Duration) ([]string, error)

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
	Claimed      bool
	Busy         bool // another worker holds an unexpired lease
	NoSub        bool
	ModeConflict bool // request tried to mix legacy and explicit-shard claims
	Mode         ClaimMode
	Generation   int64
	WakeID       string
	Holder       string
	LeaseLapsed  bool
}

// LeaseReconcileResult is the schedule-only outcome of ReconcileLeaseSchedule.
type LeaseReconcileResult struct {
	Reconciled    bool
	LeaseRepaired bool
	DueOp         string
}
