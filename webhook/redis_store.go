package webhook

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore implements Store on Redis 8, persisting the subscription control
// plane under the {__ds} hash tag. It shares the go-redis client with the stream
// store (chronicle uses one Redis), and does not own it: Close is a no-op.
type RedisStore struct {
	client redis.UniversalClient
	// metrics records claim/ack lease outcomes at the call sites (gate #6, the
	// per-type claim-contention SLIs). Defaults to NopMetrics so the store stays
	// usable without instrumentation; the binary wires the Prometheus recorder via
	// WithMetrics. It lives on the store, not the Manager, because the contention
	// fan-in driver and any direct claim/ack caller record the same signal.
	metrics Metrics
}

var _ Store = (*RedisStore)(nil)

// NewRedisStore wraps a go-redis client as a subscription Store.
func NewRedisStore(client redis.UniversalClient) *RedisStore {
	return &RedisStore{client: client, metrics: NopMetrics{}}
}

// WithMetrics sets the contention recorder and returns the store for chaining. A
// nil Metrics is treated as NopMetrics so the store is never left recording to a
// nil interface.
func (s *RedisStore) WithMetrics(m Metrics) *RedisStore {
	if m == nil {
		m = NopMetrics{}
	}
	s.metrics = m
	return s
}

func (s *RedisStore) ctx() context.Context { return context.Background() }

func nsArg(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

// evalStrings runs a script and decodes its reply as a slice of strings, the
// fixed reply shape of every subscription script.
func (s *RedisStore) evalStrings(script *redis.Script, keys []string, args ...any) ([]string, error) {
	raw, err := script.Run(s.ctx(), s.client, keys, args...).Result()
	if err != nil {
		return nil, err
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("webhook: unexpected script reply %T", raw)
	}
	out := make([]string, len(arr))
	for i, e := range arr {
		switch v := e.(type) {
		case string:
			out[i] = v
		case int64:
			out[i] = strconv.FormatInt(v, 10)
		case nil:
			out[i] = ""
		default:
			return nil, fmt.Errorf("webhook: unexpected reply element %d: %T", i, e)
		}
	}
	return out, nil
}

// CreateOrConfirm seeds the create_sub script with the config fields and the
// explicit links at their current tails.
func (s *RedisStore) CreateOrConfirm(id string, cfg Config, links []StreamLink, now time.Time) (CreateStatus, error) {
	cfg = NormalizeConfig(cfg)
	args := make([]any, 0, 10+3*len(links))
	args = append(args,
		id, ConfigHash(cfg), nsArg(now),
		string(cfg.Type), cfg.Pattern, cfg.WebhookURL, cfg.WakeStream,
		strconv.FormatInt(cfg.LeaseTTLMs, 10), cfg.Description,
		strconv.Itoa(len(links)),
	)
	for _, l := range links {
		args = append(args, l.Path, string(l.LinkType), l.AckedOffset)
	}
	reply, err := s.evalStrings(createSubScript, []string{subKey(id), subsKey, linksKey(id)}, args...)
	if err != nil {
		return 0, err
	}
	switch reply[0] {
	case "CREATED":
		for _, l := range links {
			if err := s.indexStream(l.Path, id); err != nil {
				return 0, err
			}
		}
		return CreateCreated, nil
	case "MATCHED":
		return CreateMatched, nil
	case "CONFLICT":
		return CreateConflict, nil
	default:
		return 0, fmt.Errorf("create_sub: unexpected status %q", reply[0])
	}
}

// Get hydrates a subscription and its links.
func (s *RedisStore) Get(id string) (Subscription, bool, error) {
	pipe := s.client.Pipeline()
	subCmd := pipe.HGetAll(s.ctx(), subKey(id))
	linkCmd := pipe.HGetAll(s.ctx(), linksKey(id))
	if _, err := pipe.Exec(s.ctx()); err != nil {
		return Subscription{}, false, err
	}
	fields := subCmd.Val()
	if len(fields) == 0 {
		return Subscription{}, false, nil
	}
	return subscriptionFromHash(id, fields, linkCmd.Val()), true, nil
}

// GetMany hydrates many subscriptions in one pipelined batch, chunked to bound
// the pipeline size. Missing subscriptions are skipped. It turns the recovery
// sweep's per-subscription Get round trips into a handful of batched ones.
func (s *RedisStore) GetMany(ids []string) ([]Subscription, error) {
	const chunk = 512
	out := make([]Subscription, 0, len(ids))
	for start := 0; start < len(ids); start += chunk {
		end := start + chunk
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		pipe := s.client.Pipeline()
		subCmds := make([]*redis.MapStringStringCmd, len(batch))
		linkCmds := make([]*redis.MapStringStringCmd, len(batch))
		for i, id := range batch {
			subCmds[i] = pipe.HGetAll(s.ctx(), subKey(id))
			linkCmds[i] = pipe.HGetAll(s.ctx(), linksKey(id))
		}
		if _, err := pipe.Exec(s.ctx()); err != nil {
			return nil, err
		}
		for i, id := range batch {
			fields := subCmds[i].Val()
			if len(fields) == 0 {
				continue
			}
			out = append(out, subscriptionFromHash(id, fields, linkCmds[i].Val()))
		}
	}
	return out, nil
}

// Delete removes the subscription and de-indexes its streams. Links are read
// first so the fan-out entries can be cleaned up.
func (s *RedisStore) Delete(id string) error {
	links, err := s.client.HKeys(s.ctx(), linksKey(id)).Result()
	if err != nil {
		return err
	}
	if _, err := s.evalStrings(deleteSubScript,
		[]string{subKey(id), subsKey, linksKey(id), leaseZKey, retryZKey}, id); err != nil {
		return err
	}
	for _, path := range links {
		if err := s.deindexStream(path, id); err != nil {
			return err
		}
	}
	return nil
}

// List returns all subscription ids.
func (s *RedisStore) List() ([]string, error) {
	return s.client.SMembers(s.ctx(), subsKey).Result()
}

// Link links a stream and maintains the fan-out index.
func (s *RedisStore) Link(id, path string, linkType LinkType, offset string) error {
	if _, err := s.evalStrings(linkStreamScript, []string{linksKey(id)}, path, string(linkType), offset); err != nil {
		return err
	}
	return s.indexStream(path, id)
}

// Unlink removes an explicit link; de-indexes only when the link is gone.
func (s *RedisStore) Unlink(id, path string, stillGlob bool) error {
	flag := "0"
	if stillGlob {
		flag = "1"
	}
	reply, err := s.evalStrings(unlinkStreamScript, []string{linksKey(id)}, path, flag)
	if err != nil {
		return err
	}
	if reply[0] == "REMOVED" {
		return s.deindexStream(path, id)
	}
	return nil
}

// StreamSubscribers returns the subscription ids linked to a stream.
func (s *RedisStore) StreamSubscribers(path string) ([]string, error) {
	return s.client.SMembers(s.ctx(), streamSubsKey(path)).Result()
}

// ReconcileIndexes rebuilds the per-stream fan-out index from the canonical
// links. The index (streamSubsKey) drives the low-latency OnStreamAppend trigger
// and is maintained from Go after the Lua link write, so a crash between them can
// drop an index entry while the canonical link survives — degrading that stream
// to sweep latency until repaired. This re-adds any missing SADD; it never
// invents membership (it only mirrors links). Stale-entry cleanup is deferred:
// re-adding the missing entry is the correctness-critical part.
func (s *RedisStore) ReconcileIndexes() error {
	ctx := s.ctx()
	ids, err := s.client.SMembers(ctx, subsKey).Result()
	if err != nil {
		return err
	}
	for _, id := range ids {
		paths, err := s.client.HKeys(ctx, linksKey(id)).Result()
		if err != nil {
			return err
		}
		for _, path := range paths {
			if err := s.client.SAdd(ctx, streamSubsKey(path), id).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *RedisStore) indexStream(path, id string) error {
	return s.client.SAdd(s.ctx(), streamSubsKey(path), id).Err()
}

func (s *RedisStore) deindexStream(path, id string) error {
	return s.client.SRem(s.ctx(), streamSubsKey(path), id).Err()
}

// ArmWake issues a wake if idle. An optional OwnerScope makes arm_wake inline the
// owner-epoch fence (issue #14): an owner-scoped caller deposed since it last
// claimed the slot is FENCED, suppressing its wasted re-arm. The external/hot path
// passes no scope (epoch ''), so the check is skipped and behavior is unchanged.
func (s *RedisStore) ArmWake(id string, now time.Time, leaseTTLMs int64, armLease bool, wakeID string, owner ...OwnerScope) (ArmResult, error) {
	arm := "0"
	if armLease {
		arm = "1"
	}
	sk, me, epoch := firstOwnerScope(owner)
	reply, err := s.evalStrings(armWakeScript, []string{subKey(id), leaseZKey, dueZKey, sk},
		id, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), arm, wakeID, me, epoch)
	if err != nil {
		return ArmResult{}, err
	}
	switch reply[0] {
	case "ARMED":
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		return ArmResult{Armed: true, Generation: gen, WakeID: reply[2]}, nil
	case "BUSY":
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		return ArmResult{Busy: true, Generation: gen, WakeID: reply[2]}, nil
	case "NOSUB":
		return ArmResult{NoSub: true}, nil
	case "FENCED":
		return ArmResult{Fenced: true}, nil
	default:
		return ArmResult{}, fmt.Errorf("arm_wake: unexpected status %q", reply[0])
	}
}

// Claim runs the pull-wake CAS claim on the subscription's single per-type lease
// (shard 0) — today's behavior, kept on the Store interface unchanged.
func (s *RedisStore) Claim(id, worker, wakeID string, now time.Time, leaseTTLMs int64) (ClaimResult, error) {
	return s.ClaimShard(id, 0, worker, wakeID, now, leaseTTLMs)
}

// ClaimShard runs the CAS claim against shard g of the subscription's claim space
// (claim granularity, design 08 §4): the single-holder fence is per (id, g), so
// concurrent claimants on different shards do not serialize. NOSUB keys off the
// subscription config; the per-shard fence (KEYS[2]) is minted on first claim. g
// == 0 is the bare per-type lease (== Claim), byte-for-byte today.
func (s *RedisStore) ClaimShard(id string, g int, worker, wakeID string, now time.Time, leaseTTLMs int64) (ClaimResult, error) {
	reply, err := s.evalStrings(claimScript, []string{subKey(id), subShardKey(id, g), leaseZKey},
		shardMember(id, g), worker, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), wakeID)
	if err != nil {
		return ClaimResult{}, err
	}
	switch reply[0] {
	case "CLAIMED":
		s.recordContention("claimed", id)
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		return ClaimResult{Claimed: true, Generation: gen, WakeID: reply[2], Holder: reply[3]}, nil
	case "BUSY":
		s.recordContention("already_claimed", id)
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		return ClaimResult{Busy: true, Generation: gen, Holder: reply[3]}, nil
	case "NOSUB":
		s.recordContention("nosub", id)
		return ClaimResult{NoSub: true}, nil
	default:
		return ClaimResult{}, fmt.Errorf("claim: unexpected status %q", reply[0])
	}
}

// recordContention reports a claim/ack/release lease outcome to the contention
// recorder (gate #6). status uses the fixed vocabulary documented on
// Metrics.ClaimContention; a nil recorder (a store built without NewRedisStore)
// is tolerated as a no-op.
func (s *RedisStore) recordContention(status, id string) {
	if s.metrics == nil {
		return
	}
	s.metrics.ClaimContention(status, id)
}

// Ack fences, applies acks, and releases or heartbeats on the subscription's
// single per-type lease (shard 0) — today's behavior, on the Store interface.
func (s *RedisStore) Ack(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64, owner ...OwnerScope) (string, error) {
	return s.AckShard(id, 0, reqGeneration, reqWakeID, tokenGeneration, done, acks, now, leaseTTLMs, owner...)
}

// AckShard fences, applies acks, and releases or heartbeats against shard g's
// per-(id,g) fence (claim granularity, design 08 §4). A token minted for shard g
// is FENCED against any other shard, so a holder of g cannot release or take over
// g'. The cursor hash is shared, so the named offsets advance forward-only as
// usual. g == 0 is the bare per-type lease (== Ack), byte-for-byte today.
func (s *RedisStore) AckShard(id string, g int, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64, owner ...OwnerScope) (string, error) {
	doneArg := "0"
	if done {
		doneArg = "1"
	}
	sk, me, epoch := firstOwnerScope(owner)
	args := make([]any, 0, 10+2*len(acks))
	args = append(args,
		shardMember(id, g), strconv.FormatInt(reqGeneration, 10), reqWakeID, strconv.FormatInt(tokenGeneration, 10),
		doneArg, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), strconv.Itoa(len(acks)),
	)
	for _, a := range acks {
		args = append(args, a.Stream, a.Offset)
	}
	// replica_id, expected_epoch are the trailing pair ack.lua reads via #ARGV
	// (after the variable-length acks) for the owner-epoch fence (issue #14).
	args = append(args, me, epoch)
	reply, err := s.evalStrings(ackScript, []string{subShardKey(id, g), linksKey(id), leaseZKey, retryZKey, dueZKey, sk}, args...)
	if err != nil {
		return "", err
	}
	s.recordContention(contentionStatusOf(reply[0]), id)
	return reply[0], nil
}

// Release fences then releases the lease. An optional OwnerScope makes release.lua
// inline the owner-epoch fence (GAP3 consistency, issue #14: release idles the sub
// and clears the due mark exactly like ack(done), so it joins the inline-check set).
func (s *RedisStore) Release(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, owner ...OwnerScope) (string, error) {
	sk, me, epoch := firstOwnerScope(owner)
	reply, err := s.evalStrings(releaseScript, []string{subKey(id), leaseZKey, retryZKey, dueZKey, sk},
		id, strconv.FormatInt(reqGeneration, 10), reqWakeID, strconv.FormatInt(tokenGeneration, 10), me, epoch)
	if err != nil {
		return "", err
	}
	s.recordContention(contentionStatusOf(reply[0]), id)
	return reply[0], nil
}

// contentionStatusOf maps an ack.lua/release.lua reply status to the
// ClaimContention vocabulary (OK -> "ok", FENCED -> "fenced", NOSUB -> "nosub").
// An unrecognized status is lowercased and recorded verbatim so a new reply can
// never be silently dropped from the gate-#6 rates.
func contentionStatusOf(reply string) string {
	switch reply {
	case "OK":
		return "ok"
	case "FENCED":
		return "fenced"
	case "NOSUB":
		return "nosub"
	default:
		return strings.ToLower(reply)
	}
}

// ExpireLease clears an expired lease. The lease worker is the primary owner-
// scoped caller: an OwnerScope makes expire_lease.lua inline the owner-epoch fence
// (issue #14), so a deposed owner expiring/re-owing leases it no longer owns is
// FENCED atomically with the ZREM/ZADD — the new owner alone drives the schedule.
func (s *RedisStore) ExpireLease(id string, now time.Time, owner ...OwnerScope) (string, error) {
	sk, me, epoch := firstOwnerScope(owner)
	reply, err := s.evalStrings(expireLeaseScript, []string{subKey(id), leaseZKey, dueZKey, sk}, id, nsArg(now), me, epoch)
	if err != nil {
		return "", err
	}
	return reply[0], nil
}

// LeasedIDs returns the members of the lease schedule ZSET (the failover-aware
// reconcile's view of what the lease worker can see). It is a single-key ZRANGE
// over the in-flight set, which is O(in-flight), not O(subscriptions).
func (s *RedisStore) LeasedIDs() ([]string, error) {
	return s.client.ZRange(s.ctx(), leaseZKey, 0, -1).Result()
}

// RestoreLease runs restore_lease.lua to re-derive a stranded subscription's
// dropped schedule entries from its durable sub hash (the L3 dropLeaseTail
// recovery). The script re-reads phase and lease_until_ns from the hash, so the
// restore is atomic with respect to a concurrent release/ack that idled the sub.
func (s *RedisStore) RestoreLease(id string, owed bool, now time.Time) (string, error) {
	owedArg := "0"
	if owed {
		owedArg = "1"
	}
	reply, err := s.evalStrings(restoreLeaseScript, []string{subKey(id), leaseZKey, dueZKey}, id, nsArg(now), owedArg)
	if err != nil {
		return "", err
	}
	return reply[0], nil
}

// DueLeases takes due lease-schedule members by re-scoring them forward, so a
// dropped worker's subscription recurs (docs/research/07 §6.1).
func (s *RedisStore) DueLeases(now time.Time, limit int, visibility time.Duration) ([]string, error) {
	return s.due(leaseZKey, now, limit, visibility)
}

// DueRetries takes due retry-schedule members by re-scoring them forward, the
// same re-score-never-ZREM machinery as DueLeases (docs/research/07 §6.1).
func (s *RedisStore) DueRetries(now time.Time, limit int, visibility time.Duration) ([]string, error) {
	return s.due(retryZKey, now, limit, visibility)
}

func (s *RedisStore) due(zkey string, now time.Time, limit int, visibility time.Duration) ([]string, error) {
	return s.evalStrings(claimDueScript, []string{zkey},
		nsArg(now), strconv.Itoa(limit), strconv.FormatInt(int64(visibility), 10))
}

// ClaimDue takes due members of the "needs a wake" due-set by re-scoring them
// forward — the same unchanged claim_due.lua / re-score-never-ZREM machinery as
// DueLeases/DueRetries (docs/research/07 §6.1), so the due-set is at-least-once by
// construction. The dueWorker drains it in O(owed) and reconciles each id via
// DecideDue; a mark only leaves the set on a done-ack/release ZREM or a dueWorker
// ClearDue, never here.
func (s *RedisStore) ClaimDue(now time.Time, limit int, visibility time.Duration) ([]string, error) {
	return s.due(dueZKey, now, limit, visibility)
}

// ClearDue removes a subscription's due-set wake mark. It is a single-key ZREM
// (always slot-safe), called by the dueWorker to reconcile away a mark that is no
// longer owed — the subscription is gone, or idle with its cursor caught up.
// Without it a caught-up mark would churn forever, since claim_due re-scores and
// never removes and expire_lease re-owes unconditionally.
func (s *RedisStore) ClearDue(id string) error {
	return s.client.ZRem(s.ctx(), dueZKey, id).Err()
}

// ScheduleRetry records a webhook failure and persists next_attempt; returns the
// new retry count.
func (s *RedisStore) ScheduleRetry(id string, now, nextAttempt time.Time, owner ...OwnerScope) (int, error) {
	sk, me, epoch := firstOwnerScope(owner)
	reply, err := s.evalStrings(scheduleRetryScript, []string{subKey(id), retryZKey, sk},
		id, nsArg(now), nsArg(nextAttempt), me, epoch)
	if err != nil {
		return 0, err
	}
	// NOSUB (gone) and FENCED (a deposed owner-scoped scheduler) both schedule
	// nothing; the caller treats a non-OK as "no retry recorded".
	if reply[0] == "NOSUB" || reply[0] == "FENCED" {
		return 0, nil
	}
	n, _ := strconv.Atoi(reply[1])
	return n, nil
}

// RecordSuccess clears webhook failure bookkeeping after an accepted delivery.
func (s *RedisStore) RecordSuccess(id string) error {
	_, err := s.evalStrings(recordSuccessScript, []string{subKey(id), retryZKey}, id)
	return err
}

// RecordWakeEventSent marks the current pull-wake event as durably emitted,
// fenced on (generation, wakeID) so a stamp from a superseded wake is ignored.
func (s *RedisStore) RecordWakeEventSent(id string, generation int64, wakeID string, now time.Time) error {
	_, err := s.evalStrings(recordWakeSentScript, []string{subKey(id)},
		nsArg(now), strconv.FormatInt(generation, 10), wakeID)
	return err
}

// ---- leased slot ownership (issue #14) ----

// ClaimSlot runs claim_shard.lua, the {ownership}-tagged CAS that grants slot
// ownership only when the current owner is expired, missing, or the caller, and
// bumps owner_epoch on transfer only. It is a thin wrapper: the SlotOwnership /
// OwnerFenced metrics are recorded by the Manager's slot-reconcile loop, which
// holds the SlotID and the held-lease context. slotLeaseTTL is the ownership
// lease TTL, a DIFFERENT layer from the per-subscription webhook lease_ttl_ms.
func (s *RedisStore) ClaimSlot(slotKey, replicaID string, now time.Time, slotLeaseTTL time.Duration) (SlotClaim, error) {
	reply, err := s.evalStrings(claimShardScript, []string{slotKey},
		replicaID, nsArg(now), strconv.FormatInt(slotLeaseTTL.Milliseconds(), 10))
	if err != nil {
		return SlotClaim{}, err
	}
	return parseSlotClaim(reply)
}

// CheckOwner runs check_owner.lua — the owner-epoch fence for the external
// webhook POST. expectedEpoch is the epoch the caller believes it holds, in the
// base-10 form OwnerEpoch.String produces (the same form claim_shard returned).
func (s *RedisStore) CheckOwner(slotKey, replicaID, expectedEpoch string) (OwnerCheck, error) {
	reply, err := s.evalStrings(checkOwnerScript, []string{slotKey}, replicaID, expectedEpoch)
	if err != nil {
		return OwnerCheckFenced, err
	}
	return parseOwnerCheck(reply)
}

// Heartbeat re-ZADDs this replica into the members ZSET at now+memberLeaseTTL and
// evicts members past their lease. Both ops are idempotent under single-threaded
// Redis, so every replica runs them; no leader is needed. The eviction uses an
// exclusive "(now" upper bound so a member whose lease lands exactly on now is
// kept (it is not yet expired).
func (s *RedisStore) Heartbeat(replicaID string, now time.Time, memberLeaseTTL time.Duration) error {
	ctx := s.ctx()
	expiry := float64(now.Add(memberLeaseTTL).UnixNano())
	if err := s.client.ZAdd(ctx, membersKey, redis.Z{Score: expiry, Member: replicaID}).Err(); err != nil {
		return err
	}
	upper := "(" + nsArg(now)
	return s.client.ZRemRangeByScore(ctx, membersKey, "-inf", upper).Err()
}

// LiveMembers returns the replica ids whose membership lease has not expired:
// ZRANGEBYSCORE over (now,+inf], so an entry whose expiry score is strictly
// greater than now is live. It is the set HRW assigns slots over.
func (s *RedisStore) LiveMembers(now time.Time) ([]string, error) {
	return s.client.ZRangeByScore(s.ctx(), membersKey, &redis.ZRangeBy{
		Min: "(" + nsArg(now),
		Max: "+inf",
	}).Result()
}

// LoadSigningKey adopts the persisted active key or installs a freshly-generated
// candidate, atomically (get_or_create_key). The kid is therefore stable across
// restarts (PROTOCOL §6.5).
func (s *RedisStore) LoadSigningKey(now time.Time) (SigningKey, error) {
	cand, err := GenerateSigningKey(rand.Reader, now)
	if err != nil {
		return SigningKey{}, err
	}
	reply, err := s.evalStrings(getOrCreateKeyScript, []string{jwksKey, activeKidKey},
		cand.Kid, marshalKeyMaterial(cand))
	if err != nil {
		return SigningKey{}, err
	}
	return unmarshalKeyMaterial(reply[0], reply[1])
}

// SigningKeys returns all persisted keys (active first) for the JWKS.
func (s *RedisStore) SigningKeys() ([]SigningKey, error) {
	all, err := s.client.HGetAll(s.ctx(), jwksKey).Result()
	if err != nil {
		return nil, err
	}
	activeKid, _ := s.client.Get(s.ctx(), activeKidKey).Result()
	keys := make([]SigningKey, 0, len(all))
	for kid, material := range all {
		k, err := unmarshalKeyMaterial(kid, material)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	// Active key first so the JWKS lists it as the preferred verification key.
	for i, k := range keys {
		if k.Kid == activeKid && i != 0 {
			keys[0], keys[i] = keys[i], keys[0]
			break
		}
	}
	return keys, nil
}

// LoadTokenKey adopts or installs the persisted HMAC token key, so callback and
// claim tokens issued before a restart still validate (PROTOCOL §12.9).
func (s *RedisStore) LoadTokenKey() ([]byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	cand := base64.RawURLEncoding.EncodeToString(raw)
	ok, err := s.client.SetNX(s.ctx(), tokenKeyKey, cand, 0).Result()
	if err != nil {
		return nil, err
	}
	if ok {
		return raw, nil
	}
	stored, err := s.client.Get(s.ctx(), tokenKeyKey).Result()
	if err != nil {
		return nil, err
	}
	return base64.RawURLEncoding.DecodeString(stored)
}

// marshalKeyMaterial encodes a signing key as "<priv_b64url>:<created_unix>:<status>".
// The public half is recovered from the private key.
func marshalKeyMaterial(k SigningKey) string {
	return strings.Join([]string{
		base64.RawURLEncoding.EncodeToString(k.Private),
		strconv.FormatInt(k.CreatedAt.Unix(), 10),
		k.Status,
	}, ":")
}

func unmarshalKeyMaterial(kid, material string) (SigningKey, error) {
	parts := strings.SplitN(material, ":", 3)
	if len(parts) != 3 {
		return SigningKey{}, fmt.Errorf("webhook: malformed key material for %q", kid)
	}
	priv, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return SigningKey{}, err
	}
	if len(priv) != ed25519.PrivateKeySize {
		return SigningKey{}, fmt.Errorf("webhook: bad ed25519 private key length %d", len(priv))
	}
	created, _ := strconv.ParseInt(parts[1], 10, 64)
	pk := ed25519.PrivateKey(priv)
	return SigningKey{
		Kid:       kid,
		Private:   pk,
		Public:    pk.Public().(ed25519.PublicKey),
		CreatedAt: time.Unix(created, 0),
		Status:    parts[2],
	}, nil
}

// parseLeaseUntilNs reads the lease deadline from the sub hash. arm_wake/claim/ack
// compute lease_until_ns as now_ns + ttl in Lua, where every number is an IEEE
// double, and persist it via tostring — which renders a ~1.8e18 ns value in
// %.14g scientific notation ("1.78e+18"), not a base-10 integer. strconv.ParseInt
// rejects that, so the failover-aware reconcile (issue #13), which keys off
// lease_until_ns > 0, must parse tolerantly through a float. The sub-microsecond
// precision the Lua double already dropped is irrelevant to a lease deadline, and
// the authoritative views — the lease-ZSET score and expire_lease.lua's tonumber —
// read the same value, so this only realigns the Go hash view with the schedule.
//
// Behavior note (fleet-wide, intentional): before this fix ParseInt rejected the
// float string and returned 0 for every armed/claimed webhook sub, so the Go-side
// deadline checks that read LeaseUntilNs — sweepOnce's lease-expiry flip and
// ClaimDecision's BUSY-vs-rotate deadline (state.go) — were effectively dead
// no-ops, with lease expiry driven only by the lease worker's Lua tonumber. This
// realignment activates those Go-side checks to match the Lua's authoritative
// behavior; the claim-fence outcome is pinned by TestClaimUnexpiredLeaseStillBusy
// and TestClaimExpiredLeaseRotatesFence, and a re-ZADD only writes schedule
// entries (never the generation/wake_id fence), so it cannot double-grant.
func parseLeaseUntilNs(s string) int64 {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f)
	}
	return 0
}

// subscriptionFromHash decodes the sub HASH and links HASH into a Subscription.
func subscriptionFromHash(id string, f map[string]string, linkFields map[string]string) Subscription {
	atoi := func(k string) int64 { n, _ := strconv.ParseInt(f[k], 10, 64); return n }
	createdNs := atoi("created_ns")
	sub := Subscription{
		ID: id,
		Config: Config{
			Type:        DispatchType(f["type"]),
			Pattern:     f["pattern"],
			WebhookURL:  f["webhook_url"],
			WakeStream:  f["wake_stream"],
			LeaseTTLMs:  atoi("lease_ttl_ms"),
			Description: f["description"],
		},
		CfgHash:         f["cfg_hash"],
		CreatedAt:       time.Unix(0, createdNs),
		Status:          Status(f["status"]),
		Phase:           Phase(f["phase"]),
		Generation:      atoi("generation"),
		WakeID:          f["wake_id"],
		Holder:          f["holder"] == "1",
		HolderWorker:    f["holder_worker"],
		LeaseUntilNs:    parseLeaseUntilNs(f["lease_until_ns"]),
		RetryCount:      int(atoi("retry_count")),
		FirstFailNs:     atoi("first_fail_ns"),
		NextAttemptNs:   atoi("next_attempt_ns"),
		WakeEventSentNs: atoi("wake_event_sent_ns"),
	}
	sub.Links = linksFromHash(linkFields)
	// Rebuild the normalized explicit stream list so the config round-trips for
	// idempotency checks after a reload.
	for _, l := range sub.Links {
		if l.LinkType == LinkExplicit {
			sub.Config.Streams = append(sub.Config.Streams, l.Path)
		}
	}
	sub.Config.Streams = normalizeStreams(sub.Config.Streams)
	return sub
}

func linksFromHash(linkFields map[string]string) []StreamLink {
	links := make([]StreamLink, 0, len(linkFields))
	for path, v := range linkFields {
		lt, off, ok := strings.Cut(v, ":")
		if !ok {
			continue
		}
		links = append(links, StreamLink{Path: path, LinkType: LinkType(lt), AckedOffset: off})
	}
	return links
}
