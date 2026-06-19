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
	// durPlan is the tunable-consistency durability barrier issued on the
	// fence-minting writes (#16, doc 05 Tier B). The zero value (Wait:false) is
	// Tier A — no WAIT, today's behavior — so a store built without WithConsistency
	// is byte-for-byte unchanged. Only Tier B populates it.
	durPlan DurabilityPlan
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

// WithConsistency sets the tunable-consistency tier for the fence-minting writes
// and returns the store for chaining (#16, doc 05 Tier B). Tier A/C leave the
// store on the no-WAIT default; Tier B arms a WAITAOF (numLocal=1, numReplicas)
// barrier on arm_wake/claim with the given server-side timeout. numReplicas is
// the deployment's replica requirement (1 on the STANDARD_HA substrate — the
// Redis Software HA ceiling, 06:70; 0 on the single-Redis local rig — local AOF
// fsync only). timeoutMs<=0 falls back to the default.
func (s *RedisStore) WithConsistency(tier ConsistencyTier, numReplicas, timeoutMs int) *RedisStore {
	s.durPlan = DurabilityFor(tier, numReplicas, timeoutMs)
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

// awaitDurable issues the tier's WAIT/WAITAOF barrier on the fence-minting write
// that just completed on this store's client and reduces the reply to a DURABILITY
// verdict (Tier B; a no-op for Tier A/C). It is the thin IO shell over the pure
// InterpretWaitAOF/InterpretWait core (FCIS).
//
// WAIT/WAITAOF block until the master's CURRENT replication/AOF offset is
// acknowledged, and that offset is monotonic, so issuing the barrier immediately
// after the EVAL is a correct (conservative) durability gate even though go-redis
// may route it on a different pooled connection — it can only over-wait, never
// under-wait, for our write. go-redis types WaitAOF as an IntCmd and cannot parse
// the [numlocal, numreplicas] array, so the store issues both via Do().
//
// CRITICAL (correction #3): the returned count flows ONLY into the pure
// interpreters, whose sole output is durability. It never feeds the fence /
// exclusivity decision — the Lua reply already made that. A short reply is
// surfaced as an error, never swallowed and never read as "I hold the lease".
func (s *RedisStore) awaitDurable() error {
	plan := s.durPlan
	if !plan.Wait {
		return nil
	}
	if plan.UseAOF {
		raw, err := s.client.Do(s.ctx(), "WAITAOF", plan.NumLocal, plan.NumReplicas, plan.TimeoutMs).Slice()
		if err != nil {
			return fmt.Errorf("webhook: WAITAOF: %w", err)
		}
		gotLocal, gotReplicas := waitAOFCounts(raw)
		return InterpretWaitAOF(plan, gotLocal, gotReplicas)
	}
	n, err := s.client.Do(s.ctx(), "WAIT", plan.NumReplicas, plan.TimeoutMs).Int()
	if err != nil {
		return fmt.Errorf("webhook: WAIT: %w", err)
	}
	return InterpretWait(plan, n)
}

// waitAOFCounts decodes WAITAOF's [numlocal, numreplicas] integer-pair reply.
func waitAOFCounts(raw []any) (local, replicas int) {
	if len(raw) > 0 {
		local = replyInt(raw[0])
	}
	if len(raw) > 1 {
		replicas = replyInt(raw[1])
	}
	return local, replicas
}

// replyInt coerces a RESP integer reply element to int (go-redis decodes RESP
// integers as int64).
func replyInt(v any) int {
	switch n := v.(type) {
	case int64:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
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
	// h once per id: the sub hash, its id-set, and its links share one {__ds:h}
	// slot, so create_sub.lua stays single-slot.
	h := slotOf(id)
	reply, err := s.evalStrings(createSubScript, []string{subKey(id), subsKey(h), linksKey(id)}, args...)
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

// Get hydrates a subscription and its links from the slot-homed tag. On a miss it
// lazily migrates a legacy ({__ds}) copy into the slot-homed keyspace and re-reads
// (shadow-write + lazy per-sub migration, 05 §Migration) — so a pre-slot-homing sub
// is migrated on its first access, before any slot-homed arm/ack would see it absent.
func (s *RedisStore) Get(id string) (Subscription, bool, error) {
	sub, ok, err := s.getSlotHomed(id)
	if err != nil || ok {
		return sub, ok, err
	}
	migrated, merr := s.migrateSub(id)
	if merr != nil {
		return Subscription{}, false, merr
	}
	if !migrated {
		return Subscription{}, false, nil
	}
	return s.getSlotHomed(id)
}

// getSlotHomed reads a subscription only from its slot-homed keyspace (no migration).
func (s *RedisStore) getSlotHomed(id string) (Subscription, bool, error) {
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
				// A slot-homed miss: lazily migrate a legacy copy and re-read it. Rare
				// (only during the migration window); the common batch is all hits.
				if sub, ok, err := s.Get(id); err == nil && ok {
					out = append(out, sub)
				}
				continue
			}
			out = append(out, subscriptionFromHash(id, fields, linkCmds[i].Val()))
		}
	}
	return out, nil
}

// Delete removes the subscription and de-indexes its streams. Links are read
// first so the fan-out entries can be cleaned up. h once per id keeps delete_sub.lua
// (5 keys) single-slot.
func (s *RedisStore) Delete(id string) error {
	links, err := s.client.HKeys(s.ctx(), linksKey(id)).Result()
	if err != nil {
		return err
	}
	h := slotOf(id)
	if _, err := s.evalStrings(deleteSubScript,
		[]string{subKey(id), subsKey(h), linksKey(id), leaseZKey(h), retryZKey(h)}, id); err != nil {
		return err
	}
	for _, path := range links {
		if err := s.deindexStream(path, id); err != nil {
			return err
		}
	}
	return nil
}

// List returns all subscription ids, UNIONed across the S per-slot id-sets (GAP4):
// under slot-homing the ids no longer live in one global SET, so reading a single
// key would silently see only slot 0. The S SMEMBERS are pipelined into one batch
// (go-redis groups per cluster node, so ~max-node-RTT, not S serial). The sweep's
// unguarded backstop relies on this seeing EVERY slot, owned or not (05:439).
func (s *RedisStore) List() ([]string, error) {
	ctx := s.ctx()
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, subSlots)
	for h := 0; h < subSlots; h++ {
		cmds[h] = pipe.SMembers(ctx, subsKey(h))
	}
	// Also union the legacy id-set so the sweep enumerates pre-slot-homing subs that
	// have not been touched (and thus not yet lazily migrated) since deploy — a Get on
	// each migrates it. Deduped against the slot-homed sets (a half-migrated sub can
	// appear in both for one tick).
	legacyCmd := pipe.SMembers(ctx, subsKeyLegacy)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, c := range cmds {
		for _, id := range c.Val() {
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	for _, id := range legacyCmd.Val() {
		if _, dup := seen[id]; !dup {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out, nil
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

// StreamSubscribers returns the subscriber ids linked to a stream by
// SCATTER-GATHERING across the per-slot fan-out shards (GAP4 / the gate-#2 fan-out):
// it reads the per-stream occupied-slots bitmap once, then issues one SMEMBERS per
// SET bit, pipelined into a single batch (go-redis groups per cluster node, so the
// wall-clock is ~max-node-RTT, not S serial). The bitmap collapses sparse-wide-stream
// cost from S to occupied-slots-per-stream (05:490-500). It also reports slotsProbed
// — the number of slots actually probed — for the FanOut metric OnStreamAppend feeds
// to gate #2. A subscriber homed in any slot is found; none from a foreign slot is
// returned (T5: scatter-gather set ≡ the canonical subscriber set, no foreign wake).
func (s *RedisStore) StreamSubscribers(path string) (ids []string, slotsProbed int, err error) {
	ctx := s.ctx()
	raw, gerr := s.client.Get(ctx, streamSlotsKey(path)).Result()
	if gerr == redis.Nil {
		return nil, 0, nil // no slot has a subscriber for this stream yet
	}
	if gerr != nil {
		return nil, 0, gerr
	}
	occ := decodeOccupiedSlots(raw)
	slots := occ.Slots()
	if len(slots) == 0 {
		return nil, 0, nil
	}
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(slots))
	for i, h := range slots {
		cmds[i] = pipe.SMembers(ctx, streamSubsKey(h, path))
	}
	if _, eerr := pipe.Exec(ctx); eerr != nil {
		return nil, len(slots), eerr
	}
	for _, c := range cmds {
		ids = append(ids, c.Val()...)
	}
	return ids, len(slots), nil
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
	// UNION the canonical id set across the S per-slot id-sets (GAP4) — reading a
	// single global SET would silently see only slot 0.
	ids, err := s.List()
	if err != nil {
		return err
	}
	for _, id := range ids {
		paths, err := s.client.HKeys(ctx, linksKey(id)).Result()
		if err != nil {
			return err
		}
		for _, path := range paths {
			// Re-add the fan-out membership in the SUBSCRIBER's slot AND re-assert the
			// occupied-slots bit (the bitmap is never cleared on deindex, so the
			// reconcile loop is where a missing/torn bit is repaired — 05:496-500).
			if err := s.indexStream(path, id); err != nil {
				return err
			}
		}
	}
	return nil
}

// indexStream adds a subscriber to a stream's per-slot fan-out shard and SETs the
// stream's occupied-slots bit for that slot. The bit drives OnStreamAppend's
// scatter-gather; setting it on every link keeps the bitmap a superset of the
// occupied slots, so the bitmap-gated fan-out never misses a subscriber.
func (s *RedisStore) indexStream(path, id string) error {
	ctx := s.ctx()
	h := slotOf(id)
	if err := s.client.SAdd(ctx, streamSubsKey(h, path), id).Err(); err != nil {
		return err
	}
	return s.client.SetBit(ctx, streamSlotsKey(path), int64(h), 1).Err()
}

// deindexStream removes a subscriber from its slot's fan-out shard. It does NOT
// clear the occupied-slots bit: a stale set bit only costs one empty SMEMBERS on a
// later append (race-safe), and clearing it would race a concurrent re-link in the
// same slot and drop a live subscriber from the bitmap (05:496-500). The reconcile
// loop repairs the bitmap; it is never narrowed here.
func (s *RedisStore) deindexStream(path, id string) error {
	return s.client.SRem(s.ctx(), streamSubsKey(slotOf(id), path), id).Err()
}

// ArmWake issues a wake if idle. An optional OwnerScope makes arm_wake inline the
// owner-epoch fence (issue #14): an owner-scoped caller deposed since it last
// claimed the slot is FENCED, suppressing its wasted re-arm. The external/hot path
// passes no scope (epoch ”), so the check is skipped and behavior is unchanged.
func (s *RedisStore) ArmWake(id string, now time.Time, leaseTTLMs int64, armLease bool, wakeID string, owner ...OwnerScope) (ArmResult, error) {
	arm := "0"
	if armLease {
		arm = "1"
	}
	sk, me, epoch := firstOwnerScope(owner)
	h := slotOf(id)
	reply, err := s.evalStrings(armWakeScript, []string{subKey(id), leaseZKey(h), dueZKey(h), sk},
		id, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), arm, wakeID, me, epoch)
	if err != nil {
		return ArmResult{}, err
	}
	s.recordInlineFence(epoch, reply[0])
	switch reply[0] {
	case "ARMED":
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		res := ArmResult{Armed: true, Generation: gen, WakeID: reply[2]}
		// Tier B: the generation HINCRBY (arm_wake.lua:23) just minted the fence on
		// the primary; block on WAITAOF BEFORE the caller dispatches so a wake we
		// externalize is durable to ~the AOF fsync interval (doc 05 Tier B). A short
		// reply means the write reached the primary but durability is unproven — it
		// is returned as an error so issueWake does NOT dispatch (recovery re-fires
		// the wake after lease expiry), never swallowed. The result is returned
		// alongside it as the truthful primary fence state. BUSY/NOSUB/FENCED mint no
		// fence, so they need no barrier.
		if err := s.awaitDurable(); err != nil {
			return res, err
		}
		return res, nil
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
	// h from the BASE id (slotOf strips the g-suffix), so the config hash, the
	// per-(id,g) shard hash, and the lease ZSET all share the sub's one slot —
	// claim.lua stays single-slot for any g.
	h := slotOf(id)
	reply, err := s.evalStrings(claimScript, []string{subKey(id), subShardKey(id, g), leaseZKey(h)},
		shardMember(id, g), worker, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), wakeID)
	if err != nil {
		return ClaimResult{}, err
	}
	switch reply[0] {
	case "CLAIMED":
		s.recordContention("claimed", id)
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		res := ClaimResult{Claimed: true, Generation: gen, WakeID: reply[2], Holder: reply[3]}
		// Tier B: a claim grant rotates/confirms the fence generation (claim.lua:41)
		// and arms the lease; block on WAITAOF so the worker proceeds only once the
		// claim is durable (doc 05 Tier B). A short reply is surfaced as an error so a
		// non-durable claim does not silently process — the lease self-heals via
		// expiry + takeover. Durability only; the (gen,wake_id) fence still governs
		// who may ack. BUSY/NOSUB hold no new grant, so they need no barrier.
		if err := s.awaitDurable(); err != nil {
			return res, err
		}
		return res, nil
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

// recordInlineFence reports an inlined owner-epoch fence firing (issue #14). The
// store is the single place a schedule/due script's reply is observed, so every
// owner-scoped script (arm_wake/ack/expire_lease/schedule_retry/release) records
// OwnerFenced("inline") here uniformly. It fires ONLY when the caller presented an
// active owner scope (a non-empty expected epoch) AND the reply is FENCED — so on
// the load-balanced external path (epoch "") it is a no-op, and for ack/release a
// FENCED with an active scope is the owner fence (it is the script's first gate,
// above the still-byte-for-byte (gen,wake_id) fence).
func (s *RedisStore) recordInlineFence(epoch, status string) {
	if s.metrics == nil || epoch == "" || status != "FENCED" {
		return
	}
	s.metrics.OwnerFenced("inline")
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
	// h from the base id: the shard fence hash, the shared cursor hash, all three
	// schedule ZSETs, and the due outbox share one slot — ack.lua stays single-slot.
	h := slotOf(id)
	reply, err := s.evalStrings(ackScript, []string{subShardKey(id, g), linksKey(id), leaseZKey(h), retryZKey(h), dueZKey(h), sk}, args...)
	if err != nil {
		return "", err
	}
	s.recordInlineFence(epoch, reply[0])
	s.recordContention(contentionStatusOf(reply[0]), id)
	return reply[0], nil
}

// Release fences then releases the lease. An optional OwnerScope makes release.lua
// inline the owner-epoch fence (GAP3 consistency, issue #14: release idles the sub
// and clears the due mark exactly like ack(done), so it joins the inline-check set).
func (s *RedisStore) Release(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, owner ...OwnerScope) (string, error) {
	sk, me, epoch := firstOwnerScope(owner)
	h := slotOf(id)
	reply, err := s.evalStrings(releaseScript, []string{subKey(id), leaseZKey(h), retryZKey(h), dueZKey(h), sk},
		id, strconv.FormatInt(reqGeneration, 10), reqWakeID, strconv.FormatInt(tokenGeneration, 10), me, epoch)
	if err != nil {
		return "", err
	}
	s.recordInlineFence(epoch, reply[0])
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
	h := slotOf(id)
	reply, err := s.evalStrings(expireLeaseScript, []string{subKey(id), leaseZKey(h), dueZKey(h), sk}, id, nsArg(now), me, epoch)
	if err != nil {
		return "", err
	}
	s.recordInlineFence(epoch, reply[0])
	return reply[0], nil
}

// LeasedIDs returns the members of the lease schedule ZSETs (the failover-aware
// reconcile's view of what the lease worker can see), UNIONed across the S per-slot
// lease ZSETs (GAP4): under slot-homing the in-flight set is sharded, so reading one
// global ZSET would see only slot 0 and wrongly flag every other slot's live sub as
// stranded. Pipelined into one batch; O(in-flight) across all slots, not
// O(subscriptions). The sweep's reconcileLeases diffs the durable sub set against it.
func (s *RedisStore) LeasedIDs() ([]string, error) {
	ctx := s.ctx()
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, subSlots)
	for h := 0; h < subSlots; h++ {
		cmds[h] = pipe.ZRange(ctx, leaseZKey(h), 0, -1)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	out := make([]string, 0)
	for _, c := range cmds {
		out = append(out, c.Val()...)
	}
	return out, nil
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
	h := slotOf(id)
	reply, err := s.evalStrings(restoreLeaseScript, []string{subKey(id), leaseZKey(h), dueZKey(h)}, id, nsArg(now), owedArg)
	if err != nil {
		return "", err
	}
	return reply[0], nil
}

// DueLeases takes due lease-schedule members from SLOT h by re-scoring them forward,
// so a dropped worker's subscription recurs (docs/research/07 §6.1). The lease worker
// iterates its owned slots and drains each — under slot-homing the schedule shards
// with the subs, so h selects the per-slot ZSET (claim_due.lua runs unchanged, 1 key,
// once per slot — 05:152-157).
func (s *RedisStore) DueLeases(h int, now time.Time, limit int, visibility time.Duration) ([]string, error) {
	return s.due(leaseZKey(h), now, limit, visibility)
}

// DueRetries takes due retry-schedule members from SLOT h by re-scoring them forward,
// the same re-score-never-ZREM machinery as DueLeases (docs/research/07 §6.1).
func (s *RedisStore) DueRetries(h int, now time.Time, limit int, visibility time.Duration) ([]string, error) {
	return s.due(retryZKey(h), now, limit, visibility)
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
func (s *RedisStore) ClaimDue(h int, now time.Time, limit int, visibility time.Duration) ([]string, error) {
	return s.due(dueZKey(h), now, limit, visibility)
}

// ClearDue removes a subscription's due-set wake mark. It is a single-key ZREM
// (always slot-safe), called by the dueWorker to reconcile away a mark that is no
// longer owed — the subscription is gone, or idle with its cursor caught up.
// Without it a caught-up mark would churn forever, since claim_due re-scores and
// never removes and expire_lease re-owes unconditionally.
func (s *RedisStore) ClearDue(id string) error {
	return s.client.ZRem(s.ctx(), dueZKey(slotOf(id)), id).Err()
}

// ScheduleRetry records a webhook failure and persists next_attempt; returns the
// new retry count.
func (s *RedisStore) ScheduleRetry(id string, now, nextAttempt time.Time, owner ...OwnerScope) (int, error) {
	sk, me, epoch := firstOwnerScope(owner)
	h := slotOf(id)
	reply, err := s.evalStrings(scheduleRetryScript, []string{subKey(id), retryZKey(h), sk},
		id, nsArg(now), nsArg(nextAttempt), me, epoch)
	if err != nil {
		return 0, err
	}
	s.recordInlineFence(epoch, reply[0])
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
	_, err := s.evalStrings(recordSuccessScript, []string{subKey(id), retryZKey(slotOf(id))}, id)
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
