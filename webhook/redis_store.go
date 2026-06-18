package webhook

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore implements Store on Redis 8, persisting each subscription-control
// shard under its {__ds:h} hash tag. It shares the go-redis client with the
// stream store (chronicle uses one Redis), and does not own it: Close is a no-op.
type RedisStore struct {
	client redis.UniversalClient
}

var _ Store = (*RedisStore)(nil)

var errOwnerFenced = errors.New("owner epoch fenced")

// NewRedisStore wraps a go-redis client as a subscription Store.
func NewRedisStore(client redis.UniversalClient) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) ctx() context.Context { return context.Background() }

const slotMigrationCompleteField = "_slot_migration_complete"

type subscriptionKeys struct {
	slot     int
	sub      string
	links    string
	subs     string
	lease    string
	retry    string
	due      string
	oldSub   string
	oldLinks string
	oldSubs  string
	oldLease string
	oldRetry string
	oldDue   string
}

func keysForSubscription(id string) subscriptionKeys {
	h := subscriptionSlot(id)
	return subscriptionKeys{
		slot:     h,
		sub:      subKeyForSlot(id, h),
		links:    linksKeyForSlot(id, h),
		subs:     subsKey(h),
		lease:    leaseZKey(h),
		retry:    retryZKey(h),
		due:      dueSetKey(h),
		oldSub:   legacySubKey(id),
		oldLinks: legacyLinksKey(id),
		oldSubs:  legacySubsKey(),
		oldLease: legacyLeaseZKey(),
		oldRetry: legacyRetryZKey(),
		oldDue:   legacyDueSetKey(),
	}
}

func scheduleKeysForSlot(slot OwnershipSlot) (lease, retry, due string) {
	h := slot.Int()
	return leaseZKey(h), retryZKey(h), dueSetKey(h)
}

// migrateSubscription lazily copies one legacy ds:{__ds} subscription into its
// new ds:{__ds:h} home before a per-subscription operation runs. It writes the
// complete new key set first, including schedules and fan-out bitmap entries,
// then removes the old key set; no Lua script is ever invoked with keys from
// both homes.
func (s *RedisStore) migrateSubscription(id string) error {
	k := keysForSubscription(id)
	ctx := s.ctx()
	complete, err := s.client.HGet(ctx, k.sub, slotMigrationCompleteField).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	if complete == "1" {
		exists, err := s.client.Exists(ctx, k.oldSub).Result()
		if err != nil {
			return err
		}
		if exists == 0 {
			return nil
		}
	}
	fields, err := s.client.HGetAll(ctx, k.oldSub).Result()
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		inLegacySet, err := s.client.SIsMember(ctx, k.oldSubs, id).Result()
		if err != nil {
			return err
		}
		if !inLegacySet {
			return nil
		}
		return s.cleanupLegacySubscription(id, nil)
	}
	links, err := s.client.HGetAll(ctx, k.oldLinks).Result()
	if err != nil {
		return err
	}
	if complete == "1" {
		return s.cleanupLegacySubscription(id, mapKeys(links))
	}
	pipe := s.client.Pipeline()
	pipe.HSet(ctx, k.sub, stringMapArgs(fields)...)
	pipe.SAdd(ctx, k.subs, id)
	if len(links) > 0 {
		pipe.HSet(ctx, k.links, stringMapArgs(links)...)
		for path := range links {
			pipe.SAdd(ctx, streamSubsKey(k.slot, path), id)
			pipe.SetBit(ctx, occupiedStreamSlotsKey(path), int64(k.slot), 1)
		}
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}
	if err := s.copyLegacyScheduleMembers(id, k); err != nil {
		return err
	}
	if err := s.client.HSet(ctx, k.sub, slotMigrationCompleteField, "1").Err(); err != nil {
		return err
	}
	return s.cleanupLegacySubscription(id, mapKeys(links))
}

func stringMapArgs(m map[string]string) []any {
	args := make([]any, 0, 2*len(m))
	for k, v := range m {
		args = append(args, k, v)
	}
	return args
}

func mapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func legacyLeaseMembers(id string) []string {
	members := make([]string, 0, ClaimShardCount)
	members = append(members, id)
	for n := 1; n < ClaimShardCount; n++ {
		shard, _ := NewClaimShard(n)
		members = append(members, NewLeaseRef(id, shard).Member())
	}
	return members
}

func (s *RedisStore) copyLegacyScheduleMembers(id string, k subscriptionKeys) error {
	ctx := s.ctx()
	for _, member := range legacyLeaseMembers(id) {
		score, err := s.client.ZScore(ctx, k.oldLease, member).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		if err := s.client.ZAdd(ctx, k.lease, redis.Z{Score: score, Member: member}).Err(); err != nil {
			return err
		}
	}
	for _, spec := range []struct {
		from string
		to   string
	}{
		{k.oldRetry, k.retry},
		{k.oldDue, k.due},
	} {
		score, err := s.client.ZScore(ctx, spec.from, id).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		if err := s.client.ZAdd(ctx, spec.to, redis.Z{Score: score, Member: id}).Err(); err != nil {
			return err
		}
	}
	return nil
}

func nsArg(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

func msArg(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }

func fenceKey(fence OwnershipFence, fallback string) string {
	if fence.Enabled {
		return ownershipSlotKey(fence.Slot)
	}
	return fallback
}

func fenceArgs(fence OwnershipFence) (string, string) {
	return fence.args()
}

// evalStrings runs a script and decodes its reply as a slice of strings, the
// fixed reply shape of every subscription script.
func (s *RedisStore) evalStrings(script *redis.Script, keys []string, args ...any) ([]string, error) {
	if err := validateSingleHashTag(keys); err != nil {
		return nil, err
	}
	raw, err := script.Run(s.ctx(), s.client, keys, args...).Result()
	if err != nil {
		return nil, err
	}
	return decodeScriptStrings(raw)
}

func decodeScriptStrings(raw any) ([]string, error) {
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
	if err := s.migrateSubscription(id); err != nil {
		return 0, err
	}
	k := keysForSubscription(id)
	cfg = NormalizeConfig(cfg)
	args := make([]any, 0, 10+3*len(links))
	args = append(
		args,
		id, ConfigHash(cfg), nsArg(now),
		string(cfg.Type), cfg.Pattern, cfg.WebhookURL, cfg.WakeStream,
		strconv.FormatInt(cfg.LeaseTTLMs, 10), cfg.Description,
		strconv.Itoa(len(links)),
	)
	for _, l := range links {
		args = append(args, l.Path, string(l.LinkType), l.AckedOffset)
	}
	reply, err := s.evalStrings(createSubScript, []string{k.sub, k.subs, k.links}, args...)
	if err != nil {
		return 0, err
	}
	switch reply[0] {
	case "CREATED":
		if err := s.client.HSet(s.ctx(), k.sub, slotMigrationCompleteField, "1").Err(); err != nil {
			return 0, err
		}
		for _, l := range links {
			if err := s.indexStream(l.Path, id); err != nil {
				return 0, err
			}
		}
		return CreateCreated, nil
	case "MATCHED":
		if err := s.client.HSet(s.ctx(), k.sub, slotMigrationCompleteField, "1").Err(); err != nil {
			return 0, err
		}
		return CreateMatched, nil
	case "CONFLICT":
		return CreateConflict, nil
	default:
		return 0, fmt.Errorf("create_sub: unexpected status %q", reply[0])
	}
}

// Get hydrates a subscription and its links.
func (s *RedisStore) Get(id string) (Subscription, bool, error) {
	if err := s.migrateSubscription(id); err != nil {
		return Subscription{}, false, err
	}
	k := keysForSubscription(id)
	pipe := s.client.Pipeline()
	subCmd := pipe.HGetAll(s.ctx(), k.sub)
	linkCmd := pipe.HGetAll(s.ctx(), k.links)
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
		for _, id := range batch {
			if err := s.migrateSubscription(id); err != nil {
				return nil, err
			}
		}
		pipe := s.client.Pipeline()
		subCmds := make([]*redis.MapStringStringCmd, len(batch))
		linkCmds := make([]*redis.MapStringStringCmd, len(batch))
		for i, id := range batch {
			k := keysForSubscription(id)
			subCmds[i] = pipe.HGetAll(s.ctx(), k.sub)
			linkCmds[i] = pipe.HGetAll(s.ctx(), k.links)
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
	if err := s.migrateSubscription(id); err != nil {
		return err
	}
	k := keysForSubscription(id)
	links, err := s.client.HKeys(s.ctx(), k.links).Result()
	if err != nil {
		return err
	}
	if _, err := s.evalStrings(deleteSubScript,
		[]string{k.sub, k.subs, k.links, k.lease, k.retry, k.due}, id); err != nil {
		return err
	}
	for _, path := range links {
		if err := s.deindexStream(path, id); err != nil {
			return err
		}
	}
	shardedMembers := make([]any, 0, ClaimShardCount-1)
	for n := 1; n < ClaimShardCount; n++ {
		shard, _ := NewClaimShard(n)
		shardedMembers = append(shardedMembers, NewLeaseRef(id, shard).Member())
	}
	if len(shardedMembers) > 0 {
		if err := s.client.ZRem(s.ctx(), k.lease, shardedMembers...).Err(); err != nil {
			return err
		}
	}
	_ = s.cleanupLegacySubscription(id, links)
	return nil
}

// List returns all subscription ids.
func (s *RedisStore) List() ([]string, error) {
	ctx := s.ctx()
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, subSlots)
	for h := 0; h < subSlots; h++ {
		cmds[h] = pipe.SMembers(ctx, subsKey(h))
	}
	oldCmd := pipe.SMembers(ctx, legacySubsKey())
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	for _, cmd := range cmds {
		for _, id := range cmd.Val() {
			seen[id] = struct{}{}
		}
	}
	for _, id := range oldCmd.Val() {
		if err := s.migrateSubscription(id); err != nil {
			return nil, err
		}
		if err := s.cleanupLegacySubscription(id, nil); err != nil {
			return nil, err
		}
		if exists, err := s.client.Exists(ctx, subKey(id)).Result(); err != nil {
			return nil, err
		} else if exists > 0 {
			seen[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	slices.Sort(out)
	return out, nil
}

// SubscriptionSlots returns the occupied subscription state slots.
func (s *RedisStore) SubscriptionSlots() ([]OwnershipSlot, error) {
	ctx := s.ctx()
	pipe := s.client.Pipeline()
	cmds := make([]*redis.IntCmd, subSlots)
	for h := 0; h < subSlots; h++ {
		cmds[h] = pipe.SCard(ctx, subsKey(h))
	}
	oldCmd := pipe.SMembers(ctx, legacySubsKey())
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	seen := make(map[int]struct{})
	for h, cmd := range cmds {
		if cmd.Val() > 0 {
			seen[h] = struct{}{}
		}
	}
	for _, id := range oldCmd.Val() {
		if err := s.migrateSubscription(id); err != nil {
			return nil, err
		}
		if err := s.cleanupLegacySubscription(id, nil); err != nil {
			return nil, err
		}
		if exists, err := s.client.Exists(ctx, subKey(id)).Result(); err != nil {
			return nil, err
		} else if exists > 0 {
			seen[subscriptionSlot(id)] = struct{}{}
		}
	}
	out := make([]OwnershipSlot, 0, len(seen))
	for h := range seen {
		slot, _ := NewOwnershipSlot(h)
		out = append(out, slot)
	}
	slices.Sort(out)
	return out, nil
}

// HasSubscriptions reports whether any legacy or slot-homed subscription exists.
func (s *RedisStore) HasSubscriptions() (bool, error) {
	slots, err := s.SubscriptionSlots()
	return len(slots) > 0, err
}

// Link links a stream and maintains the fan-out index.
func (s *RedisStore) Link(id, path string, linkType LinkType, offset string) error {
	if err := s.migrateSubscription(id); err != nil {
		return err
	}
	k := keysForSubscription(id)
	if _, err := s.evalStrings(linkStreamScript, []string{k.links}, path, string(linkType), offset); err != nil {
		return err
	}
	return s.indexStream(path, id)
}

// Unlink removes an explicit link; de-indexes only when the link is gone.
func (s *RedisStore) Unlink(id, path string, stillGlob bool) error {
	if err := s.migrateSubscription(id); err != nil {
		return err
	}
	k := keysForSubscription(id)
	flag := "0"
	if stillGlob {
		flag = "1"
	}
	reply, err := s.evalStrings(unlinkStreamScript, []string{k.links}, path, flag)
	if err != nil {
		return err
	}
	if reply[0] == "REMOVED" {
		return s.deindexStream(path, id)
	}
	return nil
}

// StreamSubscribers returns the subscription ids linked to a stream and how
// many occupied slot shards were probed.
func (s *RedisStore) StreamSubscribers(path string) ([]string, int, error) {
	slots, err := s.occupiedSlots(path)
	if err != nil {
		return nil, 0, err
	}
	ctx := s.ctx()
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(slots))
	for i, h := range slots {
		cmds[i] = pipe.SMembers(ctx, streamSubsKey(h, path))
	}
	oldCmd := pipe.SMembers(ctx, legacyStreamSubsKey(path))
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, len(slots), err
	}
	seen := make(map[string]struct{})
	for _, cmd := range cmds {
		for _, id := range cmd.Val() {
			seen[id] = struct{}{}
		}
	}
	for _, id := range oldCmd.Val() {
		if err := s.migrateSubscription(id); err != nil {
			return nil, len(slots), err
		}
		if err := s.cleanupLegacySubscription(id, []string{path}); err != nil {
			return nil, len(slots), err
		}
		if exists, err := s.client.Exists(ctx, subKey(id)).Result(); err != nil {
			return nil, len(slots), err
		} else if exists > 0 {
			seen[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	slices.Sort(out)
	return out, len(slots), nil
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
	ids, err := s.List()
	if err != nil {
		return err
	}
	for _, id := range ids {
		k := keysForSubscription(id)
		paths, err := s.client.HKeys(ctx, k.links).Result()
		if err != nil {
			return err
		}
		for _, path := range paths {
			if err := s.indexStream(path, id); err != nil {
				return err
			}
		}
	}
	return nil
}

// HeartbeatMember renews membership and removes expired members.
func (s *RedisStore) HeartbeatMember(replica ReplicaID, now time.Time, ttl time.Duration) error {
	ctx := s.ctx()
	pipe := s.client.Pipeline()
	expiry := now.Add(ttl).UnixNano()
	pipe.ZAdd(ctx, membersKey, redis.Z{Score: float64(expiry), Member: replica.String()})
	pipe.ZRemRangeByScore(ctx, membersKey, "-inf", nsArg(now))
	_, err := pipe.Exec(ctx)
	return err
}

// LiveMembers returns unexpired members from the ownership membership set.
func (s *RedisStore) LiveMembers(now time.Time) ([]ReplicaID, error) {
	raw, err := s.client.ZRangeByScore(s.ctx(), membersKey, &redis.ZRangeBy{
		Min: "(" + nsArg(now),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]ReplicaID, 0, len(raw))
	for _, v := range raw {
		id, err := NewReplicaID(v)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

// ClaimSlot CASes one ownership slot lease.
func (s *RedisStore) ClaimSlot(slot OwnershipSlot, replica ReplicaID, now time.Time, ttl time.Duration) (SlotClaimResult, error) {
	reply, err := s.evalStrings(claimShardScript, []string{ownershipSlotKey(slot)}, replica.String(), nsArg(now), msArg(ttl))
	if err != nil {
		return SlotClaimResult{}, err
	}
	return parseSlotClaimResult(slot, reply)
}

// ClaimSlots pipelines ownership slot CAS attempts for the occupied slots this
// replica should own.
func (s *RedisStore) ClaimSlots(slots []OwnershipSlot, replica ReplicaID, now time.Time, ttl time.Duration) ([]slotClaimAttempt, error) {
	ctx := s.ctx()
	if err := claimShardScript.Load(ctx, s.client).Err(); err != nil {
		return nil, err
	}
	pipe := s.client.Pipeline()
	cmds := make([]*redis.Cmd, len(slots))
	for i, slot := range slots {
		cmds[i] = claimShardScript.Run(ctx, pipe, []string{ownershipSlotKey(slot)}, replica.String(), nsArg(now), msArg(ttl))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	out := make([]slotClaimAttempt, 0, len(slots))
	for i, cmd := range cmds {
		raw, err := cmd.Result()
		if err != nil {
			out = append(out, slotClaimAttempt{Slot: slots[i], Err: err})
			continue
		}
		reply, err := decodeScriptStrings(raw)
		if err != nil {
			out = append(out, slotClaimAttempt{Slot: slots[i], Err: err})
			continue
		}
		result, err := parseSlotClaimResult(slots[i], reply)
		out = append(out, slotClaimAttempt{Slot: slots[i], Result: result, Err: err})
	}
	return out, nil
}

func parseSlotClaimResult(slot OwnershipSlot, reply []string) (SlotClaimResult, error) {
	if len(reply) < 4 {
		return SlotClaimResult{}, fmt.Errorf("claim_shard: short reply %v", reply)
	}
	owner, err := NewReplicaID(reply[1])
	if err != nil {
		return SlotClaimResult{}, err
	}
	exp, _ := strconv.ParseInt(reply[3], 10, 64)
	result := SlotClaimResult{
		Status: SlotClaimStatus(reply[0]),
		Lease: SlotLease{
			Slot:          slot,
			Owner:         owner,
			Epoch:         parseOwnerEpoch(reply[2]),
			LeaseExpiryNs: exp,
		},
	}
	switch result.Status {
	case SlotClaimed, SlotRenewed, SlotBusy:
		return result, nil
	default:
		return SlotClaimResult{}, fmt.Errorf("claim_shard: unexpected status %q", reply[0])
	}
}

// CheckOwner verifies the owner epoch for an external side-effect gate.
func (s *RedisStore) CheckOwner(fence OwnershipFence) (CheckOwnerStatus, error) {
	if !fence.Enabled {
		return CheckOwnerOwner, nil
	}
	reply, err := s.evalStrings(checkOwnerScript, []string{ownershipSlotKey(fence.Slot)}, fence.Owner.String(), fence.Epoch.String())
	if err != nil {
		return "", err
	}
	switch CheckOwnerStatus(reply[0]) {
	case CheckOwnerOwner, CheckOwnerFenced, CheckOwnerUnowned:
		return CheckOwnerStatus(reply[0]), nil
	default:
		return "", fmt.Errorf("check_owner: unexpected status %q", reply[0])
	}
}

func (s *RedisStore) indexStream(path, id string) error {
	k := keysForSubscription(id)
	ctx := s.ctx()
	pipe := s.client.Pipeline()
	pipe.SAdd(ctx, streamSubsKey(k.slot, path), id)
	pipe.SetBit(ctx, occupiedStreamSlotsKey(path), int64(k.slot), 1)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisStore) deindexStream(path, id string) error {
	k := keysForSubscription(id)
	return s.client.SRem(s.ctx(), streamSubsKey(k.slot, path), id).Err()
}

func (s *RedisStore) occupiedSlots(path string) ([]int, error) {
	raw, err := s.client.Get(s.ctx(), occupiedStreamSlotsKey(path)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]int, 0, subSlots)
	for h := 0; h < subSlots; h++ {
		byteIdx := h / 8
		if byteIdx >= len(raw) {
			break
		}
		mask := byte(1 << (7 - uint(h%8)))
		if raw[byteIdx]&mask != 0 {
			out = append(out, h)
		}
	}
	return out, nil
}

func (s *RedisStore) cleanupLegacySubscription(id string, paths []string) error {
	ctx := s.ctx()
	k := keysForSubscription(id)
	pipe := s.client.Pipeline()
	pipe.Del(ctx, k.oldSub, k.oldLinks)
	pipe.SRem(ctx, k.oldSubs, id)
	leaseMembers := stringSliceToAny(legacyLeaseMembers(id))
	pipe.ZRem(ctx, k.oldLease, leaseMembers...)
	pipe.ZRem(ctx, k.oldRetry, id)
	pipe.ZRem(ctx, k.oldDue, id)
	for _, path := range paths {
		pipe.SRem(ctx, legacyStreamSubsKey(path), id)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func stringSliceToAny(values []string) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}

// ArmWake issues a wake if idle.
func (s *RedisStore) ArmWake(id string, now time.Time, leaseTTLMs int64, armLease bool, wakeID string, fence OwnershipFence) (ArmResult, error) {
	if err := s.migrateSubscription(id); err != nil {
		return ArmResult{}, err
	}
	k := keysForSubscription(id)
	arm := "0"
	if armLease {
		arm = "1"
	}
	ownerID, ownerEpoch := fenceArgs(fence)
	reply, err := s.evalStrings(armWakeScript, []string{k.sub, k.lease, k.due, fenceKey(fence, k.sub)},
		id, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), arm, wakeID, ownerID, ownerEpoch)
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

// Claim runs the pull-wake CAS claim.
func (s *RedisStore) Claim(id string, mode ClaimMode, shard ClaimShard, worker, wakeID string, now time.Time, leaseTTLMs int64) (ClaimResult, error) {
	if err := s.migrateSubscription(id); err != nil {
		return ClaimResult{}, err
	}
	k := keysForSubscription(id)
	ref := NewLeaseRef(id, shard)
	reply, err := s.evalStrings(claimScript, []string{k.sub, k.lease},
		id, worker, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), wakeID, shard.String(), ref.Member(), mode.String())
	if err != nil {
		return ClaimResult{}, err
	}
	switch reply[0] {
	case "CLAIMED":
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		return ClaimResult{Claimed: true, Generation: gen, WakeID: reply[2], Holder: reply[3], LeaseLapsed: len(reply) > 4 && reply[4] == "1"}, nil
	case "BUSY":
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		return ClaimResult{Busy: true, Generation: gen, Holder: reply[3]}, nil
	case "NOSUB":
		return ClaimResult{NoSub: true}, nil
	case "MODE_CONFLICT":
		return ClaimResult{ModeConflict: true, Mode: ClaimMode(reply[1])}, nil
	default:
		return ClaimResult{}, fmt.Errorf("claim: unexpected status %q", reply[0])
	}
}

// Ack fences, applies acks, and releases or heartbeats.
func (s *RedisStore) Ack(id string, mode ClaimMode, shard ClaimShard, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64, fence OwnershipFence) (string, error) {
	if err := s.migrateSubscription(id); err != nil {
		return "", err
	}
	k := keysForSubscription(id)
	doneArg := "0"
	if done {
		doneArg = "1"
	}
	ref := NewLeaseRef(id, shard)
	ownerID, ownerEpoch := fenceArgs(fence)
	args := make([]any, 0, 13+2*len(acks))
	args = append(
		args,
		id, strconv.FormatInt(reqGeneration, 10), reqWakeID, strconv.FormatInt(tokenGeneration, 10),
		doneArg, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), strconv.Itoa(len(acks)),
		shard.String(), ref.Member(), mode.String(), ownerID, ownerEpoch,
	)
	for _, a := range acks {
		args = append(args, a.Stream, a.Offset)
	}
	reply, err := s.evalStrings(ackScript,
		[]string{k.sub, k.links, k.lease, k.retry, k.due, fenceKey(fence, k.sub)}, args...)
	if err != nil {
		return "", err
	}
	return reply[0], nil
}

// Release fences then releases the lease.
func (s *RedisStore) Release(id string, mode ClaimMode, shard ClaimShard, reqGeneration int64, reqWakeID string, tokenGeneration int64, fence OwnershipFence) (string, error) {
	if err := s.migrateSubscription(id); err != nil {
		return "", err
	}
	k := keysForSubscription(id)
	ref := NewLeaseRef(id, shard)
	ownerID, ownerEpoch := fenceArgs(fence)
	reply, err := s.evalStrings(releaseScript, []string{k.sub, k.lease, k.retry, k.due, fenceKey(fence, k.sub)},
		id, strconv.FormatInt(reqGeneration, 10), reqWakeID, strconv.FormatInt(tokenGeneration, 10), shard.String(), ref.Member(), mode.String(), ownerID, ownerEpoch)
	if err != nil {
		return "", err
	}
	return reply[0], nil
}

// ExpireLease clears an expired lease.
func (s *RedisStore) ExpireLease(ref LeaseRef, now time.Time, pending bool, fence OwnershipFence) (string, error) {
	if err := s.migrateSubscription(ref.SubID); err != nil {
		return "", err
	}
	k := keysForSubscription(ref.SubID)
	pendingArg := "0"
	if pending {
		pendingArg = "1"
	}
	ownerID, ownerEpoch := fenceArgs(fence)
	reply, err := s.evalStrings(expireLeaseScript, []string{k.sub, k.lease, k.due, fenceKey(fence, k.sub)},
		ref.SubID, nsArg(now), ref.Shard.String(), ref.Member(), pendingArg, ownerID, ownerEpoch)
	if err != nil {
		return "", err
	}
	return reply[0], nil
}

// ReconcileLeaseSchedule re-adds schedule entries implied by durable sub state.
func (s *RedisStore) ReconcileLeaseSchedule(ref LeaseRef, now time.Time, pending bool, fence OwnershipFence) (LeaseReconcileResult, error) {
	if err := s.migrateSubscription(ref.SubID); err != nil {
		return LeaseReconcileResult{}, err
	}
	k := keysForSubscription(ref.SubID)
	pendingArg := "0"
	if pending {
		pendingArg = "1"
	}
	ownerID, ownerEpoch := fenceArgs(fence)
	reply, err := s.evalStrings(reconcileLeaseScript, []string{k.sub, k.lease, k.due, fenceKey(fence, k.sub)},
		ref.SubID, nsArg(now), ref.Shard.String(), ref.Member(), pendingArg, ownerID, ownerEpoch)
	if err != nil {
		return LeaseReconcileResult{}, err
	}
	switch reply[0] {
	case "RECONCILED":
		return LeaseReconcileResult{
			Reconciled:    true,
			LeaseRepaired: len(reply) > 1 && reply[1] == "1",
			DueOp:         reply[2],
		}, nil
	case "SKIPPED", "NOSUB", "FENCED":
		return LeaseReconcileResult{}, nil
	default:
		return LeaseReconcileResult{}, fmt.Errorf("reconcile_lease: unexpected status %q", reply[0])
	}
}

// DueLeases takes due lease-schedule members by re-scoring them forward, so a
// dropped worker's subscription recurs (docs/research/07 §6.1).
func (s *RedisStore) DueLeases(slot OwnershipSlot, now time.Time, limit int, visibility time.Duration, fence OwnershipFence) ([]LeaseRef, error) {
	lease, _, _ := scheduleKeysForSlot(slot)
	members, err := s.due(lease, slot, now, limit, visibility, fence)
	if err != nil {
		return nil, err
	}
	out := make([]LeaseRef, 0, len(members))
	for _, member := range members {
		ref, err := ParseLeaseMember(member)
		if err != nil {
			return nil, err
		}
		out = append(out, ref)
	}
	return out, nil
}

// DueRetries takes due retry-schedule members by re-scoring them forward, the
// same re-score-never-ZREM machinery as DueLeases (docs/research/07 §6.1).
func (s *RedisStore) DueRetries(slot OwnershipSlot, now time.Time, limit int, visibility time.Duration, fence OwnershipFence) ([]string, error) {
	_, retry, _ := scheduleKeysForSlot(slot)
	return s.due(retry, slot, now, limit, visibility, fence)
}

// DueWakes takes owed wake-outbox members by re-scoring them forward, the same
// at-least-once claim primitive as the lease/retry schedules.
func (s *RedisStore) DueWakes(slot OwnershipSlot, now time.Time, limit int, visibility time.Duration, fence OwnershipFence) ([]string, error) {
	_, _, due := scheduleKeysForSlot(slot)
	return s.due(due, slot, now, limit, visibility, fence)
}

func (s *RedisStore) due(zkey string, slot OwnershipSlot, now time.Time, limit int, visibility time.Duration, fence OwnershipFence) ([]string, error) {
	ownerID, ownerEpoch := fenceArgs(fence)
	members, err := s.evalStrings(claimDueScript, []string{zkey, fenceKey(fence, zkey)},
		nsArg(now), strconv.Itoa(limit), strconv.FormatInt(int64(visibility), 10), ownerID, ownerEpoch)
	if err != nil {
		return nil, err
	}
	if fence.Enabled && len(members) == 1 && members[0] == "FENCED" {
		return nil, errOwnerFenced
	}
	_ = slot // #15 will derive zkey from slot; this slice keeps the single {__ds} key.
	return members, nil
}

// ScheduleRetry records a webhook failure and persists next_attempt; returns the
// new retry count.
func (s *RedisStore) ScheduleRetry(id string, now, nextAttempt time.Time, fence OwnershipFence) (int, error) {
	if err := s.migrateSubscription(id); err != nil {
		return 0, err
	}
	k := keysForSubscription(id)
	ownerID, ownerEpoch := fenceArgs(fence)
	reply, err := s.evalStrings(scheduleRetryScript, []string{k.sub, k.retry, fenceKey(fence, k.sub)},
		id, nsArg(now), nsArg(nextAttempt), ownerID, ownerEpoch)
	if err != nil {
		return 0, err
	}
	if reply[0] == "FENCED" {
		return 0, errOwnerFenced
	}
	if reply[0] == "NOSUB" {
		return 0, nil
	}
	n, _ := strconv.Atoi(reply[1])
	return n, nil
}

// RecordSuccess clears webhook failure bookkeeping after an accepted delivery.
func (s *RedisStore) RecordSuccess(id string) error {
	if err := s.migrateSubscription(id); err != nil {
		return err
	}
	k := keysForSubscription(id)
	_, err := s.evalStrings(recordSuccessScript, []string{k.sub, k.retry}, id)
	return err
}

// RecordWakeEventSent marks the current pull-wake event as durably emitted,
// fenced on (generation, wakeID) so a stamp from a superseded wake is ignored.
func (s *RedisStore) RecordWakeEventSent(id string, generation int64, wakeID string, now time.Time) error {
	if err := s.migrateSubscription(id); err != nil {
		return err
	}
	k := keysForSubscription(id)
	_, err := s.evalStrings(recordWakeSentScript, []string{k.sub},
		nsArg(now), strconv.FormatInt(generation, 10), wakeID)
	return err
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

// subscriptionFromHash decodes the sub HASH and links HASH into a Subscription.
func subscriptionFromHash(id string, f map[string]string, linkFields map[string]string) Subscription {
	atoi := func(k string) int64 {
		n, err := strconv.ParseInt(f[k], 10, 64)
		if err == nil {
			return n
		}
		fv, err := strconv.ParseFloat(f[k], 64)
		if err == nil {
			return int64(fv)
		}
		return 0
	}
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
		LeaseUntilNs:    atoi("lease_until_ns"),
		RetryCount:      int(atoi("retry_count")),
		FirstFailNs:     atoi("first_fail_ns"),
		NextAttemptNs:   atoi("next_attempt_ns"),
		WakeEventSentNs: atoi("wake_event_sent_ns"),
	}
	sub.ClaimLeases = claimLeasesFromHash(f, atoi)
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

func claimLeasesFromHash(f map[string]string, atoi func(string) int64) []ClaimShardLeaseState {
	leases := make([]ClaimShardLeaseState, 0)
	for n := 1; n < ClaimShardCount; n++ {
		shard, _ := NewClaimShard(n)
		state, ok := claimLeaseFromHash(f, shard, atoi)
		if ok {
			leases = append(leases, ClaimShardLeaseState{Shard: shard, State: state})
		}
	}
	return leases
}

func claimLeaseFromHash(f map[string]string, shard ClaimShard, atoi func(string) int64) (ClaimLeaseState, bool) {
	present := false
	for _, base := range []string{"phase", "generation", "wake_id", "holder", "holder_worker", "lease_until_ns"} {
		if _, ok := f[claimShardField(base, shard)]; ok {
			present = true
			break
		}
	}
	if !present {
		return ClaimLeaseState{}, false
	}
	phase := Phase(f[claimShardField("phase", shard)])
	if phase == "" {
		phase = PhaseIdle
	}
	return ClaimLeaseState{
		Phase:        phase,
		Generation:   atoi(claimShardField("generation", shard)),
		WakeID:       f[claimShardField("wake_id", shard)],
		Holder:       f[claimShardField("holder", shard)] == "1",
		HolderWorker: f[claimShardField("holder_worker", shard)],
		LeaseUntilNs: atoi(claimShardField("lease_until_ns", shard)),
	}, true
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
