package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// appendFenceRetention keeps a revoked marker beyond the longest accepted
// write-token lifetime and verifier skew. It only bounds stale marker cleanup;
// append.lua uses lease_until_ns, not the Redis key TTL, as the live fence.
const appendFenceRetention = 2 * time.Minute

// GrantAppendFence installs or renews the stream-slot marker for one live
// subscription claim. The marker is deliberately replicated once per linked
// stream so append.lua can compare it in the stream's Redis Cluster slot.
func (s *Store) GrantAppendFence(path string, fence auth.AppendFence) (bool, error) {
	nowNs := s.clock.Now().UnixNano()
	if !fence.Complete() || fence.LeaseUntilNs <= nowNs {
		return false, store.ErrAppendFenced
	}
	ttl := time.Duration(fence.LeaseUntilNs-nowNs) + appendFenceRetention
	status, _, err := s.runStatusScript(context.Background(), grantAppendFenceScript,
		[]string{
			metaKey(path),
			appendFenceKey(path, fence.SubscriptionID, fence.SubscriptionIncarnation, fence.Shard),
		},
		strconv.FormatInt(fence.Generation, 10), fence.WakeID, fence.Holder,
		strconv.FormatInt(fence.LeaseUntilNs, 10), strconv.FormatInt(nowNs, 10),
		strconv.FormatInt(ttl.Milliseconds(), 10))
	if err != nil {
		return false, err
	}
	switch status {
	case stOK:
		return true, nil
	case stNotFound:
		return false, nil
	case stFenced:
		return false, store.ErrAppendFenced
	default:
		return false, fmt.Errorf("grant_append_fence.lua: unexpected status %q", status)
	}
}

// RevokeAppendFence tombstones the named claim marker. Delayed revocation of an
// older generation is harmless and reports success to the lifecycle caller.
func (s *Store) RevokeAppendFence(path string, fence auth.AppendFence) error {
	if !fence.Complete() {
		return store.ErrAppendFenced
	}
	status, _, err := s.runStatusScript(context.Background(), revokeAppendFenceScript,
		[]string{appendFenceKey(path, fence.SubscriptionID, fence.SubscriptionIncarnation, fence.Shard)},
		strconv.FormatInt(fence.Generation, 10), fence.WakeID, fence.Holder,
		strconv.FormatInt(appendFenceRetention.Milliseconds(), 10))
	if err != nil {
		return err
	}
	switch status {
	case stOK, "STALE":
		return nil
	default:
		return fmt.Errorf("revoke_append_fence.lua: unexpected status %q", status)
	}
}
