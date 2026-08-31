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
// append.lua uses lease_until_ns, not the Redis key TTL, as the live fence, and
// on a write-fenced stream the per-authority seal (not the tombstone) is what
// keeps a sealed generation from ever being re-granted.
const appendFenceRetention = 2 * time.Minute

// fenceKeys is the [meta, marker] key pair every fence lifecycle script takes;
// both live in the stream's Redis Cluster slot.
func fenceKeys(path string, fence auth.AppendFence) []string {
	return []string{metaKey(path), appendFenceKey(path, fence)}
}

// GrantAppendFence installs or renews the stream-slot marker for one live
// subscription claim. The marker is deliberately replicated once per linked
// stream so append.lua can compare it in the stream's Redis Cluster slot. On a
// write-fenced stream a higher-generation grant also seals the superseded
// generation at its last fenced offset; a sealed generation is never
// re-granted (ErrAppendFenced).
func (s *Store) GrantAppendFence(path string, fence auth.AppendFence) (bool, error) {
	nowNs := s.clock.Now().UnixNano()
	if !fence.Complete() || fence.LeaseUntilNs <= nowNs {
		return false, store.ErrAppendFenced
	}
	status, rest, err := s.runStatusScript(context.Background(), grantAppendFenceScript,
		fenceKeys(path, fence),
		strconv.FormatInt(fence.Generation, 10), fence.WakeID, fence.Holder,
		strconv.FormatInt(fence.LeaseUntilNs, 10), strconv.FormatInt(nowNs, 10),
		strconv.FormatInt(appendFenceRetention.Milliseconds(), 10))
	if err != nil {
		return false, err
	}
	switch status {
	case stOK:
		if len(rest) >= 2 && rest[0] == stSuperseded {
			s.log.Debug("append fence grant sealed the superseded generation",
				"path", path, "generation", fence.Generation, "superseded", rest[1])
		}
		return true, nil
	case stNotFound:
		return false, nil
	case stFenced:
		return false, store.ErrAppendFenced
	default:
		return false, fmt.Errorf("grant_append_fence.lua: unexpected status %q", status)
	}
}

// RevokeAppendFence tombstones the named claim marker without sealing. It is
// the rollback of a partially granted claim, which is still live at its
// generation: the heartbeat re-grant after the tombstone reaps is the holder's
// recovery. Delayed revocation of an older generation is harmless and reports
// success to the lifecycle caller.
func (s *Store) RevokeAppendFence(path string, fence auth.AppendFence) error {
	if !fence.Complete() {
		return store.ErrAppendFenced
	}
	status, _, err := s.runStatusScript(context.Background(), revokeAppendFenceScript,
		[]string{appendFenceKey(path, fence)},
		strconv.FormatInt(fence.Generation, 10), fence.WakeID, fence.Holder,
		strconv.FormatInt(appendFenceRetention.Milliseconds(), 10))
	if err != nil {
		return err
	}
	switch status {
	case stOK, stStale:
		return nil
	default:
		return fmt.Errorf("revoke_append_fence.lua: unexpected status %q", status)
	}
}

// SealAppendFence tombstones the named claim marker and, on a write-fenced
// stream, records the claim generation as sealed for its authority together
// with the definite last fenced-class offset, all in the stream's slot. It
// runs at done, release, delete and unlink before the control-plane idle, so a
// late write of the sealed generation is refused atomically with the append.
// A redelivered done is idempotent (SealAlready); a stale request mutates
// nothing (SealStale).
func (s *Store) SealAppendFence(path string, fence auth.AppendFence) (store.SealResult, error) {
	if !fence.Complete() {
		return store.SealResult{}, store.ErrAppendFenced
	}
	status, rest, err := s.runStatusScript(context.Background(), sealAppendFenceScript,
		fenceKeys(path, fence),
		strconv.FormatInt(fence.Generation, 10), fence.WakeID, fence.Holder,
		strconv.FormatInt(appendFenceRetention.Milliseconds(), 10))
	if err != nil {
		return store.SealResult{}, err
	}
	switch status {
	case stStale:
		return store.SealResult{Outcome: store.SealStale}, nil
	case stNotFound:
		return store.SealResult{Outcome: store.SealNotFound}, nil
	case stOK:
		return decodeSealReply(rest)
	default:
		return store.SealResult{}, fmt.Errorf("seal_append_fence.lua: unexpected status %q", status)
	}
}

// decodeSealReply decodes the {outcome, sealGen, sealOff} tail of an OK seal
// reply. The offset is empty for an unfenced stream.
func decodeSealReply(rest []any) (store.SealResult, error) {
	parts, err := toStrings(rest)
	if err != nil || len(parts) != 3 {
		return store.SealResult{}, fmt.Errorf("seal_append_fence.lua: malformed OK reply %v", rest)
	}
	res := store.SealResult{Outcome: store.SealOutcome(parts[0])}
	switch res.Outcome {
	case store.SealSealed, store.SealAlready, store.SealUnfenced:
	default:
		return store.SealResult{}, fmt.Errorf("seal_append_fence.lua: unexpected outcome %q", parts[0])
	}
	if res.Generation, err = strconv.ParseInt(parts[1], 10, 64); err != nil {
		return store.SealResult{}, fmt.Errorf("seal_append_fence.lua: bad generation %q: %w", parts[1], err)
	}
	if parts[2] != "" {
		if res.FinalOffset, err = store.ParseOffset(parts[2]); err != nil {
			return store.SealResult{}, fmt.Errorf("seal_append_fence.lua: bad offset: %w", err)
		}
	}
	return res, nil
}
