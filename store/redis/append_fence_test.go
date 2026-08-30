package redis

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// append_fence_test.go pins the claim-marker lifecycle in the stream slot
// (#169) and, on write-fenced streams, the per-authority seal (#183, R3.1–R3.4,
// INV-FENCE-06): grant, renew, revoke, seal, supersession, tombstone reap, and
// the isolation of seals between subscription incarnations.

// claimFence is a complete fence of subscription-a / incarnation-a at gen with
// a one-minute lease from now.
func claimFence(gen int64, wake, holder string) auth.AppendFence {
	return auth.AppendFence{
		SubscriptionID:          "subscription-a",
		SubscriptionIncarnation: "incarnation-a",
		Generation:              gen,
		WakeID:                  wake,
		Holder:                  holder,
		LeaseUntilNs:            time.Now().Add(time.Minute).UnixNano(),
	}
}

// fencedAppend is a fenced-class append naming producer "p" at epoch == gen.
func fencedAppend(s *Store, path string, fence auth.AppendFence, seq int64) (store.AppendResult, error) {
	return fencedAppendAs(s, path, fence, "p", seq)
}

// fencedAppendAs is a fenced-class append naming producer at epoch == gen.
func fencedAppendAs(s *Store, path string, fence auth.AppendFence, producer string, seq int64) (store.AppendResult, error) {
	epoch := fence.Generation
	return s.Append(path, []byte(`{"value":1}`), store.AppendOptions{
		ContentType:   "application/json",
		Fence:         &fence,
		ProducerId:    producer,
		ProducerEpoch: &epoch,
		ProducerSeq:   &seq,
	})
}

func mustGrant(t *testing.T, s *Store, path string, fence auth.AppendFence) {
	t.Helper()
	if installed, err := s.GrantAppendFence(path, fence); err != nil || !installed {
		t.Fatalf("grant generation %d = installed:%t err:%v, want true/nil", fence.Generation, installed, err)
	}
}

func mustFencedAppend(t *testing.T, s *Store, path string, fence auth.AppendFence, seq int64) store.Offset {
	t.Helper()
	res, err := fencedAppend(s, path, fence, seq)
	if err != nil {
		t.Fatalf("fenced append generation %d seq %d: %v", fence.Generation, seq, err)
	}
	return res.Offset
}

// assertFenced asserts a rejection carrying the expected disclosure.
func assertFenced(t *testing.T, label string, res store.AppendResult, err error, reason store.FenceReason, gen int64, holder string) {
	t.Helper()
	if !errors.Is(err, store.ErrAppendFenced) {
		t.Fatalf("%s = %v, want ErrAppendFenced", label, err)
	}
	if res.FenceReason != reason || res.FenceGeneration != gen || res.FenceHolder != holder {
		t.Fatalf("%s disclosure = (%q, %d, %q), want (%q, %d, %q)",
			label, res.FenceReason, res.FenceGeneration, res.FenceHolder, reason, gen, holder)
	}
}

func sealField(ctx context.Context, path string, fence auth.AppendFence) (string, bool) {
	v, err := testClient.HGet(ctx, metaKey(path), fSealPrefix+store.FenceAuthority(fence)).Result()
	return v, err == nil
}

func TestAppendFenceLifecycle(t *testing.T) {
	s := newTestStore(t)
	path := testPath("append-fence")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json"})

	now := time.Now()
	fence1 := auth.AppendFence{
		SubscriptionID:          "subscription-a",
		SubscriptionIncarnation: "incarnation-a",
		Generation:              1,
		WakeID:                  "wake-a",
		Holder:                  "worker-a",
		LeaseUntilNs:            now.Add(time.Minute).UnixNano(),
	}
	appendJSON := func(fence auth.AppendFence) error {
		_, err := s.Append(path, []byte(`{"value":1}`), store.AppendOptions{
			ContentType: "application/json",
			Fence:       &fence,
		})
		return err
	}

	if err := appendJSON(fence1); !errors.Is(err, store.ErrAppendFenced) {
		t.Fatalf("append before grant = %v, want ErrAppendFenced", err)
	}
	if installed, err := s.GrantAppendFence(path, fence1); err != nil || !installed {
		t.Fatalf("grant generation 1 = installed:%t err:%v, want true/nil", installed, err)
	}
	if err := appendJSON(fence1); err != nil {
		t.Fatalf("append generation 1: %v", err)
	}
	if err := s.RevokeAppendFence(path, fence1); err != nil {
		t.Fatalf("revoke generation 1: %v", err)
	}
	if err := appendJSON(fence1); !errors.Is(err, store.ErrAppendFenced) {
		t.Fatalf("append after revoke = %v, want ErrAppendFenced", err)
	}
	if _, err := s.GrantAppendFence(path, fence1); !errors.Is(err, store.ErrAppendFenced) {
		t.Fatalf("same-generation regrant = %v, want ErrAppendFenced", err)
	}

	fence2 := fence1
	fence2.Generation = 2
	fence2.WakeID = "wake-b"
	fence2.Holder = "worker-b"
	if installed, err := s.GrantAppendFence(path, fence2); err != nil || !installed {
		t.Fatalf("grant generation 2 = installed:%t err:%v, want true/nil", installed, err)
	}
	if err := s.RevokeAppendFence(path, fence1); err != nil {
		t.Fatalf("delayed generation 1 revoke: %v", err)
	}
	if err := appendJSON(fence2); err != nil {
		t.Fatalf("append generation 2 after stale revoke: %v", err)
	}
	if err := appendJSON(fence1); !errors.Is(err, store.ErrAppendFenced) {
		t.Fatalf("append stale generation = %v, want ErrAppendFenced", err)
	}
	if err := testClient.HSet(
		context.Background(),
		appendFenceKey(path, fence2),
		"lease_until_ns",
		now.Add(-time.Second).UnixNano(),
	).Err(); err != nil {
		t.Fatalf("expire marker lease: %v", err)
	}
	if err := appendJSON(fence2); !errors.Is(err, store.ErrAppendFenced) {
		t.Fatalf("append after marker lease expiry = %v, want ErrAppendFenced", err)
	}
	if installed, err := s.GrantAppendFence(path, fence2); err != nil || !installed {
		t.Fatalf("renew generation 2 = installed:%t err:%v, want true/nil", installed, err)
	}
	if _, err := s.CloseStreamFenced(path, fence2); err != nil {
		t.Fatalf("fenced close: %v", err)
	}
}

// TestAppendFenceGrantLegacyStreamNotInstalled pins the base-protocol shape a
// legacy stream keeps (#169, unchanged by #183): a meta hash that predates the
// incarnation field (read.lua backfills it lazily; appends never do) cannot be
// granted a marker — GrantAppendFence reports (false, nil), never an error, so
// a claim linked to such a stream still succeeds — and no marker, however it
// was built, matches it: the fenced class is refused as "marker" whether the
// marker lacks stream_incarnation or carries an empty one.
func TestAppendFenceGrantLegacyStreamNotInstalled(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	path := testPath("wf-legacy")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json"})
	if err := testClient.HDel(ctx, metaKey(path), fIncarnation).Err(); err != nil {
		t.Fatalf("drop incarnation: %v", err)
	}
	f1 := claimFence(1, "w_1", "worker-a")
	if installed, err := s.GrantAppendFence(path, f1); err != nil || installed {
		t.Fatalf("grant on a legacy stream = installed:%t err:%v, want false/nil", installed, err)
	}
	if n, err := testClient.Exists(ctx, appendFenceKey(path, f1)).Result(); err != nil || n != 0 {
		t.Fatalf("grant on a legacy stream wrote a marker: exists=%d, %v", n, err)
	}

	liveRow := []any{
		"state", store.WriteFenceMarkerLive, "generation", "1", "wake_id", "w_1",
		"holder", "worker-a", "lease_until_ns", f1.LeaseUntilNs,
	}
	for _, tt := range []struct {
		name   string
		marker []any
		want   store.WriteFenceOutcome
	}{
		{"no marker", nil, store.WriteFenceOutcome{Reason: store.FenceMarker}},
		{"marker without stream_incarnation", liveRow, store.WriteFenceOutcome{Reason: store.FenceMarker, Generation: 1, Holder: "worker-a"}},
		{"marker with an empty stream_incarnation", append(liveRow, "stream_incarnation", ""), store.WriteFenceOutcome{Reason: store.FenceMarker, Generation: 1, Holder: "worker-a"}},
	} {
		if tt.marker != nil {
			if err := testClient.HSet(ctx, appendFenceKey(path, f1), tt.marker...).Err(); err != nil {
				t.Fatalf("%s: seed marker: %v", tt.name, err)
			}
		}
		got, err := fencedAppend(s, path, f1, 0)
		assertFenced(t, tt.name, got, err, tt.want.Reason, tt.want.Generation, tt.want.Holder)
	}
}

func TestAppendFenceRejectsRecreatedStream(t *testing.T) {
	s := newTestStore(t)
	path := testPath("append-fence-recreate")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json"})
	fence := auth.AppendFence{
		SubscriptionID:          "subscription-a",
		SubscriptionIncarnation: "incarnation-a",
		Generation:              1,
		WakeID:                  "wake-a",
		Holder:                  "worker-a",
		LeaseUntilNs:            time.Now().Add(time.Minute).UnixNano(),
	}
	if installed, err := s.GrantAppendFence(path, fence); err != nil || !installed {
		t.Fatalf("grant = installed:%t err:%v, want true/nil", installed, err)
	}
	if err := s.Delete(path); err != nil {
		t.Fatalf("delete: %v", err)
	}
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json"})

	_, err := s.Append(path, []byte(`{"value":1}`), store.AppendOptions{
		ContentType: "application/json",
		Fence:       &fence,
	})
	if !errors.Is(err, store.ErrAppendFenced) {
		t.Fatalf("append to recreated stream = %v, want ErrAppendFenced", err)
	}
}

// TestAppendFenceSealLifecycle pins the seal at done on a write-fenced stream
// (R3.1–R3.3, INV-FENCE-06): the seal records the last fenced offset, a later
// write of the sealed generation is refused as "sealed" (not "marker") with the
// seal generation disclosed, HEAD's summary shows it, the sealed generation can
// never be re-granted, and the successor generation proceeds.
func TestAppendFenceSealLifecycle(t *testing.T) {
	s := newTestStore(t)
	path := testPath("wf-seal")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json", WriteFence: true})
	f1 := claimFence(1, "w_1", "worker-a")
	mustGrant(t, s, path, f1)
	tail1 := mustFencedAppend(t, s, path, f1, 0)

	res, err := s.SealAppendFence(path, f1)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if res.Outcome != store.SealSealed || res.Generation != 1 || !res.FinalOffset.Equal(tail1) {
		t.Fatalf("seal = %+v, want sealed/1/%v", res, tail1)
	}
	got, err := fencedAppend(s, path, f1, 1)
	assertFenced(t, "append after seal", got, err, store.FenceSealed, 1, "")
	if tail, _ := s.GetCurrentOffset(path); !tail.Equal(tail1) {
		t.Fatalf("tail moved after a sealed write: %v != %v", tail, tail1)
	}
	meta, err := s.Get(path)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !meta.WriteFence || meta.SealedGeneration != 1 || meta.SealedOffset == nil || !meta.SealedOffset.Equal(tail1) {
		t.Fatalf("metadata seal summary = wf:%t gen:%d off:%v, want true/1/%v",
			meta.WriteFence, meta.SealedGeneration, meta.SealedOffset, tail1)
	}
	if _, err := s.GrantAppendFence(path, f1); !errors.Is(err, store.ErrAppendFenced) {
		t.Fatalf("regrant of the sealed generation = %v, want ErrAppendFenced", err)
	}

	f2 := claimFence(2, "w_2", "worker-b")
	mustGrant(t, s, path, f2)
	mustFencedAppend(t, s, path, f2, 0)
	got, err = fencedAppend(s, path, f1, 1)
	assertFenced(t, "sealed generation behind a successor", got, err, store.FenceSealed, 2, "worker-b")
}

// TestAppendFenceSealIdempotentOnRedelivery pins that a redelivered done is a
// no-op reporting the standing seal (K.1/K.5), both while the tombstone is
// present and after it has been reaped.
func TestAppendFenceSealIdempotentOnRedelivery(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	path := testPath("wf-seal-redeliver")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json", WriteFence: true})
	f1 := claimFence(1, "w_1", "worker-a")
	mustGrant(t, s, path, f1)
	tail1 := mustFencedAppend(t, s, path, f1, 0)
	first, err := s.SealAppendFence(path, f1)
	if err != nil || first.Outcome != store.SealSealed {
		t.Fatalf("first seal = %+v, %v", first, err)
	}
	for _, step := range []string{"tombstone present", "tombstone reaped"} {
		if step == "tombstone reaped" {
			if err := testClient.Del(ctx, appendFenceKey(path, f1)).Err(); err != nil {
				t.Fatalf("reap marker: %v", err)
			}
		}
		again, err := s.SealAppendFence(path, f1)
		if err != nil {
			t.Fatalf("%s: redelivered seal: %v", step, err)
		}
		if again.Outcome != store.SealAlready || again.Generation != 1 || !again.FinalOffset.Equal(tail1) {
			t.Fatalf("%s: redelivered seal = %+v, want already/1/%v", step, again, tail1)
		}
	}
}

// TestAppendFenceSealStaleNoMutation pins that a seal naming an older
// generation, or the current generation under a different wake or holder,
// mutates nothing: the live marker keeps accepting and no seal is written.
func TestAppendFenceSealStaleNoMutation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	path := testPath("wf-seal-stale")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json", WriteFence: true})
	f2 := claimFence(2, "w_2", "worker-b")
	mustGrant(t, s, path, f2)

	older := claimFence(1, "w_1", "worker-a")
	otherWake := f2
	otherWake.WakeID = "w_other"
	for _, tt := range []struct {
		name  string
		fence auth.AppendFence
	}{
		{"older generation", older},
		{"same generation, different wake", otherWake},
	} {
		res, err := s.SealAppendFence(path, tt.fence)
		if err != nil || res.Outcome != store.SealStale {
			t.Fatalf("%s: seal = %+v, %v; want stale", tt.name, res, err)
		}
		if _, ok := sealField(ctx, path, tt.fence); ok {
			t.Fatalf("%s: a stale seal wrote wfseal", tt.name)
		}
		mustFencedAppend(t, s, path, f2, 0)
	}
}

// TestAppendFenceSealUnfencedTombstonesOnly pins that on a stream that never
// opted in the seal degrades to today's tombstone: the holder is fenced by the
// revoked marker, no seal field or summary is written, and — the documented
// residual — once the tombstone is reaped the generation can be granted again.
func TestAppendFenceSealUnfencedTombstonesOnly(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	path := testPath("wf-seal-unfenced")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json"})
	f1 := claimFence(1, "w_1", "worker-a")
	mustGrant(t, s, path, f1)
	mustFencedAppend(t, s, path, f1, 0)

	res, err := s.SealAppendFence(path, f1)
	if err != nil || res.Outcome != store.SealUnfenced || res.Generation != 0 {
		t.Fatalf("seal on an unfenced stream = %+v, %v; want unfenced/0", res, err)
	}
	got, err := fencedAppend(s, path, f1, 1)
	assertFenced(t, "append after tombstone", got, err, store.FenceMarker, 1, "")
	if _, ok := sealField(ctx, path, f1); ok {
		t.Fatal("unfenced stream carries a wfseal field")
	}
	if meta, err := s.Get(path); err != nil || meta.SealedGeneration != 0 || meta.SealedOffset != nil {
		t.Fatalf("unfenced stream carries a seal summary: %+v, %v", meta, err)
	}
	if _, err := s.GrantAppendFence(path, f1); !errors.Is(err, store.ErrAppendFenced) {
		t.Fatalf("regrant against the tombstone = %v, want ErrAppendFenced", err)
	}
	if err := testClient.Del(ctx, appendFenceKey(path, f1)).Err(); err != nil {
		t.Fatalf("reap marker: %v", err)
	}
	mustGrant(t, s, path, f1) // the tombstone-only residual on unfenced streams
}

// TestAppendFenceGrantSupersessionSealsPredecessor pins R3.3 at takeover: the
// successor's grant seals the predecessor at its last accepted fenced-class
// offset, not at a later open-class tail, and the predecessor is then refused
// as "sealed" with the successor disclosed.
func TestAppendFenceGrantSupersessionSealsPredecessor(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	path := testPath("wf-supersede")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json", WriteFence: true})
	f1 := claimFence(1, "w_1", "worker-a")
	mustGrant(t, s, path, f1)
	fencedTail := mustFencedAppend(t, s, path, f1, 0)
	open := mustAppend(t, s, path, []byte(`{"inbox":"hi"}`), store.AppendOptions{ContentType: "application/json"})
	if open.Offset.Equal(fencedTail) {
		t.Fatal("open-class append did not move the tail")
	}

	f2 := claimFence(2, "w_2", "worker-b")
	mustGrant(t, s, path, f2)
	if v, _ := sealField(ctx, path, f1); v != "1:w_1:"+fencedTail.String() {
		t.Fatalf("supersession seal = %q, want %q", v, "1:w_1:"+fencedTail.String())
	}
	meta, err := s.Get(path)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if meta.SealedGeneration != 1 || meta.SealedOffset == nil || !meta.SealedOffset.Equal(fencedTail) {
		t.Fatalf("seal summary = gen:%d off:%v, want 1/%v (the fenced tail, not %v)",
			meta.SealedGeneration, meta.SealedOffset, fencedTail, open.Offset)
	}
	got, err := fencedAppend(s, path, f1, 1)
	assertFenced(t, "predecessor after supersession", got, err, store.FenceSealed, 2, "worker-b")
	mustFencedAppend(t, s, path, f2, 0)
}

// TestAppendFenceGrantRefusesSealedGenerationAfterTombstoneReap pins K.4 /
// INV-FENCE-06: a delayed grant of a sealed generation is refused even after
// its marker tombstone has been reaped, for both ways a seal is written (done
// and supersession) — the seal, not the tombstone, is the durable revocation.
func TestAppendFenceGrantRefusesSealedGenerationAfterTombstoneReap(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		seal func(t *testing.T, path string, f1, f2 auth.AppendFence)
	}{
		{"sealed by done", func(t *testing.T, path string, f1, _ auth.AppendFence) {
			if res, err := s.SealAppendFence(path, f1); err != nil || res.Outcome != store.SealSealed {
				t.Fatalf("seal = %+v, %v", res, err)
			}
		}},
		{"sealed by supersession", func(t *testing.T, path string, _, f2 auth.AppendFence) {
			mustGrant(t, s, path, f2)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := testPath("wf-reap")
			mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json", WriteFence: true})
			f1 := claimFence(1, "w_1", "worker-a")
			f2 := claimFence(2, "w_2", "worker-b")
			mustGrant(t, s, path, f1)
			mustFencedAppend(t, s, path, f1, 0)
			tt.seal(t, path, f1, f2)
			if err := testClient.Del(ctx, appendFenceKey(path, f1)).Err(); err != nil {
				t.Fatalf("reap marker: %v", err)
			}
			if _, err := s.GrantAppendFence(path, f1); !errors.Is(err, store.ErrAppendFenced) {
				t.Fatalf("delayed grant of the sealed generation after reap = %v, want ErrAppendFenced", err)
			}
			got, err := fencedAppend(s, path, f1, 1)
			assertFenced(t, "sealed generation after reap", got, err, store.FenceSealed, 1, "")
			mustGrant(t, s, path, f2)
			mustFencedAppend(t, s, path, f2, 0)
		})
	}
}

// TestAppendFenceGrantRenewalNeverShortensLease pins that a same-claim re-grant
// (the heartbeat) only ever extends the marker lease: a delayed older re-grant
// landing after a newer one is harmless.
func TestAppendFenceGrantRenewalNeverShortensLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	path := testPath("wf-renew")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json", WriteFence: true})
	base := time.Now()
	f := claimFence(1, "w_1", "worker-a")
	leaseAt := func(d time.Duration) auth.AppendFence {
		f.LeaseUntilNs = base.Add(d).UnixNano()
		return f
	}
	markerLease := func() int64 {
		v, err := testClient.HGet(ctx, appendFenceKey(path, f), "lease_until_ns").Int64()
		if err != nil {
			t.Fatalf("read marker lease: %v", err)
		}
		return v
	}
	long := leaseAt(time.Minute)
	mustGrant(t, s, path, long)
	mustGrant(t, s, path, leaseAt(30*time.Second)) // delayed, older re-grant
	if got := markerLease(); got != long.LeaseUntilNs {
		t.Fatalf("renewal shortened the lease: %d, want %d", got, long.LeaseUntilNs)
	}
	longer := leaseAt(90 * time.Second)
	mustGrant(t, s, path, longer)
	if got := markerLease(); got != longer.LeaseUntilNs {
		t.Fatalf("renewal did not extend the lease: %d, want %d", got, longer.LeaseUntilNs)
	}
}

// TestAppendFenceSealPerAuthorityIsolated pins A.0 Q5 / K.10: the seal is per
// authority (subscription incarnation), so a recreated subscription starts
// unsealed on the same fenced stream while the old incarnation stays sealed,
// and the HEAD summary follows the most recent seal on the stream.
func TestAppendFenceSealPerAuthorityIsolated(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	path := testPath("wf-authority")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json", WriteFence: true})
	old := claimFence(5, "w_5", "worker-a")
	recreated := claimFence(1, "w_1", "worker-a")
	recreated.SubscriptionIncarnation = "incarnation-b"

	mustGrant(t, s, path, old)
	oldTail := mustFencedAppend(t, s, path, old, 0)
	if res, err := s.SealAppendFence(path, old); err != nil || res.Outcome != store.SealSealed || res.Generation != 5 {
		t.Fatalf("seal old incarnation = %+v, %v", res, err)
	}

	mustGrant(t, s, path, recreated) // generation 1 < 5, but its own namespace
	// A fresh producer id: producer "p" keeps epoch 5 from the old incarnation,
	// and its stale-epoch refusal at epoch 1 is the documented K.9 limitation.
	res, err := fencedAppendAs(s, path, recreated, "p-recreated", 0)
	if err != nil {
		t.Fatalf("recreated incarnation append: %v", err)
	}
	newTail := res.Offset
	if _, err := s.GrantAppendFence(path, old); !errors.Is(err, store.ErrAppendFenced) {
		t.Fatalf("old incarnation regrant = %v, want ErrAppendFenced", err)
	}
	meta, err := s.Get(path)
	if err != nil || meta.SealedGeneration != 5 || !meta.SealedOffset.Equal(oldTail) {
		t.Fatalf("summary after the old seal = gen:%d off:%v, %v; want 5/%v", meta.SealedGeneration, meta.SealedOffset, err, oldTail)
	}

	if res, err := s.SealAppendFence(path, recreated); err != nil || res.Outcome != store.SealSealed || res.Generation != 1 {
		t.Fatalf("seal recreated incarnation = %+v, %v", res, err)
	}
	if v, _ := sealField(ctx, path, old); v != "5:w_5:"+oldTail.String() {
		t.Fatalf("old incarnation seal disturbed: %q", v)
	}
	if v, _ := sealField(ctx, path, recreated); v != "1:w_1:"+newTail.String() {
		t.Fatalf("recreated incarnation seal = %q, want %q", v, "1:w_1:"+newTail.String())
	}
	meta, err = s.Get(path)
	if err != nil || meta.SealedGeneration != 1 || !meta.SealedOffset.Equal(newTail) {
		t.Fatalf("summary after the newer seal = gen:%d off:%v, %v; want 1/%v", meta.SealedGeneration, meta.SealedOffset, err, newTail)
	}
	got, err := fencedAppend(s, path, old, 1)
	assertFenced(t, "old incarnation after both seals", got, err, store.FenceSealed, 5, "")
	if pttl, err := testClient.PTTL(ctx, appendFenceKey(path, recreated)).Result(); err != nil || pttl <= 0 || pttl > appendFenceRetention {
		t.Fatalf("sealed marker tombstone TTL = %v, %v; want within %v", pttl, err, appendFenceRetention)
	}
	if v, err := testClient.HGet(ctx, metaKey(path), fSealGen).Result(); err != nil || v != strconv.Itoa(1) {
		t.Fatalf("wfSealGen = %q, %v; want 1", v, err)
	}
}
