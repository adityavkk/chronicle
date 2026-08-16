package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

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
		appendFenceKey(path, fence2.SubscriptionID, fence2.SubscriptionIncarnation, fence2.Shard),
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
