package webhook

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// A member of a stream's fan-out shard whose subscription is gone (a Delete
// that landed between a reconcile pass's read and its re-assertion, or one torn
// before its de-index) is removed by the fan-out worker the first time an
// append hydrates nothing for it, and costs that stream nothing afterwards. The
// store's guard keeps a member whose subscription was re-created and re-linked
// in between.

const staleTail = "0000000000000001_0000000000000010"

func staleCfg() Config {
	return Config{Type: DispatchWebhook, WebhookURL: "https://w.example/h", LeaseTTLMs: 1000}
}

func newStaleFanoutManager(t *testing.T, s Store) *Manager {
	t.Helper()
	mgr, err := NewManager(s, &fakeStreams{tails: map[string]string{"events/a": staleTail}}, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		Logger:        slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	return mgr
}

// seedCaughtUp creates id linked to events/a at its tail: a live subscriber
// with nothing pending, so a tick hydrates it and arms nothing.
func seedCaughtUp(t *testing.T, s *RedisStore, id string) {
	t.Helper()
	if _, err := s.CreateOrConfirm(id, staleCfg(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.Link(id, "events/a", LinkExplicit, staleTail); err != nil {
		t.Fatal(err)
	}
}

// staleMember builds the race's outcome: a deleted subscription whose fan-out
// member was re-added.
func staleMember(t *testing.T, s *RedisStore, id string) {
	t.Helper()
	seedCaughtUp(t, s, id)
	if err := s.Delete(id); err != nil {
		t.Fatal(err)
	}
	if err := s.client.SAdd(context.Background(), streamSubsKey(slotOf(id), "events/a"), id).Err(); err != nil {
		t.Fatal(err)
	}
}

func isFanoutMember(t *testing.T, s *RedisStore, id string) bool {
	t.Helper()
	ok, err := s.client.SIsMember(context.Background(), streamSubsKey(slotOf(id), "events/a"), id).Result()
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func TestDirtyFanoutRemovesStaleMemberOfDeletedSubscription(t *testing.T) {
	s, rec := newRecordedStore(t)
	seedCaughtUp(t, s, "live")
	staleMember(t, s, "ghost")
	mgr := newStaleFanoutManager(t, s)
	ctx := context.Background()
	// Warm the script cache so the count below is the steady state, not an
	// EVALSHA that falls back to EVAL once (the TripLog logs that reload apart).
	if _, err := s.DeindexStale("warm", "events/a"); err != nil {
		t.Fatal(err)
	}

	rec.Take()
	mgr.OnStreamAppend(ctx, "events/a")
	mgr.RunDirtyWorker()
	first := rec.Take()
	if isFanoutMember(t, s, "ghost") {
		t.Fatalf("stale member survived the tick: %s", summarize(first))
	}
	if countNamed(first, "evalsha:control") != 1 {
		t.Fatalf("the removal should be one script call: %s", summarize(first))
	}

	rec.Take()
	mgr.OnStreamAppend(ctx, "events/a")
	mgr.RunDirtyWorker()
	second := rec.Take()
	// The first tick pays the stale member's cost once (the legacy-keyspace
	// probe and the removal); the second is the clean cost of a one-subscriber
	// stream: the bitmap GET, one SMEMBERS pipeline and one hydration pipeline.
	if len(first) != 5 || len(second) != 3 {
		t.Fatalf("ticks cost %d then %d trips, want 5 then 3: %s / %s", len(first), len(second), summarize(first), summarize(second))
	}
	if ids, _, _ := s.StreamSubscribers("events/a"); len(ids) != 1 || ids[0] != "live" {
		t.Fatalf("StreamSubscribers = %v, want [live]", ids)
	}
}

// recreateOnHydrate re-creates id, with its link written and its index write
// still to come, right after the worker's hydration read missed it: the member
// the worker is about to judge stale is justified again by the time the store
// checks it.
type recreateOnHydrate struct {
	*RedisStore
	t  *testing.T
	id string
}

func (r *recreateOnHydrate) GetMany(ids []string) ([]Subscription, error) {
	subs, err := r.RedisStore.GetMany(ids)
	if _, cerr := r.CreateOrConfirm(r.id, staleCfg(), nil, time.Now()); cerr != nil {
		r.t.Error(cerr)
	}
	if herr := r.client.HSet(context.Background(), linksKey(r.id), "events/a", "explicit:"+staleTail).Err(); herr != nil {
		r.t.Error(herr)
	}
	return subs, err
}

func TestDirtyFanoutKeepsMemberOfSubscriptionRecreatedAfterHydration(t *testing.T) {
	s, _ := newTestStore(t)
	staleMember(t, s, "ghost")
	mgr := newStaleFanoutManager(t, &recreateOnHydrate{RedisStore: s, t: t, id: "ghost"})
	mgr.OnStreamAppend(context.Background(), "events/a")
	mgr.RunDirtyWorker()
	if !isFanoutMember(t, s, "ghost") {
		t.Fatal("a member whose subscription was re-linked before the removal must be kept")
	}
	if ids, _, _ := s.StreamSubscribers("events/a"); len(ids) != 1 || ids[0] != "ghost" {
		t.Fatalf("StreamSubscribers = %v, want [ghost]", ids)
	}
}

func TestDeindexStaleStatuses(t *testing.T) {
	s, _ := newTestStore(t)
	seedCaughtUp(t, s, "s1")
	if removed, err := s.DeindexStale("s1", "events/a"); err != nil || removed {
		t.Fatalf("DeindexStale on a linked member = %v/%v, want kept", removed, err)
	}
	if !isFanoutMember(t, s, "s1") {
		t.Fatal("a linked member must survive DeindexStale")
	}
	if err := s.Delete("s1"); err != nil {
		t.Fatal(err)
	}
	if err := s.client.SAdd(context.Background(), streamSubsKey(slotOf("s1"), "events/a"), "s1").Err(); err != nil {
		t.Fatal(err)
	}
	if removed, err := s.DeindexStale("s1", "events/a"); err != nil || !removed {
		t.Fatalf("DeindexStale on a stale member = %v/%v, want removed", removed, err)
	}
	if isFanoutMember(t, s, "s1") {
		t.Fatal("the stale member must be gone")
	}
	if removed, err := s.DeindexStale("s1", "events/a"); err != nil || removed {
		t.Fatalf("DeindexStale on an absent member = %v/%v, want nothing removed", removed, err)
	}
}
