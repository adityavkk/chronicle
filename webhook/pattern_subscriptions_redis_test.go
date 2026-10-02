package webhook

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

// redisCallCounter counts single commands and pipelines so a test can pin the
// round-trip shape of a read: one pipeline per subReadChunk ids, never one
// command per id.
type redisCallCounter struct {
	singles, pipelines atomic.Int64
}

func (c *redisCallCounter) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (c *redisCallCounter) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		c.singles.Add(1)
		return next(ctx, cmd)
	}
}

func (c *redisCallCounter) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		c.pipelines.Add(1)
		return next(ctx, cmds)
	}
}

// seedPatternHash writes a minimal pattern subscription hash and a links key of
// the wrong type, which proves the pattern read never touches link history.
func seedPatternHash(t *testing.T, client goredis.UniversalClient, id, pattern string) {
	t.Helper()
	ctx := context.Background()
	if err := client.HSet(ctx, subKey(id), "pattern", pattern, "type", "pull").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, linksKey(id), "not-a-hash", 0).Err(); err != nil {
		t.Fatal(err)
	}
}

// TestPatternSubscriptionsPartialBatch: 515 ids span two pipelines; a hash of
// the wrong type fails its own command only, a subscription without a pattern
// is filtered, a listed id with no hash is counted missing, and no links hash
// is read.
func TestPatternSubscriptionsPartialBatch(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	ids := make([]string, 0, 517)
	for i := 0; i < 515; i++ {
		id := fmt.Sprintf("pattern-%d", i)
		ids = append(ids, id)
		seedPatternHash(t, client, id, "events/*")
	}
	broken := ids[513] // in the second pipeline
	if err := client.Del(ctx, subKey(broken)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, subKey(broken), "not-a-hash", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, subKey("explicit"), "type", "pull").Err(); err != nil {
		t.Fatal(err)
	}
	ids = append(ids, "explicit", "deleted")

	read, err := s.PatternSubscriptions(ids)
	if err == nil || !strings.Contains(err.Error(), "1 of 517 reads failed") {
		t.Fatalf("err = %v, want one failed read out of 517", err)
	}
	if read.Failed != 1 || read.Missing != 1 || len(read.Subs) != 514 {
		t.Fatalf("read: failed %d missing %d subs %d; want 1, 1, 514", read.Failed, read.Missing, len(read.Subs))
	}
	for _, sub := range read.Subs {
		if sub.ID == broken || sub.ID == "explicit" || sub.ID == "deleted" || sub.Pattern != "events/*" {
			t.Fatalf("unexpected subscription in read: %+v", sub)
		}
	}
}

// TestPatternSubscriptionsMissingIDsCostNoExtraRoundTrips: 1,100 listed ids
// with no hash (deleted after List, or legacy records) are counted in three
// pipelines and zero single commands — the hook never falls back to a per-id
// read, and missing is not an error.
func TestPatternSubscriptionsMissingIDsCostNoExtraRoundTrips(t *testing.T) {
	s, client := newTestStore(t)
	var calls redisCallCounter
	client.AddHook(&calls)
	ids := make([]string, 1100)
	for i := range ids {
		ids[i] = fmt.Sprintf("gone-%d", i)
	}
	read, err := s.PatternSubscriptions(ids)
	if err != nil {
		t.Fatalf("missing hashes must not be an error: %v", err)
	}
	if read.Missing != 1100 || read.Failed != 0 || len(read.Subs) != 0 {
		t.Fatalf("read = %+v, want 1100 missing and nothing else", read)
	}
	if p, c := calls.pipelines.Load(), calls.singles.Load(); p != 3 || c != 0 {
		t.Fatalf("round trips: %d pipelines, %d single commands; want 3 and 0", p, c)
	}
}

// TestPatternSubscriptionsTransportFailureFailsFast: when no connection can be
// acquired at all, every id is a failed read, not a missing hash, and the read
// stops after the first chunk instead of paying a dial timeout per 512 ids
// inside the create request. Needs no Redis.
func TestPatternSubscriptionsTransportFailureFailsFast(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens here any more
	client := goredis.NewClient(&goredis.Options{Addr: addr, DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	var calls redisCallCounter
	client.AddHook(&calls)
	s := NewRedisStore(client)
	ids := make([]string, 1100)
	for i := range ids {
		ids[i] = fmt.Sprintf("sub-%d", i)
	}

	start := time.Now()
	read, err := s.PatternSubscriptions(ids)
	if err == nil {
		t.Fatal("expected a transport error")
	}
	if read.Failed != 1100 || read.Missing != 0 || len(read.Subs) != 0 {
		t.Fatalf("read: failed %d missing %d subs %d; want every id failed", read.Failed, read.Missing, len(read.Subs))
	}
	if p := calls.pipelines.Load(); p != 1 {
		t.Fatalf("pipelines = %d, want 1: fail fast after the first chunk", p)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %s; an unreachable Redis must not cost a dial timeout per chunk", took)
	}
}

// TestOnStreamCreatedLinksWhatItReadAndReconcileHealsTheRest, against the
// real store: a subscription whose hash is unreadable is skipped, the healthy
// one is linked at the beginning offset, the completion line reports the
// partial failure under the request id, and once the hash is readable again
// the pattern reconcile links the missed subscription at the beginning offset
// (INV-RECOVER-03), so the stream's initial data is not lost.
func TestOnStreamCreatedLinksWhatItReadAndReconcileHealsTheRest(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	for _, id := range []string{"healthy", "broken"} {
		if _, err := s.CreateOrConfirm(id, pullWakeCfg(), nil, now); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	fields, err := client.HGetAll(ctx, subKey("broken")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Del(ctx, subKey("broken")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, subKey("broken"), "bad", 0).Err(); err != nil {
		t.Fatal(err)
	}

	path := "events/new"
	fs := &fakeStreams{tails: map[string]string{path: "0000000000000001_0000000000000010"}}
	lister := &fakeLister{streams: []StreamMeta{{Path: path, Tail: fs.tails[path], CreatedAtNs: now.Add(time.Second).UnixNano()}}}
	m, err := NewManager(s, fs, ManagerOptions{StreamRootURL: "http://x/", Lister: lister})
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	m.log = slog.New(slog.NewJSONHandler(&logs, nil))

	m.OnStreamCreatedWithContext(correlation.WithRequestID(ctx, "req-create-2"), path)

	rec := completionLine(t, &logs)
	for k, v := range map[string]any{"outcome": "partial_failure", "request_id": "req-create-2", "subscriptions": 2.0, "read_failures": 1.0, "linked": 1.0} {
		if rec[k] != v {
			t.Fatalf("completion line %s = %v, want %v (%s)", k, rec[k], v, logs.String())
		}
	}
	healthy, ok, err := s.Get("healthy")
	if err != nil || !ok || len(healthy.Links) != 1 || healthy.Links[0].AckedOffset != fs.BeginningOffset() {
		t.Fatalf("healthy subscription should be linked at the beginning: links=%+v ok=%v err=%v", healthy.Links, ok, err)
	}

	if err := client.Del(ctx, subKey("broken")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, subKey("broken"), fields).Err(); err != nil {
		t.Fatal(err)
	}
	m.RunReconcile()
	recovered, ok, err := s.Get("broken")
	if err != nil || !ok || len(recovered.Links) != 1 || recovered.Links[0].AckedOffset != fs.BeginningOffset() {
		t.Fatalf("reconcile should link the missed subscription at the beginning: links=%+v ok=%v err=%v", recovered.Links, ok, err)
	}
}
