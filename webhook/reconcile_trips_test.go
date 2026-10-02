package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// The reconcile loop talks to one Redis from every region, so what it costs is
// its number of SERIAL round trips, not its CPU: from a remote region each trip
// is tens of milliseconds and the pass used to spend 1 + N + 2L of them on the
// index repair plus N more reading subscriptions one at a time. These tests pin
// the batched shape exactly, with a hook that counts trips the way the wire
// does — one per single command, one per pipeline Exec — so a regression back
// to a per-subscription or per-link round trip fails loudly.

// tripCounter is a go-redis Hook that records every single command and every
// pipeline Exec issued through the client while armed.
type tripCounter struct {
	mu      sync.Mutex
	singles []string   // command name per single command, in order
	execs   [][]string // command names per pipeline Exec, in order
}

func (c *tripCounter) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (c *tripCounter) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		c.mu.Lock()
		c.singles = append(c.singles, strings.ToUpper(cmd.Name()))
		c.mu.Unlock()
		return next(ctx, cmd)
	}
}

func (c *tripCounter) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		names := make([]string, len(cmds))
		for i, cmd := range cmds {
			names[i] = strings.ToUpper(cmd.Name())
		}
		c.mu.Lock()
		c.execs = append(c.execs, names)
		c.mu.Unlock()
		return next(ctx, cmds)
	}
}

func (c *tripCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.singles, c.execs = nil, nil
}

// trips is the serial round-trip count: singles plus pipeline Execs.
func (c *tripCounter) trips() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.singles) + len(c.execs)
}

// execsLedBy counts the pipeline Execs whose first command is name — the
// pipelines this package builds are homogeneous (SMEMBERS, HKEYS, HGETALL) or
// the SADD/SETBIT index pairs, so the first command names the pipeline.
func (c *tripCounter) execsLedBy(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.execs {
		if len(e) > 0 && e[0] == name {
			n++
		}
	}
	return n
}

func (c *tripCounter) singlesNamed(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, s := range c.singles {
		if s == name {
			n++
		}
	}
	return n
}

func (c *tripCounter) widestExec() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := 0
	for _, e := range c.execs {
		w = max(w, len(e))
	}
	return w
}

func (c *tripCounter) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	parts := make([]string, 0, len(c.singles)+len(c.execs))
	parts = append(parts, c.singles...)
	for _, e := range c.execs {
		parts = append(parts, fmt.Sprintf("pipe[%s x%d]", e[0], len(e)))
	}
	return strings.Join(parts, " ")
}

// newCountedStore is newTestStore with a tripCounter on the client.
func newCountedStore(t *testing.T) (*RedisStore, *tripCounter) {
	t.Helper()
	_, client := newTestStore(t)
	counter := &tripCounter{}
	client.AddHook(counter)
	return NewRedisStore(client), counter
}

// seedPatternSubs creates n pull-wake pattern subscriptions "sub-i" on pattern
// "p<i>/*", each glob-linked to k streams "p<i>/s<j>" at the stream's tail, and
// returns the streams a lister should report plus their tails. Every stream
// predates every subscription (CreatedAtNs 0), so a missing link relinks at the
// tail and never has pending work — the pass does the repair without waking.
func seedPatternSubs(t *testing.T, s *RedisStore, n, k int) ([]StreamMeta, map[string]string) {
	t.Helper()
	const tail = "0000000000000001_0000000000000010"
	now := time.Now()
	streams := make([]StreamMeta, 0, n*k)
	tails := make(map[string]string, n*k)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("sub-%04d", i)
		cfg := Config{Type: DispatchPullWake, Pattern: fmt.Sprintf("p%04d/*", i), WakeStream: "wake/pool", LeaseTTLMs: 1000}
		if _, err := s.CreateOrConfirm(id, cfg, nil, now); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		for j := 0; j < k; j++ {
			path := fmt.Sprintf("p%04d/s%02d", i, j)
			if err := s.Link(id, path, LinkGlob, tail); err != nil {
				t.Fatalf("link %s %s: %v", id, path, err)
			}
			streams = append(streams, StreamMeta{Path: path, Tail: tail})
			tails[path] = tail
		}
	}
	return streams, tails
}

func newReconcileManager(t *testing.T, s Store, streams []StreamMeta, tails map[string]string) *Manager {
	t.Helper()
	mgr, err := NewManager(s, &fakeStreams{tails: tails}, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		Lister:        &fakeLister{streams: streams},
		Logger:        slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	return mgr
}

func ceilDiv(a, b int) int { return (a + b - 1) / b }

// TestReconcileRoundTripsAreBounded pins the steady-state cost of one pass:
// List (1) + HKEYS per chunk of subscriptions + one index pipeline per chunk of
// (subscription, path) entries, then List (1) + GetMany per chunk — and not a
// single unpipelined command. The three shapes are the ones the loop was
// measured on: on these same fixtures, with this same hook, the serial pass
// cost 2 + 2N + 2L = 334, 1,828 and 4,002 trips respectively (a real stream
// listing adds its own few trips to either side alike).
func TestReconcileRoundTripsAreBounded(t *testing.T) {
	cases := []struct {
		n, k      int
		wantTrips int
	}{
		{n: 83, k: 1, wantTrips: 5},
		{n: 83, k: 10, wantTrips: 6},
		{n: 1000, k: 1, wantTrips: 8},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("n%d_k%d", tc.n, tc.k), func(t *testing.T) {
			s, counter := newCountedStore(t)
			streams, tails := seedPatternSubs(t, s, tc.n, tc.k)
			mgr := newReconcileManager(t, s, streams, tails)

			counter.reset()
			mgr.RunReconcile()

			// The exact formula, so a reader can check the table above by hand.
			subChunks := ceilDiv(tc.n, pipelineChunk)
			indexChunks := 0
			for start := 0; start < tc.n; start += pipelineChunk {
				subsInChunk := min(pipelineChunk, tc.n-start)
				indexChunks += ceilDiv(subsInChunk*tc.k, pipelineChunk)
			}
			if want := 2 + 2*subChunks + indexChunks; want != tc.wantTrips {
				t.Fatalf("formula gives %d trips, table says %d", want, tc.wantTrips)
			}
			if got := counter.trips(); got != tc.wantTrips {
				t.Fatalf("one reconcile pass = %d trips, want %d: %s", got, tc.wantTrips, counter)
			}
			if n := len(counter.singles); n != 0 {
				t.Fatalf("steady state must issue no single commands, got %d: %s", n, counter)
			}
			if got := counter.execsLedBy("SMEMBERS"); got != 2 {
				t.Fatalf("List pipelines = %d, want 2 (index pass + pattern pass)", got)
			}
			if got := counter.execsLedBy("HKEYS"); got != subChunks {
				t.Fatalf("HKEYS pipelines = %d, want %d", got, subChunks)
			}
			if got := counter.execsLedBy("SADD"); got != indexChunks {
				t.Fatalf("index pipelines = %d, want %d", got, indexChunks)
			}
			if got := counter.execsLedBy("HGETALL"); got != subChunks {
				t.Fatalf("GetMany pipelines = %d, want %d", got, subChunks)
			}
			if w := counter.widestExec(); w > 2*pipelineChunk {
				t.Fatalf("a pipeline carried %d commands, the bound is %d", w, 2*pipelineChunk)
			}
		})
	}
}

// TestReconcileRepairsMissingLinksWithOneLinkEach is the drift shape: M glob
// links missing across R subscriptions cost exactly M Link calls (one Lua trip
// and one index pipeline each) plus, per relinked subscription, the fresh read
// it is linked from and maybeWake's read, on top of the steady-state pass.
// Nothing else grows with the damage.
func TestReconcileRepairsMissingLinksWithOneLinkEach(t *testing.T) {
	s, counter := newCountedStore(t)
	const n, k = 10, 2
	streams, tails := seedPatternSubs(t, s, n, k)
	// Forget the links (the lost-OnStreamCreated fault) for four streams across
	// three subscriptions.
	missing := []string{"p0000/s00", "p0000/s01", "p0001/s00", "p0002/s01"}
	const relinkedSubs = 3
	ctx := context.Background()
	for _, path := range missing {
		id := "sub-" + path[1:5]
		if err := s.client.HDel(ctx, linksKey(id), path).Err(); err != nil {
			t.Fatal(err)
		}
		if err := s.client.SRem(ctx, streamSubsKey(slotOf(id), path), id).Err(); err != nil {
			t.Fatal(err)
		}
	}
	mgr := newReconcileManager(t, s, streams, tails)

	counter.reset()
	mgr.RunReconcile()

	m := len(missing)
	if got := counter.singlesNamed("EVALSHA"); got != m {
		t.Fatalf("link_stream runs = %d, want one per missing link (%d): %s", got, m, counter)
	}
	if got := len(counter.singles); got != m {
		t.Fatalf("single commands = %d, want only the %d Lua runs: %s", got, m, counter)
	}
	if got := counter.execsLedBy("SADD"); got != 1+m {
		t.Fatalf("index pipelines = %d, want the pass's 1 plus one per Link (%d): %s", got, 1+m, counter)
	}
	// GetMany once, then per relinked subscription the fresh read its links are
	// decided from and maybeWake's read.
	if got := counter.execsLedBy("HGETALL"); got != 1+2*relinkedSubs {
		t.Fatalf("subscription reads = %d, want 1 + 2*%d: %s", got, relinkedSubs, counter)
	}
	if got, want := counter.trips(), 5+2*m+2*relinkedSubs; got != want {
		t.Fatalf("drift pass = %d trips, want %d: %s", got, want, counter)
	}
	for _, path := range missing {
		id := "sub-" + path[1:5]
		sub, _, _ := s.Get(id)
		found := false
		for _, l := range sub.Links {
			found = found || l.Path == path
		}
		if !found {
			t.Fatalf("%s should have been relinked to %s, links %+v", id, path, sub.Links)
		}
	}
}

// TestLinkIsTwoRoundTrips pins the live link path: the Lua write, then the two
// index commands in one pipeline.
func TestLinkIsTwoRoundTrips(t *testing.T) {
	s, counter := newCountedStore(t)
	if _, err := s.CreateOrConfirm("s1", pullWakeCfg(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Warm the script cache so the first Link's NOSCRIPT -> EVAL self-heal does
	// not count against the steady-state shape.
	if err := s.Link("s1", "events/warm", LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatal(err)
	}
	counter.reset()
	if err := s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatal(err)
	}
	if got := counter.trips(); got != 2 || counter.singlesNamed("EVALSHA") != 1 || counter.execsLedBy("SADD") != 1 {
		t.Fatalf("Link = %d trips, want EVALSHA + one SADD/SETBIT pipeline: %s", got, counter)
	}
	if subs, _, _ := s.StreamSubscribers("events/a"); len(subs) != 1 || subs[0] != "s1" {
		t.Fatalf("fan-out after Link = %v, want [s1]", subs)
	}
}

// TestReconcileIndexErrorDoesNotSuppressPatternRecovery pins reconcileOnce's
// error precedence: an index-pass failure is logged, and the pattern pass still
// runs. The fault is a STRING squatting a fan-out SET key (so the index write
// fails WRONGTYPE) — not a links hash, which the pattern pass reads too.
func TestReconcileIndexErrorDoesNotSuppressPatternRecovery(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	const begin = "0000000000000000_0000000000000000"
	// x: explicit-only, its fan-out SET replaced by a STRING.
	xCfg := Config{Type: DispatchWebhook, WebhookURL: "https://w.example/h", LeaseTTLMs: 1000, Streams: []string{"events/x"}}
	xLinks := []StreamLink{{Path: "events/x", LinkType: LinkExplicit, AckedOffset: begin}}
	if _, err := s.CreateOrConfirm("x", xCfg, xLinks, time.Now()); err != nil {
		t.Fatal(err)
	}
	squatted := streamSubsKey(slotOf("x"), "events/x")
	if err := s.client.Del(ctx, squatted).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.client.Set(ctx, squatted, "not-a-set", 0).Err(); err != nil {
		t.Fatal(err)
	}
	// y: a pattern subscription missing its matching stream.
	if _, err := s.CreateOrConfirm("y", pullWakeCfg(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	stream := StreamMeta{Path: "events/y", Tail: begin}
	mgr := newReconcileManager(t, s, []StreamMeta{stream}, map[string]string{stream.Path: stream.Tail})

	if err := s.ReconcileIndexes(); err == nil {
		t.Fatal("ReconcileIndexes should surface the WRONGTYPE index write")
	}
	mgr.RunReconcile()
	sub, _, _ := s.Get("y")
	if len(sub.Links) != 1 || sub.Links[0].Path != "events/y" {
		t.Fatalf("pattern recovery must still run after an index error, links %+v", sub.Links)
	}
}
