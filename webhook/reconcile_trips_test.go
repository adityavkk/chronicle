package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/internal/redistest"
)

// The reconcile loop talks to one Redis from every region, so what it costs is
// its number of SERIAL round trips, not its CPU: from a remote region each trip
// is tens of milliseconds and the pass used to spend 1 + N + 2L of them on the
// index repair plus N more reading subscriptions one at a time. These tests pin
// the batched shape exactly, with a hook that counts trips the way the wire
// does — one per single command, one per pipeline Exec — so a regression back
// to a per-subscription or per-link round trip fails loudly.

// The hook is the shared redistest.TripLog: one entry per single command,
// named, and one per pipeline Exec as "pipe(name+name+...)". A script that
// does not run in a stream's slot (link_stream.lua here) is logged as
// "evalsha:control". The helpers below read a pass's entries.

// pipelinesLedBy counts the pipeline Execs whose first command is name — the
// pipelines this package builds are homogeneous (smembers, hkeys, hgetall) or
// the sadd/setbit index pairs, so the first command names the pipeline.
func pipelinesLedBy(trips []string, name string) int {
	n := 0
	for _, t := range trips {
		if t == "pipe("+name+")" || strings.HasPrefix(t, "pipe("+name+"+") {
			n++
		}
	}
	return n
}

// singleCommands is the entries that were not pipelined.
func singleCommands(trips []string) []string {
	var singles []string
	for _, t := range trips {
		if !strings.HasPrefix(t, "pipe(") {
			singles = append(singles, t)
		}
	}
	return singles
}

func countNamed(trips []string, name string) int {
	n := 0
	for _, t := range trips {
		if t == name {
			n++
		}
	}
	return n
}

// widestPipeline is the most commands one Exec carried.
func widestPipeline(trips []string) int {
	w := 0
	for _, t := range trips {
		if strings.HasPrefix(t, "pipe(") {
			w = max(w, strings.Count(t, "+")+1)
		}
	}
	return w
}

// summarize prints a pass with each pipeline folded to its lead command and
// width, so a failure message stays readable at 1,024 commands per Exec.
func summarize(trips []string) string {
	parts := make([]string, len(trips))
	for i, t := range trips {
		parts[i] = t
		if cmds, ok := strings.CutPrefix(t, "pipe("); ok {
			lead, _, _ := strings.Cut(strings.TrimSuffix(cmds, ")"), "+")
			parts[i] = fmt.Sprintf("pipe[%s x%d]", lead, strings.Count(cmds, "+")+1)
		}
	}
	return strings.Join(parts, " ")
}

// newRecordedStore is newTestStore with a TripLog on the client.
func newRecordedStore(t *testing.T) (*RedisStore, *redistest.TripLog) {
	t.Helper()
	_, client := newTestStore(t)
	rec := &redistest.TripLog{}
	client.AddHook(rec)
	return NewRedisStore(client), rec
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
			s, rec := newRecordedStore(t)
			streams, tails := seedPatternSubs(t, s, tc.n, tc.k)
			mgr := newReconcileManager(t, s, streams, tails)

			rec.Take()
			mgr.RunReconcile()
			trips := rec.Take()

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
			if got := len(trips); got != tc.wantTrips {
				t.Fatalf("one reconcile pass = %d trips, want %d: %s", got, tc.wantTrips, summarize(trips))
			}
			if n := len(singleCommands(trips)); n != 0 {
				t.Fatalf("steady state must issue no single commands, got %d: %s", n, summarize(trips))
			}
			if got := pipelinesLedBy(trips, "smembers"); got != 2 {
				t.Fatalf("List pipelines = %d, want 2 (index pass + pattern pass)", got)
			}
			if got := pipelinesLedBy(trips, "hkeys"); got != subChunks {
				t.Fatalf("HKEYS pipelines = %d, want %d", got, subChunks)
			}
			if got := pipelinesLedBy(trips, "sadd"); got != indexChunks {
				t.Fatalf("index pipelines = %d, want %d", got, indexChunks)
			}
			if got := pipelinesLedBy(trips, "hgetall"); got != subChunks {
				t.Fatalf("GetMany pipelines = %d, want %d", got, subChunks)
			}
			if w := widestPipeline(trips); w > 2*pipelineChunk {
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
	s, rec := newRecordedStore(t)
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

	rec.Take()
	mgr.RunReconcile()
	trips := rec.Take()

	m := len(missing)
	if got := countNamed(trips, "evalsha:control"); got != m {
		t.Fatalf("link_stream runs = %d, want one per missing link (%d): %s", got, m, summarize(trips))
	}
	if got := len(singleCommands(trips)); got != m {
		t.Fatalf("single commands = %d, want only the %d Lua runs: %s", got, m, summarize(trips))
	}
	if got := pipelinesLedBy(trips, "sadd"); got != 1+m {
		t.Fatalf("index pipelines = %d, want the pass's 1 plus one per Link (%d): %s", got, 1+m, summarize(trips))
	}
	// GetMany once, then per relinked subscription the fresh read its links are
	// decided from and maybeWake's read.
	if got := pipelinesLedBy(trips, "hgetall"); got != 1+2*relinkedSubs {
		t.Fatalf("subscription reads = %d, want 1 + 2*%d: %s", got, relinkedSubs, summarize(trips))
	}
	if got, want := len(trips), 5+2*m+2*relinkedSubs; got != want {
		t.Fatalf("drift pass = %d trips, want %d: %s", got, want, summarize(trips))
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
	s, rec := newRecordedStore(t)
	if _, err := s.CreateOrConfirm("s1", pullWakeCfg(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Warm the script cache so the first Link's NOSCRIPT -> EVAL self-heal does
	// not count against the steady-state shape.
	if err := s.Link("s1", "events/warm", LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatal(err)
	}
	rec.Take()
	if err := s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatal(err)
	}
	if trips := rec.Take(); len(trips) != 2 || countNamed(trips, "evalsha:control") != 1 || pipelinesLedBy(trips, "sadd") != 1 {
		t.Fatalf("Link = %d trips, want EVALSHA + one SADD/SETBIT pipeline: %s", len(trips), summarize(trips))
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

// TestReconcileIndexPassSurvivesAnUnreadableLinksHash pins the index pass's
// fault isolation: one links hash of the wrong type fails its own HKEYS and is
// reported, and every other subscription in the same chunk still gets its
// dropped fan-out member back. The per-subscription loop repaired only the
// subscriptions it reached before the bad one; a pipelined pass must not do
// worse by failing the whole chunk.
func TestReconcileIndexPassSurvivesAnUnreadableLinksHash(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	const begin = "0000000000000000_0000000000000000"
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for _, id := range ids {
		cfg := Config{Type: DispatchWebhook, WebhookURL: "https://w.example/h", LeaseTTLMs: 1000, Streams: []string{"events/" + id}}
		links := []StreamLink{{Path: "events/" + id, LinkType: LinkExplicit, AckedOffset: begin}}
		if _, err := s.CreateOrConfirm(id, cfg, links, time.Now()); err != nil {
			t.Fatal(err)
		}
		// Drop the fan-out member the pass exists to repair.
		if err := s.client.SRem(ctx, streamSubsKey(slotOf(id), "events/"+id), id).Err(); err != nil {
			t.Fatal(err)
		}
	}
	const broken = "d"
	if err := s.client.Del(ctx, linksKey(broken)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.client.Set(ctx, linksKey(broken), "not-a-hash", 0).Err(); err != nil {
		t.Fatal(err)
	}

	err := s.ReconcileIndexes()
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("ReconcileIndexes should report the unreadable links hash, got %v", err)
	}
	for _, id := range ids {
		member, merr := s.client.SIsMember(ctx, streamSubsKey(slotOf(id), "events/"+id), id).Result()
		if merr != nil {
			t.Fatal(merr)
		}
		if id == broken && member {
			t.Fatalf("subscription %s has no readable links; nothing should have been indexed for it", id)
		}
		if id != broken && !member {
			t.Fatalf("subscription %s lost its repair to another subscription's bad key", id)
		}
	}
	// The next tick reports the same key again and keeps the repairs.
	if err := s.ReconcileIndexes(); err == nil {
		t.Fatal("the unreadable links hash must stay reported until it is fixed")
	}
}

// TestReconcilePatternPassCostSurvivesAnUnreadableSubscription pins that a sub
// hash of the wrong type costs the pattern pass nothing but a log line: the pass
// is the same five pipeline Execs as the healthy one, with no read per listed
// subscription. When the pass fell back to one Get per id on a failed batch
// read, a bad key that stayed bad put the N serial trips the batch removed back
// into every tick: measured on these fixtures, 88 trips here and 1,008 at 1,000
// subscriptions, about 7 s and 80 s at 80 ms a trip against a 30 s interval.
func TestReconcilePatternPassCostSurvivesAnUnreadableSubscription(t *testing.T) {
	s, rec := newRecordedStore(t)
	const n = 83
	streams, tails := seedPatternSubs(t, s, n, 1)
	ctx := context.Background()
	const broken = "sub-0035"
	if err := s.client.Del(ctx, subKey(broken)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.client.Set(ctx, subKey(broken), "not-a-hash", 0).Err(); err != nil {
		t.Fatal(err)
	}
	mgr := newReconcileManager(t, s, streams, tails)

	for pass := 1; pass <= 2; pass++ {
		rec.Take()
		mgr.RunReconcile()
		trips := rec.Take()
		if got := len(trips); got != 5 {
			t.Fatalf("pass %d with one unreadable sub hash = %d trips, want the healthy pass's 5: %s", pass, got, summarize(trips))
		}
		if got := pipelinesLedBy(trips, "hgetall"); got != 1 {
			t.Fatalf("pass %d read subscriptions in %d pipelines, want the one batch and no per-subscription reads: %s", pass, got, summarize(trips))
		}
		if got := len(singleCommands(trips)); got != 0 {
			t.Fatalf("pass %d issued %d single commands, want none: %s", pass, got, summarize(trips))
		}
	}
}

// TestGetManyFallbackIsCountedAndPaidOnce pins the one place a batched read
// still goes serial: a subscription its pipelined HGETALLs did not find. A
// legacy ({__ds}) record costs its migration, the 14 commands and the index
// pipeline of migrateSub plus one re-read, exactly once, because the copy is
// flipped; a listed id with no record under either tag costs one serial HGETALL
// on every call. Neither re-reads what the pipeline already read, and both are
// counted on the Metrics seam by outcome, since the trip count is the only
// other sign that a pass went serial.
func TestGetManyFallbackIsCountedAndPaidOnce(t *testing.T) {
	s, rec := newRecordedStore(t)
	fm := &fakeMetrics{}
	s.WithMetrics(fm)
	ctx := context.Background()
	const n, legacy = 8, 3
	const migrationTrips = 16 // 14 single commands + the index pipeline + the re-read
	seedPatternSubs(t, s, n, 1)
	now := time.Now()
	for i := 0; i < legacy; i++ {
		id := fmt.Sprintf("legacy-%d", i)
		if err := s.client.HSet(ctx, subKeyLegacy(id), map[string]any{
			"id": id, "type": string(DispatchPullWake), "pattern": "events/*", "wake_stream": "wake/pool",
			"lease_ttl_ms": "1000", "status": "active", "phase": "idle", "generation": "0",
			"wake_id": "", "holder": "0", "lease_until_ns": "0", "created_ns": nsArg(now),
		}).Err(); err != nil {
			t.Fatal(err)
		}
		if err := s.client.HSet(ctx, linksKeyLegacy(id), "events/a", "glob:0000000000000000_0000000000000000").Err(); err != nil {
			t.Fatal(err)
		}
		if err := s.client.SAdd(ctx, subsKeyLegacy, id).Err(); err != nil {
			t.Fatal(err)
		}
	}
	// A listed id with no record anywhere: its hashes removed out of band, its
	// id-set membership left behind.
	if err := s.client.Del(ctx, subKey("sub-0000"), linksKey("sub-0000")).Err(); err != nil {
		t.Fatal(err)
	}
	ids, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != n+legacy {
		t.Fatalf("List = %d ids, want %d", len(ids), n+legacy)
	}

	rec.Take()
	subs, err := s.GetMany(ids)
	trips := rec.Take()
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != n-1+legacy {
		t.Fatalf("GetMany returned %d subscriptions, want %d (the migrated ones, not the absent one)", len(subs), n-1+legacy)
	}
	if got, want := len(trips), 1+migrationTrips*legacy+1; got != want {
		t.Fatalf("first GetMany = %d trips, want the batch + %d per legacy record + 1 for the absent id = %d: %s", got, migrationTrips, want, summarize(trips))
	}
	if got := countNamed(trips, "pipe(hgetall+hgetall)"); got != legacy {
		t.Fatalf("re-reads = %d, want one per migrated record (%d), none for the miss the batch already saw: %s", got, legacy, summarize(trips))
	}
	if got := fm.readFallbacks(); got["migrated"] != legacy || got["absent"] != 1 || got["error"] != 0 {
		t.Fatalf("fallbacks after the first call = %v, want migrated=%d absent=1", got, legacy)
	}
	if left, _ := s.client.SCard(ctx, subsKeyLegacy).Result(); left != 0 {
		t.Fatalf("legacy id-set still holds %d ids after the batched read migrated them", left)
	}

	// The next call pays only for the absent id, and says so.
	rec.Take()
	subs, err = s.GetMany(ids)
	trips = rec.Take()
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != n-1+legacy {
		t.Fatalf("second GetMany returned %d subscriptions, want %d", len(subs), n-1+legacy)
	}
	if len(trips) != 2 || countNamed(trips, "hgetall") != 1 {
		t.Fatalf("second GetMany = %d trips, want the batch + one HGETALL for the absent id: %s", len(trips), summarize(trips))
	}
	if got := fm.readFallbacks(); got["migrated"] != legacy || got["absent"] != 2 {
		t.Fatalf("fallbacks after the second call = %v, want migrated=%d absent=2", got, legacy)
	}
}
