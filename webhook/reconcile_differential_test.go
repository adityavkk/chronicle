package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// The batched reconcile must be the serial reconcile with only its transport
// changed: the same reads, the same idempotent writes, the same Link calls in
// the same order, the same wakes. This file keeps the pre-batching code as an
// oracle and proves that, on randomized damaged fixtures, the oracle and the
// shipped code leave Redis byte-identical and make the same Store calls.

// oracleReconcileIndexes is ReconcileIndexes as shipped before batching: one
// HKEYS per subscription, then a serial SADD and SETBIT per link.
func oracleReconcileIndexes(s *RedisStore) error {
	ctx := s.ctx()
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
			h := slotOf(id)
			if err := s.client.SAdd(ctx, streamSubsKey(h, path), id).Err(); err != nil {
				return err
			}
			if err := s.client.SetBit(ctx, streamSlotsKey(path), int64(h), 1).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// oracleReconcilePatternLinks is reconcilePatternLinks as shipped before
// batching: one store.Get per subscription id.
func oracleReconcilePatternLinks(m *Manager) {
	if m.lister == nil {
		return
	}
	ids, err := m.store.List()
	if err != nil {
		return
	}
	streams, err := m.lister.ListStreams()
	if err != nil || len(streams) == 0 {
		return
	}
	begin := m.streams.BeginningOffset()
	for _, id := range ids {
		sub, ok, err := m.store.Get(id)
		if err != nil || !ok || sub.Config.Pattern == "" {
			continue
		}
		linked := make(map[string]struct{}, len(sub.Links))
		for _, l := range sub.Links {
			linked[l.Path] = struct{}{}
		}
		subCreatedNs := sub.CreatedAt.UnixNano()
		relinked := false
		for _, st := range streams {
			if _, ok := linked[st.Path]; ok {
				continue
			}
			if !GlobMatch(sub.Config.Pattern, st.Path) {
				continue
			}
			offset := st.Tail
			if st.CreatedAtNs > subCreatedNs {
				offset = begin
			}
			if err := m.store.Link(id, st.Path, LinkGlob, offset); err != nil {
				m.log.Warn("webhook: reconcile link", "sub", id, "path", st.Path, "error", err)
				continue
			}
			relinked = true
		}
		if relinked {
			m.maybeWake(id, "")
		}
	}
}

// oracleReconcileOnce is reconcileOnce's error precedence over the oracles.
func oracleReconcileOnce(m *Manager, s *RedisStore) {
	if err := oracleReconcileIndexes(s); err != nil {
		m.log.Warn("webhook: reconcile fan-out indexes", "error", err)
	}
	oracleReconcilePatternLinks(m)
}

// recordingStore traces the Store calls whose order and arguments define the
// pattern pass's observable behaviour: every Link, and every wake it arms. The
// wake id is omitted because it is random.
type recordingStore struct {
	Store
	mu    sync.Mutex
	calls []string
}

func (r *recordingStore) record(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recordingStore) Link(id, path string, linkType LinkType, offset string) error {
	r.record(fmt.Sprintf("link %s %s %s %s", id, path, linkType, offset))
	return r.Store.Link(id, path, linkType, offset)
}

func (r *recordingStore) ArmWakeUnscoped(id string, now time.Time, leaseTTLMs int64, armLease bool, wakeID string) (ArmResult, error) {
	r.record("arm " + id)
	return r.Store.ArmWakeUnscoped(id, now, leaseTTLMs, armLease, wakeID)
}

func (r *recordingStore) trace() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.calls, "\n")
}

// ---- randomized fixtures ----

const (
	fixtureBegin = "0000000000000000_0000000000000000"
	fixtureTail  = "0000000000000001_0000000000000010"
)

// Two subscription epochs and three stream creation times, so a missing link is
// relinked at the beginning (stream newer than the subscription), at the tail
// (stream older) and at the tail for a stream with no creation time (0).
var (
	fixtureEpochs         = []time.Time{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)}
	fixtureStreamCreated  = []int64{0, time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), time.Date(2150, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()}
	fixturePatterns       = []string{"events/*", "logs/*", "events/**"}
	fixturePathFamilies   = []string{"events", "logs", "events/deep", "other"}
	fixturePathsPerFamily = 8
)

type listerMode int

const (
	listerNil listerMode = iota
	listerEmpty
	listerStreams
)

func (m listerMode) String() string {
	return [...]string{"lister_nil", "lister_empty", "lister_streams"}[m]
}

// fixture is what a seeded keyspace looks like from the manager's side.
type fixture struct {
	streams []StreamMeta
	tails   map[string]string
}

func fixturePaths() []string {
	paths := make([]string, 0, len(fixturePathFamilies)*fixturePathsPerFamily)
	for _, fam := range fixturePathFamilies {
		for i := 0; i < fixturePathsPerFamily; i++ {
			paths = append(paths, fmt.Sprintf("%s/s%02d", fam, i))
		}
	}
	return paths
}

// seedFixture builds a deterministic keyspace from rng through the real store:
// pattern pull-wake subscriptions, explicit-only webhook subscriptions (never
// relinked, so no delivery goroutine ever starts) and mixed ones, with glob and
// explicit links, then damages it every way recovery must heal — dropped
// fan-out members, cleared or deleted bitmaps, dropped glob links, deleted
// subscriptions whose streams stay listed, ghost ids in the per-slot id-sets
// and legacy-keyspace subscriptions awaiting lazy migration.
func seedFixture(t *testing.T, s *RedisStore, client goredis.UniversalClient, rng *rand.Rand) fixture {
	t.Helper()
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	paths := fixturePaths()
	rng.Shuffle(len(paths), func(i, j int) { paths[i], paths[j] = paths[j], paths[i] })

	fx := fixture{tails: make(map[string]string)}
	for _, p := range paths[:rng.Intn(31)] {
		tail := fixtureBegin
		if rng.Intn(2) == 0 {
			tail = fixtureTail
		}
		fx.streams = append(fx.streams, StreamMeta{Path: p, Tail: tail, CreatedAtNs: fixtureStreamCreated[rng.Intn(3)]})
		fx.tails[p] = tail
	}

	type link struct{ id, path string }
	var subs []string
	var links []link
	for i, n := 0, rng.Intn(25); i < n; i++ {
		id := fmt.Sprintf("sub-%02d", i)
		var cfg Config
		switch rng.Intn(3) {
		case 0, 2: // pattern pull-wake, maybe with explicit links too
			cfg = Config{Type: DispatchPullWake, Pattern: fixturePatterns[rng.Intn(3)], WakeStream: "wake/pool", LeaseTTLMs: 1000}
		case 1: // explicit-only webhook
			cfg = Config{Type: DispatchWebhook, WebhookURL: "https://w.example/h", LeaseTTLMs: 1000}
		}
		if _, err := s.CreateOrConfirm(id, cfg, nil, fixtureEpochs[rng.Intn(2)]); err != nil {
			t.Fatal(err)
		}
		subs = append(subs, id)
		for k := rng.Intn(6); k > 0; k-- {
			path := paths[rng.Intn(len(paths))]
			linkType := LinkExplicit
			if cfg.Pattern != "" && GlobMatch(cfg.Pattern, path) && rng.Intn(4) > 0 {
				linkType = LinkGlob
			}
			offset := fixtureBegin
			if rng.Intn(2) == 0 {
				offset = fixtureTail
			}
			must(s.Link(id, path, linkType, offset))
			links = append(links, link{id, path})
		}
	}

	// Damage the fan-out index and the links (INV-RECOVER-04 / INV-RECOVER-03
	// faults), independently per link; one pipeline, the fixture is built often.
	damage := client.Pipeline()
	for _, l := range links {
		h := slotOf(l.id)
		if rng.Float64() < 0.25 {
			damage.SRem(ctx, streamSubsKey(h, l.path), l.id)
		}
		if rng.Float64() < 0.15 {
			damage.SetBit(ctx, streamSlotsKey(l.path), int64(h), 0)
		}
		if rng.Float64() < 0.05 {
			damage.Del(ctx, streamSlotsKey(l.path))
		}
		if rng.Float64() < 0.25 {
			damage.HDel(ctx, linksKey(l.id), l.path)
			if rng.Intn(2) == 0 {
				damage.SRem(ctx, streamSubsKey(h, l.path), l.id)
			}
		}
	}
	if _, err := damage.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	// Deleted subscriptions whose streams remain, and torn deletes that left an
	// id behind in a per-slot id-set.
	for _, id := range subs {
		if rng.Float64() < 0.1 {
			must(s.Delete(id))
		}
	}
	for g := rng.Intn(3); g > 0; g-- {
		ghost := fmt.Sprintf("ghost-%d", g)
		must(client.SAdd(ctx, subsKey(slotOf(ghost)), ghost).Err())
	}
	// Legacy {__ds} subscriptions: move a surviving subscription back to the
	// pre-slot-homing keyspace, sometimes dropping one of its links on the way.
	for _, id := range subs {
		if rng.Float64() >= 0.15 {
			continue
		}
		fields, err := client.HGetAll(ctx, subKey(id)).Result()
		must(err)
		if len(fields) == 0 {
			continue // deleted above
		}
		h := slotOf(id)
		must(client.HSet(ctx, subKeyLegacy(id), fields).Err())
		linkFields, err := client.HGetAll(ctx, linksKey(id)).Result()
		must(err)
		linkPaths := make([]string, 0, len(linkFields))
		for path := range linkFields {
			linkPaths = append(linkPaths, path)
		}
		sort.Strings(linkPaths) // map order would make the fixture non-deterministic
		dropped := false
		for _, path := range linkPaths {
			must(client.SRem(ctx, streamSubsKey(h, path), id).Err())
			if !dropped && rng.Intn(2) == 0 {
				dropped = true
				continue
			}
			must(client.HSet(ctx, linksKeyLegacy(id), path, linkFields[path]).Err())
			must(client.SAdd(ctx, streamSubsKeyLegacy(path), id).Err())
		}
		must(client.SAdd(ctx, subsKeyLegacy, id).Err())
		must(client.SRem(ctx, subsKey(h), id).Err())
		must(client.Del(ctx, subKey(id), linksKey(id)).Err())
	}
	return fx
}

// ---- canonical keyspace dump ----

// volatileSubFields are the sub-hash fields a wake stamps with a random id or
// the wall clock; everything else, including generation and phase, is compared.
var volatileSubFields = map[string]bool{"wake_id": true, "lease_until_ns": true, "wake_event_sent_ns": true}

// signingKeySingletons are written by NewManager with fresh random key material.
var signingKeySingletons = map[string]bool{
	jwksKey: true, activeKidKey: true, tokenKeyKey: true,
	wakeKeysKey: true, wakeActiveKidKey: true, kidDenylistKey: true,
}

// canonicalDump renders every control-plane key as one sorted line: hashes as
// sorted field=value (minus the volatile wake stamps), sets and sorted sets as
// sorted members (scores are wall-clock stamps), strings raw — which is how a
// bitmap's exact bytes, not just its set bits, enter the comparison.
func canonicalDump(t *testing.T, client goredis.UniversalClient) string {
	t.Helper()
	ctx := context.Background()
	var keys []string
	iter := client.Scan(ctx, 0, "ds:*", 1000).Iterator()
	for iter.Next(ctx) {
		if !signingKeySingletons[iter.Val()] {
			keys = append(keys, iter.Val())
		}
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	// Two pipelines (types, then values) rather than three commands per key: the
	// dump runs a few hundred times per test.
	pipe := client.Pipeline()
	typeCmds := make([]*goredis.StatusCmd, len(keys))
	for i, k := range keys {
		typeCmds[i] = pipe.Type(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	pipe = client.Pipeline()
	valueCmds := make([]goredis.Cmder, len(keys))
	for i, k := range keys {
		switch typ := typeCmds[i].Val(); typ {
		case "hash":
			valueCmds[i] = pipe.HGetAll(ctx, k)
		case "set":
			valueCmds[i] = pipe.SMembers(ctx, k)
		case "zset":
			valueCmds[i] = pipe.ZRange(ctx, k, 0, -1)
		case "string":
			valueCmds[i] = pipe.Get(ctx, k)
		default:
			t.Fatalf("unexpected key type %s for %s", typ, k)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for i, k := range keys {
		var val string
		switch cmd := valueCmds[i].(type) {
		case *goredis.MapStringStringCmd:
			fields := make([]string, 0, len(cmd.Val()))
			for f, v := range cmd.Val() {
				if !volatileSubFields[f] {
					fields = append(fields, f+"="+v)
				}
			}
			sort.Strings(fields)
			val = strings.Join(fields, ";")
		case *goredis.StringSliceCmd:
			members := append([]string(nil), cmd.Val()...)
			sort.Strings(members)
			val = strings.Join(members, ",")
		case *goredis.StringCmd:
			val = fmt.Sprintf("%x", cmd.Val())
		}
		fmt.Fprintf(&b, "%s\t%s\t%s\n", k, typeCmds[i].Val(), val)
	}
	return b.String()
}

func firstDiff(a, b string) string {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := 0; i < max(len(al), len(bl)); i++ {
		var x, y string
		if i < len(al) {
			x = al[i]
		}
		if i < len(bl) {
			y = bl[i]
		}
		if x != y {
			return fmt.Sprintf("line %d:\n  oracle: %s\n  new:    %s", i+1, x, y)
		}
	}
	return "(identical)"
}

// assertIndexCoversLinks is INV-RECOVER-04 checked directly on the final state:
// every slot-homed link (sub, path) has sub in the stream's fan-out shard for
// sub's slot and the stream's occupied bit set for that slot.
func assertIndexCoversLinks(t *testing.T, client goredis.UniversalClient) {
	t.Helper()
	ctx := context.Background()
	var ids []string
	iter := client.Scan(ctx, 0, "ds:{__ds:*}:sub:*:links", 1000).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		ids = append(ids, strings.TrimSuffix(key[strings.Index(key, "}:sub:")+len("}:sub:"):], ":links"))
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	pipe := client.Pipeline()
	linkCmds := make([]*goredis.StringSliceCmd, len(ids))
	for i, id := range ids {
		linkCmds[i] = pipe.HKeys(ctx, linksKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	type entry struct {
		id, path string
		member   *goredis.BoolCmd
		bit      *goredis.IntCmd
	}
	var entries []entry
	pipe = client.Pipeline()
	for i, id := range ids {
		h := slotOf(id)
		for _, path := range linkCmds[i].Val() {
			entries = append(entries, entry{
				id: id, path: path,
				member: pipe.SIsMember(ctx, streamSubsKey(h, path), id),
				bit:    pipe.GetBit(ctx, streamSlotsKey(path), int64(h)),
			})
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.member.Val() {
			t.Fatalf("INV-RECOVER-04: %s links %s but is missing from its fan-out shard", e.id, e.path)
		}
		if e.bit.Val() != 1 {
			t.Fatalf("INV-RECOVER-04: %s links %s but slot %d's occupied bit is clear", e.id, e.path, slotOf(e.id))
		}
	}
}

// snapshotKeyspace DUMPs every key so the same seeded pre-state can be RESTOREd
// byte-for-byte under every pass and lister mode (seeding runs once per seed).
func snapshotKeyspace(t *testing.T, client goredis.UniversalClient) map[string]string {
	t.Helper()
	ctx := context.Background()
	var keys []string
	iter := client.Scan(ctx, 0, "*", 1000).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	pipe := client.Pipeline()
	cmds := make([]*goredis.StringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.Dump(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	snap := make(map[string]string, len(keys))
	for i, k := range keys {
		snap[k] = cmds[i].Val()
	}
	return snap
}

func restoreKeyspace(t *testing.T, client goredis.UniversalClient, snap map[string]string) {
	t.Helper()
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	pipe := client.Pipeline()
	for k, payload := range snap {
		pipe.Restore(ctx, k, 0, payload)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
}

// seedAndSnapshot flushes, seeds the fixture for seed and snapshots the result.
func seedAndSnapshot(t *testing.T, client goredis.UniversalClient, seed int64) (fixture, map[string]string) {
	t.Helper()
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	fx := seedFixture(t, NewRedisStore(client), client, rand.New(rand.NewSource(seed)))
	return fx, snapshotKeyspace(t, client)
}

// reconcileWith builds a manager over a recording store for the current keyspace
// and runs one pass (the oracle's or the shipped one), returning the keyspace
// dump and the Store call trace.
func reconcileWith(t *testing.T, client goredis.UniversalClient, fx fixture, mode listerMode, pass func(m *Manager, s *RedisStore)) (dump, trace string, mgr *Manager) {
	t.Helper()
	s := NewRedisStore(client)
	rec := &recordingStore{Store: s}
	opts := ManagerOptions{StreamRootURL: "http://x/v1/stream/", Logger: slog.New(slog.DiscardHandler)}
	switch mode {
	case listerEmpty:
		opts.Lister = &fakeLister{}
	case listerStreams:
		opts.Lister = &fakeLister{streams: fx.streams}
	case listerNil:
	}
	mgr, err := NewManager(rec, &fakeStreams{tails: fx.tails}, opts)
	if err != nil {
		t.Fatal(err)
	}
	pass(mgr, s)
	return canonicalDump(t, client), rec.trace(), mgr
}

// TestReconcileDifferentialMatchesLegacy is the equivalence proof for the
// batched reconcile: over randomized damaged fixtures under every lister mode,
// the oracle (pre-batching code) and reconcileOnce leave a byte-identical
// keyspace and an identical Store call trace; a second shipped pass is a no-op;
// and the final state satisfies INV-RECOVER-04 directly.
func TestReconcileDifferentialMatchesLegacy(t *testing.T) {
	_, client := newTestStore(t)
	const seeds = 32
	for seed := int64(0); seed < seeds; seed++ {
		// Seeding through the real store is the expensive part: once per seed,
		// then every mode and pass starts from the RESTOREd snapshot.
		fx, snap := seedAndSnapshot(t, client, seed)
		for _, mode := range []listerMode{listerNil, listerEmpty, listerStreams} {
			t.Run(fmt.Sprintf("seed%02d/%s", seed, mode), func(t *testing.T) {
				restoreKeyspace(t, client, snap)
				wantDump, wantTrace, _ := reconcileWith(t, client, fx, mode, oracleReconcileOnce)
				restoreKeyspace(t, client, snap)
				gotDump, gotTrace, mgr := reconcileWith(t, client, fx, mode, func(m *Manager, _ *RedisStore) { m.RunReconcile() })
				if gotDump != wantDump {
					t.Fatalf("keyspace differs from the oracle's at %s", firstDiff(wantDump, gotDump))
				}
				if gotTrace != wantTrace {
					t.Fatalf("Store calls differ from the oracle's:\n--- oracle\n%s\n--- new\n%s", wantTrace, gotTrace)
				}
				mgr.RunReconcile()
				if again := canonicalDump(t, client); again != gotDump {
					t.Fatalf("a second pass must be a no-op, differs at %s", firstDiff(gotDump, again))
				}
				assertIndexCoversLinks(t, client)
			})
		}
	}
}

// TestReconcileIndexesDifferentialOnTornIndex is the index pass alone, against
// the serial oracle, on the same damaged fixtures: the repair is identical, and
// neither pass touches a legacy {__ds} subscription (the index pass reads only
// the slot-homed links hash and never migrates).
func TestReconcileIndexesDifferentialOnTornIndex(t *testing.T) {
	_, client := newTestStore(t)
	legacyLines := func(dump string) (n int) {
		for _, line := range strings.Split(dump, "\n") {
			if strings.HasPrefix(line, keyPrefix+":sub:") || strings.HasPrefix(line, keyPrefix+":subs\t") {
				n++
			}
		}
		return n
	}
	sawLegacy := false
	for seed := int64(100); seed < 116; seed++ {
		_, snap := seedAndSnapshot(t, client, seed)
		before := canonicalDump(t, client)
		if err := oracleReconcileIndexes(NewRedisStore(client)); err != nil {
			t.Fatal(err)
		}
		want := canonicalDump(t, client)
		restoreKeyspace(t, client, snap)
		if err := NewRedisStore(client).ReconcileIndexes(); err != nil {
			t.Fatal(err)
		}
		got := canonicalDump(t, client)
		if got != want {
			t.Fatalf("seed %d: index repair differs from the oracle's at %s", seed, firstDiff(want, got))
		}
		if n := legacyLines(got); n > 0 {
			sawLegacy = true
			if n != legacyLines(before) {
				t.Fatalf("seed %d: the index pass must leave legacy subscriptions alone", seed)
			}
		}
	}
	if !sawLegacy {
		t.Fatal("fixtures never produced a legacy subscription; the untouched-legacy claim was not exercised")
	}
}
