package webhook

import (
	"hash/fnv"
	"net"
	"strings"
	"testing"
	"time"
)

// redisCRC16 is the CCITT/XMODEM CRC16 Redis Cluster uses to map a key (or its
// {hash tag}) to one of 16384 slots — table-free, so the guard test depends on no
// internal go-redis package. It is the cluster's authority on "same slot".
func redisCRC16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc ^= uint16(s[i]) << 8
		for b := 0; b < 8; b++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// clusterSlot is the Redis Cluster slot of a key: CRC16 of the hash tag (the
// substring between the first '{' and the next '}' when that span is non-empty),
// else the whole key, mod 16384. This is exactly how a real cluster routes a key,
// so two keys share a slot iff this agrees — the only thing that makes a multi-key
// Lua script single-slot.
func clusterSlot(key string) int {
	if i := strings.IndexByte(key, '{'); i >= 0 {
		if j := strings.IndexByte(key[i+1:], '}'); j > 0 {
			key = key[i+1 : i+1+j]
		}
	}
	return int(redisCRC16(key) % 16384)
}

func fnv32aMod(s string, m int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % uint32(m))
}

// TestSlotHomingGuard is the T5 STATIC PRECONDITION (issue #15 guard test): every
// key a single subscription touches — its config/runtime hash, links hash, each
// claim-granularity shard hash (#11), and its per-slot lease/retry/due/id-set/fan-out
// keys — resolves to ONE Redis cluster slot, so ack.lua (4-5 keys), delete_sub.lua
// (5 keys), arm_wake, and claim stay byte-for-byte single-slot. It also proves the
// slot index is FNV-1a (NOT CRC16), that a g>0 shard member resolves back to its
// shard hash, and that a deliberately mis-tagged sub is DETECTED (lands in a
// different cluster slot — CROSSSLOT, not silent).
func TestSlotHomingGuard(t *testing.T) {
	const id = "sub-with-{braces}-and-:colons"
	h := slotOf(id)

	// The slot is the FNV-1a/32 of the (g-suffix-stripped) id mod S — an independent,
	// language-stable application hash, NOT the Redis CRC16 of the tag.
	if got, want := h, fnv32aMod(id, subSlots); got != want {
		t.Fatalf("slotOf(%q) = %d, want fnv32a%%%d = %d (must be FNV-1a, not CRC16)", id, got, subSlots, want)
	}
	if slotTag(id) != slotTagAt(h) {
		t.Fatalf("slotTag(%q) = %q, want %q", id, slotTag(id), slotTagAt(h))
	}

	// Every per-sub key for one id resolves to the SAME cluster slot.
	keyset := map[string]string{
		"subKey":          subKey(id),
		"linksKey":        linksKey(id),
		"subShardKey(0)":  subShardKey(id, 0),
		"subShardKey(1)":  subShardKey(id, 1),
		"subShardKey(16)": subShardKey(id, 16),
		"leaseZKey":       leaseZKey(h),
		"retryZKey":       retryZKey(h),
		"dueZKey":         dueZKey(h),
		"subsKey":         subsKey(h),
		"streamSubs":      streamSubsKey(h, "events/a"),
	}
	wantSlot := clusterSlot(subKey(id))
	wantTag := slotTagAt(h)
	for name, k := range keyset {
		if got := clusterSlot(k); got != wantSlot {
			t.Errorf("%s = %q: cluster slot %d, want %d (CROSSSLOT — breaks the atomic script)", name, k, got, wantSlot)
		}
		open := strings.IndexByte(k, '{')
		closeb := strings.IndexByte(k, '}')
		if open < 0 || closeb < open || k[open:closeb+1] != wantTag {
			t.Errorf("%s = %q: first hash tag is not %s", name, k, wantTag)
		}
	}

	// A g>0 schedule member resolves back to its shard hash (the lease/retry/due
	// worker drains "<id>:g:<g>" and operates on subShardKey(id,g) in the SAME slot).
	if subKey(shardMember(id, 1)) != subShardKey(id, 1) {
		t.Errorf("subKey(shardMember(id,1)) = %q != subShardKey(id,1) = %q", subKey(shardMember(id, 1)), subShardKey(id, 1))
	}
	if slotOf(shardMember(id, 7)) != h {
		t.Errorf("a g>0 shard member must home to its parent sub's slot %d, got %d", h, slotOf(shardMember(id, 7)))
	}

	// A deliberately mis-tagged sub is DETECTED, not silent: its key lands in a
	// DIFFERENT cluster slot (a real cluster would CROSSSLOT the script).
	misTagged := "ds:" + slotTagAt((h+1)%subSlots) + ":sub:" + id
	if clusterSlot(misTagged) == wantSlot {
		t.Errorf("a mis-tagged sub must land in a different cluster slot than its home (CROSSSLOT detectable)")
	}

	// The cluster-wide singletons share ONE slot among themselves (the fixed {__ds}
	// tag), so get_or_create_key.lua (jwks + active_kid) stays single-slot.
	jw := clusterSlot(jwksKey)
	if clusterSlot(activeKidKey) != jw || clusterSlot(tokenKeyKey) != jw {
		t.Errorf("singleton keys must share one slot: jwks=%d active_kid=%d token=%d",
			jw, clusterSlot(activeKidKey), clusterSlot(tokenKeyKey))
	}
}

// TestSlotOfIsFNVNotCRC16AndStrips proves the slot hash is the independent FNV-1a
// choice (so it disagrees with CRC16 for typical ids — re-using CRC16 would
// re-introduce the CROSSSLOT slot-homing kills) and that the g-suffix strips.
func TestSlotOfIsFNVNotCRC16AndStrips(t *testing.T) {
	disagreements := 0
	for i := 0; i < 64; i++ {
		id := "sub-" + itoa(i)
		if slotOf(id) != fnv32aMod(id, subSlots) {
			t.Fatalf("slotOf(%q) must equal fnv32a%%S", id)
		}
		if slotOf(id) != int(redisCRC16(id))%subSlots {
			disagreements++
		}
		// g-suffix strips to the base id's slot.
		if slotOf(id+":g:5") != slotOf(id) {
			t.Fatalf("slotOf(%q:g:5) must equal slotOf(%q) (g-shards live in the sub's slot)", id, id)
		}
	}
	if disagreements == 0 {
		t.Fatal("slotOf agreed with CRC16 on every id — the FNV-1a choice is not independent")
	}
}

// TestDecodeOccupiedSlots is the pure decode of the per-stream occupied-slots
// bitmap: bits are MSB-first per byte (Redis SETBIT offset semantics), and the
// decoded set is the ascending occupied slot indices.
func TestDecodeOccupiedSlots(t *testing.T) {
	if got := decodeOccupiedSlots("").Slots(); len(got) != 0 {
		t.Fatalf("empty bitmap must decode to no slots, got %v", got)
	}
	// Build a 32-byte (256-bit) buffer with bits 0, 7, 8, 200 set (MSB-first).
	raw := make([]byte, subSlots/8)
	set := func(h int) { raw[h/8] |= 1 << (7 - uint(h%8)) }
	for _, h := range []int{0, 7, 8, 200} {
		set(h)
	}
	got := decodeOccupiedSlots(string(raw)).Slots()
	want := []int{0, 7, 8, 200}
	if len(got) != len(want) {
		t.Fatalf("decoded %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("decoded %v, want %v", got, want)
		}
	}
}

func TestHasPendingWorkFrom(t *testing.T) {
	begin := "0000000000000000_0000000000000000"
	ahead := "0000000000000001_0000000000000000"
	links := []StreamLink{
		{Path: "a", AckedOffset: begin},
		{Path: "b", AckedOffset: ahead},
		{Path: "c", AckedOffset: begin},
	}
	if HasPendingWorkFrom(links, map[string]string{}) {
		t.Fatal("empty tail map must be not-pending (missing streams omitted)")
	}
	if HasPendingWorkFrom(links, map[string]string{"b": ahead}) {
		t.Fatal("tail == acked is not pending")
	}
	if !HasPendingWorkFrom(links, map[string]string{"a": ahead}) {
		t.Fatal("tail > acked must be pending")
	}
	// A path present but not beyond its cursor, alongside a missing one: still not
	// pending — must match the per-link Snapshot decision.
	if HasPendingWorkFrom(links, map[string]string{"c": begin}) {
		t.Fatal("tail == acked with a missing sibling is not pending")
	}
}

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, path string
		want          bool
	}{
		{"events/*", "events/abc", true},
		{"events/*", "events/abc/def", false}, // * is one segment
		{"events/**", "events/abc/def", true}, // ** is zero or more
		{"events/**", "events", true},         // trailing ** matches zero
		{"**", "a/b/c", true},
		{"events/*", "other/abc", false},
		{"a/*/c", "a/b/c", true},
		{"a/*/c", "a/b/d", false},
		{"manual/a", "manual/a", true},
		{"manual/a", "manual/b", false},
	}
	for _, c := range cases {
		if got := GlobMatch(c.pattern, c.path); got != c.want {
			t.Errorf("GlobMatch(%q,%q)=%v want %v", c.pattern, c.path, got, c.want)
		}
	}
}

func TestConfigHashIdempotentAndOrderIndependent(t *testing.T) {
	a := Config{Type: DispatchWebhook, Pattern: "events/*", Streams: []string{"b", "a"}, WebhookURL: "https://x/h", LeaseTTLMs: 1000, Description: "d"}
	b := Config{Type: DispatchWebhook, Pattern: "events/*", Streams: []string{"a", "b", "a"}, WebhookURL: "https://x/h", LeaseTTLMs: 1000, Description: "d"}
	if ConfigHash(a) != ConfigHash(b) {
		t.Fatal("hash must be independent of stream order and duplicates")
	}
	c := a
	c.WebhookURL = "https://x/other"
	if ConfigHash(a) == ConfigHash(c) {
		t.Fatal("different webhook URL must change the hash")
	}
	// lease_ttl_ms is clamped before hashing: 0 and the default collide.
	z := a
	z.LeaseTTLMs = 0
	d := a
	d.LeaseTTLMs = DefaultLeaseTTLMs
	if ConfigHash(z) != ConfigHash(d) {
		t.Fatal("clamped lease TTL must be hashed, so 0 == default")
	}
}

func TestConfigHashFieldBoundaries(t *testing.T) {
	// Length-prefixing must stop "pattern+streams" from colliding with a
	// shifted split of the same bytes.
	a := Config{Type: DispatchWebhook, Pattern: "ab", Streams: []string{"c"}, WebhookURL: "https://x/h"}
	b := Config{Type: DispatchWebhook, Pattern: "a", Streams: []string{"bc"}, WebhookURL: "https://x/h"}
	if ConfigHash(a) == ConfigHash(b) {
		t.Fatal("field boundaries must not be ambiguous")
	}
}

func TestValidateConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"webhook ok", Config{Type: DispatchWebhook, Pattern: "events/*", WebhookURL: "https://x/h"}, false},
		{"pullwake ok", Config{Type: DispatchPullWake, Pattern: "events/*", WakeStream: "wake/pool"}, false},
		{"explicit only ok", Config{Type: DispatchWebhook, Streams: []string{"a"}, WebhookURL: "https://x/h"}, false},
		{"bad type", Config{Type: "sse", Pattern: "events/*", WebhookURL: "https://x/h"}, true},
		{"no pattern or streams", Config{Type: DispatchWebhook, WebhookURL: "https://x/h"}, true},
		{"webhook without url", Config{Type: DispatchWebhook, Pattern: "events/*"}, true},
		{"pullwake without wake_stream", Config{Type: DispatchPullWake, Pattern: "events/*"}, true},
	}
	for _, c := range cases {
		got := ValidateConfig(NormalizeConfig(c.cfg)) != ""
		if got != c.wantErr {
			t.Errorf("%s: got err=%v want %v", c.name, got, c.wantErr)
		}
	}
}

func TestClassifyWebhookURL(t *testing.T) {
	noResolve := func(string) ([]net.IP, error) { return nil, nil }
	cases := []struct {
		name, url string
		want      bool
	}{
		{"loopback http allowed (conformance receiver)", "http://127.0.0.1:54321/webhook", true},
		{"localhost http allowed", "http://localhost:8080/h", true},
		{"rfc1918 rejected", "http://10.0.0.1/hook", false},
		{"rfc1918 192.168 rejected", "https://192.168.1.10/h", false},
		{"link-local metadata rejected", "http://169.254.169.254/latest", false},
		{"public http rejected (needs https)", "http://93.184.216.34/h", false},
		{"public https allowed", "https://93.184.216.34/h", true},
		{"bad scheme rejected", "ftp://example.com/h", false},
	}
	for _, c := range cases {
		got, _ := ClassifyWebhookURL(c.url, noResolve, false)
		if got != c.want {
			t.Errorf("%s: ClassifyWebhookURL(%q)=%v want %v", c.name, c.url, got, c.want)
		}
	}
	// Trusted-network mode accepts private and plain-http targets.
	if ok, _ := ClassifyWebhookURL("http://10.0.0.1/hook", noResolve, true); !ok {
		t.Error("allowPrivate should accept an RFC1918 http target")
	}
	if ok, _ := ClassifyWebhookURL("ftp://10.0.0.1/hook", noResolve, true); ok {
		t.Error("allowPrivate must still reject a non-http(s) scheme")
	}
}

func TestSnapshotPending(t *testing.T) {
	links := []StreamLink{
		{Path: "a", LinkType: LinkGlob, AckedOffset: "0000000000000001_0000000000000010"},
		{Path: "b", LinkType: LinkExplicit, AckedOffset: "-1"},
		{Path: "gone", LinkType: LinkGlob, AckedOffset: "0000000000000001_0000000000000005"},
	}
	tails := map[string]string{
		"a": "0000000000000001_0000000000000010", // equal -> not pending
		"b": "0000000000000001_0000000000000001", // > -1 -> pending
	}
	tailOf := func(p string) (string, bool) { v, ok := tails[p]; return v, ok }
	snap, pending := Snapshot(links, tailOf)
	if !pending {
		t.Fatal("expected pending work on stream b")
	}
	if snap[0].HasPending {
		t.Error("a should not be pending (tail == acked)")
	}
	if !snap[1].HasPending {
		t.Error("b should be pending (tail > -1)")
	}
	if snap[2].HasPending || snap[2].TailOffset != snap[2].AckedOffset {
		t.Error("a vanished stream should report acked as tail and not pending")
	}
}

func TestRetryDelayBounds(t *testing.T) {
	// No jitter: 1s, 2s, 4s, ... capped at 60s.
	if d := RetryDelay(1, 0); d != time.Second {
		t.Errorf("attempt1=%v want 1s", d)
	}
	if d := RetryDelay(2, 0); d != 2*time.Second {
		t.Errorf("attempt2=%v want 2s", d)
	}
	if d := RetryDelay(20, 0); d != 60*time.Second {
		t.Errorf("attempt20=%v want cap 60s", d)
	}
	// Full jitter adds at most 20%.
	if d := RetryDelay(1, 0.999999); d <= time.Second || d > time.Duration(1.2*float64(time.Second))+1 {
		t.Errorf("jittered attempt1=%v want (1s,1.2s]", d)
	}
}

func TestFenceDecision(t *testing.T) {
	sub := Subscription{Generation: 7, WakeID: "w_abc"}
	if FenceDecision(sub, 7, "w_abc", 7) != "" {
		t.Error("matching generation+wake+token should pass")
	}
	if FenceDecision(sub, 6, "w_abc", 7) != ErrCodeFenced {
		t.Error("stale request generation should be fenced")
	}
	if FenceDecision(sub, 7, "w_old", 7) != ErrCodeFenced {
		t.Error("stale wake_id should be fenced")
	}
	if FenceDecision(sub, 7, "w_abc", 6) != ErrCodeFenced {
		t.Error("stale token generation should be fenced")
	}
	if FenceDecision(sub, 7, "", 7) != ErrCodeFenced {
		t.Error("empty wake_id should be fenced")
	}
}

func TestClaimDecision(t *testing.T) {
	now := time.Unix(1000, 0)
	held := Subscription{Phase: PhaseLive, Holder: true, HolderWorker: "worker-1", LeaseUntilNs: now.Add(time.Second).UnixNano()}
	if code, holder := ClaimDecision(held, now); code != ErrCodeAlreadyClaimed || holder != "worker-1" {
		t.Errorf("held lease should be ALREADY_CLAIMED by worker-1, got %q/%q", code, holder)
	}
	expired := held
	expired.LeaseUntilNs = now.Add(-time.Second).UnixNano()
	if code, _ := ClaimDecision(expired, now); code != "" {
		t.Error("expired lease should be claimable")
	}
	idle := Subscription{Phase: PhaseIdle}
	if code, _ := ClaimDecision(idle, now); code != "" {
		t.Error("idle subscription should be claimable")
	}
}

func TestClaimRotatesFence(t *testing.T) {
	// The wake is reused only for the normal first claim of an issued pull-wake
	// event (waking with a wake set); everything else rotates the fence.
	if ClaimRotatesFence(PhaseWaking, "w_x") {
		t.Error("waking first-claim should reuse the in-flight wake, not rotate")
	}
	if !ClaimRotatesFence(PhaseIdle, "") {
		t.Error("idle claim should rotate (mint a fresh fence)")
	}
	if !ClaimRotatesFence(PhaseLive, "w_x") {
		t.Error("expired-lease takeover (phase live past the BUSY guard) must rotate")
	}
	if !ClaimRotatesFence(PhaseWaking, "") {
		t.Error("waking with a cleared wake should rotate")
	}
}

func TestDecideDue(t *testing.T) {
	cases := []struct {
		name       string
		exists     bool
		phase      Phase
		hasPending bool
		want       DueAction
	}{
		{"gone subscription clears the phantom mark", false, PhaseIdle, false, DueClear},
		{"idle with pending work fires a wake", true, PhaseIdle, true, DueFire},
		{"idle with cursor caught up clears the mark", true, PhaseIdle, false, DueClear},
		{"waking coalesces (wake already in flight)", true, PhaseWaking, true, DueSkip},
		{"live coalesces even with pending work", true, PhaseLive, true, DueSkip},
		// A non-idle subscription always skips: the mark is owned by the in-flight
		// wake and clears on its done-ack/release, never by the dueWorker.
		{"waking with no pending still skips", true, PhaseWaking, false, DueSkip},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecideDue(tc.exists, tc.phase, tc.hasPending); got != tc.want {
				t.Errorf("DecideDue(%v,%q,%v) = %v, want %v", tc.exists, tc.phase, tc.hasPending, got, tc.want)
			}
		})
	}
}

func TestDecideLeaseReconcile(t *testing.T) {
	cases := []struct {
		name         string
		phase        Phase
		leaseUntilNs int64
		inLeaseZSet  bool
		want         LeaseReconcile
	}{
		// The L3 fault: a live/waking sub with a lease deadline dropped from the
		// lease ZSET — the lease worker is blind to it, so re-derive the tail.
		{"live, lease set, absent from zset -> stranded", PhaseLive, 1234, false, LeaseStranded},
		{"waking, lease set, absent from zset -> stranded", PhaseWaking, 1234, false, LeaseStranded},
		// Still present in the zset: the lease worker can already see it.
		{"live, lease set, present in zset -> intact", PhaseLive, 1234, true, LeaseIntact},
		{"waking, lease set, present in zset -> intact", PhaseWaking, 1234, true, LeaseIntact},
		// Idle holds no lease, so it is never stranded here (the due-set, not the
		// lease schedule, recovers an idle sub — DecideDue).
		{"idle never strands on the lease schedule", PhaseIdle, 0, false, LeaseIntact},
		{"idle with a stale deadline but no lease still intact", PhaseIdle, 1234, false, LeaseIntact},
		// A live/waking sub with no lease deadline (lease_until_ns==0) is a pull-wake
		// awaiting claim, not a held lease — nothing to restore to the lease ZSET.
		{"waking pull-wake with no lease deadline -> intact", PhaseWaking, 0, false, LeaseIntact},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecideLeaseReconcile(tc.phase, tc.leaseUntilNs, tc.inLeaseZSet); got != tc.want {
				t.Errorf("DecideLeaseReconcile(%q,%d,%v) = %v, want %v",
					tc.phase, tc.leaseUntilNs, tc.inLeaseZSet, got, tc.want)
			}
		})
	}
}

func TestMergeAcksForwardOnly(t *testing.T) {
	links := []StreamLink{{Path: "a", AckedOffset: "0000000000000001_0000000000000010"}}
	// Backward ack ignored.
	got := MergeAcks(links, []Ack{{Stream: "a", Offset: "0000000000000001_0000000000000005"}})
	if got[0].AckedOffset != "0000000000000001_0000000000000010" {
		t.Error("backward ack must be ignored")
	}
	// Forward ack applied.
	got = MergeAcks(links, []Ack{{Stream: "a", Offset: "0000000000000001_0000000000000020"}})
	if got[0].AckedOffset != "0000000000000001_0000000000000020" {
		t.Error("forward ack must advance the cursor")
	}
}

func TestStreamRootURLNormalization(t *testing.T) {
	// A misconfigured --stream-root must never leak a malformed URL like
	// ".../v1/stream__ds/jwks.json" to an external webhook receiver (issue #78):
	// callbackURL and JWKSURL must join to exactly one slash for a base with a
	// missing, single, or doubled trailing slash.
	const wantCallback = "http://h/v1/stream/__ds/subscriptions/sub-1/callback"
	const wantJWKS = "http://h/v1/stream/__ds/jwks.json"
	for _, base := range []string{
		"http://h/v1/stream",   // missing trailing slash
		"http://h/v1/stream/",  // single trailing slash
		"http://h/v1/stream//", // doubled trailing slash
	} {
		m := &Manager{streamRootURL: normalizeStreamRootURL(base)}
		if got := m.callbackURL("sub-1"); got != wantCallback {
			t.Errorf("callbackURL for base %q = %q, want %q", base, got, wantCallback)
		}
		if got := m.JWKSURL(); got != wantJWKS {
			t.Errorf("JWKSURL for base %q = %q, want %q", base, got, wantJWKS)
		}
	}
}
