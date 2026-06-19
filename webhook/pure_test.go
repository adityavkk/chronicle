package webhook

import (
	"bytes"
	"net"
	"testing"
	"time"
)

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
	tierB := a
	tierB.ConsistencyTier = ConsistencyTierB
	if ConfigHash(a) == ConfigHash(tierB) {
		t.Fatal("different consistency tier must change the config hash")
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
		{"unsupported tier C", Config{Type: DispatchWebhook, Pattern: "events/*", WebhookURL: "https://x/h", ConsistencyTier: ConsistencyTierC}, true},
	}
	for _, c := range cases {
		got := ValidateConfig(NormalizeConfig(c.cfg)) != ""
		if got != c.wantErr {
			t.Errorf("%s: got err=%v want %v", c.name, got, c.wantErr)
		}
	}
}

func TestConsistencyTierParsingDefaultingAndPolicy(t *testing.T) {
	unspecified, err := ParseConsistencyTier("")
	if err != nil || unspecified != ConsistencyTierUnspecified {
		t.Fatalf("empty tier parse = %v/%v, want unspecified nil", unspecified, err)
	}
	tierB, err := ParseConsistencyTier("b")
	if err != nil || tierB != ConsistencyTierB {
		t.Fatalf("tier b parse = %v/%v, want B", tierB, err)
	}
	if _, err := ParseConsistencyTier("strong"); err == nil {
		t.Fatal("unknown tier should be rejected")
	}
	if got := NormalizeConsistencyTier(ConsistencyTierUnspecified, ConsistencyTierB); got != ConsistencyTierB {
		t.Fatalf("defaulted tier = %s, want B", got)
	}
	if reason := ValidateConsistencyTier(ConsistencyTierC); reason == "" {
		t.Fatal("tier C must be explicit but unsupported on Redis")
	}
	a, err := fenceDurabilityForTier(ConsistencyTierA)
	if err != nil || a.enabled() {
		t.Fatalf("tier A policy = %+v err=%v, want disabled", a, err)
	}
	b, err := fenceDurabilityForTier(ConsistencyTierB)
	if err != nil || b.waitReplicas != 1 || b.aofLocal != 1 || b.aofReplicas != 1 {
		t.Fatalf("tier B policy = %+v err=%v, want WAIT 1 + WAITAOF 1 1", b, err)
	}
}

func TestConsistencyTierDoesNotGrantLeaseAuthority(t *testing.T) {
	policy, err := fenceDurabilityForTier(ConsistencyTierB)
	if err != nil || !policy.enabled() {
		t.Fatalf("tier B policy unavailable: %+v err=%v", policy, err)
	}
	current := ClaimLeaseState{Generation: 2, WakeID: "w_current"}
	if got := FenceLeaseDecision(current, 1, "w_old", 1); got != ErrCodeFenced {
		t.Fatalf("stale generation with durable write policy = %q, want FENCED", got)
	}
}

func TestParseCreateConfigConsistencyTier(t *testing.T) {
	cfg, reason := ParseCreateConfigWithDefault([]byte(`{
		"type":"webhook",
		"pattern":"events/*",
		"webhook":{"url":"https://x/h"}
	}`), ConsistencyTierB)
	if reason != "" || cfg.ConsistencyTier != ConsistencyTierB {
		t.Fatalf("defaulted create config = tier %s reason %q, want B", cfg.ConsistencyTier, reason)
	}
	cfg, reason = ParseCreateConfig([]byte(`{
		"type":"pull-wake",
		"pattern":"events/*",
		"wake_stream":"wake/pool",
		"consistency_tier":"B"
	}`))
	if reason != "" || cfg.ConsistencyTier != ConsistencyTierB {
		t.Fatalf("explicit create config = tier %s reason %q, want B", cfg.ConsistencyTier, reason)
	}
	if _, reason = ParseCreateConfig([]byte(`{
		"type":"webhook",
		"pattern":"events/*",
		"webhook":{"url":"https://x/h"},
		"consistency_tier":"C"
	}`)); reason == "" {
		t.Fatal("tier C create request should be rejected on Redis")
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

func TestClaimShardDomain(t *testing.T) {
	shard, err := NewClaimShard(15)
	if err != nil {
		t.Fatalf("valid shard rejected: %v", err)
	}
	if shard.Int() != 15 || shard.String() != "15" {
		t.Fatalf("shard rendering = %d/%q, want 15/15", shard.Int(), shard.String())
	}
	for _, n := range []int{-1, ClaimShardCount} {
		if _, err := NewClaimShard(n); err == nil {
			t.Fatalf("invalid shard %d should be rejected", n)
		}
	}
}

func TestLeaseRefMemberRoundTrip(t *testing.T) {
	zero := NewLeaseRef("sub-plain", DefaultClaimShard)
	if zero.Member() != "sub-plain" {
		t.Fatalf("shard zero must keep the legacy ZSET member, got %q", zero.Member())
	}
	parsedZero, err := ParseLeaseMember(zero.Member())
	if err != nil || parsedZero != zero {
		t.Fatalf("parse legacy member = %+v err=%v, want %+v", parsedZero, err, zero)
	}

	shard, _ := NewClaimShard(7)
	ref := NewLeaseRef("sub:with:separators", shard)
	parsed, err := ParseLeaseMember(ref.Member())
	if err != nil {
		t.Fatalf("parse sharded member: %v", err)
	}
	if parsed != ref {
		t.Fatalf("round trip = %+v, want %+v", parsed, ref)
	}
	if _, err := ParseLeaseMember("@shard:not-a-number:abc"); err == nil {
		t.Fatal("malformed sharded member should fail")
	}
}

func TestSubscriptionSlotTagUsesStableFNV1aShard(t *testing.T) {
	if subSlots != 256 {
		t.Fatalf("subSlots = %d, want immutable 256", subSlots)
	}
	if got := slotTag("s1"); got != "{__ds:201}" {
		t.Fatalf("slotTag(s1) = %q, want FNV-1a slot tag {__ds:201}", got)
	}
	if got := slotTag("sub-0"); got != "{__ds:200}" {
		t.Fatalf("slotTag(sub-0) = %q, want high-slot test fixture {__ds:200}", got)
	}
	if got := subKey("s1"); got != "ds:{__ds:201}:sub:s1" {
		t.Fatalf("subKey(s1) = %q", got)
	}
	if got := subscriptionOwnershipSlot("s1").Int(); got != 201 {
		t.Fatalf("subscriptionOwnershipSlot(s1) = %d, want the subscription slot", got)
	}
}

func TestRedisScriptKeySetsUseOneHashTag(t *testing.T) {
	id := "sub:{attempted-escape}"
	h := subscriptionSlot(id)
	slot, _ := NewOwnershipSlot(h)
	ownerSlot := ownershipSlotKey(slot)

	cases := []struct {
		name string
		keys []string
	}{
		{"create_sub", []string{subKey(id), subsKey(h), linksKey(id)}},
		{"link_stream", []string{linksKey(id)}},
		{"unlink_stream", []string{linksKey(id)}},
		{"delete_sub", []string{subKey(id), subsKey(h), linksKey(id), leaseZKey(h), retryZKey(h), dueSetKey(h)}},
		{"claim_shard", []string{ownerSlot}},
		{"check_owner", []string{ownerSlot}},
		{"arm_wake/no_owner", []string{subKey(id), leaseZKey(h), dueSetKey(h), subKey(id)}},
		{"arm_wake/owner_fenced", []string{subKey(id), leaseZKey(h), dueSetKey(h), ownerSlot}},
		{"claim", []string{subKey(id), leaseZKey(h)}},
		{"ack/no_owner", []string{subKey(id), linksKey(id), leaseZKey(h), retryZKey(h), dueSetKey(h), subKey(id)}},
		{"ack/owner_fenced", []string{subKey(id), linksKey(id), leaseZKey(h), retryZKey(h), dueSetKey(h), ownerSlot}},
		{"release/no_owner", []string{subKey(id), leaseZKey(h), retryZKey(h), dueSetKey(h), subKey(id)}},
		{"release/owner_fenced", []string{subKey(id), leaseZKey(h), retryZKey(h), dueSetKey(h), ownerSlot}},
		{"expire_lease/no_owner", []string{subKey(id), leaseZKey(h), dueSetKey(h), subKey(id)}},
		{"expire_lease/owner_fenced", []string{subKey(id), leaseZKey(h), dueSetKey(h), ownerSlot}},
		{"reconcile_lease/no_owner", []string{subKey(id), leaseZKey(h), dueSetKey(h), subKey(id)}},
		{"reconcile_lease/owner_fenced", []string{subKey(id), leaseZKey(h), dueSetKey(h), ownerSlot}},
		{"claim_due/lease/no_owner", []string{leaseZKey(h), leaseZKey(h)}},
		{"claim_due/lease/owner_fenced", []string{leaseZKey(h), ownerSlot}},
		{"claim_due/retry/no_owner", []string{retryZKey(h), retryZKey(h)}},
		{"claim_due/retry/owner_fenced", []string{retryZKey(h), ownerSlot}},
		{"claim_due/due/no_owner", []string{dueSetKey(h), dueSetKey(h)}},
		{"claim_due/due/owner_fenced", []string{dueSetKey(h), ownerSlot}},
		{"schedule_retry/no_owner", []string{subKey(id), retryZKey(h), subKey(id)}},
		{"schedule_retry/owner_fenced", []string{subKey(id), retryZKey(h), ownerSlot}},
		{"record_success", []string{subKey(id), retryZKey(h)}},
		{"record_wake_sent", []string{subKey(id)}},
		{"get_or_create_key", []string{jwksKey, activeKidKey}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.keys) == 0 {
				t.Fatal("script key set is empty")
			}
			want := redisHashTag(tc.keys[0])
			for _, key := range tc.keys[1:] {
				if got := redisHashTag(key); got != want {
					t.Fatalf("mixed hash tags: keys=%v first tag=%q key %q tag=%q", tc.keys, want, key, got)
				}
			}
		})
	}
}

func TestRedisScriptKeySetRejectsMixedHashTags(t *testing.T) {
	if err := validateSingleHashTag([]string{subKeyForSlot("a", 0), subKeyForSlot("a", 1)}); err == nil {
		t.Fatal("mixed subscription slots should be rejected before Redis")
	}
}

func TestFenceLeaseDecisionIsPerShard(t *testing.T) {
	shardA := ClaimLeaseState{Generation: 1, WakeID: "w_a"}
	shardB := ClaimLeaseState{Generation: 1, WakeID: "w_b"}
	if FenceLeaseDecision(shardA, 1, "w_a", 1) != "" {
		t.Fatal("matching shard A fence should pass")
	}
	if FenceLeaseDecision(shardB, 1, "w_a", 1) != ErrCodeFenced {
		t.Fatal("token and request for shard A must not pass shard B's wake fence")
	}
}

func TestClaimShardFiltersPartitionStreams(t *testing.T) {
	snap := []StreamSnapshot{
		{Path: "events/entity-a"},
		{Path: "events/entity-b"},
		{Path: "events/entity-c"},
		{Path: "events/entity-d"},
	}
	seen := map[string]bool{}
	for n := 0; n < ClaimShardCount; n++ {
		shard, _ := NewClaimShard(n)
		for _, s := range FilterSnapshotForClaimShard(snap, shard) {
			if StreamClaimShard(s.Path) != shard {
				t.Fatalf("stream %s returned for wrong shard %d", s.Path, shard.Int())
			}
			if seen[s.Path] {
				t.Fatalf("stream %s appeared in more than one shard", s.Path)
			}
			seen[s.Path] = true
		}
	}
	if len(seen) != len(snap) {
		t.Fatalf("partition covered %d streams, want %d", len(seen), len(snap))
	}
}

func TestReplicaIDDefaultUsesPodNameAndNonce(t *testing.T) {
	got, err := GenerateReplicaID(func(key string) (string, bool) {
		if key == "POD_NAME" {
			return "pod-a", true
		}
		return "", false
	}, bytes.NewReader(bytes.Repeat([]byte{0xab}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != "pod-a-abababababababababababababababab" {
		t.Fatalf("replica id = %q", got)
	}
	if _, err := NewReplicaID(""); err == nil {
		t.Fatal("empty replica id should be rejected")
	}
}

func TestHRWOwnerArgmaxAndTieBreak(t *testing.T) {
	members := []ReplicaID{"replica-a", "replica-b", "replica-c"}
	slot, _ := NewOwnershipSlot(17)
	want := members[0]
	wantScore := hrwScore(want, slot)
	for _, member := range members[1:] {
		score := hrwScore(member, slot)
		if score > wantScore || (score == wantScore && member.String() > want.String()) {
			want, wantScore = member, score
		}
	}
	got, ok := HRWOwner(members, slot)
	if !ok || got != want {
		t.Fatalf("HRWOwner = %q/%v, want %q/true", got, ok, want)
	}

	tieA, _ := NewReplicaID("same")
	tieB, _ := NewReplicaID("same")
	got, ok = HRWOwner([]ReplicaID{tieA, tieB}, slot)
	if !ok || got != tieB {
		t.Fatalf("identical-score tie should choose greatest replica id, got %q", got)
	}
}

func TestHRWAddingReplicaMovesRoughlyOneShare(t *testing.T) {
	slots := ownershipSlots(65536)
	beforeMembers := []ReplicaID{
		"pod-a-00112233445566778899aabbccddeeff",
		"pod-b-ffeeddccbbaa99887766554433221100",
		"pod-c-0123456789abcdef0123456789abcdef",
	}
	afterMembers := append(append([]ReplicaID{}, beforeMembers...), ReplicaID("pod-d-fedcba9876543210fedcba9876543210"))
	moved := 0
	for _, slot := range slots {
		before, _ := HRWOwner(beforeMembers, slot)
		after, _ := HRWOwner(afterMembers, slot)
		if before != after {
			moved++
		}
	}
	ratio := float64(moved) / float64(len(slots))
	if ratio < 0.18 || ratio > 0.32 {
		t.Fatalf("adding one of four replicas moved %.3f of slots, want roughly 0.25", ratio)
	}
}

func TestOwnershipTimingInvariants(t *testing.T) {
	if err := validateOwnershipTiming(9*time.Second, 3*time.Second, 9*time.Second, 3*time.Second); err != nil {
		t.Fatalf("defaults rejected: %v", err)
	}
	if err := validateOwnershipTiming(9*time.Second, 5*time.Second, 9*time.Second, 3*time.Second); err == nil {
		t.Fatal("heartbeat interval >= memberLeaseTTL/2 should be rejected")
	}
	if err := validateOwnershipTiming(9*time.Second, 3*time.Second, 9*time.Second, 4*time.Second); err == nil {
		t.Fatal("slot reconcile interval > heartbeat interval should be rejected")
	}
}

func TestOwnershipSlotRejectsOverflow(t *testing.T) {
	if _, err := NewOwnershipSlot(1 << 16); err == nil {
		t.Fatal("expected ownership slot overflow to be rejected")
	}
}

func TestFilterAcksForClaimShardDropsForeignStreams(t *testing.T) {
	owned := "events/owned"
	shard := StreamClaimShard(owned)
	foreign := "events/foreign"
	for StreamClaimShard(foreign) == shard {
		foreign += "x"
	}
	acks := []Ack{{Stream: owned, Offset: "2"}, {Stream: foreign, Offset: "9"}}
	got := FilterAcksForClaimShard(acks, shard)
	if len(got) != 1 || got[0].Stream != owned {
		t.Fatalf("filtered acks = %+v, want only %s", got, owned)
	}
}

func TestParseRequestClaimShardFencesExplicitShardTokenWithoutRequestShard(t *testing.T) {
	tv := TokenValidation{Valid: true, Generation: 1, Shard: DefaultClaimShard, Sharded: true}
	_, fenced, err := parseRequestClaimShard(nil, tv)
	if err != nil {
		t.Fatal(err)
	}
	if !fenced {
		t.Fatal("explicit shard token must require a shard on ack/release")
	}
}

func TestParseRequestClaimShardAllowsLegacyToken(t *testing.T) {
	tv := TokenValidation{Valid: true, Generation: 1, Shard: DefaultClaimShard}
	selection, fenced, err := parseRequestClaimShard(nil, tv)
	if err != nil {
		t.Fatal(err)
	}
	if selection.Shard != DefaultClaimShard || selection.Sharded() || fenced {
		t.Fatalf("legacy token parse = shard %d sharded=%v fenced=%v", selection.Shard.Int(), selection.Sharded(), fenced)
	}
}

func TestParseRequestClaimShardAllowsExplicitShardZeroToken(t *testing.T) {
	requestShard := 0
	tv := TokenValidation{Valid: true, Generation: 1, Shard: DefaultClaimShard, Sharded: true}
	selection, fenced, err := parseRequestClaimShard(&requestShard, tv)
	if err != nil {
		t.Fatal(err)
	}
	if selection.Shard != DefaultClaimShard || !selection.Sharded() || fenced {
		t.Fatalf("explicit shard-zero parse = shard %d sharded=%v fenced=%v", selection.Shard.Int(), selection.Sharded(), fenced)
	}
}

func TestParseRequestClaimShardFencesLegacyTokenWithExplicitShard(t *testing.T) {
	requestShard := 0
	tv := TokenValidation{Valid: true, Generation: 1, Shard: DefaultClaimShard}
	_, fenced, err := parseRequestClaimShard(&requestShard, tv)
	if err != nil {
		t.Fatal(err)
	}
	if !fenced {
		t.Fatal("legacy token must not become explicit shard zero by adding request shard")
	}
}

func TestParseRequestClaimShardFencesShardMismatch(t *testing.T) {
	tokenShard, _ := NewClaimShard(3)
	requestShard := 4
	tv := TokenValidation{Valid: true, Generation: 1, Shard: tokenShard, Sharded: true}
	_, fenced, err := parseRequestClaimShard(&requestShard, tv)
	if err != nil {
		t.Fatal(err)
	}
	if !fenced {
		t.Fatal("mismatched request shard must be fenced")
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
