package webhook

import (
	"net"
	"testing"
	"time"
)

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
		got, _ := ClassifyWebhookURL(c.url, noResolve)
		if got != c.want {
			t.Errorf("%s: ClassifyWebhookURL(%q)=%v want %v", c.name, c.url, got, c.want)
		}
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
