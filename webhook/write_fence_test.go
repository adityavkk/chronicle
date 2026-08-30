package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// write_fence_test.go pins the control-plane half of the write-fence extension
// (#183, design §D): the claim predicates and the derived webhook holder, the
// check_write_fence.lua webhook branch against its Go mirror, and the marker
// lifecycle the Manager drives around a webhook delivery — grant before the
// POST, write_token in the notification, renewal on retry and heartbeat, and
// the seal before the done-ack idles the subscription.

// fenceRecorder is a Streams double that also implements WriteFenceStreams,
// recording every grant, revoke, and seal so a test can assert the marker
// lifecycle around a delivery. grantErr and sealErr inject failures.
type fenceRecorder struct {
	*fakeStreams
	mu       sync.Mutex
	grants   []auth.AppendFence
	seals    []sealCall
	revokes  []string
	grantErr error
	sealErr  error
}

type sealCall struct {
	path  string
	fence auth.AppendFence
}

func newFenceRecorder(tails map[string]string) *fenceRecorder {
	return &fenceRecorder{fakeStreams: &fakeStreams{tails: tails}}
}

func (r *fenceRecorder) GrantAppendFence(_ string, fence auth.AppendFence) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.grantErr != nil {
		return false, r.grantErr
	}
	r.grants = append(r.grants, fence)
	return true, nil
}

func (r *fenceRecorder) RevokeAppendFence(path string, _ auth.AppendFence) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revokes = append(r.revokes, path)
	return nil
}

func (r *fenceRecorder) SealAppendFence(path string, fence auth.AppendFence) (store.SealResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealErr != nil {
		return store.SealResult{}, r.sealErr
	}
	r.seals = append(r.seals, sealCall{path: path, fence: fence})
	return store.SealResult{Outcome: store.SealSealed, Generation: fence.Generation}, nil
}

func (f *fakeMetrics) fenceSealCount(outcome string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fenceSeals[outcome]
}

func (f *fakeMetrics) grantFailCount(site string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.grantFails[site]
}

func (r *fenceRecorder) grantCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.grants)
}

func (r *fenceRecorder) sealCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seals)
}

// captureTransport answers every webhook POST with status/body and records the
// decoded notification plus how many markers had been granted when the POST
// left the Manager — the grant-before-POST ordering under test.
type captureTransport struct {
	rec    *fenceRecorder
	status int
	body   string

	mu           sync.Mutex
	notifs       []WakeNotification
	grantsAtPost []int
}

func (t *captureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var n WakeNotification
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.notifs = append(t.notifs, n)
	t.grantsAtPost = append(t.grantsAtPost, t.rec.grantCount())
	t.mu.Unlock()
	return &http.Response{
		StatusCode: t.status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(t.body)),
	}, nil
}

func (t *captureTransport) notifications() []WakeNotification {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]WakeNotification(nil), t.notifs...)
}

// sealOrderStore records how many seals the fence recorder had seen when the
// auto-ack's done reached ack.lua, pinning seal-before-idle.
type sealOrderStore struct {
	Store
	rec        *fenceRecorder
	mu         sync.Mutex
	sealsAtAck []int
}

func (s *sealOrderStore) AckUnscoped(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64) (string, error) {
	s.mu.Lock()
	s.sealsAtAck = append(s.sealsAtAck, s.rec.sealCount())
	s.mu.Unlock()
	return s.Store.AckUnscoped(id, reqGeneration, reqWakeID, tokenGeneration, done, acks, now, leaseTTLMs)
}

// newWebhookFenceManager builds a Manager over a Redis subscription store, the
// fence recorder, the capturing transport, and a fakeMetrics, with a webhook
// subscription linked to events/a and events/b that both have pending work.
func newWebhookFenceManager(t *testing.T, status int, body string) (*Manager, *RedisStore, *fenceRecorder, *captureTransport, *fakeMetrics) {
	t.Helper()
	base, _ := newTestStore(t)
	rec := newFenceRecorder(map[string]string{
		"events/a": "0000000000000001_0000000000000000",
		"events/b": "0000000000000001_0000000000000000",
	})
	post := &captureTransport{rec: rec, status: status, body: body}
	fm := &fakeMetrics{}
	mgr, err := NewManager(base, rec, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		HTTPClient:    &http.Client{Transport: post},
		Metrics:       fm,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	now := time.Now()
	if _, err := base.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now); err != nil {
		t.Fatalf("create sub: %v", err)
	}
	begin := "0000000000000000_0000000000000000"
	for _, p := range []string{"events/a", "events/b"} {
		if err := base.Link("s1", p, LinkGlob, begin); err != nil {
			t.Fatalf("link %s: %v", p, err)
		}
	}
	return mgr, base, rec, post, fm
}

// armWebhookWake arms a webhook wake on s1 with a one-minute lease.
func armWebhookWake(t *testing.T, base *RedisStore) ArmResult {
	t.Helper()
	arm, err := base.ArmWakeUnscoped("s1", time.Now(), 60_000, true, "w_1")
	if err != nil || !arm.Armed {
		t.Fatalf("arm = %+v err=%v", arm, err)
	}
	return arm
}

// TestClaimLiveByDispatch pins the claim predicates of design §D.1 (#183):
// claimHolds names exactly the claim shapes that own stream-slot markers — a
// pull-wake holder in phase live, or a webhook wake in flight from waking or
// live — and claimLive additionally requires the lease to be unexpired at now.
func TestClaimLiveByDispatch(t *testing.T) {
	now := time.Unix(1_000, 0)
	live, lapsed := now.Add(time.Second).UnixNano(), now.Add(-time.Second).UnixNano()
	sub := func(typ DispatchType, phase Phase, holder bool, wake string, lease int64) Subscription {
		return Subscription{Config: Config{Type: typ}, Phase: phase, Holder: holder, WakeID: wake, LeaseUntilNs: lease}
	}
	cases := []struct {
		name  string
		sub   Subscription
		holds bool
		live  bool
	}{
		{"pull-wake live holder", sub(DispatchPullWake, PhaseLive, true, "w", live), true, true},
		{"pull-wake lease lapsed", sub(DispatchPullWake, PhaseLive, true, "w", lapsed), true, false},
		{"pull-wake lease equals now", sub(DispatchPullWake, PhaseLive, true, "w", now.UnixNano()), true, false},
		{"pull-wake waking, unclaimed", sub(DispatchPullWake, PhaseWaking, false, "w", live), false, false},
		{"pull-wake live without holder", sub(DispatchPullWake, PhaseLive, false, "w", live), false, false},
		{"pull-wake idle", sub(DispatchPullWake, PhaseIdle, false, "", 0), false, false},
		{"pull-wake empty wake", sub(DispatchPullWake, PhaseLive, true, "", live), false, false},
		{"webhook waking", sub(DispatchWebhook, PhaseWaking, false, "w", live), true, true},
		{"webhook live", sub(DispatchWebhook, PhaseLive, false, "w", live), true, true},
		{"webhook lease lapsed", sub(DispatchWebhook, PhaseWaking, false, "w", lapsed), true, false},
		{"webhook idle", sub(DispatchWebhook, PhaseIdle, false, "", 0), false, false},
		{"webhook holder flag set", sub(DispatchWebhook, PhaseLive, true, "w", live), false, false},
		{"webhook empty wake", sub(DispatchWebhook, PhaseWaking, false, "", live), false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := claimHolds(tc.sub); got != tc.holds {
				t.Errorf("claimHolds = %v, want %v", got, tc.holds)
			}
			if got := claimLive(tc.sub, now); got != tc.live {
				t.Errorf("claimLive = %v, want %v", got, tc.live)
			}
		})
	}
}

// TestWriteFenceDecisionWebhookHolder pins the Go mirror of
// check_write_fence.lua (#183, design §C.5): a write token proceeds only when
// the claim is live inside its lease, names the current generation and wake,
// and carries the claim's holder — the worker for pull-wake, WebhookHolder for
// webhook — so a worker-style holder never authorizes a webhook wake and the
// derived holder never authorizes a pull-wake claim.
func TestWriteFenceDecisionWebhookHolder(t *testing.T) {
	now := time.Unix(1_000, 0)
	live, lapsed := now.Add(time.Second).UnixNano(), now.Add(-time.Second).UnixNano()
	sub := func(typ DispatchType, phase Phase, holder bool, lease int64) Subscription {
		return Subscription{
			Config: Config{Type: typ}, Phase: phase, Holder: holder,
			Generation: 7, WakeID: "w_7", HolderWorker: "worker-A", LeaseUntilNs: lease,
		}
	}
	webhook := sub(DispatchWebhook, PhaseWaking, false, live)
	pull := sub(DispatchPullWake, PhaseLive, true, live)
	cases := []struct {
		name   string
		sub    Subscription
		gen    int64
		wake   string
		holder string
		want   string
	}{
		{"webhook waking, derived holder", webhook, 7, "w_7", WebhookHolder("w_7"), ""},
		{"webhook live, derived holder", sub(DispatchWebhook, PhaseLive, false, live), 7, "w_7", "wake:w_7", ""},
		{"webhook worker-style holder", webhook, 7, "w_7", "worker-A", ErrCodeFenced},
		{"webhook empty holder", webhook, 7, "w_7", "", ErrCodeFenced},
		{"webhook another wake's holder", webhook, 7, "w_7", WebhookHolder("w_6"), ErrCodeFenced},
		{"webhook stale generation", webhook, 6, "w_7", WebhookHolder("w_7"), ErrCodeFenced},
		{"webhook stale wake", webhook, 7, "w_6", WebhookHolder("w_6"), ErrCodeFenced},
		{"webhook empty wake", webhook, 7, "", WebhookHolder(""), ErrCodeFenced},
		{"webhook idle", sub(DispatchWebhook, PhaseIdle, false, live), 7, "w_7", WebhookHolder("w_7"), ErrCodeFenced},
		{"webhook holder flag set", sub(DispatchWebhook, PhaseWaking, true, live), 7, "w_7", WebhookHolder("w_7"), ErrCodeFenced},
		{"webhook lease lapsed", sub(DispatchWebhook, PhaseWaking, false, lapsed), 7, "w_7", WebhookHolder("w_7"), ErrCodeFenced},
		{"webhook lease equals now", sub(DispatchWebhook, PhaseWaking, false, now.UnixNano()), 7, "w_7", WebhookHolder("w_7"), ErrCodeFenced},
		{"pull-wake live holder, worker", pull, 7, "w_7", "worker-A", ""},
		{"pull-wake derived holder refused", pull, 7, "w_7", WebhookHolder("w_7"), ErrCodeFenced},
		{"pull-wake other worker", pull, 7, "w_7", "worker-B", ErrCodeFenced},
		{"pull-wake waking, unclaimed", sub(DispatchPullWake, PhaseWaking, false, live), 7, "w_7", "worker-A", ErrCodeFenced},
		{"pull-wake live without holder flag", sub(DispatchPullWake, PhaseLive, false, live), 7, "w_7", "worker-A", ErrCodeFenced},
		{"pull-wake lease lapsed", sub(DispatchPullWake, PhaseLive, true, lapsed), 7, "w_7", "worker-A", ErrCodeFenced},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := WriteFenceDecision(tc.sub, tc.gen, tc.wake, tc.holder, now); got != tc.want {
				t.Errorf("WriteFenceDecision = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCheckWriteFenceWebhookBranch is the live differential for the
// check_write_fence.lua webhook branch (#183, design §C.5): over a generated
// domain of dispatch type × phase × holder flag × lease × (generation, wake,
// holder) request tuples, the Go mirror WriteFenceDecision and the shipped
// script agree on every tuple, and both agree with an independent restatement
// of the rule so a both-sides-wrong drift is still caught. State is seeded by
// HSET on the subscription hash the script reads and hydrated through Get, so
// the Go side sees exactly what the Lua side sees.
func TestCheckWriteFenceWebhookBranch(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	now0 := time.Now()
	if _, err := s.CreateOrConfirm("wh", webhookCfg("https://w.example/h"), nil, now0); err != nil {
		t.Fatalf("create webhook sub: %v", err)
	}
	if _, err := s.CreateOrConfirm("pw", pullWakeCfg(), nil, now0); err != nil {
		t.Fatalf("create pull-wake sub: %v", err)
	}
	wakes := []string{"", "w_a", "w_b"}

	rapid.Check(t, func(rt *rapid.T) {
		id := rapid.SampledFrom([]string{"wh", "pw"}).Draw(rt, "sub")
		phase := rapid.SampledFrom([]string{"idle", "waking", "live"}).Draw(rt, "phase")
		holderFlag := rapid.SampledFrom([]string{"0", "1"}).Draw(rt, "holderFlag")
		gen := rapid.Int64Range(0, 3).Draw(rt, "gen")
		wake := rapid.SampledFrom(wakes).Draw(rt, "wake")
		worker := rapid.SampledFrom([]string{"", "worker-A"}).Draw(rt, "worker")
		// Small clocks keep every nanosecond value exactly representable on both
		// sides (Lua compares doubles), so the <= boundary is pinned honestly.
		nowNs := rapid.Int64Range(1, 1<<40).Draw(rt, "now")
		lease := nowNs + rapid.SampledFrom([]int64{-1_000_000, -1, 0, 1, 1_000_000}).Draw(rt, "leaseDelta")
		if err := client.HSet(ctx, subKey(id),
			"phase", phase, "holder", holderFlag, "holder_worker", worker,
			"generation", strconv.FormatInt(gen, 10), "wake_id", wake,
			"lease_until_ns", strconv.FormatInt(lease, 10)).Err(); err != nil {
			rt.Fatalf("seed sub state: %v", err)
		}
		sub, ok, err := s.Get(id)
		if err != nil || !ok {
			rt.Fatalf("get %s = ok:%v err:%v", id, ok, err)
		}

		reqGen := gen + rapid.SampledFrom([]int64{0, 0, 1, -1}).Draw(rt, "genDelta")
		reqWake := wake
		if rapid.Bool().Draw(rt, "perturbWake") {
			reqWake = rapid.SampledFrom(wakes).Draw(rt, "reqWake")
		}
		reqHolder := rapid.SampledFrom([]string{"", "worker-A", "worker-B", WebhookHolder(wake), "wake:w_a", "wake:w_b"}).Draw(rt, "reqHolder")
		now := time.Unix(0, nowNs)

		goFenced := WriteFenceDecision(sub, reqGen, reqWake, reqHolder, now) == ErrCodeFenced
		status, err := s.CheckWriteFence(id, 0, reqGen, reqWake, reqHolder, now)
		if err != nil || status == "NOSUB" {
			rt.Fatalf("check_write_fence = %q err=%v", status, err)
		}
		luaFenced := status == "FENCED"
		if goFenced != luaFenced {
			rt.Fatalf("divergence on sub=%s phase=%s holder=%s gen=%d wake=%q worker=%q lease-now=%d req=(%d,%q,%q): Go fenced=%v, Lua fenced=%v",
				id, phase, holderFlag, gen, wake, worker, lease-nowNs, reqGen, reqWake, reqHolder, goFenced, luaFenced)
		}
		// Independent restatement of the rule (design §C.5 / ack.lua liveness).
		var wantProceed bool
		if id == "wh" {
			wantProceed = holderFlag == "0" && (phase == "waking" || phase == "live") && reqHolder == "wake:"+wake
		} else {
			wantProceed = holderFlag == "1" && phase == "live" && reqHolder == worker
		}
		wantProceed = wantProceed && lease > nowNs && reqGen == gen && reqWake != "" && reqWake == wake && reqHolder != ""
		if goFenced == wantProceed {
			rt.Fatalf("semantics wrong on sub=%s phase=%s holder=%s gen=%d wake=%q worker=%q lease-now=%d req=(%d,%q,%q): fenced=%v wantProceed=%v",
				id, phase, holderFlag, gen, wake, worker, lease-nowNs, reqGen, reqWake, reqHolder, goFenced, wantProceed)
		}
	})
}

// TestDeliverWebhookMintsWriteTokenAfterGrant pins the webhook half of design
// §D.2 (#183): a delivery installs the wake's marker on every linked stream
// BEFORE the POST leaves, and the notification carries a write_token bound to
// the wake's generation, wake id, and derived holder that the Manager's own
// authorizer accepts through the check_write_fence.lua webhook branch.
func TestDeliverWebhookMintsWriteTokenAfterGrant(t *testing.T) {
	mgr, base, rec, post, fm := newWebhookFenceManager(t, http.StatusOK, `{}`)
	arm := armWebhookWake(t, base)

	mgr.deliverWebhookUnscoped("s1", arm.Generation, arm.WakeID)

	notifs := post.notifications()
	if len(notifs) != 1 {
		t.Fatalf("POST count = %d, want 1", len(notifs))
	}
	if got := post.grantsAtPost[0]; got != 2 {
		t.Fatalf("markers granted before the POST = %d, want 2 (one per linked stream)", got)
	}
	for _, g := range rec.grants {
		if g.Generation != arm.Generation || g.WakeID != arm.WakeID || g.Holder != WebhookHolder(arm.WakeID) || g.LeaseUntilNs <= time.Now().UnixNano() {
			t.Fatalf("granted fence = %+v, want gen %d wake %s holder %s with a live lease", g, arm.Generation, arm.WakeID, WebhookHolder(arm.WakeID))
		}
	}
	n := notifs[0]
	if n.WriteToken == "" {
		t.Fatal("notification missing write_token")
	}
	v := ValidateWriteToken(mgr.tokenKey, n.WriteToken, mustPath(t, "events/a"), time.Now())
	if v.Status != WriteTokenValid || v.Generation != arm.Generation || v.WakeID != arm.WakeID || v.Holder != WebhookHolder(arm.WakeID) {
		t.Fatalf("write token binding = %+v, want gen %d wake %s holder %s", v, arm.Generation, arm.WakeID, WebhookHolder(arm.WakeID))
	}
	d, fence := mgr.WriteAuthorizer().AuthorizeAppendFence(n.WriteToken, mustPath(t, "events/b"), time.Now())
	if !d.Allowed() || fence == nil || fence.Holder != WebhookHolder(arm.WakeID) {
		t.Fatalf("authorizer = allowed:%v fence:%+v detail:%s, want allowed with the derived holder", d.Allowed(), fence, d.Detail())
	}
	if d := mgr.WriteAuthorizer().AuthorizeAppend(n.WriteToken, mustPath(t, "events/c"), time.Now()); d.Allowed() {
		t.Fatal("write token must not authorize a stream outside the notified snapshot")
	}
	if fm.grantFailCount("webhook") != 0 {
		t.Fatalf("grant failures = %d, want 0", fm.grantFailCount("webhook"))
	}
}

// TestDeliverWebhookGrantFailureStillDelivers pins fail-open delivery,
// fail-closed token (#183, A.0 Q6): when the marker grant fails the POST still
// goes out with its callback token, the notification carries no write_token,
// and the failure is counted at the webhook site.
func TestDeliverWebhookGrantFailureStillDelivers(t *testing.T) {
	mgr, base, rec, post, fm := newWebhookFenceManager(t, http.StatusOK, `{}`)
	rec.grantErr = errors.New("stream slot unavailable")
	arm := armWebhookWake(t, base)

	mgr.deliverWebhookUnscoped("s1", arm.Generation, arm.WakeID)

	notifs := post.notifications()
	if len(notifs) != 1 {
		t.Fatalf("POST count = %d, want 1 (delivery fails open)", len(notifs))
	}
	if notifs[0].WriteToken != "" {
		t.Fatal("grant failure must not mint a write_token (token fails closed)")
	}
	if notifs[0].CallbackToken == "" || notifs[0].WakeID != arm.WakeID {
		t.Fatalf("notification lost its base fields: %+v", notifs[0])
	}
	if got := fm.grantFailCount("webhook"); got != 1 {
		t.Fatalf("AppendFenceGrantFailed(webhook) = %d, want 1", got)
	}
	if sub, _, _ := base.Get("s1"); sub.Phase != PhaseWaking {
		t.Fatalf("delivery must leave the wake in flight, got phase %s", sub.Phase)
	}
}

// TestDeliverWebhookStaleWakeMintsNoWriteToken pins the snapshot assertion of
// design §D.2 (#183): a delivery whose (generation, wake_id) no longer matches
// the subscription — a retry racing a newer arm — grants nothing and carries
// no write_token, so a stale POST can never hand out a live capability.
func TestDeliverWebhookStaleWakeMintsNoWriteToken(t *testing.T) {
	cases := []struct {
		name string
		gen  func(ArmResult) int64
		wake func(ArmResult) string
	}{
		{"stale wake id", func(a ArmResult) int64 { return a.Generation }, func(ArmResult) string { return "w_stale" }},
		{"stale generation", func(a ArmResult) int64 { return a.Generation - 1 }, func(a ArmResult) string { return a.WakeID }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mgr, base, rec, post, fm := newWebhookFenceManager(t, http.StatusOK, `{}`)
			arm := armWebhookWake(t, base)

			mgr.deliverWebhookUnscoped("s1", tc.gen(arm), tc.wake(arm))

			if n := post.notifications(); len(n) != 1 || n[0].WriteToken != "" {
				t.Fatalf("notifications = %+v, want one without write_token", n)
			}
			if rec.grantCount() != 0 {
				t.Fatalf("stale delivery granted %d markers, want 0", rec.grantCount())
			}
			if fm.grantFailCount("webhook") != 0 {
				t.Fatal("a stale delivery is not a grant failure")
			}
		})
	}
}

// TestDeliverWebhookRetryRenewsSameGeneration pins the retry row of design
// §D.2 (#183): re-delivering the same wake re-grants the same
// (generation, wake, holder) on every link — a renewal that never shortens the
// marker lease — and mints an equivalent write_token each time.
func TestDeliverWebhookRetryRenewsSameGeneration(t *testing.T) {
	mgr, base, rec, post, _ := newWebhookFenceManager(t, http.StatusOK, `{}`)
	arm := armWebhookWake(t, base)

	mgr.deliverWebhookUnscoped("s1", arm.Generation, arm.WakeID)
	mgr.deliverWebhookUnscoped("s1", arm.Generation, arm.WakeID)

	notifs := post.notifications()
	if len(notifs) != 2 {
		t.Fatalf("POST count = %d, want 2", len(notifs))
	}
	if rec.grantCount() != 4 {
		t.Fatalf("grants = %d, want 4 (two links, granted on each delivery)", rec.grantCount())
	}
	first := rec.grants[0]
	for _, g := range rec.grants {
		if g.Generation != first.Generation || g.WakeID != first.WakeID || g.Holder != first.Holder {
			t.Fatalf("re-grant changed the claim identity: %+v vs %+v", g, first)
		}
		if g.LeaseUntilNs < first.LeaseUntilNs {
			t.Fatalf("re-grant shortened the lease: %d < %d", g.LeaseUntilNs, first.LeaseUntilNs)
		}
	}
	for i, n := range notifs {
		v := ValidateWriteToken(mgr.tokenKey, n.WriteToken, mustPath(t, "events/a"), time.Now())
		if v.Status != WriteTokenValid || v.Generation != arm.Generation || v.WakeID != arm.WakeID || v.Holder != WebhookHolder(arm.WakeID) {
			t.Fatalf("delivery %d write token = %+v, want the wake's binding", i, v)
		}
	}
}

// TestDeliverWebhookAutoAckSealsBeforeIdle pins the auto-ack row of design
// §D.2 and crash window K.5 (#183): a 2xx {done:true} seals every linked
// stream BEFORE ack.lua idles the subscription, and a seal error leaves the
// wake in flight (no ack), so a sealed generation is never reopened.
func TestDeliverWebhookAutoAckSealsBeforeIdle(t *testing.T) {
	t.Run("seals then idles", func(t *testing.T) {
		mgr, base, rec, _, fm := newWebhookFenceManager(t, http.StatusOK, `{"done":true}`)
		order := &sealOrderStore{Store: base, rec: rec}
		mgr.store = order
		arm := armWebhookWake(t, base)

		mgr.deliverWebhookUnscoped("s1", arm.Generation, arm.WakeID)

		if len(order.sealsAtAck) != 1 || order.sealsAtAck[0] != 2 {
			t.Fatalf("seals seen at ack.lua = %v, want [2] (both links sealed first)", order.sealsAtAck)
		}
		for _, sc := range rec.seals {
			if sc.fence.Generation != arm.Generation || sc.fence.WakeID != arm.WakeID || sc.fence.Holder != WebhookHolder(arm.WakeID) {
				t.Fatalf("seal %s = %+v, want the wake's fence", sc.path, sc.fence)
			}
		}
		if got := fm.fenceSealCount("sealed"); got != 2 {
			t.Fatalf("AppendFenceSeal(sealed) = %d, want 2", got)
		}
		if sub, _, _ := base.Get("s1"); sub.Phase != PhaseIdle || sub.WakeID != "" {
			t.Fatalf("done auto-ack must idle the sub, got phase=%s wake=%q", sub.Phase, sub.WakeID)
		}
	})
	t.Run("seal error skips the ack", func(t *testing.T) {
		mgr, base, rec, _, fm := newWebhookFenceManager(t, http.StatusOK, `{"done":true}`)
		rec.sealErr = errors.New("stream slot unavailable")
		arm := armWebhookWake(t, base)

		mgr.deliverWebhookUnscoped("s1", arm.Generation, arm.WakeID)

		if got := fm.fenceSealCount("error"); got != 2 {
			t.Fatalf("AppendFenceSeal(error) = %d, want 2", got)
		}
		if sub, _, _ := base.Get("s1"); sub.Phase == PhaseIdle || sub.WakeID != arm.WakeID {
			t.Fatalf("a failed seal must not idle the sub, got phase=%s wake=%q", sub.Phase, sub.WakeID)
		}
	})
}

// TestWebhookCallbackDoneBodyUnchanged pins the callback rows of design §D.2
// (#183): a webhook heartbeat callback renews the markers and returns a
// write_token bound to the derived holder, while a done callback seals every
// link before ack.lua idles the wake and answers with the byte-identical base
// body {"ok":true,"next_wake":false} the conformance suite deep-equals.
func TestWebhookCallbackDoneBodyUnchanged(t *testing.T) {
	mgr, base, rec, _, fm := newWebhookFenceManager(t, http.StatusOK, `{}`)
	rt := NewRoutes(mgr)
	arm := armWebhookWake(t, base)
	token, err := GenerateToken(mgr.tokenKey, "s1", arm.Generation, time.Now(), time.Hour, randReader)
	if err != nil {
		t.Fatal(err)
	}

	hb := postHeartbeat(t, rt, "s1", token, arm.Generation, arm.WakeID)
	if hb.Code != http.StatusOK {
		t.Fatalf("heartbeat = %d body %q", hb.Code, hb.Body.String())
	}
	var resp AckResponse
	if err := json.Unmarshal(hb.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.WriteToken == "" {
		t.Fatalf("webhook heartbeat must return write_token: %s", hb.Body.String())
	}
	v := ValidateWriteToken(mgr.tokenKey, resp.WriteToken, mustPath(t, "events/a"), time.Now())
	if v.Status != WriteTokenValid || v.Holder != WebhookHolder(arm.WakeID) || v.Generation != arm.Generation {
		t.Fatalf("heartbeat write token = %+v, want the derived holder at gen %d", v, arm.Generation)
	}
	if rec.grantCount() != 2 {
		t.Fatalf("heartbeat grants = %d, want 2 (renewal on every link)", rec.grantCount())
	}

	done := true
	tail := "0000000000000001_0000000000000000"
	body, _ := json.Marshal(CallbackRequest{
		WakeID: arm.WakeID, Generation: arm.Generation, Done: &done,
		Acks: []Ack{{Stream: "events/a", Offset: tail}, {Stream: "events/b", Offset: tail}},
	})
	rec2 := doDS(t, rt, http.MethodPost, subsPrefix+"s1/callback", token, string(body))
	if rec2.Code != http.StatusOK {
		t.Fatalf("done = %d body %q", rec2.Code, rec2.Body.String())
	}
	if got := strings.TrimSpace(rec2.Body.String()); got != `{"ok":true,"next_wake":false}` {
		t.Fatalf("done-ack body = %s, want the base {ok,next_wake} shape", got)
	}
	if rec.sealCount() != 2 || fm.fenceSealCount("sealed") != 2 {
		t.Fatalf("done sealed %d links (metric %d), want 2", rec.sealCount(), fm.fenceSealCount("sealed"))
	}
	if sub, _, _ := base.Get("s1"); sub.Phase != PhaseIdle {
		t.Fatalf("done must idle the sub, got phase %s", sub.Phase)
	}
}
