package chronicle

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// claim_verify_test.go pins the claim/verify route (WRITE-FENCING.md §9.1,
// #192) on the Redis fence stack: WF-29 as an agreement between verify and the
// fenced append that follows it, in every state the fence matrix covers; the
// no-side-effects invariant at the byte level of the Redis keyspace; the
// linearization of verify with a deposition; and WF-30's server half — verify
// never renews.

const verifyTarget = "/__ds/subscriptions/s1/claim/verify"

// verify presents token to the claim/verify route as the bearer.
func (s *redisFenceStack) verify(token string) *httptest.ResponseRecorder {
	return s.control(http.MethodPost, verifyTarget, token, "")
}

// mintWriteToken mints a write token under the stack's own key for s1's
// current claim, so a test can shape the credential (expiry, shard) without a
// second claim.
func (s *redisFenceStack) mintWriteToken(t *testing.T, cr webhook.ClaimResponse, shard int, at time.Time, ttl time.Duration) string {
	t.Helper()
	key, err := s.subStore.LoadTokenKey()
	if err != nil {
		t.Fatal(err)
	}
	sub, ok, err := s.subStore.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get s1 = ok:%v err:%v", ok, err)
	}
	tok, err := webhook.GenerateClaimWriteToken(key, "s1", sub.Incarnation, cr.Generation, cr.WakeID, "worker-A", shard,
		verifyScope(t), at, ttl, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// verifyScope is the token scope of s1's single link.
func verifyScope(t *testing.T) []auth.StreamPath {
	t.Helper()
	p, err := auth.NormalizeStreamPath("events/a")
	if err != nil {
		t.Fatal(err)
	}
	return []auth.StreamPath{p}
}

func decodeVerifyBody(t *testing.T, rec *httptest.ResponseRecorder) webhook.ClaimVerifyResponse {
	t.Helper()
	var res webhook.ClaimVerifyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode verify body: %v; raw=%q", err, rec.Body.String())
	}
	return res
}

// depose grants s1 to worker-B at the moment A's lease has lapsed and acks
// as B, the takeover shape TestHandleAppendRejectsDeposedWriteToken uses.
func (s *redisFenceStack) depose(t *testing.T, crA webhook.ClaimResponse) webhook.ClaimResult {
	t.Helper()
	takeoverAt := time.Now().Add(2 * time.Second)
	crB, err := s.subStore.Claim("s1", "worker-B", "w_b", takeoverAt, 1000)
	if err != nil || !crB.Claimed || crB.Generation == crA.Generation {
		t.Fatalf("takeover claim = %+v err=%v", crB, err)
	}
	if st, _ := s.subStore.AckUnscoped("s1", crB.Generation, crB.WakeID, crB.Generation, true, nil, takeoverAt, 1000); st != "OK" {
		t.Fatalf("current holder ack = %q, want OK", st)
	}
	return crB
}

// TestClaimVerifyAgreesWithAppend is WF-29 on Redis: for every credential and
// claim state, the verify status is the status of the fenced append that
// immediately follows under the same token — 200 with 200, 401 with 401, 409
// with 409 — and every 409 is the same envelope (FENCED, reason precheck, the
// same message). No refusal moves the stream tail. The one shape with no
// append counterpart — a token for another subscription — is a separate 401
// pin in the webhook package (the route is per subscription; the append is
// judged against the token's own subscription).
func TestClaimVerifyAgreesWithAppend(t *testing.T) {
	type state struct {
		name  string
		want  int
		token func(t *testing.T, s *redisFenceStack, cr webhook.ClaimResponse) string
	}
	states := []state{
		{"live claim", http.StatusOK, func(_ *testing.T, _ *redisFenceStack, cr webhook.ClaimResponse) string { return cr.WriteToken }},
		{"callback token as bearer", http.StatusUnauthorized, func(_ *testing.T, _ *redisFenceStack, cr webhook.ClaimResponse) string { return cr.Token }},
		{"malformed token", http.StatusUnauthorized, func(*testing.T, *redisFenceStack, webhook.ClaimResponse) string { return "not-a-token" }},
		{"foreign MAC", http.StatusUnauthorized, func(t *testing.T, _ *redisFenceStack, cr webhook.ClaimResponse) string {
			other := make([]byte, 32)
			for i := range other {
				other[i] = byte(0x80 + i)
			}
			tok, err := webhook.GenerateClaimWriteToken(other, "s1", "inc", cr.Generation, cr.WakeID, "worker-A", 0,
				verifyScope(t), time.Now(), time.Minute, rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			return tok
		}},
		{"expired token", http.StatusUnauthorized, func(t *testing.T, s *redisFenceStack, cr webhook.ClaimResponse) string {
			return s.mintWriteToken(t, cr, 0, time.Now().Add(-time.Hour), time.Minute)
		}},
		{"unfenceable shard", http.StatusUnauthorized, func(t *testing.T, s *redisFenceStack, cr webhook.ClaimResponse) string {
			return s.mintWriteToken(t, cr, 1, time.Now(), time.Minute)
		}},
		{"deposed at g+1", http.StatusConflict, func(t *testing.T, s *redisFenceStack, cr webhook.ClaimResponse) string {
			s.depose(t, cr)
			return cr.WriteToken
		}},
		{"released by done", http.StatusConflict, func(t *testing.T, s *redisFenceStack, cr webhook.ClaimResponse) string {
			s.done(t, cr, "/events/a")
			return cr.WriteToken
		}},
		{"lapsed lease, no successor", http.StatusConflict, func(_ *testing.T, _ *redisFenceStack, cr webhook.ClaimResponse) string {
			time.Sleep(1200 * time.Millisecond) // the 1 s lease lapses inside the token's 5 s grace
			return cr.WriteToken
		}},
		{"deleted subscription", http.StatusConflict, func(t *testing.T, s *redisFenceStack, cr webhook.ClaimResponse) string {
			if err := s.subStore.Delete("s1"); err != nil {
				t.Fatal(err)
			}
			return cr.WriteToken
		}},
	}
	for _, st := range states {
		t.Run(st.name, func(t *testing.T) {
			s := newRedisFenceStack(t)
			s.createFenced(t, "/events/a")
			cr := claimForWriteFence(t, s.rt, s.subStore)
			token := st.token(t, s, cr)
			before := tailOf(t, s.h, "/events/a")

			v := s.verify(token)
			a := s.fencedAppend("/events/a", token, "entity-verify", cr.Generation, 0)
			if v.Code != st.want || a.Code != st.want {
				t.Fatalf("verify = %d %s; append = %d %s; want both %d", v.Code, v.Body.String(), a.Code, a.Body.String(), st.want)
			}
			if cc := v.Header().Get("Cache-Control"); cc != "no-store" {
				t.Fatalf("verify Cache-Control = %q, want no-store", cc)
			}
			switch st.want {
			case http.StatusOK:
				sub, ok, err := s.subStore.Get("s1")
				if err != nil || !ok {
					t.Fatalf("get s1 = ok:%v err:%v", ok, err)
				}
				want := webhook.ClaimVerifyResponse{
					Generation: cr.Generation, WakeID: cr.WakeID, Holder: "worker-A",
					Streams: []string{"events/a"}, LeaseUntilMs: sub.LeaseUntilNs / int64(time.Millisecond),
				}
				if got := decodeVerifyBody(t, v); !reflect.DeepEqual(got, want) {
					t.Fatalf("verify body = %+v, want %+v", got, want)
				}
			case http.StatusConflict:
				ve, ae := decodeEnvelope(t, v), decodeEnvelope(t, a)
				want := webhook.ErrorDetail{Code: webhook.ErrCodeFenced, Message: "write token claim is fenced", Reason: webhook.FenceReasonPrecheck}
				if ve.Error != want || ae.Error != want {
					t.Fatalf("verify envelope = %+v; append envelope = %+v; want both %+v", ve.Error, ae.Error, want)
				}
			}
			if st.want != http.StatusOK {
				if after := tailOf(t, s.h, "/events/a"); !after.Equal(before) {
					t.Fatalf("refused append moved the tail %s -> %s", before, after)
				}
			}
		})
	}
}

// fenceStackClient opens a client on the fence stack's Redis database for
// keyspace snapshots. It mirrors newRedisFenceStack's URL resolution.
func fenceStackClient(t *testing.T) *goredis.Client {
	t.Helper()
	rawURL := os.Getenv("CHRONICLE_ITEST_REDIS_URL")
	if rawURL == "" {
		rawURL = "redis://localhost:6379/13"
	}
	options, err := goredis.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	client := goredis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// redisSnapshot dumps every key of the database by type. It compares values
// for TTL-bearing keys too, while deliberately ignoring the TTL itself. This
// makes marker grants or lease-field renewal visible without treating ordinary
// clock-driven expiry countdown as a route side effect.
func redisSnapshot(t *testing.T, c *goredis.Client) map[string]any {
	t.Helper()
	ctx := context.Background()
	keys, err := c.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]any, len(keys))
	for _, k := range keys {
		typ, err := c.Type(ctx, k).Result()
		if err != nil {
			t.Fatal(err)
		}
		var v any
		switch typ {
		case "hash":
			v, err = c.HGetAll(ctx, k).Result()
		case "zset":
			v, err = c.ZRangeWithScores(ctx, k, 0, -1).Result()
		case "set":
			var members []string
			members, err = c.SMembers(ctx, k).Result()
			sort.Strings(members)
			v = members
		case "string":
			v, err = c.Get(ctx, k).Result()
		case "list":
			v, err = c.LRange(ctx, k, 0, -1).Result()
		case "stream":
			v, err = c.XRange(ctx, k, "-", "+").Result()
		default:
			v = typ
		}
		if err != nil {
			t.Fatalf("snapshot %s (%s): %v", k, typ, err)
		}
		out[k] = v
	}
	return out
}

// TestClaimVerifyHasNoSideEffects pins the invariant behind WF-29's "without a
// write" at the byte level: the whole Redis keyspace — the subscription hash
// (phase, generation, wake_id, holder, lease_until_ns, retry bookkeeping,
// links), the stream slot with its marker and seal state, every schedule set
// — and the hydrated Subscription are identical before and after fifty
// verifies, in the live, expired, and deposed states; and the expired answer
// carries no minted token.
func TestClaimVerifyHasNoSideEffects(t *testing.T) {
	s := newRedisFenceStack(t)
	client := fenceStackClient(t)
	s.createFenced(t, "/events/a")
	cr := claimForWriteFence(t, s.rt, s.subStore)
	expired := s.mintWriteToken(t, cr, 0, time.Now().Add(-time.Hour), time.Minute)

	check := func(label, token string, want int) {
		t.Helper()
		subBefore, ok, err := s.subStore.Get("s1")
		if err != nil || !ok {
			t.Fatalf("%s: get s1 = ok:%v err:%v", label, ok, err)
		}
		before := redisSnapshot(t, client)
		for i := 0; i < 50; i++ {
			rec := s.verify(token)
			if rec.Code != want {
				t.Fatalf("%s: verify #%d = %d %s, want %d", label, i, rec.Code, rec.Body.String(), want)
			}
			if want == http.StatusUnauthorized {
				if eb := decodeEnvelope(t, rec); eb.Error.Code != webhook.ErrCodeTokenExpired || eb.Token != "" {
					t.Fatalf("%s: expired answer = %+v, want bare TOKEN_EXPIRED", label, eb)
				}
			}
		}
		subAfter, ok, err := s.subStore.Get("s1")
		if err != nil || !ok {
			t.Fatalf("%s: get s1 after = ok:%v err:%v", label, ok, err)
		}
		if !reflect.DeepEqual(subBefore, subAfter) {
			t.Fatalf("%s: verify changed the subscription:\nbefore %+v\nafter  %+v", label, subBefore, subAfter)
		}
		if after := redisSnapshot(t, client); !reflect.DeepEqual(before, after) {
			t.Fatalf("%s: verify changed the keyspace:\nbefore %v\nafter  %v", label, before, after)
		}
	}
	check("live", cr.WriteToken, http.StatusOK)
	check("expired", expired, http.StatusUnauthorized)
	s.depose(t, cr)
	check("deposed", cr.WriteToken, http.StatusConflict)
}

// TestClaimVerifyLinearizedWithDeposition pins the linearizability clause of
// WF-29: while goroutines hammer verify with A's token, worker-B's takeover
// commits; no verify that started after the takeover returned answers 200 for
// A's token, every 200 names A's generation and A's lease deadline, and
// answers never flip back. TestVerifyClaimUsesOneAtomicStoreRead separately
// pins that the decision and lease come from the one slot-homed EVAL.
func TestClaimVerifyLinearizedWithDeposition(t *testing.T) {
	s := newRedisFenceStack(t)
	s.createFenced(t, "/events/a")
	cr := claimForWriteFence(t, s.rt, s.subStore)
	subA, ok, err := s.subStore.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get s1 = ok:%v err=%v", ok, err)
	}
	wantLeaseUntilMs := subA.LeaseUntilNs / int64(time.Millisecond)

	type sample struct {
		before, after time.Time
		code          int
		generation    int64
		leaseUntilMs  int64
	}
	const workers = 8
	samples := make([][]sample, workers)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				before := time.Now()
				rec := s.verify(cr.WriteToken)
				sm := sample{before: before, after: time.Now(), code: rec.Code}
				if rec.Code == http.StatusOK {
					var res webhook.ClaimVerifyResponse
					if err := json.Unmarshal(rec.Body.Bytes(), &res); err == nil {
						sm.generation = res.Generation
						sm.leaseUntilMs = res.LeaseUntilMs
					}
				}
				samples[i] = append(samples[i], sm)
			}
		}(i)
	}
	time.Sleep(100 * time.Millisecond)
	takeoverAt := time.Now().Add(2 * time.Second)
	crB, err := s.subStore.Claim("s1", "worker-B", "w_b", takeoverAt, 1000)
	committed := time.Now()
	if err != nil || !crB.Claimed || crB.Generation == cr.Generation {
		t.Fatalf("takeover claim = %+v err=%v", crB, err)
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	var okBefore, fencedAfter int
	for i, ss := range samples {
		seenFenced := false
		for _, sm := range ss {
			switch sm.code {
			case http.StatusOK:
				if sm.before.After(committed) {
					t.Fatalf("worker %d: verify started %s after the takeover committed but answered 200", i, sm.before.Sub(committed))
				}
				if sm.generation != cr.Generation {
					t.Fatalf("worker %d: 200 named generation %d, want A's %d", i, sm.generation, cr.Generation)
				}
				if sm.leaseUntilMs != wantLeaseUntilMs {
					t.Fatalf("worker %d: 200 named lease %d, want A's %d", i, sm.leaseUntilMs, wantLeaseUntilMs)
				}
				if seenFenced {
					t.Fatalf("worker %d: a 200 after a 409 — answers must not flip back", i)
				}
				okBefore++
			case http.StatusConflict:
				seenFenced = true
				if sm.before.After(committed) {
					fencedAfter++
				}
			default:
				t.Fatalf("worker %d: unexpected verify status %d", i, sm.code)
			}
		}
	}
	if okBefore == 0 || fencedAfter == 0 {
		t.Fatalf("inconclusive race: %d live answers before, %d fenced answers after the takeover", okBefore, fencedAfter)
	}
}

// TestClaimVerifyNeverRenews is WF-30's server half: polled across a 1 s
// lease, every 200 reports the same lease_until_ms — the deadline the claim
// was granted, unchanged by the polling — and the answer becomes 409 once the
// lease lapses and stays there. A heartbeat at this cadence would have kept
// the claim live; verify extends nothing.
func TestClaimVerifyNeverRenews(t *testing.T) {
	s := newRedisFenceStack(t)
	s.createFenced(t, "/events/a")
	cr := claimForWriteFence(t, s.rt, s.subStore)
	sub, ok, err := s.subStore.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get s1 = ok:%v err:%v", ok, err)
	}
	wantLease := sub.LeaseUntilNs / int64(time.Millisecond)

	var live, fenced int
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		rec := s.verify(cr.WriteToken)
		switch rec.Code {
		case http.StatusOK:
			if fenced > 0 {
				t.Fatal("a 200 after a 409: the lapsed claim came back")
			}
			if got := decodeVerifyBody(t, rec).LeaseUntilMs; got != wantLease {
				t.Fatalf("lease_until_ms = %d, want the granted %d (verify must not renew)", got, wantLease)
			}
			live++
		case http.StatusConflict:
			if eb := decodeEnvelope(t, rec); eb.Error.Reason != webhook.FenceReasonPrecheck {
				t.Fatalf("lapsed answer = %+v, want reason precheck", eb)
			}
			fenced++
		default:
			t.Fatalf("verify = %d %s", rec.Code, rec.Body.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if live == 0 || fenced == 0 {
		t.Fatalf("inconclusive: %d live and %d fenced answers across the lease", live, fenced)
	}
	if after, _, _ := s.subStore.Get("s1"); after.LeaseUntilNs != sub.LeaseUntilNs {
		t.Fatalf("lease_until_ns moved %d -> %d under verify polling", sub.LeaseUntilNs, after.LeaseUntilNs)
	}
}

// TestClaimVerifyDoesNotConsultServiceIdentity pins the deliberate boundary of
// §9.1: a named write token is verified on its own, while the same request's
// rejected service bearer remains terminal on the append path's phase-1 gate.
func TestClaimVerifyDoesNotConsultServiceIdentity(t *testing.T) {
	s := newRedisFenceStack(t)
	s.createFenced(t, "/events/a")
	cr := claimForWriteFence(t, s.rt, s.subStore)

	verifyReq := httptest.NewRequest(http.MethodPost, verifyTarget, nil)
	verifyReq.Header.Set(WriteTokenHeader, cr.WriteToken)
	verifyReq.Header.Set(tb4XFCCHdr, "URI="+tb4OtherID)
	verifyRec := httptest.NewRecorder()
	if !s.rt.HandleRequest(verifyRec, verifyReq) || verifyRec.Code != http.StatusOK {
		t.Fatalf("verify with rejected service bearer = %d %s, want 200", verifyRec.Code, verifyRec.Body.String())
	}

	appendRec := do(s.h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":   "application/json",
		WriteTokenHeader: cr.WriteToken,
		tb4XFCCHdr:       "URI=" + tb4OtherID,
		"Producer-Id":    "entity-service-boundary",
		"Producer-Epoch": strconv.FormatInt(cr.Generation, 10),
		"Producer-Seq":   "0",
	}, []byte(`{"seq":0}`))
	if appendRec.Code != http.StatusUnauthorized {
		t.Fatalf("append with rejected service bearer = %d %s, want 401", appendRec.Code, appendRec.Body.String())
	}
}
