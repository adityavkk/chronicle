package webhook

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

const verifyTarget = subsPrefix + "s1/claim/verify"

// doVerify drives the claim/verify route with exactly the given headers (a
// name may repeat) and body, asserting the reserved path was claimed.
func doVerify(t *testing.T, rt *Routes, method string, headers http.Header, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, verifyTarget, strings.NewReader(body))
	for name, values := range headers {
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	rec := httptest.NewRecorder()
	if !rt.HandleRequest(rec, req) {
		t.Fatalf("HandleRequest did not claim %s %s", method, verifyTarget)
	}
	return rec
}

func bearerHeader(token string) http.Header {
	return http.Header{"Authorization": {"Bearer " + token}}
}

// setupLongClaim is setupClaim with a 30 s lease, so a sequence of live
// assertions is not racing the 1 s lease of pullWakeCfg.
func setupLongClaim(t *testing.T, rt *Routes, store *RedisStore, id string) ClaimResponse {
	t.Helper()
	cfg := pullWakeCfg()
	cfg.LeaseTTLMs = 30_000
	if _, err := store.CreateOrConfirm(id, cfg, nil, time.Now()); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.Link(id, "events/a", LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatalf("link: %v", err)
	}
	rec := doDS(t, rt, http.MethodPost, subsPrefix+id+"/claim", "", `{"worker":"w1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("claim status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	var cr ClaimResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &cr); err != nil {
		t.Fatalf("decode claim: %v", err)
	}
	if cr.WriteToken == "" {
		t.Fatalf("claim response missing write_token: %+v", cr)
	}
	return cr
}

func decodeVerify(t *testing.T, rec *httptest.ResponseRecorder) ClaimVerifyResponse {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("verify = %d body %q, want 200", rec.Code, rec.Body.String())
	}
	var res ClaimVerifyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode verify: %v; raw=%q", err, rec.Body.String())
	}
	return res
}

func requireNoStore(t *testing.T, label string, rec *httptest.ResponseRecorder) {
	t.Helper()
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("%s: Cache-Control = %q, want no-store", label, cc)
	}
}

// requireServerRelativeLease pins the WF-30 shape of a 200's lease_remaining_ms
// for a request that ran between before and after: it is bounded by the
// subscription's lease TTL, and it equals lease_until_ms minus the server's
// own now — so it can be no more than the deadline minus the client's time
// before the request, and no less than the deadline minus the client's time
// after it (one millisecond of slack for the floor). It returns the body with
// the field zeroed so the caller can compare the rest exactly.
func requireServerRelativeLease(t *testing.T, label string, got ClaimVerifyResponse, before, after time.Time, leaseTTLMs int64) ClaimVerifyResponse {
	t.Helper()
	hi := got.LeaseUntilMs - before.UnixMilli()
	lo := got.LeaseUntilMs - after.UnixMilli() - 1
	if got.LeaseRemainingMs <= 0 || got.LeaseRemainingMs > leaseTTLMs || got.LeaseRemainingMs > hi || got.LeaseRemainingMs < lo {
		t.Fatalf("%s: lease_remaining_ms = %d, want in (0, %d] and within [%d, %d] of lease_until_ms %d",
			label, got.LeaseRemainingMs, leaseTTLMs, lo, hi, got.LeaseUntilMs)
	}
	got.LeaseRemainingMs = 0
	return got
}

// TestHandleClaimVerifyRoute pins the HTTP surface of claim verification
// (WRITE-FENCING.md §9.1, #192): the token is the sole credential and is read
// from the append gate's carriers with the malformed-carrier rule; a live
// claim answers 200 with the claim facts, the token's scope, and the lease
// deadline the fence hash holds; an unusable token is 401 TOKEN_INVALID; an
// expired one 401 TOKEN_EXPIRED with no refreshed token (unlike the ack route,
// which mints one in band); a superseded or deleted claim is the pre-check's
// 409 FENCED reason precheck, never a 404 — 404 stays the plain-text answer
// for a method or action the route does not serve; and every answer is
// Cache-Control: no-store.
func TestHandleClaimVerifyRoute(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupLongClaim(t, rt, store, "s1")
	sub, ok, err := store.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get s1 = ok:%v err:%v", ok, err)
	}
	want := ClaimVerifyResponse{
		Generation: cr.Generation, WakeID: cr.WakeID, Holder: "w1",
		Streams: []string{"events/a"}, LeaseUntilMs: sub.LeaseUntilNs / int64(time.Millisecond),
	}

	// Live claim over each carrier, bearer last (the append gate's order).
	for _, carrier := range []http.Header{
		bearerHeader(cr.WriteToken),
		{WriteTokenHeader: {cr.WriteToken}},
		{ClaimTokenHeader: {cr.WriteToken}},
		{WriteTokenHeader: {cr.WriteToken}, "Authorization": {"Bearer " + cr.Token}},
	} {
		before := time.Now()
		rec := doVerify(t, rt, http.MethodPost, carrier, "")
		after := time.Now()
		requireNoStore(t, "live", rec)
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("live Content-Type = %q", ct)
		}
		got := requireServerRelativeLease(t, "live", decodeVerify(t, rec), before, after, 30_000)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("live verify over %v = %+v, want %+v", carrier, got, want)
		}
	}
	// A body is ignored, not parsed.
	if rec := doVerify(t, rt, http.MethodPost, bearerHeader(cr.WriteToken), `{"wake_id":"forged","generation":99}`); rec.Code != http.StatusOK {
		t.Fatalf("verify with a body = %d body %q, want 200", rec.Code, rec.Body.String())
	}

	// Presented-but-malformed named carriers never fall through to the bearer.
	for label, carrier := range map[string]http.Header{
		"duplicated Write-Token":     {WriteTokenHeader: {cr.WriteToken, cr.WriteToken}, "Authorization": {"Bearer " + cr.WriteToken}},
		"empty Write-Token":          {WriteTokenHeader: {""}, "Authorization": {"Bearer " + cr.WriteToken}},
		"empty electric-claim-token": {ClaimTokenHeader: {""}, "Authorization": {"Bearer " + cr.WriteToken}},
		"no credential":              {},
		"callback token":             bearerHeader(cr.Token),
		"malformed token":            bearerHeader("not-a-token"),
		"non-bearer authorization":   {"Authorization": {"Basic " + cr.WriteToken}},
	} {
		rec := doVerify(t, rt, http.MethodPost, carrier, "")
		requireNoStore(t, label, rec)
		if rec.Code != http.StatusUnauthorized || errCodeOf(t, rec) != ErrCodeTokenInvalid {
			t.Fatalf("%s = %d %s, want 401 TOKEN_INVALID", label, rec.Code, rec.Body.String())
		}
	}

	// A token the MAC proves ours but that names another subscription or an
	// unfenceable shard is unusable here (401), revealing nothing.
	scope := []auth.StreamPath{mustPath(t, "events/a")}
	mint := func(subID string, shard int, at time.Time) string {
		t.Helper()
		tok, err := GenerateClaimWriteToken(mgr.tokenKey, subID, sub.Incarnation, cr.Generation, cr.WakeID, "w1", shard, scope, at, time.Minute, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	for label, tok := range map[string]string{"another subscription": mint("s2", 0, time.Now()), "shard 1": mint("s1", 1, time.Now())} {
		if rec := doVerify(t, rt, http.MethodPost, bearerHeader(tok), ""); rec.Code != http.StatusUnauthorized || errCodeOf(t, rec) != ErrCodeTokenInvalid {
			t.Fatalf("%s = %d %s, want 401 TOKEN_INVALID", label, rec.Code, rec.Body.String())
		}
	}

	// A MAC-valid legacy write token with no claim binding reaches the shared
	// pre-check arm and preserves its specific refusal text in the 409 envelope.
	unbound, err := GenerateWriteToken(mgr.tokenKey, "s1", cr.Generation, scope, time.Now(), time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	unboundRec := doVerify(t, rt, http.MethodPost, bearerHeader(unbound), "")
	var unboundBody ErrorBody
	if err := json.Unmarshal(unboundRec.Body.Bytes(), &unboundBody); err != nil {
		t.Fatal(err)
	}
	wantUnbound := ErrorBody{Error: ErrorDetail{Code: ErrCodeFenced, Message: "write token is not bound to a live claim", Reason: FenceReasonPrecheck}}
	if unboundRec.Code != http.StatusConflict || !reflect.DeepEqual(unboundBody, wantUnbound) {
		t.Fatalf("unbound verify = %d %s, want 409 %+v", unboundRec.Code, unboundRec.Body.String(), wantUnbound)
	}

	// Expired: TOKEN_EXPIRED and nothing minted — contrast the ack route,
	// whose expired-token answer carries a fresh callback token in band.
	rec := doVerify(t, rt, http.MethodPost, bearerHeader(mint("s1", 0, time.Now().Add(-time.Hour))), "")
	requireNoStore(t, "expired", rec)
	var expired ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &expired); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusUnauthorized || expired.Error.Code != ErrCodeTokenExpired || expired.Token != "" {
		t.Fatalf("expired verify = %d %s, want bare 401 TOKEN_EXPIRED", rec.Code, rec.Body.String())
	}
	staleCallback, err := GenerateToken(mgr.tokenKey, "s1", cr.Generation, time.Now().Add(-2*time.Hour), time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ackRec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/ack", staleCallback, `{"wake_id":"`+cr.WakeID+`","generation":`+strconv.FormatInt(cr.Generation, 10)+`}`)
	var ackBody ErrorBody
	if err := json.Unmarshal(ackRec.Body.Bytes(), &ackBody); err != nil {
		t.Fatal(err)
	}
	if ackRec.Code != http.StatusUnauthorized || ackBody.Error.Code != ErrCodeTokenExpired || ackBody.Token == "" {
		t.Fatalf("ack contrast = %d %s, want 401 TOKEN_EXPIRED with a refreshed token", ackRec.Code, ackRec.Body.String())
	}

	// Wrong method and a longer action keep today's plain-text 404, so 404 is
	// unambiguous as "route absent" to a client that must fall back.
	for _, tc := range []struct{ method, target string }{
		{http.MethodGet, verifyTarget},
		{http.MethodPut, verifyTarget},
		{http.MethodPost, verifyTarget + "/x"},
	} {
		req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(""))
		req.Header.Set("Authorization", "Bearer "+cr.WriteToken)
		notFound := httptest.NewRecorder()
		if !rt.HandleRequest(notFound, req) || notFound.Code != http.StatusNotFound || !strings.HasPrefix(notFound.Header().Get("Content-Type"), "text/plain") {
			t.Fatalf("%s %s = %d %q, want plain-text 404", tc.method, tc.target, notFound.Code, notFound.Header().Get("Content-Type"))
		}
	}

	// Superseded: the append pre-check's 409 — FENCED, reason precheck, the
	// detail as the message, no generation or holder disclosed.
	takeoverAt := time.Now().Add(31 * time.Second)
	crB, err := store.Claim("s1", "w2", "w_b", takeoverAt, 1000)
	if err != nil || !crB.Claimed || crB.Generation == cr.Generation {
		t.Fatalf("takeover = %+v err=%v", crB, err)
	}
	requireFenced := func(label string, rec *httptest.ResponseRecorder) {
		t.Helper()
		requireNoStore(t, label, rec)
		var eb ErrorBody
		if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil {
			t.Fatalf("%s: decode: %v; raw=%q", label, err, rec.Body.String())
		}
		want := ErrorBody{Error: ErrorDetail{Code: ErrCodeFenced, Message: "write token claim is fenced", Reason: FenceReasonPrecheck}}
		if rec.Code != http.StatusConflict || !reflect.DeepEqual(eb, want) {
			t.Fatalf("%s = %d %s, want 409 %+v", label, rec.Code, rec.Body.String(), want)
		}
	}
	requireFenced("deposed", doVerify(t, rt, http.MethodPost, bearerHeader(cr.WriteToken), ""))
	if err := store.Delete("s1"); err != nil {
		t.Fatal(err)
	}
	requireFenced("deleted subscription", doVerify(t, rt, http.MethodPost, bearerHeader(cr.WriteToken), ""))
}

// failingVerifyStore fails VerifyWriteFence with err, and delegates to the
// embedded Store while err is nil, so a test can make the fence store
// unavailable for one answer.
type failingVerifyStore struct {
	Store
	err error
}

func (s *failingVerifyStore) VerifyWriteFence(id string, shard int, incarnation string, generation int64, wakeID, holder string, now time.Time) (WriteFenceCheck, error) {
	if s.err != nil {
		return WriteFenceCheck{}, s.err
	}
	return s.Store.VerifyWriteFence(id, shard, incarnation, generation, wakeID, holder, now)
}

func TestHandleClaimVerifyStoreFailureIsUnavailable(t *testing.T) {
	base, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	failed := errors.New("verify store unavailable")
	mgr, err := NewManager(&failingVerifyStore{Store: base, err: failed}, fs, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	rt := NewRoutes(mgr)
	cr := setupLongClaim(t, rt, base, "s1")

	rec := doVerify(t, rt, http.MethodPost, bearerHeader(cr.WriteToken), "")
	requireNoStore(t, "store failure", rec)
	if rec.Code != http.StatusInternalServerError || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") || rec.Body.String() != "internal error\n" {
		t.Fatalf("verify store failure = %d %q %q, want plain 500", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}

	d, _ := mgr.WriteAuthorizer().AuthorizeAppendFence(cr.WriteToken, mustPath(t, "events/a"), time.Now())
	if d.Allowed() || d.Reason() != auth.ReasonUnauthenticated || d.Detail() != "write token fence unavailable" {
		t.Fatalf("append decision = allowed:%v reason:%s detail:%q", d.Allowed(), d.Reason(), d.Detail())
	}
}

// TestHandleClaimVerifyRecordsOutcomes pins chronicle_claim_verify_total's
// closed vocabulary at the route (#192): every answer is counted exactly once
// under the outcome a consumer's fallback rate is read from — ok; invalid for
// a missing, malformed, or unproven token; expired; fenced for a deposed or
// gone claim; and unavailable for the store failure a consumer must not cache.
func TestHandleClaimVerifyRecordsOutcomes(t *testing.T) {
	base, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	fm := &fakeMetrics{}
	store := &failingVerifyStore{Store: base}
	mgr, err := NewManager(store, fs, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:       fm,
	})
	if err != nil {
		t.Fatal(err)
	}
	rt := NewRoutes(mgr)
	cr := setupLongClaim(t, rt, base, "s1")
	sub, ok, err := base.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get s1 = ok:%v err:%v", ok, err)
	}
	scope := []auth.StreamPath{mustPath(t, "events/a")}
	expired, err := GenerateClaimWriteToken(mgr.tokenKey, "s1", sub.Incarnation, cr.Generation, cr.WakeID, "w1", 0, scope, time.Now().Add(-time.Hour), time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	answer := func(label string, headers http.Header, want int) {
		t.Helper()
		if rec := doVerify(t, rt, http.MethodPost, headers, ""); rec.Code != want {
			t.Fatalf("%s = %d %s, want %d", label, rec.Code, rec.Body.String(), want)
		}
	}
	answer("live", bearerHeader(cr.WriteToken), http.StatusOK)
	answer("no credential", http.Header{}, http.StatusUnauthorized)
	answer("malformed token", bearerHeader("not-a-token"), http.StatusUnauthorized)
	answer("expired", bearerHeader(expired), http.StatusUnauthorized)
	store.err = errors.New("verify store unavailable")
	answer("store failure", bearerHeader(cr.WriteToken), http.StatusInternalServerError)
	store.err = nil
	if crB, err := base.Claim("s1", "w2", "w_b", time.Now().Add(31*time.Second), 1000); err != nil || !crB.Claimed {
		t.Fatalf("takeover = %+v err=%v", crB, err)
	}
	answer("deposed", bearerHeader(cr.WriteToken), http.StatusConflict)

	want := map[string]int{"ok": 1, "invalid": 2, "expired": 1, "unavailable": 1, "fenced": 1}
	if got := fm.claimVerifies(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ClaimVerify outcomes = %v, want %v", got, want)
	}
}

func TestHandleClaimVerifyLogsRefusalsWithoutCredential(t *testing.T) {
	base, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	var logs bytes.Buffer
	mgr, err := NewManager(base, fs, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		Logger:        slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	rt := NewRoutes(mgr)
	cr := setupLongClaim(t, rt, base, "s1")
	sub, ok, err := base.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get s1 = ok:%v err:%v", ok, err)
	}
	scope := []auth.StreamPath{mustPath(t, "events/a")}
	expired, err := GenerateClaimWriteToken(mgr.tokenKey, "s1", sub.Incarnation, cr.Generation, cr.WakeID, "w1", 0, scope, time.Now().Add(-time.Hour), time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	secret := "credential-bytes-must-not-appear"
	if rec := doVerify(t, rt, http.MethodPost, bearerHeader(secret), ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid verify = %d", rec.Code)
	}
	if rec := doVerify(t, rt, http.MethodPost, bearerHeader(expired), ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("expired verify = %d", rec.Code)
	}
	takeoverAt := time.Now().Add(31 * time.Second)
	if crB, err := base.Claim("s1", "w2", "w_b", takeoverAt, 1000); err != nil || !crB.Claimed {
		t.Fatalf("takeover = %+v err=%v", crB, err)
	}
	if rec := doVerify(t, rt, http.MethodPost, bearerHeader(cr.WriteToken), ""); rec.Code != http.StatusConflict {
		t.Fatalf("fenced verify = %d", rec.Code)
	}

	got := logs.String()
	for _, outcome := range []string{"outcome=invalid", "outcome=expired", "outcome=fenced"} {
		if !strings.Contains(got, outcome) {
			t.Fatalf("logs missing %q: %s", outcome, got)
		}
	}
	for _, credential := range []string{secret, expired, cr.WriteToken} {
		if strings.Contains(got, credential) {
			t.Fatalf("logs contain credential bytes %q: %s", credential, got)
		}
	}
}
