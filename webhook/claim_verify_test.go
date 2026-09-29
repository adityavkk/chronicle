package webhook

import (
	"crypto/rand"
	"errors"
	"reflect"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

type verifyCallStore struct {
	Store
	verifyCalls int
	getCalls    int
	checkCalls  int
	reply       WriteFenceCheck
	err         error
}

func (s *verifyCallStore) VerifyWriteFence(string, int, string, int64, string, string, time.Time) (WriteFenceCheck, error) {
	s.verifyCalls++
	return s.reply, s.err
}

func (s *verifyCallStore) Get(string) (Subscription, bool, error) {
	s.getCalls++
	return Subscription{}, false, errors.New("unexpected Get")
}

func (s *verifyCallStore) CheckWriteFence(string, int, string, int64, string, string, time.Time) (string, error) {
	s.checkCalls++
	return "", errors.New("unexpected CheckWriteFence")
}

// TestVerifyClaimUsesOneAtomicStoreRead pins the structural half of WF-29:
// VerifyClaim obtains both the decision and lease from one VerifyWriteFence
// call and never reconstructs either through Get or CheckWriteFence.
func TestVerifyClaimUsesOneAtomicStoreRead(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	now := time.Unix(1_700_000_000, 0)
	scope := []auth.StreamPath{mustPath(t, "events/a")}
	token, err := GenerateClaimWriteToken(key, "s1", "inc-1", 7, "w_a", "worker-A", 0, scope, now, time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wantLease := now.Add(37 * time.Second).UnixNano()
	store := &verifyCallStore{reply: WriteFenceCheck{Status: "OK", LeaseUntilNs: wantLease, LeaseTTLMs: 60_000}}
	a := WriteTokenAuthorizer{key: key, store: store, atomic: true}
	got, err := a.VerifyClaim(token, "s1", now)
	if err != nil {
		t.Fatal(err)
	}
	if store.verifyCalls != 1 || store.getCalls != 0 || store.checkCalls != 0 {
		t.Fatalf("store calls = verify:%d get:%d check:%d, want 1/0/0", store.verifyCalls, store.getCalls, store.checkCalls)
	}
	if got.Status != ClaimVerifyOK || got.LeaseUntilNs != wantLease {
		t.Fatalf("verification = %+v, want OK with lease %d", got, wantLease)
	}
	// The remaining lease is the same read's deadline minus the now the
	// predicate ran at — server-relative, never a second read or clock.
	if got.LeaseRemainingNs != 37*time.Second.Nanoseconds() {
		t.Fatalf("remaining lease = %d, want %d", got.LeaseRemainingNs, 37*time.Second.Nanoseconds())
	}
	store.reply = WriteFenceCheck{Status: "OK", LeaseUntilNs: now.Add(-time.Millisecond).UnixNano(), LeaseTTLMs: 60_000}
	if got, err := a.VerifyClaim(token, "s1", now); err != nil || got.Status != ClaimVerifyOK || got.LeaseRemainingNs != 0 {
		t.Fatalf("remaining lease past the deadline = %+v err=%v, want OK floored at 0", got, err)
	}
}

// TestVerifyClaimClampsRemainingLeaseToTTL pins §9.1's "MUST NOT exceed the
// subscription's lease_ttl_ms" by construction. The deadline was written by
// whichever replica handled the last claim or heartbeat, on that replica's
// clock; a replica whose clock lags it would otherwise report more remaining
// lease than the subscription can grant. The TTL arrives in the same atomic
// read as the deadline and caps the remaining lease; a deadline inside the
// TTL is reported as is, and lease_until itself is never adjusted.
func TestVerifyClaimClampsRemainingLeaseToTTL(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	now := time.Unix(1_700_000_000, 0)
	scope := []auth.StreamPath{mustPath(t, "events/a")}
	token, err := GenerateClaimWriteToken(key, "s1", "inc-1", 7, "w_a", "worker-A", 0, scope, now, time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	store := &verifyCallStore{}
	a := WriteTokenAuthorizer{key: key, store: store, atomic: true}
	for _, tc := range []struct {
		name  string
		until time.Duration // deadline relative to now
		ttlMs int64
		want  time.Duration
	}{
		{"deadline inside the TTL", 12 * time.Second, 30_000, 12 * time.Second},
		{"deadline exactly the TTL", 30 * time.Second, 30_000, 30 * time.Second},
		{"deadline written by a clock ahead of ours", 30*time.Second + 250*time.Millisecond, 30_000, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store.reply = WriteFenceCheck{Status: "OK", LeaseUntilNs: now.Add(tc.until).UnixNano(), LeaseTTLMs: tc.ttlMs}
			got, err := a.VerifyClaim(token, "s1", now)
			if err != nil || got.Status != ClaimVerifyOK {
				t.Fatalf("verify = %+v err=%v, want OK", got, err)
			}
			if got.LeaseRemainingNs != tc.want.Nanoseconds() {
				t.Fatalf("remaining lease = %d, want %d", got.LeaseRemainingNs, tc.want.Nanoseconds())
			}
			if got.LeaseUntilNs != store.reply.LeaseUntilNs {
				t.Fatalf("lease_until = %d, want the deadline %d reported unclamped", got.LeaseUntilNs, store.reply.LeaseUntilNs)
			}
		})
	}
}

// TestParseWriteTokenSharesValidateRules pins the parser split behind the
// claim/verify route (#192): ValidateWriteToken is ParseWriteToken plus the
// scope check and nothing else, so a per-subscription operation with no path
// in hand applies exactly the MAC, shape, and expiry rules the append gate
// applies. Every MAC-proven parse also exposes the token's scope and expiry.
func TestParseWriteTokenSharesValidateRules(t *testing.T) {
	key := make([]byte, 32)
	other := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
		other[i] = byte(0xff - i)
	}
	now := time.Unix(1_700_000_000, 0)
	scope := []auth.StreamPath{mustPath(t, "events/a"), mustPath(t, "events/b")}
	mint := func(k []byte, at time.Time) string {
		t.Helper()
		tok, err := GenerateClaimWriteToken(k, "s1", "inc-1", 3, "w_a", "worker-A", 0, scope, at, time.Minute, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	callback, err := GenerateToken(key, "s1", 3, now, time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		token string
		want  WriteTokenStatus
		exp   int64
	}{
		{"live", mint(key, now), WriteTokenValid, now.Add(time.Minute).Unix()},
		{"expired", mint(key, now.Add(-2*time.Minute)), WriteTokenExpired, now.Add(-time.Minute).Unix()},
		{"foreign key", mint(other, now), WriteTokenInvalid, 0},
		{"callback token", callback, WriteTokenInvalid, 0},
		{"malformed", "not-a-token", WriteTokenInvalid, 0},
		{"empty", "", WriteTokenInvalid, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := ParseWriteToken(key, tc.token, now)
			if p.Status != tc.want {
				t.Fatalf("parse status = %d, want %d (%+v)", p.Status, tc.want, p)
			}
			if p.Status == WriteTokenInvalid {
				if !reflect.DeepEqual(p, WriteTokenValidation{}) {
					t.Fatalf("invalid parse leaked fields: %+v", p)
				}
			} else {
				want := WriteTokenValidation{
					Status: tc.want, SubID: "s1", Incarnation: "inc-1", Generation: 3, WakeID: "w_a",
					Holder: "worker-A", Streams: []string{"events/a", "events/b"}, Exp: tc.exp,
				}
				if !reflect.DeepEqual(p, want) {
					t.Fatalf("parse = %+v, want %+v", p, want)
				}
			}
			// Validate is parse plus scope: identical in scope, WrongPath out of
			// it (only a Valid parse can be re-classified), otherwise the same.
			in := ValidateWriteToken(key, tc.token, mustPath(t, "events/a"), now)
			if !reflect.DeepEqual(in, p) {
				t.Fatalf("validate in scope = %+v, parse = %+v", in, p)
			}
			wantOut := p
			if p.Status == WriteTokenValid {
				wantOut.Status = WriteTokenWrongPath
			}
			if out := ValidateWriteToken(key, tc.token, mustPath(t, "events/c"), now); !reflect.DeepEqual(out, wantOut) {
				t.Fatalf("validate out of scope = %+v, want %+v", out, wantOut)
			}
		})
	}
	// The degenerate-key guard fails closed in the parser, so it fails closed
	// everywhere the parser is used.
	if p := ParseWriteToken(key[:8], mint(key[:8], now), now); !reflect.DeepEqual(p, WriteTokenValidation{}) {
		t.Fatalf("short key parse = %+v, want invalid", p)
	}
}

// TestVerifyClaimAgreesWithAppendFence pins WF-29 below the HTTP surface: for
// every credential and claim state the fence matrix covers, VerifyClaim's
// status and detail are the append pre-check's (AuthorizeAppendFence) — the
// two share ParseWriteToken and the live-state arm, so this is a property of
// the code, not of two implementations kept in step by hand. The one shape
// the append gate cannot judge — a token minted for another subscription — is
// refused by verify as unproven, and a missing claim store is an error rather
// than a decision.
func TestVerifyClaimAgreesWithAppendFence(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")
	now := time.Now()
	sub, ok, err := store.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get s1 = ok:%v err:%v", ok, err)
	}
	az := mgr.WriteAuthorizer()
	path := mustPath(t, "events/a")
	scope := []auth.StreamPath{path}
	mint := func(subID string, shard int, at time.Time) string {
		t.Helper()
		tok, err := GenerateClaimWriteToken(mgr.tokenKey, subID, sub.Incarnation, cr.Generation, cr.WakeID, "w1", shard, scope, at, time.Minute, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	unbound, err := GenerateWriteToken(mgr.tokenKey, "s1", cr.Generation, scope, now, time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	agree := func(label, token string, at time.Time, want ClaimVerifyStatus) ClaimVerification {
		t.Helper()
		res, err := az.VerifyClaim(token, "s1", at)
		if err != nil {
			t.Fatalf("%s: verify error: %v", label, err)
		}
		if res.Status != want {
			t.Fatalf("%s: verify status = %d detail %q, want %d", label, res.Status, res.Detail, want)
		}
		d, _ := az.AuthorizeAppendFence(token, path, at)
		switch want {
		case ClaimVerifyOK:
			if !d.Allowed() {
				t.Fatalf("%s: verify OK but append denied: %s %s", label, d.Reason(), d.Detail())
			}
		case ClaimVerifyFenced:
			if d.Allowed() || d.Reason() != auth.ReasonFenced || d.Detail() != res.Detail {
				t.Fatalf("%s: verify fenced %q but append = allowed:%v %s %q", label, res.Detail, d.Allowed(), d.Reason(), d.Detail())
			}
		default:
			if d.Allowed() || d.Reason() != auth.ReasonUnauthenticated || d.Detail() != res.Detail {
				t.Fatalf("%s: verify 401 %q but append = allowed:%v %s %q", label, res.Detail, d.Allowed(), d.Reason(), d.Detail())
			}
		}
		return res
	}

	live := agree("live", cr.WriteToken, now, ClaimVerifyOK)
	want := ClaimVerification{
		Status: ClaimVerifyOK, Generation: cr.Generation, WakeID: cr.WakeID, Holder: "w1",
		Streams: []string{"events/a"}, LeaseUntilNs: sub.LeaseUntilNs, LeaseRemainingNs: sub.LeaseUntilNs - now.UnixNano(),
	}
	if !reflect.DeepEqual(live, want) {
		t.Fatalf("live verification = %+v, want %+v", live, want)
	}
	agree("missing", "", now, ClaimVerifyInvalid)
	agree("malformed", "not-a-token", now, ClaimVerifyInvalid)
	agree("callback token", cr.Token, now, ClaimVerifyInvalid)
	agree("expired", mint("s1", 0, now.Add(-time.Hour)), now, ClaimVerifyExpired)
	agree("unfenceable shard", mint("s1", 1, now), now, ClaimVerifyInvalid)
	agree("unbound legacy token", unbound, now, ClaimVerifyFenced)
	lapsed := now.Add(time.Duration(sub.Config.LeaseTTLMs)*time.Millisecond + time.Second)
	agree("lapsed lease, no successor", cr.WriteToken, lapsed, ClaimVerifyFenced)

	// A token for another subscription is MAC-valid and in scope, so the
	// append gate would judge it against *its* subscription; the verify route
	// is bound to {id} and refuses it as unproven, revealing nothing.
	if res, err := az.VerifyClaim(mint("s2", 0, now), "s1", now); err != nil || res.Status != ClaimVerifyInvalid {
		t.Fatalf("other subscription's token = %+v err=%v, want invalid", res, err)
	}

	crB, err := store.Claim("s1", "w2", "w_b", lapsed, sub.Config.LeaseTTLMs)
	if err != nil || !crB.Claimed || crB.Generation == cr.Generation {
		t.Fatalf("takeover = %+v err=%v", crB, err)
	}
	if res := agree("deposed", cr.WriteToken, lapsed.Add(100*time.Millisecond), ClaimVerifyFenced); res.Detail != "write token claim is fenced" {
		t.Fatalf("deposed detail = %q", res.Detail)
	}
	if err := store.Delete("s1"); err != nil {
		t.Fatal(err)
	}
	if res := agree("deleted subscription", cr.WriteToken, now, ClaimVerifyFenced); res.Detail != "write token claim is fenced" {
		t.Fatalf("deleted detail = %q, want the bare fenced detail (never a not-found)", res.Detail)
	}

	if _, err := NewWriteTokenAuthorizer(mgr.tokenKey).VerifyClaim(cr.WriteToken, "s1", now); err == nil {
		t.Fatal("verify without a claim store must be an error, not a decision")
	}
}

// TestVerifyClaimFencesRecreatedIncarnation is the deterministic regression
// for the incarnation predicate (#192 follow-up): a subscription is claimed,
// deleted, recreated under the same id, and claimed again with the same
// worker and wake id, so the new claim's (generation, wake_id, holder) equal
// the predecessor's exactly and only the incarnation differs. The
// predecessor's token must then be fenced by verify and by the append
// pre-check alike — through check_write_fence.lua, not a second read — while
// a token minted for the new incarnation passes both.
func TestVerifyClaimFencesRecreatedIncarnation(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	az := mgr.WriteAuthorizer()
	now := time.Now()
	path := mustPath(t, "events/a")
	scope := []auth.StreamPath{path}
	begin := "0000000000000000_0000000000000000"

	claimAs := func(label string) (ClaimResult, Subscription) {
		t.Helper()
		if _, err := store.CreateOrConfirm("s1", pullWakeCfg(), nil, now); err != nil {
			t.Fatalf("%s: create: %v", label, err)
		}
		if err := store.Link("s1", "events/a", LinkGlob, begin); err != nil {
			t.Fatalf("%s: link: %v", label, err)
		}
		cr, err := store.Claim("s1", "w1", "w_same", now, 30_000)
		if err != nil || !cr.Claimed {
			t.Fatalf("%s: claim = %+v err=%v", label, cr, err)
		}
		sub, ok, err := store.Get("s1")
		if err != nil || !ok || sub.Incarnation == "" {
			t.Fatalf("%s: get s1 = %+v ok:%v err:%v", label, sub, ok, err)
		}
		return cr, sub
	}
	mint := func(sub Subscription, cr ClaimResult) string {
		t.Helper()
		tok, err := GenerateClaimWriteToken(mgr.tokenKey, "s1", sub.Incarnation, cr.Generation, cr.WakeID, "w1", 0, scope, now, time.Minute, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	expect := func(label, token string, want ClaimVerifyStatus) {
		t.Helper()
		res, err := az.VerifyClaim(token, "s1", now)
		if err != nil || res.Status != want {
			t.Fatalf("%s: verify = %+v err=%v, want status %d", label, res, err, want)
		}
		d, _ := az.AuthorizeAppendFence(token, path, now)
		if d.Allowed() != (want == ClaimVerifyOK) {
			t.Fatalf("%s: append pre-check allowed=%v %s %q, want allowed=%v", label, d.Allowed(), d.Reason(), d.Detail(), want == ClaimVerifyOK)
		}
		if want == ClaimVerifyFenced && (d.Reason() != auth.ReasonFenced || d.Detail() != res.Detail || res.Detail != "write token claim is fenced") {
			t.Fatalf("%s: fenced detail = verify %q / append %s %q, want the bare pre-check detail on both", label, res.Detail, d.Reason(), d.Detail())
		}
	}

	crA, subA := claimAs("first incarnation")
	old := mint(subA, crA)
	expect("live predecessor", old, ClaimVerifyOK)

	if err := store.Delete("s1"); err != nil {
		t.Fatal(err)
	}
	crB, subB := claimAs("recreated incarnation")
	// The collision under test: identical fence tuple, different incarnation.
	if crB.Generation != crA.Generation || crB.WakeID != crA.WakeID || crB.Holder != crA.Holder {
		t.Fatalf("precondition: recreated claim %+v must repeat the predecessor's tuple %+v", crB, crA)
	}
	if subB.Incarnation == subA.Incarnation {
		t.Fatalf("precondition: recreate kept incarnation %q", subA.Incarnation)
	}

	expect("predecessor token after recreate", old, ClaimVerifyFenced)
	expect("token for the new incarnation", mint(subB, crB), ClaimVerifyOK)

	// The store-level predicate, isolated: only the token's own incarnation is
	// compared; an empty one asserts none and is left to the Go arm.
	for _, tc := range []struct {
		inc  string
		want string
	}{{subA.Incarnation, "FENCED"}, {subB.Incarnation, "OK"}, {"", "OK"}} {
		if st, err := store.CheckWriteFence("s1", 0, tc.inc, crB.Generation, crB.WakeID, "w1", now); err != nil || st != tc.want {
			t.Fatalf("check_write_fence(incarnation %q) = %q err=%v, want %s", tc.inc, st, err, tc.want)
		}
	}
}
