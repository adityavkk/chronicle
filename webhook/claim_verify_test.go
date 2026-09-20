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

func (s *verifyCallStore) VerifyWriteFence(string, int, int64, string, string, time.Time) (WriteFenceCheck, error) {
	s.verifyCalls++
	return s.reply, s.err
}

func (s *verifyCallStore) Get(string) (Subscription, bool, error) {
	s.getCalls++
	return Subscription{}, false, errors.New("unexpected Get")
}

func (s *verifyCallStore) CheckWriteFence(string, int, int64, string, string, time.Time) (string, error) {
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
	store := &verifyCallStore{reply: WriteFenceCheck{Status: "OK", LeaseUntilNs: wantLease}}
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
		Streams: []string{"events/a"}, LeaseUntilNs: sub.LeaseUntilNs,
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
