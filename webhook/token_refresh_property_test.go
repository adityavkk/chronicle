package webhook

import (
	"testing"

	"pgregory.net/rapid"
)

// token_refresh_property_test.go is the property-based companion to the in-band
// callback-token refresh (issue #77). The fix turns a permanent-lockout bug —
// an expired token yielded a bare 401 the worker could neither release nor
// re-claim past — into a self-healing loop. These properties pin the two PURE
// decision functions (TokenExpired, ShouldRefreshToken) against an independent
// oracle over a generated domain, and a small stateful simulation exercises the
// actual ack-time decision to establish the liveness invariant the fix exists
// for: a heartbeating worker ALWAYS leaves an ack holding a usable (not-yet-
// expired) token, and any expired arrival recovers to a fresh one — so it is
// never permanently locked out.
//
// The pure functions take (exp, now) as unix-second parameters, so the whole
// suite runs with no clock, no Redis, and no HMAC — it is exact arithmetic. It
// runs on the default `go test` build (the `test` and `property-based-testing`
// CI jobs). The behavioral counterpart lives in the TLA+ model
// formal/tla/TokenRefresh.tla (safety + leads-to liveness + negative control).

// thrSecs is the refresh threshold in whole seconds. TestThresholdMatches guards
// it against drift from the production tokenRefreshThreshold constant.
const thrSecs = int64(300)

// aTime draws a plausible unix-second timestamp. The window is wide but bounded
// so exp-now and now+ttl arithmetic stay far from int64 overflow.
func aTime(t *rapid.T, label string) int64 {
	return rapid.Int64Range(0, 1<<40).Draw(t, label)
}

// ttlSecsFor mirrors Manager.tokenTTL: leaseTTL + 1h, in whole seconds. leaseMs
// spans [0, 600_000] — the protocol's 0..10-minute lease range.
func ttlSecsFor(t *rapid.T, label string) int64 {
	leaseMs := rapid.Int64Range(0, 600_000).Draw(t, label)
	return leaseMs/1000 + 3600
}

func TestThresholdMatches(t *testing.T) {
	if got := int64(tokenRefreshThreshold.Seconds()); got != thrSecs {
		t.Fatalf("tokenRefreshThreshold = %ds but the property suite assumes %ds", got, thrSecs)
	}
}

// TestTokenExpiredProperties: the expiry predicate is exactly `now > exp`, is
// valid at the exact expiry second, expired one second later, and is monotone in
// now (a token that is expired stays expired as time advances).
func TestTokenExpiredProperties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		exp := aTime(rt, "exp")
		now := aTime(rt, "now")

		if TokenExpired(exp, now) != (now > exp) {
			rt.Fatalf("TokenExpired(%d,%d)=%v, want %v", exp, now, TokenExpired(exp, now), now > exp)
		}
		if TokenExpired(exp, exp) {
			rt.Fatalf("token must be valid at now==exp (%d)", exp)
		}
		if !TokenExpired(exp, exp+1) {
			rt.Fatalf("token must be expired at now==exp+1 (exp=%d)", exp)
		}
		d := rapid.Int64Range(0, 1<<20).Draw(rt, "advance")
		if TokenExpired(exp, now) && !TokenExpired(exp, now+d) {
			rt.Fatalf("expiry not monotone in now: exp=%d now=%d d=%d", exp, now, d)
		}
	})
}

// TestShouldRefreshTokenProperties: refresh is eligible iff the token is still
// valid AND within the threshold of expiry, it is mutually exclusive with
// expiry, and it holds exactly on the closed window [exp-thr, exp].
func TestShouldRefreshTokenProperties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		exp := aTime(rt, "exp")
		now := aTime(rt, "now")

		got := ShouldRefreshToken(exp, now, tokenRefreshThreshold)
		want := now <= exp && exp-now <= thrSecs
		if got != want {
			rt.Fatalf("ShouldRefreshToken(%d,%d)=%v, want %v", exp, now, got, want)
		}
		if TokenExpired(exp, now) && got {
			rt.Fatalf("an expired token must never be refresh-eligible (exp=%d now=%d)", exp, now)
		}
	})
}

// TestRefreshWindowEdges pins the closed refresh window at its boundaries: at the
// far edge (exp-thr) and at the exact expiry second refresh is due; just outside
// the window (exp-thr-1) and just past expiry (exp+1) it is not.
func TestRefreshWindowEdges(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		exp := rapid.Int64Range(thrSecs, 1<<40).Draw(rt, "exp") // room for exp-thr-1 >= 0
		cases := []struct {
			now  int64
			want bool
		}{
			{exp - thrSecs - 1, false}, // just before the window opens
			{exp - thrSecs, true},      // window opens
			{exp, true},                // last valid second still refreshes
			{exp + 1, false},           // expired: not a refresh (TOKEN_EXPIRED path)
		}
		for _, c := range cases {
			if got := ShouldRefreshToken(exp, c.now, tokenRefreshThreshold); got != c.want {
				rt.Fatalf("ShouldRefreshToken(exp=%d, now=%d)=%v, want %v", exp, c.now, got, c.want)
			}
		}
	})
}

// TestFreshTokenNotImmediatelyRefreshed: a just-minted token (ttl = leaseTTL+1h,
// always > the 300s threshold) is neither expired nor refresh-due at mint, so a
// refresh never thrashes (it does not immediately re-arm another refresh).
func TestFreshTokenNotImmediatelyRefreshed(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		now := aTime(rt, "now")
		ttlSecs := ttlSecsFor(rt, "ttl")
		exp := now + ttlSecs

		if TokenExpired(exp, now) {
			rt.Fatalf("fresh token expired at mint (ttl=%ds)", ttlSecs)
		}
		if ShouldRefreshToken(exp, now, tokenRefreshThreshold) {
			rt.Fatalf("fresh token (ttl=%ds > thr=%ds) is already refresh-due — would thrash", ttlSecs, thrSecs)
		}
	})
}

// ackDecision mirrors the ack-time token handling in routes.go handleAckLike:
//   - an already-expired token is replaced with a freshly minted one (the
//     TOKEN_EXPIRED retry path in writeTokenRejected),
//   - a still-valid token within the refresh threshold is refreshed in-band,
//   - otherwise the token is kept unchanged.
//
// It returns the new expiry and whether a fresh/refreshed token was issued.
func ackDecision(exp, now, ttlSecs int64) (newExp int64, issued bool) {
	switch {
	case TokenExpired(exp, now):
		return now + ttlSecs, true
	case ShouldRefreshToken(exp, now, tokenRefreshThreshold):
		return now + ttlSecs, true
	default:
		return exp, false
	}
}

// TestNoPermanentLockoutLiveness is the stateful, model-based heart of the suite.
// It simulates a worker heartbeating over an arbitrary sequence of acks — with
// inter-ack gaps that may stall past the token TTL — and asserts the liveness
// invariant the #77 fix guarantees:
//
//	After EVERY ack the worker holds a strictly-usable token (newExp > now), and
//	any ack that ARRIVED expired recovers to a fresh full-TTL token.
//
// Together these say the worker is never permanently locked out: an expiry is
// always transient and self-heals on the very next ack.
func TestNoPermanentLockoutLiveness(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ttlSecs := ttlSecsFor(rt, "ttl")
		now := aTime(rt, "start")
		exp := now + ttlSecs // initial mint at claim
		steps := rapid.IntRange(1, 200).Draw(rt, "steps")

		for i := 0; i < steps; i++ {
			// Heartbeat: now advances by an arbitrary positive gap. Gaps may
			// exceed ttl (a stalled worker) to exercise the expired-recovery path.
			gap := rapid.Int64Range(1, ttlSecs+7200).Draw(rt, "gap")
			now += gap

			expiredOnArrival := TokenExpired(exp, now)
			newExp, issued := ackDecision(exp, now, ttlSecs)

			// Liveness/safety: the worker never leaves an ack expired.
			if newExp <= now {
				rt.Fatalf("worker left ack still expired: exp=%d now=%d newExp=%d", exp, now, newExp)
			}
			// Recovery: an expired arrival must mint a fresh full-TTL token.
			if expiredOnArrival && (newExp != now+ttlSecs || !issued) {
				rt.Fatalf("expired arrival not recovered to fresh ttl: now=%d newExp=%d issued=%v", now, newExp, issued)
			}
			// No regression: the expiry never moves backward across an ack.
			if newExp < exp {
				rt.Fatalf("ack shortened token life: exp=%d newExp=%d", exp, newExp)
			}
			exp = newExp
		}
	})
}

// TestDiligentWorkerNeverExpires complements the stalled-worker case: a worker
// that heartbeats at least once every threshold seconds NEVER observes an
// expired token. The in-band refresh fires whenever a token is within the
// threshold of expiry, so a worker acking with gap <= threshold always renews
// while still valid and stays perpetually ahead of expiry. (Bounding the gap by
// the full TTL would NOT suffice — two sub-TTL gaps can sum past a coasting,
// not-yet-refresh-eligible token — which is exactly why the bound is the
// threshold, and why the stalled-worker case above relies on recovery instead.)
func TestDiligentWorkerNeverExpires(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ttlSecs := ttlSecsFor(rt, "ttl")
		now := aTime(rt, "start")
		exp := now + ttlSecs
		steps := rapid.IntRange(1, 500).Draw(rt, "steps")

		for i := 0; i < steps; i++ {
			gap := rapid.Int64Range(1, thrSecs).Draw(rt, "gap")
			now += gap

			if TokenExpired(exp, now) {
				rt.Fatalf("worker acking every <=%ds still hit expiry: exp=%d now=%d", thrSecs, exp, now)
			}
			exp, _ = ackDecision(exp, now, ttlSecs)
		}
	})
}
