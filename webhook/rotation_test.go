package webhook

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func rotKey(t testing.TB, status string, retireAfter time.Time) SigningKey {
	t.Helper()
	k, err := GenerateSigningKey(rand.Reader, time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	k.Status = status
	k.RetireAfter = retireAfter
	return k
}

// ---- pure state-machine tables ----

func TestBuildKeyFamilyTransitions(t *testing.T) {
	now := time.Unix(10_000, 0)
	active := rotKey(t, keyStatusActive, time.Time{})
	retiringLive := rotKey(t, keyStatusRetiring, now.Add(time.Hour))
	retiringExpired := rotKey(t, keyStatusRetiring, now.Add(-time.Second))
	retiringUnstamped := rotKey(t, keyStatusRetiring, time.Time{})

	t.Run("active plus live retiring both verify", func(t *testing.T) {
		fam, err := buildKeyFamily([]SigningKey{retiringLive, active}, nil, now)
		if err != nil {
			t.Fatal(err)
		}
		if fam.active.Kid != active.Kid || len(fam.verify) != 2 || fam.verify[0].Kid != active.Kid {
			t.Fatalf("fam = active %s, verify %d", fam.active.Kid, len(fam.verify))
		}
	})
	t.Run("expired retiring is removed", func(t *testing.T) {
		fam, err := buildKeyFamily([]SigningKey{active, retiringExpired}, nil, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(fam.verify) != 1 {
			t.Fatalf("expired retiring key still verifies: %d keys", len(fam.verify))
		}
	})
	t.Run("unstamped retiring serves until stamped", func(t *testing.T) {
		fam, err := buildKeyFamily([]SigningKey{active, retiringUnstamped}, nil, now)
		if err != nil || len(fam.verify) != 2 {
			t.Fatalf("verify = %d (err %v), want the legacy retiring key kept", len(fam.verify), err)
		}
	})
	t.Run("denylisted retiring never verifies", func(t *testing.T) {
		deny := map[string]struct{}{retiringLive.Kid: {}}
		fam, err := buildKeyFamily([]SigningKey{active, retiringLive}, deny, now)
		if err != nil || len(fam.verify) != 1 || fam.verify[0].Kid != active.Kid {
			t.Fatalf("verify = %v (err %v)", fam.verify, err)
		}
	})
	t.Run("denylisted active is the emergency stop", func(t *testing.T) {
		deny := map[string]struct{}{active.Kid: {}}
		fam, err := buildKeyFamily([]SigningKey{active, retiringLive}, deny, now)
		if err != nil {
			t.Fatal(err)
		}
		if fam.active.Kid != "" {
			t.Fatal("denylisted active must not mint")
		}
		if len(fam.verify) != 1 || fam.verify[0].Kid != retiringLive.Kid {
			t.Fatalf("verify = %v; only the live retiring key may verify", fam.verify)
		}
	})
	t.Run("no active errors", func(t *testing.T) {
		if _, err := buildKeyFamily([]SigningKey{retiringLive}, nil, now); err == nil {
			t.Fatal("family without an active key must refuse to build")
		}
	})
	t.Run("two active errors", func(t *testing.T) {
		if _, err := buildKeyFamily([]SigningKey{active, rotKey(t, keyStatusActive, time.Time{})}, nil, now); err == nil {
			t.Fatal("two usable active keys must refuse to build")
		}
	})
	t.Run("unknown status errors", func(t *testing.T) {
		bad := rotKey(t, "revoked", time.Time{})
		if _, err := buildKeyFamily([]SigningKey{active, bad}, nil, now); err == nil {
			t.Fatal("unknown status must refuse to build")
		}
	})
}

func TestBuildKeyStateRejectsConflatedFamilies(t *testing.T) {
	now := time.Unix(10_000, 0)
	k := rotKey(t, keyStatusActive, time.Time{})
	if _, err := buildKeyState([]SigningKey{k}, []SigningKey{k}, nil, now); err == nil {
		t.Fatal("one key serving both families must refuse to build (RFC 8725 §2.8)")
	}
}

// TestResolverRejectsAtUseTime: a retiring key that lapses BETWEEN reload
// sweeps is rejected by the resolver at verification time, not just at the
// next snapshot rebuild.
func TestResolverRejectsAtUseTime(t *testing.T) {
	now := time.Unix(10_000, 0)
	active := rotKey(t, keyStatusActive, time.Time{})
	retiring := rotKey(t, keyStatusRetiring, now.Add(30*time.Second))
	st, err := buildKeyState([]SigningKey{active, retiring}, []SigningKey{rotKey(t, keyStatusActive, time.Time{})}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.resolver(KeyPurposeWebhook, now)(retiring.Kid); !ok {
		t.Fatal("retiring key must resolve inside its window")
	}
	if _, ok := st.resolver(KeyPurposeWebhook, now.Add(31*time.Second))(retiring.Kid); ok {
		t.Fatal("retiring key must not resolve past retire_after, even on a stale snapshot")
	}
	if _, ok := st.resolver(KeyPurposeWebhook, now)("ds_unknown"); ok {
		t.Fatal("unknown kid resolved")
	}
}

// TestKeyStateProperty: across random families, denylists, and clocks —
// at most one usable active per family; every verify-set kid is active or
// unexpired-retiring; a denylisted kid never appears and never resolves.
func TestKeyStateProperty(t *testing.T) {
	pool := make([]SigningKey, 8)
	for i := range pool {
		pool[i] = rotKey(t, keyStatusActive, time.Time{})
	}
	rapid.Check(t, func(t *rapid.T) {
		now := time.Unix(rapid.Int64Range(1000, 1<<32).Draw(t, "now"), 0)
		mkFamily := func(label string, offset int) []SigningKey {
			n := rapid.IntRange(1, 3).Draw(t, label+"N")
			keys := make([]SigningKey, n)
			for i := 0; i < n; i++ {
				k := pool[(offset+i)%len(pool)]
				if i == 0 {
					k.Status, k.RetireAfter = keyStatusActive, time.Time{}
				} else {
					k.Status = keyStatusRetiring
					switch rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("%sr%d", label, i)) {
					case 0:
						k.RetireAfter = time.Time{}
					case 1:
						k.RetireAfter = now.Add(time.Duration(rapid.Int64Range(1, 3600).Draw(t, fmt.Sprintf("%sfut%d", label, i))) * time.Second)
					default:
						k.RetireAfter = now.Add(-time.Duration(rapid.Int64Range(0, 3600).Draw(t, fmt.Sprintf("%spast%d", label, i))) * time.Second)
					}
				}
				keys[i] = k
			}
			return keys
		}
		webhookKeys := mkFamily("wh", 0)
		wakeKeys := mkFamily("wk", 4) // disjoint pool halves: families never share
		var denylist []string
		for _, k := range append(append([]SigningKey{}, webhookKeys...), wakeKeys...) {
			if rapid.Bool().Draw(t, "deny"+k.Kid[:8]) {
				denylist = append(denylist, k.Kid)
			}
		}

		st, err := buildKeyState(webhookKeys, wakeKeys, denylist, now)
		if err != nil {
			// The only legal build failures here are denylist-independent
			// (this generator always supplies exactly one active per family).
			t.Fatalf("build: %v", err)
		}
		deny := make(map[string]struct{})
		for _, kid := range denylist {
			deny[kid] = struct{}{}
		}
		for _, fam := range []struct {
			name string
			st   keyFamilyState
			p    KeyPurpose
		}{{"webhook", st.webhook, KeyPurposeWebhook}, {"wake", st.wake, KeyPurposeWake}} {
			if fam.st.active.Kid != "" {
				if _, denied := deny[fam.st.active.Kid]; denied {
					t.Fatalf("%s: denylisted kid is the mint key", fam.name)
				}
			}
			actives := 0
			for _, k := range fam.st.verify {
				if _, denied := deny[k.Kid]; denied {
					t.Fatalf("%s: denylisted kid %s in verify set", fam.name, k.Kid)
				}
				switch k.Status {
				case keyStatusActive:
					actives++
				case keyStatusRetiring:
					if !k.RetireAfter.IsZero() && !now.Before(k.RetireAfter) {
						t.Fatalf("%s: expired retiring kid %s in verify set", fam.name, k.Kid)
					}
				default:
					t.Fatalf("%s: status %q in verify set", fam.name, k.Status)
				}
				if _, ok := st.resolver(fam.p, now)(k.Kid); !ok {
					t.Fatalf("%s: verify-set kid %s does not resolve", fam.name, k.Kid)
				}
			}
			if actives > 1 {
				t.Fatalf("%s: %d active keys in verify set", fam.name, actives)
			}
			for kid := range deny {
				if _, ok := st.resolver(fam.p, now)(kid); ok {
					t.Fatalf("%s: denylisted kid %s resolves", fam.name, kid)
				}
			}
		}
	})
}

func TestKeyMaterialRetireAfterRoundTrip(t *testing.T) {
	k := rotKey(t, keyStatusRetiring, time.Unix(123_456, 0))
	got, err := unmarshalKeyMaterial(k.Kid, marshalKeyMaterial(k))
	if err != nil {
		t.Fatal(err)
	}
	if !got.RetireAfter.Equal(k.RetireAfter) || got.Status != keyStatusRetiring {
		t.Fatalf("round trip = status %q retire %v", got.Status, got.RetireAfter)
	}
	// Legacy three-field material parses with a zero RetireAfter.
	legacy := base64.RawURLEncoding.EncodeToString(k.Private) + ":100:active"
	got, err = unmarshalKeyMaterial(k.Kid, legacy)
	if err != nil || !got.RetireAfter.IsZero() || got.Status != keyStatusActive {
		t.Fatalf("legacy parse = %+v (err %v)", got, err)
	}
	if _, err := unmarshalKeyMaterial(k.Kid, legacy+":notaunix"); err == nil {
		t.Fatal("malformed retire_after must error")
	}
}

// ---- integration: the acceptance criteria against Redis ----

// TestRotateOverlapAcceptance is the TB's verifiable done, per family: a
// token signed by the retiring kid verifies during the overlap window and is
// rejected after removal; a new token verifies against the freshly published
// key.
func TestRotateOverlapAcceptance(t *testing.T) {
	store, client := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	mgr, err := NewManager(store, fs, ManagerOptions{
		StreamRootURL:      "http://x/v1/stream/",
		KeyRotationOverlap: time.Second, // a short window the test can outlive
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()

	// --- webhook family, exercised through the caller token ---
	oldSigning := mgr.activeSigningKey()
	oldCallerTok, err := GenerateCallerToken(oldSigning, mgr.streamRootURL, "u:1", nil, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	successor, err := mgr.RotateSigningKey(KeyPurposeWebhook)
	if err != nil {
		t.Fatal(err)
	}
	if successor.Kid == oldSigning.Kid {
		t.Fatal("successor must be a fresh key")
	}
	if got := mgr.activeSigningKey().Kid; got != successor.Kid {
		t.Fatalf("active kid after rotation = %s, want %s", got, successor.Kid)
	}

	// Overlap: the old-kid token still verifies; a new-kid token verifies too.
	if _, err := mgr.verifyCaller(oldCallerTok, time.Now()); err != nil {
		t.Fatalf("old-kid caller token must verify during the overlap: %v", err)
	}
	newCallerTok, err := GenerateCallerToken(successor, mgr.streamRootURL, "u:2", nil, time.Now(), time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.verifyCaller(newCallerTok, time.Now()); err != nil {
		t.Fatalf("successor-signed caller token must verify immediately: %v", err)
	}
	// Both kids serve in the JWKS during the overlap.
	jwks, _ := mgr.JWKS()
	if !jwksHasKid(jwks, oldSigning.Kid) || !jwksHasKid(jwks, successor.Kid) {
		t.Fatalf("JWKS during overlap must carry old + new webhook kids: %v", jwksKids(jwks))
	}

	// --- wake family ---
	oldWake := mgr.activeWakeKey()
	claims, err := NewWakeTokenClaims(mgr.streamRootURL, "agents/a-1", "", 1, "w_1", time.Now(), 30*time.Second, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	oldWakeTok, err := MintWakeToken(oldWake, claims)
	if err != nil {
		t.Fatal(err)
	}
	wakeSuccessor, err := mgr.RotateSigningKey(KeyPurposeWake)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateWakeToken(oldWakeTok, mgr.WakeKidResolver(time.Now()), "", time.Now(), 5*time.Second); err != nil {
		t.Fatalf("old-kid wake token must verify during the overlap: %v", err)
	}
	freshClaims, err := NewWakeTokenClaims(mgr.streamRootURL, "agents/a-1", "", 2, "w_2", time.Now(), 30*time.Second, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	newWakeTok, err := MintWakeToken(mgr.activeWakeKey(), freshClaims)
	if err != nil {
		t.Fatal(err)
	}
	if mgr.activeWakeKey().Kid != wakeSuccessor.Kid {
		t.Fatal("wake mint key must be the successor")
	}
	if _, err := ValidateWakeToken(newWakeTok, mgr.WakeKidResolver(time.Now()), "", time.Now(), 5*time.Second); err != nil {
		t.Fatalf("successor wake token must verify: %v", err)
	}

	// --- after the window: removed, rejected, reaped ---
	time.Sleep(1100 * time.Millisecond)
	if err := mgr.ReloadKeys(); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.verifyCaller(oldCallerTok, time.Now()); err == nil {
		t.Fatal("old-kid caller token must be rejected after the overlap window")
	}
	if _, err := ValidateWakeToken(oldWakeTok, mgr.WakeKidResolver(time.Now()), "", time.Now(), 5*time.Second); err == nil {
		t.Fatal("old-kid wake token must be rejected after the overlap window")
	}
	jwks, _ = mgr.JWKS()
	if jwksHasKid(jwks, oldSigning.Kid) || jwksHasKid(jwks, oldWake.Kid) {
		t.Fatalf("retired kids still served after the window: %v", jwksKids(jwks))
	}
	// The reap made removal physical: the hash entries are gone.
	if n, _ := client.HExists(t.Context(), jwksKey, oldSigning.Kid).Result(); n {
		t.Fatal("retired webhook kid still persisted after reap")
	}
	if n, _ := client.HExists(t.Context(), wakeKeysKey, oldWake.Kid).Result(); n {
		t.Fatal("retired wake kid still persisted after reap")
	}
	// Post-window tokens under the successors still verify (removal never
	// touches the active keys).
	if _, err := mgr.verifyCaller(newCallerTok, time.Now()); err != nil {
		t.Fatalf("successor caller token broken by the sweep: %v", err)
	}
}

// TestConcurrentRotationExactlyOneSuccessor: two racing rotations produce
// exactly one successor; the loser reports ErrRotationConflict.
func TestConcurrentRotationExactlyOneSuccessor(t *testing.T) {
	store, client := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = mgr.RotateSigningKey(KeyPurposeWebhook)
		}(i)
	}
	wg.Wait()
	var conflicts, wins int
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrRotationConflict):
			conflicts++
		default:
			t.Fatalf("unexpected rotation error: %v", err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("wins=%d conflicts=%d, want exactly one of each", wins, conflicts)
	}
	// The store holds exactly one active key in the family.
	all, err := client.HGetAll(t.Context(), jwksKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	actives := 0
	for kid, material := range all {
		k, err := unmarshalKeyMaterial(kid, material)
		if err != nil {
			t.Fatal(err)
		}
		if k.Status == keyStatusActive {
			actives++
		}
	}
	if actives != 1 {
		t.Fatalf("family holds %d active keys, want 1", actives)
	}
}

// TestRotationPicksUpOnSecondReplica: a rotation done by one Manager is live
// on a second Manager over the same Redis — explicitly via ReloadKeys, and
// via the reload loop within its interval.
func TestRotationPicksUpOnSecondReplica(t *testing.T) {
	store, client := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	mgr1, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	store2 := NewRedisStore(client)
	mgr2, err := NewManager(store2, fs, ManagerOptions{
		StreamRootURL:      "http://x/v1/stream/",
		KeysReloadInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if mgr1.activeSigningKey().Kid != mgr2.activeSigningKey().Kid {
		t.Fatal("replicas must adopt the same key")
	}

	successor, err := mgr1.RotateSigningKey(KeyPurposeWebhook)
	if err != nil {
		t.Fatal(err)
	}
	// Explicit reload: immediate pickup.
	if err := mgr2.ReloadKeys(); err != nil {
		t.Fatal(err)
	}
	if mgr2.activeSigningKey().Kid != successor.Kid {
		t.Fatal("second replica must see the successor after ReloadKeys")
	}

	// Loop pickup: rotate again and let mgr2's reload loop find it.
	mgr2.Start()
	defer mgr2.Stop()
	successor2, err := mgr1.RotateSigningKey(KeyPurposeWebhook)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if mgr2.activeSigningKey().Kid == successor2.Kid {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("second replica's reload loop did not pick up the rotation")
}

// TestDenylistKid: an emergency-revoked kid stops verifying and serving
// immediately on the revoking replica; denylisting the active kid is the
// family's emergency stop until a successor rotates in.
func TestDenylistKid(t *testing.T) {
	store, client := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()

	// Rotate first (the correct emergency order), then denylist the retiring kid.
	oldKid := mgr.activeSigningKey()
	oldTok, err := GenerateCallerToken(oldKid, mgr.streamRootURL, "u:1", nil, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.RotateSigningKey(KeyPurposeWebhook); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.verifyCaller(oldTok, time.Now()); err != nil {
		t.Fatalf("pre-denylist overlap must verify: %v", err)
	}
	if err := mgr.DenylistKid(oldKid.Kid); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.verifyCaller(oldTok, time.Now()); err == nil {
		t.Fatal("denylisted kid verified mid-overlap")
	}
	jwks, _ := mgr.JWKS()
	if jwksHasKid(jwks, oldKid.Kid) {
		t.Fatal("denylisted kid still served in JWKS")
	}
	if member, _ := client.SIsMember(t.Context(), kidDenylistKey, oldKid.Kid).Result(); !member {
		t.Fatal("denylist entry not persisted")
	}

	// Emergency stop: denylisting the ACTIVE wake kid halts wake minting.
	activeWake := mgr.activeWakeKey()
	if err := mgr.DenylistKid(activeWake.Kid); err != nil {
		t.Fatal(err)
	}
	if got := mgr.activeWakeKey().Kid; got != "" {
		t.Fatalf("denylisted active wake kid still minting (kid %s)", got)
	}
	if _, err := mgr.RotateSigningKey(KeyPurposeWake); err == nil {
		t.Fatal("rotation from a fully-denylisted family must error (recover via SREM + reload)")
	}
}

// TestFileWatcherRotation: file custody rotates by replacing the mounted
// file — the watcher picks the change up on the next reload; an invalid
// replacement keeps the last good state.
func TestFileWatcherRotation(t *testing.T) {
	store, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}

	path := writeKeysFile(t, validKeysFile())
	watcher, err := NewFileKeyWatcher(path, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Keys: watcher})
	if err != nil {
		t.Fatal(err)
	}
	oldKid := mgr.activeSigningKey()
	oldTok, err := GenerateCallerToken(oldKid, mgr.streamRootURL, "u:1", nil, time.Now(), time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	// Replace the file: new active webhook key (seed 4), old one retiring
	// with a future window; wake family unchanged.
	rotated := fmt.Sprintf(`{
		"token_key": %q,
		"signing_keys": [
			{"seed": %q, "created_at": "2026-07-02T00:00:00Z", "status": "active", "purpose": "webhook"},
			{"seed": %q, "created_at": "2026-07-01T00:00:00Z", "status": "retiring", "purpose": "webhook", "retire_after": %q},
			%s
		]
	}`, b64TokenKey(32), b64Seed(4), b64Seed(1), time.Now().Add(time.Hour).UTC().Format(time.RFC3339), wakeEntry())
	if err := os.WriteFile(path, []byte(rotated), 0o600); err != nil {
		t.Fatal(err)
	}
	// Ensure the stat fingerprint changes even on coarse mtime filesystems.
	if err := os.Chtimes(path, time.Now(), time.Now().Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := mgr.ReloadKeys(); err != nil {
		t.Fatal(err)
	}
	if mgr.activeSigningKey().Kid == oldKid.Kid {
		t.Fatal("file replacement did not rotate the active key")
	}
	if _, err := mgr.verifyCaller(oldTok, time.Now()); err != nil {
		t.Fatalf("retiring file key must keep verifying through its window: %v", err)
	}

	// An invalid replacement keeps the last good state.
	goodKid := mgr.activeSigningKey().Kid
	if err := os.WriteFile(path, []byte(`{"broken": true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Now(), time.Now().Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := mgr.ReloadKeys(); err != nil {
		t.Fatal(err)
	}
	if mgr.activeSigningKey().Kid != goodKid {
		t.Fatal("invalid file replacement must keep the last good key state")
	}
	// In-band rotation is a file-custody posture error.
	if _, err := mgr.RotateSigningKey(KeyPurposeWebhook); !errors.Is(err, ErrFileCustodyRotation) {
		t.Fatalf("RotateSigningKey under file custody = %v, want ErrFileCustodyRotation", err)
	}
	if err := mgr.DenylistKid(oldKid.Kid); err == nil {
		t.Fatal("DenylistKid under file custody must direct to the file's denylist array")
	}
}

func jwksHasKid(j JWKS, kid string) bool {
	for _, k := range j.Keys {
		if k.Kid == kid {
			return true
		}
	}
	return false
}

func jwksKids(j JWKS) []string {
	out := make([]string, len(j.Keys))
	for i, k := range j.Keys {
		out[i] = k.Kid
	}
	return out
}
