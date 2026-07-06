package webhook

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

// Key rotation (#123's rotation deliverable, #126 TBrot): the state machine
// that lets a signing key be replaced without invalidating tokens in flight,
// and pulled instantly when compromised.
//
// Per Ed25519 family (webhook-envelope, wake-token):
//
//	mint successor (active) ──► predecessor becomes retiring with
//	retire_after = now + overlap ──► past retire_after it leaves the served
//	JWKS and every verification set on the next reload sweep (and, under
//	Redis custody, is reaped from the hash).
//
// The overlap window default derives from the longest-lived token the family
// verifies (see rotationOverlap); the kid denylist cuts across every state —
// a denylisted kid never mints, never verifies, and never serves, even
// mid-overlap. Live replicas pick rotations up through the keysReloadLoop:
// nothing caches a key past one reload interval.

// KeyPurpose names an Ed25519 signing-key family.
type KeyPurpose string

const (
	// KeyPurposeWebhook is the webhook-envelope family: Webhook-Signature
	// headers and the control-plane caller token verify against it.
	KeyPurposeWebhook KeyPurpose = "webhook"
	// KeyPurposeWake is the wake_token family (#123) — purpose-separate from
	// the envelope key by construction.
	KeyPurposeWake KeyPurpose = "wake"
)

// Rotation/custody errors, typed so operators and tests can distinguish
// postures from failures.
var (
	// ErrFileCustodyRotation: with CHRONICLE_KEYS_FILE custody the file is
	// the source of truth — rotate by replacing the file (the secrets mount
	// updates out-of-band); the running process only ever reads it.
	ErrFileCustodyRotation = errors.New("webhook: file custody rotates by replacing the keys file, not in-band")
	// ErrRotationConflict: another replica rotated first; the caller's view
	// of the active kid was stale. Reload and retry if still desired.
	ErrRotationConflict = errors.New("webhook: rotation conflict — the active kid changed underneath")
	// ErrRotationUnsupported: the store backing this Manager cannot rotate.
	ErrRotationUnsupported = errors.New("webhook: store does not support key rotation")
)

// defaultKeysReloadInterval bounds how stale any replica's view of the key
// state may be: a rotation or denylist entry lands everywhere within one
// interval without a restart.
const defaultKeysReloadInterval = 15 * time.Second

// Per-family overlap-window defaults: how long a retiring key keeps
// verifying after its successor takes over. Each must cover the family's
// longest-lived outstanding token plus clock skew (#123: overlap >= max
// token exp + max skew), so rotation never strands an unexpired token.
const (
	// wake_tokens live at most maxWakeTokenTTL (60s); allow generous
	// verifier skew on top.
	defaultWakeRotationOverlap = maxWakeTokenTTL + 5*time.Minute
	// The envelope family verifies Webhook-Signature headers (checked within
	// one delivery timeout — seconds) and caller tokens (operator-minted;
	// the issuance convention is <=1h). One hour plus the caller verifier's
	// skew covers both.
	defaultWebhookRotationOverlap = time.Hour + callerClockSkew
)

// rotationOverlap resolves the overlap window for a family: the operator
// override when configured (one knob for both families), else the family
// default above.
func (m *Manager) rotationOverlap(purpose KeyPurpose) time.Duration {
	if m.keyRotationOverlap > 0 {
		return m.keyRotationOverlap
	}
	if purpose == KeyPurposeWake {
		return defaultWakeRotationOverlap
	}
	return defaultWebhookRotationOverlap
}

// ---- the immutable key-state snapshot ----

// keyFamilyState is one family's view at snapshot time. active is the mint
// key; a zero-Kid active means the family cannot mint (its active kid was
// denylisted before a successor existed — the emergency-stop posture).
// verify is the ordered verification/serving set: active first, then
// unexpired retiring keys; never a denylisted or expired-retiring key.
type keyFamilyState struct {
	active SigningKey
	verify []SigningKey
}

// keyState is the atomically-swapped snapshot every mint, verification, and
// JWKS read consults. Immutable after build.
type keyState struct {
	webhook keyFamilyState
	wake    keyFamilyState
	deny    map[string]struct{}
}

// buildKeyFamily classifies one family's raw key list into its snapshot
// state. Pure. Rules:
//   - exactly one status=active key is expected; none is an error (a custody
//     source that lost its active key must not silently wipe the last good
//     snapshot's), more than one is an error;
//   - a denylisted active key is NOT an error: the family loads with a
//     zero mint key and the kid excluded from verification — denylisting the
//     active kid is the emergency stop, and it must take effect;
//   - retiring keys verify until their retire_after passes (a zero
//     retire_after retires never — the legacy pre-rotation form — and waits
//     for a rotation to stamp it);
//   - denylisted kids never appear anywhere.
func buildKeyFamily(keys []SigningKey, deny map[string]struct{}, now time.Time) (keyFamilyState, error) {
	var fam keyFamilyState
	var usableActive, deniedActive int
	retiring := make([]SigningKey, 0, len(keys))
	for _, k := range keys {
		if _, denied := deny[k.Kid]; denied {
			if k.Status == keyStatusActive {
				deniedActive++
			}
			continue
		}
		switch k.Status {
		case keyStatusActive:
			usableActive++
			fam.active = k
		case keyStatusRetiring:
			if k.RetireAfter.IsZero() || now.Before(k.RetireAfter) {
				retiring = append(retiring, k)
			}
		default:
			return keyFamilyState{}, fmt.Errorf("webhook: unknown key status %q for kid %s", k.Status, k.Kid)
		}
	}
	if usableActive > 1 {
		return keyFamilyState{}, fmt.Errorf("webhook: %d active keys in one family", usableActive)
	}
	if usableActive+deniedActive == 0 {
		return keyFamilyState{}, errors.New("webhook: no active key in family")
	}
	if fam.active.Kid != "" {
		fam.verify = append(fam.verify, fam.active)
	}
	fam.verify = append(fam.verify, retiring...)
	return fam, nil
}

// buildKeyState assembles the full snapshot. Pure.
func buildKeyState(webhookKeys, wakeKeys []SigningKey, denylist []string, now time.Time) (*keyState, error) {
	deny := make(map[string]struct{}, len(denylist))
	for _, kid := range denylist {
		deny[kid] = struct{}{}
	}
	wh, err := buildKeyFamily(webhookKeys, deny, now)
	if err != nil {
		return nil, fmt.Errorf("webhook family: %w", err)
	}
	wk, err := buildKeyFamily(wakeKeys, deny, now)
	if err != nil {
		return nil, fmt.Errorf("wake family: %w", err)
	}
	if wh.active.Kid != "" && wh.active.Kid == wk.active.Kid {
		// One key serving both grammars is the cross-protocol-confusion setup
		// RFC 8725 §2.8 warns about (#123); refuse the snapshot.
		return nil, fmt.Errorf("webhook: wake-token key equals the envelope signing key (kid %s)", wh.active.Kid)
	}
	return &keyState{webhook: wh, wake: wk, deny: deny}, nil
}

// family selects a snapshot family by purpose.
func (s *keyState) family(purpose KeyPurpose) keyFamilyState {
	if purpose == KeyPurposeWake {
		return s.wake
	}
	return s.webhook
}

// resolver returns the KidResolver for one family at now: only keys in the
// verification set resolve, and a retiring key whose window lapsed between
// reload sweeps is rejected at use time as well.
func (s *keyState) resolver(purpose KeyPurpose, now time.Time) KidResolver {
	fam := s.family(purpose)
	return func(kid string) (ed25519.PublicKey, bool) {
		for _, k := range fam.verify {
			if k.Kid != kid {
				continue
			}
			if k.Status == keyStatusRetiring && !k.RetireAfter.IsZero() && !now.Before(k.RetireAfter) {
				return nil, false
			}
			return k.Public, true
		}
		return nil, false
	}
}

// ---- Manager: snapshot access, reload, rotation, denylist ----

// keySnapshot returns the current key state. Never nil after NewManager.
func (m *Manager) keySnapshot() *keyState { return m.keySnap.Load() }

// activeSigningKey and activeWakeKey are the current mint keys per family —
// snapshot reads, never cached by callers (#123 rotation: nothing may hold a
// key across a reload interval).
func (m *Manager) activeSigningKey() SigningKey { return m.keySnapshot().webhook.active }

func (m *Manager) activeWakeKey() SigningKey { return m.keySnapshot().wake.active }

// callerKidResolver is the webhook-family trust set for caller tokens — the
// same custody-source-fed snapshot the JWKS serves, so the two can never
// disagree.
func (m *Manager) callerKidResolver(now time.Time) KidResolver {
	return m.keySnapshot().resolver(KeyPurposeWebhook, now)
}

// WakeKidResolver is the wake-family trust set at now, for wake_token
// verification (TB6b's entity principal and tests).
func (m *Manager) WakeKidResolver(now time.Time) KidResolver {
	return m.keySnapshot().resolver(KeyPurposeWake, now)
}

// keyRefresher is the optional hook a KeySource implements when its backing
// material can change underneath it (the keys-file watcher re-stats here).
type keyRefresher interface{ Refresh(now time.Time) }

// denylistSource is the optional denylist feed. A KeySource without one has
// an empty denylist.
type denylistSource interface{ DenylistedKids() ([]string, error) }

// retiredKeyReaper physically removes retiring keys whose window has lapsed
// (Redis custody hygiene: once reaped, removal is permanent even if the
// denylist or clock moves). File custody has no reaper — the file is
// operator-managed.
type retiredKeyReaper interface {
	ReapRetiredKeys(now time.Time) error
}

// keyDenylister persists an emergency kid denylist entry.
type keyDenylister interface{ DenylistKid(kid string) error }

// keyRotator executes the atomic rotation transition in the custody store.
type keyRotator interface {
	RotateKey(purpose KeyPurpose, expectedActiveKid string, successor SigningKey, retireAfter time.Time) (bool, error)
}

// ReloadKeys rebuilds the key snapshot from the custody source now: the
// reload loop's body, exported for tests and for operators who just rotated
// or denylisted and want the flip immediately on this replica. On any read
// or validation failure the last good snapshot stays — availability posture:
// a transient custody-source hiccup must not wipe a serving key set (a bad
// keys file at BOOT still refuses startup in NewSubscriptions, unchanged).
func (m *Manager) ReloadKeys() error {
	now := time.Now()
	if r, ok := m.keys.(keyRefresher); ok {
		r.Refresh(now)
	}
	webhookKeys, err := m.keys.SigningKeys()
	if err != nil {
		return fmt.Errorf("webhook: reload signing keys: %w", err)
	}
	wakeKeys, err := m.keys.WakeSigningKeys()
	if err != nil {
		return fmt.Errorf("webhook: reload wake keys: %w", err)
	}
	var denylist []string
	if ds, ok := m.keys.(denylistSource); ok {
		denylist, err = ds.DenylistedKids()
		if err != nil {
			return fmt.Errorf("webhook: reload kid denylist: %w", err)
		}
	}
	st, err := buildKeyState(webhookKeys, wakeKeys, denylist, now)
	if err != nil {
		return err
	}
	prev := m.keySnap.Swap(st)
	if prev != nil && prev.webhook.active.Kid != st.webhook.active.Kid {
		m.log.Info("webhook: envelope signing key changed", "kid", st.webhook.active.Kid)
	}
	if prev != nil && prev.wake.active.Kid != st.wake.active.Kid {
		m.log.Info("webhook: wake-token key changed", "kid", st.wake.active.Kid)
	}
	if reaper, ok := m.keys.(retiredKeyReaper); ok {
		if err := reaper.ReapRetiredKeys(now); err != nil {
			m.log.Warn("webhook: reap retired keys", "error", err)
		}
	}
	return nil
}

// keysReloadLoop refreshes the key snapshot every keysReloadInterval, so a
// rotation or denylist entry made by any replica (or an out-of-band keys-file
// replacement) is live on this one within one interval. Errors are logged and
// the last good snapshot keeps serving.
func (m *Manager) keysReloadLoop() {
	defer m.wg.Done()
	t := time.NewTicker(m.keysReloadInterval)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			if err := m.ReloadKeys(); err != nil {
				m.log.Warn("webhook: key reload failed; keeping last good key state", "error", err)
			}
		}
	}
}

// RotateSigningKey mints a successor for the family, flips the active kid,
// and marks the predecessor retiring until now + the family's overlap window
// — atomically in the custody store, so two replicas rotating concurrently
// produce exactly one successor (the loser gets ErrRotationConflict). The
// returned key is the successor (its public half is immediately in this
// replica's JWKS; other replicas pick it up within one reload interval).
//
// Redis custody only: file custody rotates by replacing the mounted file
// (ErrFileCustodyRotation). No HTTP surface — rotation is operator tooling.
func (m *Manager) RotateSigningKey(purpose KeyPurpose) (SigningKey, error) {
	if purpose != KeyPurposeWebhook && purpose != KeyPurposeWake {
		return SigningKey{}, fmt.Errorf("webhook: unknown key purpose %q", purpose)
	}
	if !m.custodyIsStore {
		return SigningKey{}, ErrFileCustodyRotation
	}
	rot, ok := m.store.(keyRotator)
	if !ok {
		return SigningKey{}, ErrRotationUnsupported
	}
	now := time.Now()
	successor, err := GenerateSigningKey(rand.Reader, now)
	if err != nil {
		return SigningKey{}, err
	}
	cur := m.keySnapshot().family(purpose)
	expected := cur.active.Kid
	if expected == "" {
		// Emergency posture: the active kid was denylisted with no successor
		// yet. There is no expected kid to CAS on from the snapshot; re-read
		// the store's pointer via a reload first.
		return SigningKey{}, fmt.Errorf("webhook: %s family has no usable active key; reload and rotate from the store's current state", purpose)
	}
	rotated, err := rot.RotateKey(purpose, expected, successor, now.Add(m.rotationOverlap(purpose)))
	if err != nil {
		return SigningKey{}, err
	}
	if !rotated {
		return SigningKey{}, ErrRotationConflict
	}
	if err := m.ReloadKeys(); err != nil {
		m.log.Warn("webhook: post-rotation reload", "error", err)
	}
	m.log.Info("webhook: rotated signing key", "purpose", string(purpose),
		"new_kid", successor.Kid, "retiring_kid", expected,
		"overlap", m.rotationOverlap(purpose).String())
	return successor, nil
}

// DenylistKid is the emergency revocation entry point: the kid stops
// verifying and stops being served on this replica immediately, and on every
// replica within one reload interval. Under Redis custody the entry persists
// in ds:{__ds}:kid_denylist (equivalently, out-of-band:
// redis-cli SADD 'ds:{__ds}:kid_denylist' <kid>). Under file custody, add
// the kid to the file's "denylist" array instead — the watcher picks it up.
//
// Denylisting the CURRENT active kid is the family's emergency stop: minting
// fails closed until a successor is rotated in.
func (m *Manager) DenylistKid(kid string) error {
	if kid == "" {
		return errors.New("webhook: empty kid")
	}
	if !m.custodyIsStore {
		return errors.New("webhook: file custody denylists via the keys file's denylist array")
	}
	dl, ok := m.store.(keyDenylister)
	if !ok {
		return ErrRotationUnsupported
	}
	if err := dl.DenylistKid(kid); err != nil {
		return err
	}
	m.log.Warn("webhook: kid denylisted", "kid", kid)
	return m.ReloadKeys()
}
