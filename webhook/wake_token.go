package webhook

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// This file is the pure core of the wake_token mint (issues #123/#126 TB6a):
// the entity-identity assertion chronicle attaches to a wake. It is an EdDSA
// compact JWS with mandatory typ=application/wake+jwt, minted under a
// purpose-separate Ed25519 key — never the webhook-envelope key, so an
// envelope signature is cryptographically incapable of validating as a
// wake_token (RFC 8725 §2.8) — and it attests exactly what chronicle can
// honestly attest: "this stream path is linked to this subscription under a
// live fence, unexpired". It is an identity assertion, NOT a liveness/fence
// assertion: an offline verifier (JWKS + exp) cannot reproduce the live Redis
// fence; the (gen, wake_id) claims are the replay key a gateway tracks, and
// the per-entity kill switch is gateway policy, not this token.

// WakeTokenTyp is the mandatory JOSE typ of a wake_token (RFC 8725 §3.11);
// verification is mutually exclusive with every other chronicle token family
// (§3.12).
const WakeTokenTyp = "application/wake+jwt"

// WakeTokenClaims is the #123 claim set. Aud is present only when the
// deployment configures a gateway audience; Generation and WakeID bind the
// token to the fence event that minted it.
type WakeTokenClaims struct {
	Iss        string `json:"iss"`
	Sub        string `json:"sub"`
	Aud        string `json:"aud,omitempty"`
	Exp        int64  `json:"exp"`
	Nbf        int64  `json:"nbf"`
	Iat        int64  `json:"iat"`
	Jti        string `json:"jti"`
	Generation int64  `json:"gen"`
	WakeID     string `json:"wake_id"`
}

// NewWakeTokenClaims builds the claim set for a wake on the entity at sub,
// expiring at now+ttl. Pure given its inputs (rand supplies the jti).
func NewWakeTokenClaims(iss, sub, aud string, generation int64, wakeID string, now time.Time, ttl time.Duration, rand io.Reader) (WakeTokenClaims, error) {
	jti := make([]byte, 12)
	if _, err := io.ReadFull(rand, jti); err != nil {
		return WakeTokenClaims{}, fmt.Errorf("wake token jti: %w", err)
	}
	return WakeTokenClaims{
		Iss:        iss,
		Sub:        sub,
		Aud:        aud,
		Exp:        now.Add(ttl).Unix(),
		Nbf:        now.Unix(),
		Iat:        now.Unix(),
		Jti:        hex.EncodeToString(jti),
		Generation: generation,
		WakeID:     wakeID,
	}, nil
}

// MintWakeToken signs the claims as a compact EdDSA JWS under the wake-token
// key. The caller is responsible for passing the WAKE key, never the webhook
// envelope key; NewManager enforces that the two are distinct at load time.
func MintWakeToken(wakeKey SigningKey, c WakeTokenClaims) (string, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return SignCompactJWS(wakeKey, WakeTokenTyp, payload)
}

// ValidateWakeToken verifies a wake_token end to end: the pinned-grammar JWS
// checks (alg exactly EdDSA, typ exactly application/wake+jwt, kid resolved
// only by keyFor), then a strict claim decode, then the time window with
// bounded skew (exp is rejected once now > exp+skew; nbf once now < nbf-skew).
// It returns the claims only when every check passes. The caller supplies the
// expected audience: when the mint was audience-scoped, a verifier must pin
// it; an empty expectedAud accepts only tokens minted without an audience.
func ValidateWakeToken(token string, keyFor KidResolver, expectedAud string, now time.Time, skew time.Duration) (WakeTokenClaims, error) {
	payload, err := VerifyCompactJWS(token, WakeTokenTyp, keyFor)
	if err != nil {
		return WakeTokenClaims{}, err
	}
	var c WakeTokenClaims
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return WakeTokenClaims{}, fmt.Errorf("wake token claims: %w", err)
	}
	if dec.More() {
		return WakeTokenClaims{}, errors.New("wake token claims: trailing data")
	}
	if c.Iss == "" || c.Sub == "" || c.Jti == "" || c.WakeID == "" {
		return WakeTokenClaims{}, errors.New("wake token claims: required claim missing")
	}
	if c.Aud != expectedAud {
		return WakeTokenClaims{}, errors.New("wake token claims: audience mismatch")
	}
	nowU := now.Unix()
	skewS := int64(skew / time.Second)
	if nowU > c.Exp+skewS {
		return WakeTokenClaims{}, errors.New("wake token expired")
	}
	if nowU < c.Nbf-skewS {
		return WakeTokenClaims{}, errors.New("wake token not yet valid")
	}
	if c.Iat > c.Exp {
		return WakeTokenClaims{}, errors.New("wake token claims: iat after exp")
	}
	return c, nil
}

// maxWakeTokenTTL caps the wake_token life for long leases: identity
// assertions stay short even when an operator configures a 10-minute lease.
const maxWakeTokenTTL = 60 * time.Second

// WakeTokenTTL derives the wake_token life from the subscription's lease:
// half the lease, capped at maxWakeTokenTTL — strictly under one lease_ttl by
// construction, and NEVER the callback-token convention of lease+1h (#123: a
// non-live wake must stop being attestable within the lease band). Returns 0
// (mint nothing) when the lease is non-positive.
func WakeTokenTTL(leaseTTLMs int64) time.Duration {
	if leaseTTLMs <= 0 {
		return 0
	}
	ttl := time.Duration(leaseTTLMs) * time.Millisecond / 2
	if ttl > maxWakeTokenTTL {
		ttl = maxWakeTokenTTL
	}
	return ttl
}

// WakeEntityPath is the honest single-entity rule (#123): chronicle names an
// entity in a wake_token only when the subscription links exactly one stream
// — the per-entity dispatch-subscription topology, where the linked stream IS
// the entity path. A multi-link (pattern / fan-in) subscription names no
// single entity, so no wake_token is minted for it: no assertion rather than
// an ambiguous one.
func WakeEntityPath(links []StreamLink) (string, bool) {
	if len(links) != 1 {
		return "", false
	}
	return links[0].Path, true
}

// ShouldRefreshWakeToken decides the in-band wake_token refresh on an ack:
// every successful HEARTBEAT ack (done absent/false) re-mints, a done ack
// never does — the wake is over, so a fresh identity assertion for it would
// be a lie. Refreshing on every heartbeat (rather than a near-expiry
// threshold like #77's callback-token refresh) is deliberate: the token's
// mint time is not stored, and estimating it as lease_until − lease_ttl
// starves under heartbeat lease extension — each extension moves the
// estimate forward, the threshold never fires, and the token expires under a
// live worker. A wake_token lives half a lease (WakeTokenTTL), so heartbeat
// cadence IS the refresh cadence #123 asks for ("a non-live wake stops
// receiving fresh tokens within one lease tick"), and a per-heartbeat
// Ed25519 sign is microseconds. The conformance suite only ever acks with
// done:true, so the {ok,next_wake} body it deep-equals never gains a field.
func ShouldRefreshWakeToken(done bool) bool { return !done }
