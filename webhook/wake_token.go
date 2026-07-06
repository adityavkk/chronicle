package webhook

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
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
	if err := strictJSONUnmarshal(payload, &c); err != nil {
		return WakeTokenClaims{}, fmt.Errorf("wake token claims: %w", err)
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

// wakeGateClockSkew bounds clock drift when chronicle's own data-plane gate
// verifies a wake_token it minted (issue #126 TB6b). Deliberately tight —
// mint and gate run in the same fleet, and a wake_token's whole point is a
// short attestable life (WakeTokenTTL caps it at 60s); the model-wide 60s
// caller-token skew would double that life at the gate.
const wakeGateClockSkew = 5 * time.Second

// WakeTokenAuthorizer authorizes data-plane actions by a woken entity
// presenting its wake_token (issue #126 TB6b): the verified token becomes an
// agent principal whose scope is exactly its own entity subtree. It
// implements chronicle's EntityAuthorizer seam; keys is called per decision
// so wake-key rotation is honored without restart.
type WakeTokenAuthorizer struct {
	aud      string
	resolver func(now time.Time) KidResolver
}

// NewWakeTokenAuthorizer builds an authorizer accepting wake_tokens minted
// for aud (empty accepts only audience-less mints — ValidateWakeToken's
// exact-aud rule), verified against the wake key family the resolver returns
// at the decision time.
func NewWakeTokenAuthorizer(aud string, resolver func(now time.Time) KidResolver) WakeTokenAuthorizer {
	return WakeTokenAuthorizer{aud: aud, resolver: resolver}
}

// EntityAuthorizer exposes the Manager's custody-source-fed wake key snapshot
// and configured audience as the entity authorizer the HTTP handler enforces
// with — the same aud the mint stamps (one CHRONICLE_WAKE_TOKEN_AUD on both
// sides of the loop) and the same rotation- and denylist-aware wake trust set
// the JWKS publishes, so file custody and an emergency kid revocation are
// honored on the entity data plane too (issue #126 hardening: this previously
// verified against m.store, bypassing both).
func (m *Manager) EntityAuthorizer() WakeTokenAuthorizer {
	return NewWakeTokenAuthorizer(m.wakeTokenAud, m.WakeKidResolver)
}

// AuthorizeEntity maps a presented wake_token to the Decision for an action
// at path. Fail-closed: a key-set error denies, an unverifiable token denies
// (401-class), and a verified entity acting outside its own subtree is
// forbidden (403-class). The token is an identity assertion, not a liveness
// one (#123): this gate deliberately does NOT consult the live fence — a
// deposed-but-unexpired token still names its entity, bounded by the
// half-lease/60s TTL. That residual is the documented trade of keeping a
// Redis fence read off the data-plane hot path.
func (a WakeTokenAuthorizer) AuthorizeEntity(token string, path auth.StreamPath, now time.Time) auth.Decision {
	if token == "" {
		return auth.Deny(auth.ReasonUnauthenticated, "missing wake token")
	}
	claims, err := ValidateWakeToken(token, a.resolver(now), a.aud, now, wakeGateClockSkew)
	if err != nil {
		return auth.Deny(auth.ReasonUnauthenticated, "invalid wake token")
	}
	entity, err := auth.NormalizeStreamPath(claims.Sub)
	if err != nil {
		return auth.Deny(auth.ReasonUnauthenticated, "invalid wake token subject")
	}
	// The agent's data-plane scope is its own entity subtree: the entity
	// path and everything under it, the same whole-segment predicate every
	// other scope in the model uses.
	if !auth.PathWithinPrefixes(path, []auth.StreamPath{entity}) {
		return auth.Deny(auth.ReasonForbidden, "wake token entity does not cover this stream")
	}
	return auth.Allow()
}
