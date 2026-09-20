package webhook

import (
	"errors"
	"net/http"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
)

// Carriers of the claim-scoped write token, in the order they are read:
// WriteTokenHeader is the write-fencing extension's own header (#183),
// ClaimTokenHeader the compatibility alias Electric producers present, and
// Authorization: Bearer the fallback for both (Electric's claimTokenFromRequest
// order). The append gate and the claim/verify route both read the named
// carriers through NamedWriteTokenCarrier, so WF-29 parity holds on the
// carrier as well as on the decision.
const (
	WriteTokenHeader = protocol.HeaderWriteToken
	ClaimTokenHeader = "electric-claim-token"
)

// NamedWriteTokenCarrier reads the write token from its named carriers in
// order: Write-Token, then electric-claim-token. present is false when neither
// header is set, and the caller may then fall back to Authorization: Bearer.
// A duplicated or empty named header is presented-but-malformed
// (WRITE-FENCING.md §4): it is reported as such and never falls through to
// the next carrier or downgrades the request.
func NamedWriteTokenCarrier(r *http.Request) (token string, present, malformed bool) {
	for _, name := range []string{WriteTokenHeader, ClaimTokenHeader} {
		if values := r.Header.Values(name); len(values) > 0 {
			if len(values) > 1 || values[0] == "" {
				return "", true, true
			}
			return values[0], true, false
		}
	}
	return "", false, false
}

// WriteTokenAuthorizer authorizes data-plane appends by validating the
// claim-scoped write token against the subscription layer's HMAC token key.
// It is the capability half of the issue-#126 authorization seam: pure given
// its inputs and safe to share across goroutines. chronicle's Handler
// enforces with it through the AppendAuthorizer interface.
type WriteTokenAuthorizer struct {
	key    []byte
	store  Store
	atomic bool
}

// NewWriteTokenAuthorizer builds an authorizer around an HMAC token key.
// Tests construct one directly; production wires Manager.WriteAuthorizer().
func NewWriteTokenAuthorizer(key []byte) WriteTokenAuthorizer {
	return WriteTokenAuthorizer{key: key}
}

// WriteAuthorizer exposes the Manager's persisted token key as the append
// authorizer the HTTP handler enforces with. One shared key means the claim
// mint and the append gate can never disagree about what a token proves.
func (m *Manager) WriteAuthorizer() WriteTokenAuthorizer {
	return WriteTokenAuthorizer{key: m.tokenKey, store: m.store, atomic: m.writeFences != nil}
}

// AuthorizeAppend maps a presented (possibly absent) claim token to the
// Decision for an append at path. Fail-closed: only a token that MAC-verifies,
// is unexpired, and carries path in its scope allows; every other outcome —
// including an empty or misconfigured key — denies.
func (a WriteTokenAuthorizer) AuthorizeAppend(token string, path auth.StreamPath, now time.Time) auth.Decision {
	d, _ := a.AuthorizeAppendFence(token, path, now)
	return d
}

// DetailWriteTokenShard is the denial detail of a write token minted for a
// shard other than 0 (#183, A.0 Q9). Both mints hardcode shard 0 and no marker
// is ever granted elsewhere, so such a token can only be a forgery or a drift;
// the handler reports the rejection under reason "shard".
const DetailWriteTokenShard = "write token shard is not fenceable"

// AuthorizeAppendCredential validates the non-live-token properties. It is safe
// to run before stream metadata lookup: no Redis access and no existence leak.
// A MAC-proven token naming a shard other than 0 is refused before its status
// is read: the stream-slot fence exists for shard 0 only.
func (a WriteTokenAuthorizer) AuthorizeAppendCredential(token string, path auth.StreamPath, now time.Time) auth.Decision {
	if token == "" {
		return auth.Deny(auth.ReasonUnauthenticated, "missing write credential")
	}
	v := ValidateWriteToken(a.key, token, path, now)
	if v.Status != WriteTokenInvalid && v.Shard != 0 {
		return auth.Deny(auth.ReasonUnauthenticated, DetailWriteTokenShard)
	}
	switch v.Status {
	case WriteTokenValid:
		return auth.Allow()
	case WriteTokenExpired:
		return auth.Deny(auth.ReasonUnauthenticated, "write token expired")
	case WriteTokenWrongPath:
		return auth.Deny(auth.ReasonForbidden, "write token not scoped to this stream")
	case WriteTokenInvalid:
		return auth.Deny(auth.ReasonUnauthenticated, "invalid write token")
	default:
		return auth.Deny(auth.ReasonUnauthenticated, "invalid write token")
	}
}

// AuthorizeAppendFence revalidates the token and checks live claim state. When
// the manager's stream store supports same-slot markers, it also returns the
// identity the data store must compare inside the append transaction.
func (a WriteTokenAuthorizer) AuthorizeAppendFence(token string, path auth.StreamPath, now time.Time) (auth.Decision, *auth.AppendFence) {
	if d := a.AuthorizeAppendCredential(token, path, now); !d.Allowed() {
		return d, nil
	}
	v := ValidateWriteToken(a.key, token, path, now)
	if a.store == nil {
		return auth.Allow(), nil
	}
	d, _, err := a.liveClaimDecision(v, now)
	if err != nil {
		return auth.Deny(auth.ReasonUnauthenticated, "write token fence unavailable"), nil
	}
	if !d.Allowed() {
		return d, nil
	}
	if !a.atomic {
		return auth.Allow(), nil
	}
	return auth.Allow(), &auth.AppendFence{
		SubscriptionID:          v.SubID,
		SubscriptionIncarnation: v.Incarnation,
		Shard:                   v.Shard,
		Generation:              v.Generation,
		WakeID:                  v.WakeID,
		Holder:                  v.Holder,
	}
}

// liveClaimDecision is the live-state arm shared by the append pre-check and
// claim verification, written once so the two cannot drift (WF-29): the token
// must be bound to a claim, that claim must be the subscription's live one
// (check_write_fence.lua, one atomic read that also yields the lease), and
// under an atomic stream store the token must carry the subscription
// incarnation the in-slot rung compares. A store error is returned rather than
// mapped so each caller keeps its own posture: the append gate denies
// unauthenticated, verify reports a server error.
func (a WriteTokenAuthorizer) liveClaimDecision(v WriteTokenValidation, now time.Time) (auth.Decision, WriteFenceCheck, error) {
	if v.WakeID == "" || v.Holder == "" {
		return auth.Deny(auth.ReasonFenced, "write token is not bound to a live claim"), WriteFenceCheck{}, nil
	}
	check, err := a.store.VerifyWriteFence(v.SubID, v.Shard, v.Generation, v.WakeID, v.Holder, now)
	if err != nil {
		return auth.Decision{}, WriteFenceCheck{}, err
	}
	if check.Status != "OK" {
		return auth.Deny(auth.ReasonFenced, "write token claim is fenced"), check, nil
	}
	if a.atomic && v.Incarnation == "" {
		return auth.Deny(auth.ReasonFenced, "write token has no subscription incarnation"), check, nil
	}
	return auth.Allow(), check, nil
}

// ClaimVerifyStatus classifies a claim verification (WRITE-FENCING.md §9.1).
// The route maps it to HTTP: Invalid and Expired are a 401, Fenced a 409, OK
// a 200. These are the write-token credential and live-claim pre-check
// outcomes; service routing and stream-slot checks are outside this result.
type ClaimVerifyStatus int

const (
	// ClaimVerifyInvalid is a token that is not a usable credential: malformed,
	// a foreign MAC, not a write token, minted for another subscription, or
	// naming an unfenceable shard. The zero value: any unproven token.
	ClaimVerifyInvalid ClaimVerifyStatus = iota
	// ClaimVerifyExpired is ours and well-formed, but past exp.
	ClaimVerifyExpired
	// ClaimVerifyFenced is ours and unexpired, but its claim is not the
	// subscription's live claim: deposed, released, completed, lapsed, or the
	// subscription is gone.
	ClaimVerifyFenced
	// ClaimVerifyOK names the live claim: the token passes the credential and
	// live-claim pre-check at this instant.
	ClaimVerifyOK
)

// ClaimVerification is VerifyClaim's answer. Detail is the operator-facing
// refusal text (never credential material). The claim fields, Streams (the
// token's exact scope), and LeaseUntilNs are set with OK only, and the lease
// comes from the same atomic read as the decision.
type ClaimVerification struct {
	Status       ClaimVerifyStatus
	Detail       string
	Generation   int64
	WakeID       string
	Holder       string
	Streams      []string
	LeaseUntilNs int64
}

// VerifyClaim answers whether token is the live claim of subID using the
// append gate's write-token credential and live-state arms (WF-29): the same
// parser, the same shard rule, and the same live-state arm as
// AuthorizeAppendFence — and no write of any kind. A token minted for another
// subscription is refused as unproven, like ValidateToken's subject binding,
// so the route reveals nothing about it. An error means the answer could not
// be computed (no claim store, or the store failed); the route reports that
// as a server error, never as a credential decision a client might cache.
func (a WriteTokenAuthorizer) VerifyClaim(token, subID string, now time.Time) (ClaimVerification, error) {
	if token == "" {
		return ClaimVerification{Status: ClaimVerifyInvalid, Detail: "missing write credential"}, nil
	}
	v := ParseWriteToken(a.key, token, now)
	if v.Status == WriteTokenInvalid || v.SubID != subID {
		return ClaimVerification{Status: ClaimVerifyInvalid, Detail: "invalid write token"}, nil
	}
	if v.Shard != 0 {
		return ClaimVerification{Status: ClaimVerifyInvalid, Detail: DetailWriteTokenShard}, nil
	}
	if v.Status == WriteTokenExpired {
		return ClaimVerification{Status: ClaimVerifyExpired, Detail: "write token expired"}, nil
	}
	if a.store == nil {
		return ClaimVerification{}, errors.New("claim verify: no claim store")
	}
	d, check, err := a.liveClaimDecision(v, now)
	if err != nil {
		return ClaimVerification{}, err
	}
	if !d.Allowed() {
		return ClaimVerification{Status: ClaimVerifyFenced, Detail: d.Detail()}, nil
	}
	return ClaimVerification{
		Status:       ClaimVerifyOK,
		Generation:   v.Generation,
		WakeID:       v.WakeID,
		Holder:       v.Holder,
		Streams:      v.Streams,
		LeaseUntilNs: check.LeaseUntilNs,
	}, nil
}
