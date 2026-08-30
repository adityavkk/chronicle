package webhook

import (
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

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
	if v.WakeID == "" || v.Holder == "" {
		return auth.Deny(auth.ReasonFenced, "write token is not bound to a live claim"), nil
	}
	status, err := a.store.CheckWriteFence(v.SubID, v.Shard, v.Generation, v.WakeID, v.Holder, now)
	if err != nil {
		return auth.Deny(auth.ReasonUnauthenticated, "write token fence unavailable"), nil
	}
	if status != "OK" {
		return auth.Deny(auth.ReasonFenced, "write token claim is fenced"), nil
	}
	if !a.atomic {
		return auth.Allow(), nil
	}
	if v.Incarnation == "" {
		return auth.Deny(auth.ReasonFenced, "write token has no subscription incarnation"), nil
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
