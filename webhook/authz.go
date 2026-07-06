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
	key []byte
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
	return WriteTokenAuthorizer{key: m.tokenKey}
}

// AuthorizeAppend maps a presented (possibly absent) claim token to the
// Decision for an append at path. Fail-closed: only a token that MAC-verifies,
// is unexpired, and carries path in its scope allows; every other outcome —
// including an empty or misconfigured key — denies.
func (a WriteTokenAuthorizer) AuthorizeAppend(token string, path auth.StreamPath, now time.Time) auth.Decision {
	if token == "" {
		return auth.Deny(auth.ReasonUnauthenticated, "missing write credential")
	}
	v := ValidateWriteToken(a.key, token, path, now)
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
