package auth

// Action is a protocol operation subject to an authorization decision
// (issue #126: Authorize(principal, path, action)). The data plane uses
// Read/Append/Create/Delete; the subscription control plane uses
// Subscribe/Link/Claim.
type Action int

const (
	// ActionRead is GET/HEAD on a stream.
	ActionRead Action = iota
	// ActionAppend is POST on a stream (including a close-only append).
	ActionAppend
	// ActionCreate is config PUT on a stream.
	ActionCreate
	// ActionDelete is DELETE on a stream.
	ActionDelete
	// ActionSubscribe is creating or deleting a subscription.
	ActionSubscribe
	// ActionLink is attaching a stream path to a subscription (create seeds,
	// add-streams, wake_stream).
	ActionLink
	// ActionClaim is claiming a pull-wake, which mints the write token.
	ActionClaim
)

func (a Action) String() string {
	switch a {
	case ActionRead:
		return "read"
	case ActionAppend:
		return "append"
	case ActionCreate:
		return "create"
	case ActionDelete:
		return "delete"
	case ActionSubscribe:
		return "subscribe"
	case ActionLink:
		return "link"
	case ActionClaim:
		return "claim"
	default:
		return "unknown"
	}
}

// DenyReason classifies a denial and picks its HTTP mapping: a missing,
// malformed, or expired credential is ReasonUnauthenticated (401
// UNAUTHENTICATED); a verified credential whose scope does not cover the
// (path, action) is ReasonForbidden (403 FORBIDDEN); a verified but deposed
// claim holder is ReasonFenced (409 FENCED).
type DenyReason int

const (
	// ReasonNone is the reason carried by an Allow decision.
	ReasonNone DenyReason = iota
	// ReasonUnauthenticated maps to 401: no usable credential was presented.
	ReasonUnauthenticated
	// ReasonForbidden maps to 403: the credential verified but does not grant
	// this (path, action).
	ReasonForbidden
	// ReasonFenced maps to 409: the credential verified, but no longer matches
	// the live monotonic claim state.
	ReasonFenced
)

func (r DenyReason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonUnauthenticated:
		return "unauthenticated"
	case ReasonForbidden:
		return "forbidden"
	case ReasonFenced:
		return "fenced"
	default:
		return "unknown"
	}
}

// Decision is the outcome of one authorization check. Build one only with
// Allow or Deny: an allowed decision can never carry a deny reason, and a
// denial always carries a classifying one.
type Decision struct {
	allowed bool
	reason  DenyReason
	detail  string
}

// Allow is the single way to authorize.
func Allow() Decision { return Decision{allowed: true} }

// Deny builds a fail-closed denial. detail is operator-facing (logs and the
// error envelope message); it must never contain credential material.
func Deny(reason DenyReason, detail string) Decision {
	if reason == ReasonNone {
		// A denial must classify itself; an unclassified one fails closed to 401.
		reason = ReasonUnauthenticated
	}
	return Decision{reason: reason, detail: detail}
}

// Allowed reports whether the check authorized the request.
func (d Decision) Allowed() bool { return d.allowed }

// Reason is the denial classification (ReasonNone when allowed).
func (d Decision) Reason() DenyReason { return d.reason }

// Detail is the operator-facing explanation of a denial; it never contains
// credential material.
func (d Decision) Detail() string { return d.detail }
