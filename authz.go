package chronicle

import (
	"net/http"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// This file is the data-plane authorization seam (issue #126): the one place
// a request's credential is turned into an auth.Decision, and a Deny becomes
// an HTTP error *before any store access*. TB1 gates appends with the
// claim-scoped write token; later tracer bullets widen this same seam with
// the remaining actions (read, create, delete) and real principals. The
// decision logic itself lives in the pure auth and webhook packages.

// ClaimTokenHeader is the header Electric producers present the claim-scoped
// write token in, with Authorization: Bearer as the fallback (Electric's
// claimTokenFromRequest order).
const ClaimTokenHeader = "electric-claim-token"

// AppendAuthorizer authorizes a data-plane append with a claim-scoped write
// token. webhook.WriteTokenAuthorizer implements it; the interface lives here
// so the handler depends on the capability, not the webhook package's
// internals.
type AppendAuthorizer interface {
	AuthorizeAppend(token string, path auth.StreamPath, now time.Time) auth.Decision
}

// claimTokenFromRequest extracts the presented write credential:
// electric-claim-token first, then Authorization: Bearer. Empty means no
// credential was presented.
func claimTokenFromRequest(r *http.Request) string {
	if t := r.Header.Get(ClaimTokenHeader); t != "" {
		return t
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return ""
}

// Authorization error codes in the JSON error envelope (the control-plane
// ErrorBody shape, extended to the data plane by issue #126).
const (
	errCodeUnauthenticated = "UNAUTHENTICATED"
	errCodeForbidden       = "FORBIDDEN"
)

// authError is a denial mapped to HTTP: 401 UNAUTHENTICATED or 403 FORBIDDEN,
// written by writeError as the JSON error envelope rather than the plaintext
// http.Error the base protocol paths use.
type authError struct {
	status int
	code   string
	msg    string
}

func (e *authError) Error() string { return e.msg }

// denyError maps a Deny decision to its wire error. An unclassified denial
// fails closed to 401.
func denyError(d auth.Decision) *authError {
	switch d.Reason() {
	case auth.ReasonForbidden:
		return &authError{status: http.StatusForbidden, code: errCodeForbidden, msg: d.Detail()}
	case auth.ReasonNone, auth.ReasonUnauthenticated:
		return &authError{status: http.StatusUnauthorized, code: errCodeUnauthenticated, msg: d.Detail()}
	default:
		return &authError{status: http.StatusUnauthorized, code: errCodeUnauthenticated, msg: d.Detail()}
	}
}

// authorizeAppend runs the append gate: compute the decision, enforce it in
// ModeEnforce, log it as telemetry in ModeInsecure. It is called before any
// store access, so a denied append neither reads nor mutates stream state,
// and never fires a subscription wake.
func (h *Handler) authorizeAppend(r *http.Request, rawPath string) error {
	d := h.appendDecision(r, rawPath)
	if d.Allowed() {
		return nil
	}
	if h.AuthMode == auth.ModeEnforce {
		h.logger().Warn("append denied",
			"path", rawPath, "reason", d.Reason().String(), "detail", d.Detail())
		return denyError(d)
	}
	// Telemetry (insecure mode): record what enforcement would deny so an
	// operator can observe the blast radius before flipping AuthMode.
	h.logger().Info("authz telemetry: append would be denied",
		"path", rawPath, "reason", d.Reason().String(), "detail", d.Detail())
	return nil
}

// appendDecision evaluates the append capability with no side effects. Every
// failure path is a Deny: an unnormalizable path, a missing authorizer
// (nothing to prove a credential against), or a failed token check.
func (h *Handler) appendDecision(r *http.Request, rawPath string) auth.Decision {
	path, err := auth.NormalizeStreamPath(subStreamPath(rawPath))
	if err != nil {
		return auth.Deny(auth.ReasonForbidden, "invalid stream path")
	}
	if h.AppendAuth == nil {
		return auth.Deny(auth.ReasonUnauthenticated, "no append authorizer configured")
	}
	return h.AppendAuth.AuthorizeAppend(claimTokenFromRequest(r), path, time.Now())
}
