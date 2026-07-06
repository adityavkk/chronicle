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
	return bearerFromRequest(r)
}

// bearerFromRequest returns the Authorization: Bearer value, or "".
func bearerFromRequest(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return ""
}

// xfccHeader is the header Istio/Envoy sidecars inject with the mTLS-verified
// peer certificate chain (TB4's in-mesh service identity).
const xfccHeader = "X-Forwarded-Client-Cert"

// ServiceAuth configures trusted service-principal authentication (issue
// #126 TB4, the trusted-backend topology): a request from a verified service
// principal — the Electric agents-server — is served pre-authorized, because
// that server already ran per-entity authorization before forwarding (Q4,
// decided). Nil disables service auth entirely.
type ServiceAuth struct {
	// Credentials are the static bearer identities (the required path: the
	// stock agents-server presents exactly its DURABLE_STREAMS_BEARER on
	// Authorization: Bearer, and nothing else).
	Credentials []auth.ServiceCredential
	// TrustedSPIFFEIDs is the in-mesh add-on: the X-Forwarded-Client-Cert
	// SPIFFE allowlist. Configuring it asserts a sidecar fronts chronicle
	// and sanitizes inbound XFCC; leave it empty otherwise.
	TrustedSPIFFEIDs []string
}

// resolvePrincipal extracts the verified caller principal from a request, or
// the anonymous Principal when nothing verifies. TB4 resolves service
// principals only (static bearer first — the required path — then mesh
// XFCC); later bullets widen this to users (PingFed) and agents
// (wake_token). It must run BEFORE any claim-token interpretation: the
// service's Authorization: Bearer would otherwise be misread as a
// claim-scoped write token and denied.
func (h *Handler) resolvePrincipal(r *http.Request) auth.Principal {
	if h.ServiceAuth == nil {
		return auth.Principal{}
	}
	if p, ok := auth.VerifyServiceBearer(bearerFromRequest(r), h.ServiceAuth.Credentials); ok {
		return p
	}
	if p, ok := auth.VerifyXFCC(r.Header.Get(xfccHeader), h.ServiceAuth.TrustedSPIFFEIDs); ok {
		return p
	}
	return auth.Principal{}
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

// appendDecision evaluates the append authorization with no side effects.
// Every failure path is a Deny: an unnormalizable path, a missing authorizer
// (nothing to prove a credential against), or a failed token check.
//
// A verified service principal is served pre-authorized without consulting
// any electric-claim-token riding on the request (issue #126 Q4, decided):
// in the trusted-backend topology the agents-server validated the claim
// token against its own ClaimWriteTokenStore before forwarding, so the token
// is meaningless to chronicle here. The service check therefore runs before
// the claim-token path — and before the AppendAuth nil-guard, so a
// trusted-backend-only deployment (no subscription layer) still serves its
// service while denying everyone else.
func (h *Handler) appendDecision(r *http.Request, rawPath string) auth.Decision {
	path, err := auth.NormalizeStreamPath(subStreamPath(rawPath))
	if err != nil {
		return auth.Deny(auth.ReasonForbidden, "invalid stream path")
	}
	if p := h.resolvePrincipal(r); p.Kind() == auth.KindService {
		return auth.Allow()
	}
	if h.AppendAuth == nil {
		return auth.Deny(auth.ReasonUnauthenticated, "no append authorizer configured")
	}
	return h.AppendAuth.AuthorizeAppend(claimTokenFromRequest(r), path, time.Now())
}
