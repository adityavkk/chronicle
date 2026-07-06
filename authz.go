package chronicle

import (
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
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

// ReadAuthorizer authorizes a data-plane read with a chronicle
// read-capability JWS (issue #126 TB5). webhook.ReadCapabilityAuthorizer
// implements it.
type ReadAuthorizer interface {
	AuthorizeRead(token string, path auth.StreamPath, now time.Time) auth.Decision
}

// CallerAuthorizer authorizes a namespace-scoped mutation (create/delete)
// with a chronicle caller token — the same control-plane credential TB2/TB3
// gate claim and linking on. webhook.CallerTokenAuthorizer implements it.
type CallerAuthorizer interface {
	AuthorizeCaller(token string, path auth.StreamPath, now time.Time) auth.Decision
}

// EntityAuthorizer authorizes a woken entity acting as itself with its
// wake_token (issue #126 TB6b). webhook.WakeTokenAuthorizer implements it.
type EntityAuthorizer interface {
	AuthorizeEntity(token string, path auth.StreamPath, now time.Time) auth.Decision
}

// Authorizers bundles the webhook-layer token authorizers the handler
// enforces with, all sharing the subscription layer's persisted keys.
type Authorizers struct {
	Append AppendAuthorizer
	Read   ReadAuthorizer
	Caller CallerAuthorizer
	Entity EntityAuthorizer
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

	// SidecarMarkerName / SidecarMarkerValue, when Name is non-empty, add a
	// required-header gate in front of XFCC trust (issue #126 hardening,
	// defense in depth against an XFCC-passthrough misconfiguration): an XFCC
	// mesh identity is honored only when the request also carries this header
	// with this exact value. The operator configures the sidecar to inject it
	// on mTLS-verified traffic AND to strip any client-supplied copy, so an
	// external peer that forges XFCC cannot also produce the marker. Empty
	// Name leaves XFCC trust resting on the documented sidecar-sanitization
	// requirement alone.
	SidecarMarkerName  string
	SidecarMarkerValue string
}

// xfccGatePasses reports whether the sidecar-marker precondition for honoring
// an XFCC mesh identity is met: trivially true when no marker is configured,
// otherwise a constant-time match of the configured marker header.
func (s *ServiceAuth) xfccGatePasses(r *http.Request) bool {
	if s.SidecarMarkerName == "" {
		return true
	}
	got := r.Header.Get(s.SidecarMarkerName)
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.SidecarMarkerValue)) == 1
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
	// Mesh identity (XFCC) is trusted only on the operator's assertion that a
	// sidecar fronts chronicle and sanitizes inbound XFCC. Two hardenings make
	// that assertion less fragile. First, join ALL X-Forwarded-Client-Cert
	// header lines before VerifyXFCC selects the last element: HTTP treats
	// repeated headers as one comma-joined value and Envoy appends its
	// attested element last, so a client that injects its own XFCC line can
	// never make its element the last one — whereas Header.Get (first line
	// only) would let that injection win. Second, the optional sidecar-marker
	// gate above must pass.
	if h.ServiceAuth.xfccGatePasses(r) {
		joined := strings.Join(r.Header.Values(xfccHeader), ",")
		if p, ok := auth.VerifyXFCC(joined, h.ServiceAuth.TrustedSPIFFEIDs); ok {
			return p
		}
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
	// Normalize the EXACT store path (rawPath), not subStreamPath(rawPath):
	// subStreamPath strips one leading slash and NormalizeStreamPath strips
	// another, so the pair would double-strip and silently accept a
	// key-colliding spelling like "//events/a" (authorized as "events/a" while
	// the store operates on "//events/a" — an authorize-A-operate-on-B bypass).
	// NormalizeStreamPath alone strips exactly one leading slash and rejects
	// the empty segment, so the authorized path and the store key are the same
	// string (§12.2).
	path, err := auth.NormalizeStreamPath(rawPath)
	if err != nil {
		return auth.Deny(auth.ReasonForbidden, "invalid stream path")
	}
	if p := h.resolvePrincipal(r); p.Kind() == auth.KindService {
		return auth.Allow()
	}
	// A woken entity acting as itself appends on its wake_token alone — no
	// claim needed (issue #126 TB6b); the claim-token path below remains the
	// producer contract. Routed by JOSE typ from the Bearer only: a wake
	// token on the electric-claim-token header is not a write token and
	// fails the HMAC path (pinned by test).
	if bearer := bearerFromRequest(r); bearer != "" {
		if d, routed := h.agentDecision(bearer, path); routed {
			return d
		}
	}
	if h.AppendAuth == nil {
		return auth.Deny(auth.ReasonUnauthenticated, "no append authorizer configured")
	}
	return h.AppendAuth.AuthorizeAppend(claimTokenFromRequest(r), path, time.Now())
}

// credentialPresented reports whether the request carries any read/mutate
// credential (a Bearer, a mesh identity, or a claim token). It drives the
// Q3 cache posture: a response to a credentialed request is never shared-
// cacheable, mode-independent — while uncredentialed insecure-mode responses
// keep today's headers byte for byte.
func (h *Handler) credentialPresented(r *http.Request) bool {
	return bearerFromRequest(r) != "" ||
		r.Header.Get(ClaimTokenHeader) != "" ||
		r.Header.Get(xfccHeader) != ""
}

// authorizeRead runs the read gate for GET/HEAD (long-poll and SSE
// included): compute the decision, enforce in ModeEnforce, log as telemetry
// in ModeInsecure. Called before any store access, so a 401 never leaks
// stream existence (§12.2). The returned private flag is the Q3 answer: the
// response to a credentialed read carries Cache-Control: private with no
// ETag, so no shared cache stores it and the credential-keying problem of
// §12.7 never arises.
func (h *Handler) authorizeRead(r *http.Request, rawPath string) (private bool, err error) {
	d := h.readDecision(r, rawPath)
	private = h.credentialPresented(r)
	if d.Allowed() {
		return private, nil
	}
	if h.AuthMode == auth.ModeEnforce {
		h.logger().Warn("read denied",
			"path", rawPath, "reason", d.Reason().String(), "detail", d.Detail())
		return private, denyError(d)
	}
	h.logger().Info("authz telemetry: read would be denied",
		"path", rawPath, "reason", d.Reason().String(), "detail", d.Detail())
	return private, nil
}

// readDecision evaluates read authorization with no side effects. The order
// mirrors the trust chain: a verified service principal is pre-authorized;
// a token routed to the configured OIDC issuer becomes a user principal
// whose namespace grant must cover the path; anything else must be a
// chronicle read-capability JWS scoped to the path. The claim-scoped write
// token is append-only — it is never consulted here, and presented as a
// Bearer it fails JWS verification (401), never grants a read.
func (h *Handler) readDecision(r *http.Request, rawPath string) auth.Decision {
	path, err := auth.NormalizeStreamPath(rawPath)
	if err != nil {
		return auth.Deny(auth.ReasonForbidden, "invalid stream path")
	}
	if p := h.resolvePrincipal(r); p.Kind() == auth.KindService {
		return auth.Allow()
	}
	bearer := bearerFromRequest(r)
	if bearer == "" {
		return auth.Deny(auth.ReasonUnauthenticated, "missing read credential")
	}
	if d, routed := h.userDecision(bearer, path); routed {
		return d
	}
	if d, routed := h.agentDecision(bearer, path); routed {
		return d
	}
	if h.ReadAuth == nil {
		return auth.Deny(auth.ReasonUnauthenticated, "no read authorizer configured")
	}
	return h.ReadAuth.AuthorizeRead(bearer, path, time.Now())
}

// userDecision routes a bearer to the configured OIDC issuer when its
// (unverified, routing-only) iss claim names it, and evaluates the verified
// user's namespace grant against the path. routed=false means the token is
// not an OIDC-issuer token and the caller should try the chronicle-family
// verifier instead — never that the token was acceptable.
func (h *Handler) userDecision(bearer string, path auth.StreamPath) (d auth.Decision, routed bool) {
	if h.UserAuth == nil {
		return auth.Decision{}, false
	}
	iss, ok := webhook.PeekIssuer(bearer)
	if !ok || iss != h.UserAuth.Issuer() {
		return auth.Decision{}, false
	}
	p, err := h.UserAuth.VerifyUser(bearer, time.Now())
	if err != nil {
		return auth.Deny(auth.ReasonUnauthenticated, "invalid identity token"), true
	}
	if !p.NamespaceCovers(path) {
		return auth.Deny(auth.ReasonForbidden, "namespace grant does not cover this stream"), true
	}
	return auth.Allow(), true
}

// agentDecision routes a bearer to the entity arm when its (unverified,
// routing-only) JOSE typ is the wake_token's, and evaluates the verified
// entity's subtree scope against the path. routed=false means the bearer is
// not wake-typed and the caller should try the next family — never that the
// token was acceptable. Full verification happens inside AuthorizeEntity;
// the peek grants nothing.
func (h *Handler) agentDecision(bearer string, path auth.StreamPath) (d auth.Decision, routed bool) {
	typ, ok := webhook.PeekTyp(bearer)
	if !ok || typ != webhook.WakeTokenTyp {
		return auth.Decision{}, false
	}
	if h.EntityAuth == nil {
		return auth.Deny(auth.ReasonUnauthenticated, "no entity authorizer configured"), true
	}
	return h.EntityAuth.AuthorizeEntity(bearer, path, time.Now()), true
}

// authorizeMutate runs the create/delete gate (the seam-completion slice:
// with it, every data-plane action — read, append, create, delete — passes
// one enforcement seam). Same telemetry/enforce split as the other gates;
// runs before any store access.
func (h *Handler) authorizeMutate(r *http.Request, rawPath string, action auth.Action) error {
	d := h.mutateDecision(r, rawPath)
	if d.Allowed() {
		return nil
	}
	if h.AuthMode == auth.ModeEnforce {
		h.logger().Warn(action.String()+" denied",
			"path", rawPath, "reason", d.Reason().String(), "detail", d.Detail())
		return denyError(d)
	}
	h.logger().Info("authz telemetry: "+action.String()+" would be denied",
		"path", rawPath, "reason", d.Reason().String(), "detail", d.Detail())
	return nil
}

// mutateDecision evaluates create/delete authorization: a verified service
// principal is pre-authorized; an OIDC user's namespace grant must cover the
// path; otherwise the bearer must be a chronicle caller token whose
// namespaces cover the path — the same credential that authorizes linking
// (TB3), because creating or deleting a stream is the same namespace-scoped
// authority. Read capabilities and write tokens never authorize a mutation,
// and neither does a wake_token (issue #126 TB6b, deliberate): an entity
// acts within its subtree but does not create or destroy entities, so there
// is no agent arm here — a wake-typed bearer falls through to the caller
// verifier and fails its typ pin (401).
func (h *Handler) mutateDecision(r *http.Request, rawPath string) auth.Decision {
	path, err := auth.NormalizeStreamPath(rawPath)
	if err != nil {
		return auth.Deny(auth.ReasonForbidden, "invalid stream path")
	}
	if p := h.resolvePrincipal(r); p.Kind() == auth.KindService {
		return auth.Allow()
	}
	bearer := bearerFromRequest(r)
	if bearer == "" {
		return auth.Deny(auth.ReasonUnauthenticated, "missing caller credential")
	}
	if d, routed := h.userDecision(bearer, path); routed {
		return d
	}
	if h.CallerAuth == nil {
		return auth.Deny(auth.ReasonUnauthenticated, "no caller authorizer configured")
	}
	return h.CallerAuth.AuthorizeCaller(bearer, path, time.Now())
}
