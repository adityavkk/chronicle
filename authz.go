package chronicle

import (
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

// AppendTwoPhaseAuthorizer lets the handler validate credentials before store
// lookup, then produce a live claim fence immediately before mutation. The
// fence is passed to the store and compared inside the atomic stream commit.
type AppendTwoPhaseAuthorizer interface {
	AuthorizeAppendCredential(token string, path auth.StreamPath, now time.Time) auth.Decision
	AuthorizeAppendFence(token string, path auth.StreamPath, now time.Time) (auth.Decision, *auth.AppendFence)
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

// ServiceAuth is the shared service authentication and explicit policy
// evaluator used by the data plane and subscription control plane. SPIFFE is
// primary; static bearer credentials are a compatibility fallback.
type ServiceAuth = auth.ServiceAccess

// ServiceMetrics records low-cardinality service access outcomes.
type ServiceMetrics interface {
	ServiceSPIFFEAuthentication()
	ServiceBearerAuthentication()
	ServiceAuthenticationFailure()
	ServiceAuthorizationFailure()
	ServiceDelegatedGateway()
}

// serviceDecision resolves a service identity and evaluates its action and
// namespace policy. routed is true when service authentication was attempted
// or succeeded; callers must not fall through to another credential family in
// that case.
func (h *Handler) serviceDecision(r *http.Request, path auth.StreamPath, action auth.Action) (decision auth.Decision, routed bool) {
	if h.ServiceAuth == nil {
		return auth.Decision{}, false
	}
	joinedXFCC := strings.Join(r.Header.Values(xfccHeader), ",")
	marker := ""
	if values := r.Header.Values(h.ServiceAuth.SidecarMarkerName); len(values) == 1 {
		marker = values[0]
	}
	principal, status := h.ServiceAuth.Authenticate(bearerFromRequest(r), joinedXFCC, marker)
	switch status {
	case auth.ServiceRejected:
		if h.ServiceMetrics != nil {
			h.ServiceMetrics.ServiceAuthenticationFailure()
		}
		return auth.Deny(auth.ReasonUnauthenticated, "invalid service identity"), true
	case auth.ServiceAuthenticated:
		if h.ServiceMetrics != nil {
			if joinedXFCC != "" {
				h.ServiceMetrics.ServiceSPIFFEAuthentication()
			} else {
				h.ServiceMetrics.ServiceBearerAuthentication()
			}
		}
		decision, delegated := h.ServiceAuth.Authorize(principal, action, path)
		if !decision.Allowed() {
			if h.ServiceMetrics != nil {
				h.ServiceMetrics.ServiceAuthorizationFailure()
			}
		} else if delegated && h.ServiceMetrics != nil {
			h.ServiceMetrics.ServiceDelegatedGateway()
		}
		return decision, true
	default:
		return auth.Decision{}, false
	}
}

// Authorization error codes in the JSON error envelope (the control-plane
// ErrorBody shape, extended to the data plane by issue #126).
const (
	errCodeUnauthenticated = "UNAUTHENTICATED"
	errCodeForbidden       = "FORBIDDEN"
	errCodeFenced          = webhook.ErrCodeFenced
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
	case auth.ReasonFenced:
		return &authError{status: http.StatusConflict, code: errCodeFenced, msg: d.Detail()}
	case auth.ReasonNone, auth.ReasonUnauthenticated:
		return &authError{status: http.StatusUnauthorized, code: errCodeUnauthenticated, msg: d.Detail()}
	default:
		return &authError{status: http.StatusUnauthorized, code: errCodeUnauthenticated, msg: d.Detail()}
	}
}

func (h *Handler) authorizeAppendCredential(r *http.Request, rawPath string) error {
	_, err := h.authorizeAppendPhase(r, rawPath, appendPhaseCredential, "append credential")
	return err
}

func (h *Handler) authorizeAppendFence(r *http.Request, rawPath string) (*auth.AppendFence, error) {
	return h.authorizeAppendPhase(r, rawPath, appendPhaseFence, "append fence")
}

func (h *Handler) authorizeAppendPhase(
	r *http.Request,
	rawPath string,
	phase appendAuthPhase,
	label string,
) (*auth.AppendFence, error) {
	d, fence := h.appendDecision(r, rawPath, phase)
	if d.Allowed() {
		if h.AuthMode == auth.ModeEnforce {
			return fence, nil
		}
		return nil, nil
	}
	if h.AuthMode == auth.ModeEnforce {
		h.logger().Warn(label+" denied",
			"path", rawPath, "reason", d.Reason().String(), "detail", d.Detail())
		return nil, denyError(d)
	}
	// Telemetry (insecure mode): record what enforcement would deny so an
	// operator can observe the blast radius before flipping AuthMode.
	h.logger().Info("authz telemetry: "+label+" would be denied",
		"path", rawPath, "reason", d.Reason().String(), "detail", d.Detail())
	return nil, nil
}

type appendAuthPhase int

const (
	appendPhaseCredential appendAuthPhase = iota
	appendPhaseFence
)

// appendDecision evaluates the append authorization with no side effects.
// Every failure path is a Deny: an unnormalizable path, a missing authorizer
// (nothing to prove a credential against), or a failed token check.
//
// A verified service principal is evaluated against its explicit append and
// namespace policy before any claim-token interpretation. A trusted_gateway
// policy still implements the delegated-backend topology: the upstream service
// validated entity authority, so a claim token riding alongside it is not
// reinterpreted here. The service decision runs before the AppendAuth nil guard,
// so a policy-authorized service can run without the subscription token layer.
func (h *Handler) appendDecision(
	r *http.Request,
	rawPath string,
	phase appendAuthPhase,
) (auth.Decision, *auth.AppendFence) {
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
		return auth.Deny(auth.ReasonForbidden, "invalid stream path"), nil
	}
	if decision, routed := h.serviceDecision(r, path, auth.ActionAppend); routed {
		return decision, nil
	}
	// A woken entity acting as itself appends on its wake_token alone — no
	// claim needed (issue #126 TB6b); the claim-token path below remains the
	// producer contract. Routed by JOSE typ from the Bearer only: a wake
	// token on the electric-claim-token header is not a write token and
	// fails the HMAC path (pinned by test).
	if bearer := bearerFromRequest(r); bearer != "" {
		if d, routed := h.agentDecision(bearer, path); routed {
			return d, nil
		}
	}
	if h.AppendAuth == nil {
		return auth.Deny(auth.ReasonUnauthenticated, "no append authorizer configured"), nil
	}
	token := claimTokenFromRequest(r)
	if tp, ok := h.AppendAuth.(AppendTwoPhaseAuthorizer); ok {
		switch phase {
		case appendPhaseCredential:
			return tp.AuthorizeAppendCredential(token, path, time.Now()), nil
		case appendPhaseFence:
			return tp.AuthorizeAppendFence(token, path, time.Now())
		}
	}
	return h.AppendAuth.AuthorizeAppend(token, path, time.Now()), nil
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
// mirrors the trust chain: a verified service principal uses its explicit
// action and namespace policy; a token routed to the configured OIDC issuer
// becomes a user principal whose namespace grant must cover the path; anything
// else must be a chronicle read-capability JWS scoped to the path. The
// claim-scoped write token is append-only. It is never consulted here, and
// presented as a Bearer it fails JWS verification (401), never granting a read.
func (h *Handler) readDecision(r *http.Request, rawPath string) auth.Decision {
	path, err := auth.NormalizeStreamPath(rawPath)
	if err != nil {
		return auth.Deny(auth.ReasonForbidden, "invalid stream path")
	}
	if decision, routed := h.serviceDecision(r, path, auth.ActionRead); routed {
		return decision
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
	d := h.mutateDecision(r, rawPath, action)
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
// principal uses its explicit action and namespace policy; an OIDC user's
// namespace grant must cover the path; otherwise the bearer must be a
// chronicle caller token whose namespaces cover the path. Creating or deleting
// a stream is namespace-scoped authority, like linking. Read capabilities and
// write tokens never authorize a mutation,
// and neither does a wake_token (issue #126 TB6b, deliberate): an entity
// acts within its subtree but does not create or destroy entities, so there
// is no agent arm here — a wake-typed bearer falls through to the caller
// verifier and fails its typ pin (401).
func (h *Handler) mutateDecision(r *http.Request, rawPath string, action auth.Action) auth.Decision {
	path, err := auth.NormalizeStreamPath(rawPath)
	if err != nil {
		return auth.Deny(auth.ReasonForbidden, "invalid stream path")
	}
	if decision, routed := h.serviceDecision(r, path, action); routed {
		return decision
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
