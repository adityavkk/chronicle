package chronicle

import (
	"errors"
	"net/http"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// This file is the write-fencing half of the append authorization seam
// (#183, docs/spec/WRITE-FENCING.md): the write-token carriers and their
// fail-closed presentation rule, the Write-Fence class assertion, the write
// class a request takes on a fenced stream, the phase-2 fence of the fenced
// class, and the disclosure a fence rejection carries on the wire. Streams that
// never opt in take exactly the pre-#183 path: phase 1 and phase 2 route the
// credential families in today's order and classifyFencedStream never runs.

// appendCredentialFamily is the credential family phase 1 routed a request to.
type appendCredentialFamily int

const (
	// familyNone routed nothing: the stream path could not be normalized.
	familyNone appendCredentialFamily = iota
	// familyService is a verified service principal (bearer or XFCC).
	familyService
	// familyAgent is a wake-typed Bearer (a woken entity acting as itself).
	familyAgent
	// familyClaim fell through to AppendAuth: the claim-scoped write token.
	familyClaim
)

// appendCredential is what phase 1 learned about a request before any store
// access, carried to the class decision and the phase-2 fence.
type appendCredential struct {
	family    appendCredentialFamily
	path      auth.StreamPath // the normalized store path phase 1 authorized
	decision  auth.Decision   // the phase-1 decision, enforced or logged per AuthMode
	token     string          // the write-token carrier value; "" when none or malformed
	malformed bool            // a carrier was presented duplicated or empty
	declared  bool            // Write-Fence: true on POST
}

// presented reports whether the request carries a write-token credential: a
// value, or a carrier that failed the presentation rule and can never verify.
func (c appendCredential) presented() bool { return c.token != "" || c.malformed }

// writeClass is the server-derived class of a POST on a fenced stream.
type writeClass int

const (
	// writeClassOpen is the base §5.2/§5.2.1 write of a verified principal; it
	// is also the class of every write to a stream that never opted in.
	writeClassOpen writeClass = iota
	// writeClassFenced is a write under the claim-scoped write token: producer
	// headers mandatory, Producer-Epoch bound to the claim generation, and the
	// marker, seal, and binding checks run in the stream slot.
	writeClassFenced
)

// Disclosure reasons of the fence rejections decided before the store call.
// The in-slot reasons are store.FenceReason's values verbatim.
const (
	reasonCredential = "credential"
	reasonShard      = "shard"
	reasonPrincipal  = "principal"
	reasonWakeToken  = "wake_token"
	reasonPrecheck   = "precheck"
	reasonStore      = "store"
)

// fenceDisclosure is what a fence rejection reveals to the writer so it stands
// down without a read (design §E.5): the reason, the current generation and
// holder when the stream slot knows them, and — when the request carried
// producer headers — the terminal gap pair Producer-Expected-Seq ==
// Producer-Received-Seq == ReqSeq that the pinned Electric producer treats as
// a clean stop. It also drives the single rejection counter.
type fenceDisclosure struct {
	Reason      string // ErrorDetail.Reason
	Generation  int64  // ErrorDetail.Generation and the Producer-Epoch echo; 0 = unknown, omitted
	Holder      string // ErrorDetail.CurrentHolder; "" = omitted
	HasProducer bool   // the request carried all three producer headers
	ReqSeq      int64  // Producer-Seq as sent
}

// presentedWriteToken returns the write-token carrier value in carrier order:
// Write-Token, then electric-claim-token, then Authorization: Bearer unless
// phase 1 consumed the bearer as a service or wake credential. A duplicated or
// empty Write-Token or electric-claim-token is presented-but-malformed: it is
// reported as such and never falls through to the next carrier.
func presentedWriteToken(r *http.Request, fam appendCredentialFamily) (token string, malformed bool) {
	for _, name := range []string{WriteTokenHeader, ClaimTokenHeader} {
		if values := r.Header.Values(name); len(values) > 0 {
			if len(values) > 1 || values[0] == "" {
				return "", true
			}
			return values[0], false
		}
	}
	if fam == familyService || fam == familyAgent {
		return "", false
	}
	return bearerFromRequest(r), false
}

// credentialReason classifies a token-arm denial for the disclosure: the
// shard rule (design A.0 Q9) is reported as such, every other refusal of a
// presented token as a credential failure.
func credentialReason(d auth.Decision) string {
	if d.Detail() == webhook.DetailWriteTokenShard {
		return reasonShard
	}
	return reasonCredential
}

// authorizeAppendCredential is phase 1 of the append gate: it runs before the
// stream lookup, touches no store, and reveals no stream existence. Routing is
// byte-for-byte today's for an undeclared request. Two header-only rules bind
// regardless of AuthMode: a POST asserting Write-Fence: true must present a
// write token, and when it does, the token must verify alongside whatever
// principal routed (both valid); a write token minted for a shard other than 0
// is refused on every stream (A.0 Q9).
func (h *Handler) authorizeAppendCredential(r *http.Request, rawPath string) (appendCredential, error) {
	path, fam, d := h.routeAppend(r, rawPath)
	cred := appendCredential{
		family:   fam,
		path:     path,
		declared: r.Header.Get(protocol.HeaderWriteFence) == "true",
	}
	cred.token, cred.malformed = presentedWriteToken(r, fam)
	reason := "" // disclosure of a token-arm denial; "" keeps today's plain envelope
	switch {
	case cred.declared && !cred.presented():
		d, reason = auth.Deny(auth.ReasonUnauthenticated, "fenced write requires a write token"), reasonCredential
	case fam == familyClaim || (cred.declared && d.Allowed()):
		d, _ = h.tokenDecision(cred.token, cred.malformed, path, appendPhaseCredential)
		if cred.presented() {
			reason = credentialReason(d)
		}
	}
	cred.decision = d
	if d.Allowed() {
		return cred, nil
	}
	if !h.enforceOrTelemetry(rawPath, "append credential", d, cred.declared || reason == reasonShard) {
		return cred, nil
	}
	if reason == "" {
		return cred, denyError(d)
	}
	return cred, fenceDenied(d, reason)
}

// classifyFencedStream decides the write class of a request on a fenced stream
// (design §E.3), after the stream lookup and before any mutation. The class is
// fenced iff the request presented a write-token carrier or asserted the class;
// the fence semantics then bind in every AuthMode, so a phase-1 token denial
// that insecure mode only logged is enforced here, and a token riding with a
// routed principal must verify too. Otherwise the class is open: a verified
// service principal writes under its policy, a wake token never writes a
// fenced stream, and an anonymous write needs a principal per AuthMode.
func (h *Handler) classifyFencedStream(rawPath string, cred appendCredential) (writeClass, error) {
	if cred.presented() || cred.declared {
		d := cred.decision
		if cred.family != familyClaim && d.Allowed() && !cred.declared {
			// A routed principal with a token: on an unfenced stream the token is
			// passed through unread, so the token arm has not run yet.
			d, _ = h.tokenDecision(cred.token, cred.malformed, cred.path, appendPhaseCredential)
		}
		if !d.Allowed() {
			h.enforceOrTelemetry(rawPath, "fenced write credential", d, true)
			return 0, fenceDenied(d, credentialReason(d))
		}
		return writeClassFenced, nil
	}
	switch cred.family {
	case familyAgent:
		d := auth.Deny(auth.ReasonForbidden, "wake token cannot write to a fenced stream")
		h.enforceOrTelemetry(rawPath, "fenced stream open class", d, true)
		return 0, fenceDenied(d, reasonWakeToken)
	case familyService:
		// Phase 1 enforced or logged the service decision per AuthMode; a
		// denial reaching here is insecure-mode telemetry, and the open class
		// follows AuthMode.
		return writeClassOpen, nil
	default:
		d := auth.Deny(auth.ReasonUnauthenticated, "fenced stream requires an authenticated principal")
		if h.enforceOrTelemetry(rawPath, "fenced stream open class", d, false) {
			return 0, fenceDenied(d, reasonPrincipal)
		}
		return writeClassOpen, nil
	}
}

// authorizeAppendFence is phase 2 of the append gate, run immediately before
// the mutation. The open class (and every write to a stream that never opted
// in) takes today's path under AuthMode. The fenced class is the token's
// alone — no service or agent routing — and binds in every mode: the phase-2
// check must produce the in-slot fence, so an authorizer that cannot (no
// two-phase authorizer, no control-plane store, or a non-atomic stream store)
// is a rejection, never a silent downgrade to an unfenced write.
func (h *Handler) authorizeAppendFence(r *http.Request, rawPath string, cred appendCredential, class writeClass) (*auth.AppendFence, error) {
	if class != writeClassFenced {
		return h.authorizeAppendPhase(r, rawPath, appendPhaseFence, "append fence", false)
	}
	d, fence := h.tokenDecision(cred.token, cred.malformed, cred.path, appendPhaseFence)
	var reason string
	switch {
	case !d.Allowed() && d.Reason() == auth.ReasonFenced:
		reason = reasonPrecheck
	case !d.Allowed():
		reason = credentialReason(d)
	case fence == nil:
		d, reason = auth.Deny(auth.ReasonFenced, "atomic append fence unavailable"), reasonStore
	default:
		return fence, nil
	}
	h.enforceOrTelemetry(rawPath, "fenced write", d, true)
	return nil, fenceDenied(d, reason)
}

// fenceDenied maps a fence denial decided before the store call to its wire
// error: denyError's status mapping plus the disclosure reason. No generation,
// holder, or epoch echo — nothing in the stream slot was consulted.
func fenceDenied(d auth.Decision, reason string) *authError {
	e := denyError(d)
	e.fence = &fenceDisclosure{Reason: reason}
	return e
}

// fencedError maps an in-slot fence rejection (store.ErrAppendFenced) to the
// 409 FENCED envelope with full disclosure (design §B.4): the reason and
// message, the current generation and live holder the store reported, and the
// request's producer headers for the terminal gap pair.
func fencedError(reason store.FenceReason, generation int64, holder string, hasProducer bool, reqSeq int64) *authError {
	return &authError{
		status: http.StatusConflict,
		code:   errCodeFenced,
		msg:    fenceMessage(reason),
		fence: &fenceDisclosure{
			Reason:      string(reason),
			Generation:  generation,
			Holder:      holder,
			HasProducer: hasProducer,
			ReqSeq:      reqSeq,
		},
	}
}

// fenceMessage is the operator-facing message of each in-slot fence reason.
func fenceMessage(reason store.FenceReason) string {
	switch reason {
	case store.FenceSealed:
		return "claim generation is sealed"
	case store.FenceProducerRequired:
		return "fenced write requires producer headers"
	case store.FenceEpoch:
		return "producer epoch must equal the claim generation"
	case store.FenceBound:
		return "producer is bound to the write fence"
	case store.FenceMarker, store.FenceNone:
		return "write token claim is fenced"
	default:
		return "write token claim is fenced"
	}
}

// producerDisclosure attaches the request's producer headers to a fence
// rejection raised before the store call, so its 409 carries the terminal pair
// exactly as an in-slot one does. Every other error passes through untouched.
func producerDisclosure(err error, hasProducer bool, reqSeq int64) error {
	var authErr *authError
	if errors.As(err, &authErr) && authErr.fence != nil {
		authErr.fence.HasProducer, authErr.fence.ReqSeq = hasProducer, reqSeq
	}
	return err
}

// countFenceRejection records one fence rejection. Every rejection that leaves
// the handler as an error counts in writeError, the single sink; the one
// plaintext 400 (producer headers missing on the fenced class) counts here.
func (h *Handler) countFenceRejection(reason string) {
	if h.FenceMetrics != nil {
		h.FenceMetrics.AppendFenceRejection(reason)
	}
}
