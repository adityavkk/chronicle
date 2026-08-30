package webhook

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// Routes is the HTTP surface of the reserved subscription APIs (PROTOCOL §6–7).
// It parses requests, calls the Manager, and writes JSON responses; all business
// logic lives in the Manager.
type Routes struct {
	mgr *Manager
}

// NewRoutes builds the HTTP router for the subscription Manager.
func NewRoutes(mgr *Manager) *Routes { return &Routes{mgr: mgr} }

const subsPrefix = "/__ds/subscriptions/"

// HandleRequest dispatches a reserved __ds request. It returns true when it has
// handled (or claimed) the request — every /__ds/ path is reserved, so unknown
// ones get a 404 rather than falling through to stream handling (PROTOCOL §6).
// Non-__ds paths return false for normal stream handling. The path is the
// stream-root-relative path the chronicle handler sees (leading slash, decoded).
func (rt *Routes) HandleRequest(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path
	if path == "/__ds/jwks.json" {
		rt.handleJWKS(w, r)
		return true
	}
	if strings.HasPrefix(path, subsPrefix) {
		rt.handleSubscription(w, r, strings.TrimPrefix(path, subsPrefix))
		return true
	}
	if path == "/__ds" || strings.HasPrefix(path, "/__ds/") {
		http.NotFound(w, r)
		return true
	}
	return false
}

func (rt *Routes) handleSubscription(w http.ResponseWriter, r *http.Request, rest string) {
	id, action, _ := strings.Cut(rest, "/")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	switch {
	case action == "":
		switch r.Method {
		case http.MethodPut:
			rt.handleCreate(w, r, id)
		case http.MethodGet:
			rt.handleGet(w, id)
		case http.MethodDelete:
			rt.handleDelete(w, r, id)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case action == "streams" && r.Method == http.MethodPost:
		rt.handleAddStreams(w, r, id)
	case strings.HasPrefix(action, "streams/") && r.Method == http.MethodDelete:
		rt.handleRemoveStream(w, r, id, strings.TrimPrefix(action, "streams/"))
	case action == "callback" && r.Method == http.MethodPost:
		rt.handleAckLike(w, r, id)
	case action == "ack" && r.Method == http.MethodPost:
		rt.handleAckLike(w, r, id)
	case action == "claim" && r.Method == http.MethodPost:
		rt.handleClaim(w, r, id)
	case action == "release" && r.Method == http.MethodPost:
		rt.handleRelease(w, r, id)
	default:
		http.NotFound(w, r)
	}
}

func (rt *Routes) handleJWKS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	jwks, err := rt.mgr.JWKS()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/jwk-set+json")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_ = json.NewEncoder(w).Encode(jwks)
}

func (rt *Routes) handleCreate(w http.ResponseWriter, r *http.Request, id string) {
	// Authentication runs before the body is even read (issue #126 TB3): an
	// unauthenticated probe learns nothing about body validation, SSRF rules,
	// or subscription existence. Authorization of the parsed paths follows
	// the 400 parse step but still precedes the SSRF check and every store
	// mutation — the ordering the #126 grounding pass called out.
	caller, authnErr := rt.authenticateCaller(r)
	if authnErr != nil {
		if !rt.controlDeny(w, "create", id, http.StatusUnauthorized, ErrCodeUnauthenticated, authnErr.Error()) {
			return
		}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, ErrCodeInvalidRequest)
		return
	}
	cfg, reason := ParseCreateConfig(body)
	if reason != "" {
		writeErrMsg(w, ErrCodeInvalidRequest, reason)
		return
	}
	if authnErr == nil {
		reason = linkAuthz(caller, auth.ActionSubscribe, cfg)
		if reason == "" {
			reason = linkAuthz(caller, auth.ActionLink, cfg)
		}
		rt.recordServiceAuthorization(caller, "create", reason)
		if reason != "" &&
			!rt.controlDeny(w, "create", id, http.StatusForbidden, ErrCodeForbidden, reason) {
			return
		}
	}
	if cfg.Type == DispatchWebhook {
		if reason := rt.mgr.validateWebhookURL(cfg.WebhookURL); reason != "" {
			writeErrMsg(w, ErrCodeWebhookURLRejected, reason)
			return
		}
	}
	links := rt.mgr.seedLinks(cfg)
	// The owner stamp is the verified caller subject, empty without a
	// credential (insecure mode): ownership begins with the first owned
	// create and is immutable after (see Subscription.OwnerSubject).
	ownerStamp := ""
	if authnErr == nil {
		ownerStamp = caller.Subject()
	}
	status, storedOwner, err := rt.mgr.store.CreateOrConfirmOwned(id, cfg, links, time.Now(), ownerStamp)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// Ownership gate on a pre-existing subscription. Safe after the store
	// call: MATCHED and CONFLICT mutate nothing and the stored owner is read
	// in the same atomic script step. A stranger probing an owned id gets
	// 403 for both the matched and the conflicting config, so the response
	// does not reveal whether their config matched the owner's.
	if status != CreateCreated && authnErr == nil {
		if reason := controlOwnershipAuthz(storedOwner, caller); reason != "" {
			rt.recordServiceAuthorization(caller, "create", reason)
			if !rt.controlDeny(w, "create", id, http.StatusForbidden, ErrCodeForbidden, reason) {
				return
			}
		}
	}
	switch status {
	case CreateConflict:
		writeErr(w, http.StatusConflict, ErrCodeConfigConflict)
		return
	case CreateCreated:
		rt.mgr.backfill(id, cfg)
	case CreateMatched:
		// idempotent re-confirm of an identical config; nothing to backfill.
	}
	sub, ok, err := rt.mgr.store.Get(id)
	if err != nil || !ok {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	code := http.StatusOK
	if status == CreateCreated {
		code = http.StatusCreated
	}
	writeJSON(w, code, BuildSubscriptionView(sub, rt.mgr.signingViewFor(sub)))
}

func (rt *Routes) handleGet(w http.ResponseWriter, id string) {
	sub, ok, err := rt.mgr.store.Get(id)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, ErrCodeNotFound)
		return
	}
	writeJSON(w, http.StatusOK, BuildSubscriptionView(sub, rt.mgr.signingViewFor(sub)))
}

func (rt *Routes) handleDelete(w http.ResponseWriter, r *http.Request, id string) {
	// Same ownership seam as create/add-streams (issue #126 TB3): deleting a
	// subscription is as destructive as re-pointing it. Idempotent-delete
	// semantics are preserved: a missing subscription stays 204 in both
	// modes, so a stranger cannot use delete as an existence probe.
	caller, authnErr := rt.authenticateCaller(r)
	if authnErr != nil {
		if !rt.controlDeny(w, "delete", id, http.StatusUnauthorized, ErrCodeUnauthenticated, authnErr.Error()) {
			return
		}
	}
	var expected SubscriptionExpectation
	hasExpected := false
	var existing Subscription
	if authnErr == nil {
		sub, ok, err := rt.mgr.store.Get(id)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		reason := ""
		if ok {
			existing = sub
			if caller.isService() {
				reason = linkAuthz(caller, auth.ActionSubscribe, sub.Config)
			}
			if reason == "" {
				reason = controlOwnershipAuthz(sub.OwnerSubject, caller)
			}
			expected = subscriptionExpectationFromSubscription(sub)
			hasExpected = true
		} else if decision := caller.authorizeAction(auth.ActionSubscribe); !decision.Allowed() {
			reason = decision.Detail()
		}
		rt.recordServiceAuthorization(caller, "delete", reason)
		if reason != "" &&
			!rt.controlDeny(w, "delete", id, http.StatusForbidden, ErrCodeForbidden, reason) {
			return
		}
	}
	if rt.mgr.authMode == auth.ModeEnforce {
		if !hasExpected {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// Seal before crossing to the control-plane slot: the deleted
		// generation gets a definite last offset, and if the guarded mutation
		// then loses a race it fails closed until the next generation rather
		// than reviving a marker that a delayed request could reuse.
		if err := rt.mgr.sealWriteFences(existing); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		res, err := rt.mgr.store.DeleteAuthorized(id, expected)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if res.Forbidden {
			writeErr(w, http.StatusForbidden, ErrCodeForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !hasExpected {
		var ok bool
		var err error
		existing, ok, err = rt.mgr.store.Get(id)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	if err := rt.mgr.sealWriteFences(existing); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := rt.mgr.store.Delete(id); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (rt *Routes) handleAddStreams(w http.ResponseWriter, r *http.Request, id string) {
	caller, authnErr := rt.authenticateCaller(r)
	if authnErr != nil {
		if !rt.controlDeny(w, "add-streams", id, http.StatusUnauthorized, ErrCodeUnauthenticated, authnErr.Error()) {
			return
		}
	}
	var body struct {
		Streams []string `json:"streams"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, ErrCodeInvalidRequest)
		return
	}
	var expected SubscriptionExpectation
	if authnErr == nil {
		reason := linkPathsAuthz(caller, auth.ActionLink, body.Streams)
		rt.recordServiceAuthorization(caller, "add-streams", reason)
		if reason != "" &&
			!rt.controlDeny(w, "add-streams", id, http.StatusForbidden, ErrCodeForbidden, reason) {
			return
		}
		// Subscription ownership (issue #126 TB3): extending someone else's
		// subscription is the confused-deputy this bullet closes. The read is
		// authorization-only; in enforce mode a missing subscription is 404
		// (today's blind-link behavior is preserved in insecure mode).
		sub, ok, err := rt.mgr.store.Get(id)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if !ok {
			if rt.mgr.authMode == auth.ModeEnforce {
				writeErr(w, http.StatusNotFound, ErrCodeNotFound)
				return
			}
		} else {
			expected = subscriptionExpectationFromSubscription(sub)
			if reason := controlOwnershipAuthz(sub.OwnerSubject, caller); reason != "" {
				rt.recordServiceAuthorization(caller, "add-streams", reason)
				if !rt.controlDeny(w, "add-streams", id, http.StatusForbidden, ErrCodeForbidden, reason) {
					return
				}
			}
		}
	}
	for _, path := range body.Streams {
		path = strings.Trim(path, "/")
		if path == "" {
			continue
		}
		off := rt.mgr.streams.BeginningOffset()
		if tail, ok := rt.mgr.tailOf(path); ok {
			off = tail
		}
		if rt.mgr.authMode == auth.ModeEnforce {
			res, err := rt.mgr.store.LinkAuthorized(id, path, LinkExplicit, off, expected)
			if err != nil {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			if res.NoSub {
				writeErr(w, http.StatusNotFound, ErrCodeNotFound)
				return
			}
			if res.Forbidden {
				writeErr(w, http.StatusForbidden, ErrCodeForbidden)
				return
			}
			addExpectedPath(&expected, path)
			continue
		}
		if err := rt.mgr.store.Link(id, path, LinkExplicit, off); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (rt *Routes) handleRemoveStream(w http.ResponseWriter, r *http.Request, id, path string) {
	caller, authnErr := rt.authenticateCaller(r)
	if authnErr != nil {
		if !rt.controlDeny(w, "remove-stream", id, http.StatusUnauthorized, ErrCodeUnauthenticated, authnErr.Error()) {
			return
		}
	}
	path = strings.Trim(path, "/")
	if authnErr == nil && caller.isService() {
		reason := linkPathsAuthz(caller, auth.ActionLink, []string{path})
		rt.recordServiceAuthorization(caller, "remove-stream", reason)
		if reason != "" &&
			!rt.controlDeny(w, "remove-stream", id, http.StatusForbidden, ErrCodeForbidden, reason) {
			return
		}
	}
	sub, ok, err := rt.mgr.store.Get(id)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if ok && authnErr == nil {
		if reason := controlOwnershipAuthz(sub.OwnerSubject, caller); reason != "" {
			rt.recordServiceAuthorization(caller, "remove-stream", reason)
			if !rt.controlDeny(w, "remove-stream", id, http.StatusForbidden, ErrCodeForbidden, reason) {
				return
			}
		}
	}
	stillGlob := ok && sub.Config.Pattern != "" && GlobMatch(sub.Config.Pattern, path)
	if rt.mgr.authMode == auth.ModeEnforce {
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// A retained glob link is still part of this claim's resource set.
		// Otherwise seal before the cross-slot unlink for fail-closed order.
		if !stillGlob {
			if err := rt.mgr.sealWriteFencePath(sub, path); err != nil {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
		}
		expected := subscriptionExpectationFromSubscription(sub)
		res, err := rt.mgr.store.UnlinkAuthorized(id, path, stillGlob, expected)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if res.Forbidden {
			writeErr(w, http.StatusForbidden, ErrCodeForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if ok && !stillGlob {
		if err := rt.mgr.sealWriteFencePath(sub, path); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}
	if err := rt.mgr.store.Unlink(id, path, stillGlob); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func addExpectedPath(expected *SubscriptionExpectation, path string) {
	for _, existing := range expected.Paths {
		if existing == path {
			return
		}
	}
	expected.Paths = append(expected.Paths, path)
}

// handleAckLike serves both the webhook callback and the pull-wake ack: both are
// Bearer-authenticated, fence on (generation, wake_id), and return
// {ok, next_wake}. A body missing the fenced fields is 400 INVALID_REQUEST; a
// subscription that no longer exists is 410 SUBSCRIPTION_GONE; a present-but-
// stale (generation, wake_id) is 409 FENCED (PROTOCOL §7.1, §7.2).
func (rt *Routes) handleAckLike(w http.ResponseWriter, r *http.Request, id string) {
	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, ErrCodeTokenInvalid)
		return
	}
	now := time.Now()
	tv := ValidateToken(rt.mgr.tokenKey, token, id, now)
	if !tv.Valid {
		rt.writeTokenRejected(w, id, tv, now)
		return
	}
	var req CallbackRequest
	body, err := readJSON(r, &req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, ErrCodeInvalidRequest)
		return
	}
	if missing := missingField(body, "generation", "wake_id"); missing != "" {
		writeErrMsg(w, ErrCodeInvalidRequest, "missing required field: "+missing)
		return
	}
	fenced, gone, nextWake, err := rt.mgr.applyAck(id, req, tv.Generation)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if gone {
		writeErr(w, http.StatusGone, ErrCodeSubscriptionGone)
		return
	}
	if fenced {
		writeErr(w, http.StatusConflict, ErrCodeFenced)
		return
	}
	done := req.Done != nil && *req.Done
	resp := AckResponse{OK: true, NextWake: nextWake}
	// In-band refresh (issue #77): a successful callback whose token is within the
	// refresh threshold of expiry re-mints it and returns it in the "token" field.
	// When not refreshing, Token stays empty and `omitempty` keeps the body
	// byte-identical to {ok,next_wake} — the shape the conformance suite deep-equals.
	if ShouldRefreshToken(tv.Exp, now.Unix(), tokenRefreshThreshold) {
		if fresh, ok := rt.mgr.mintToken(id, tv.Generation, now); ok {
			resp.Token = fresh
		}
	}
	if wt, ok, err := rt.mgr.mintWriteTokenOnAck(id, req.Generation, req.WakeID, done, now); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	} else if ok {
		resp.WriteToken = wt
	}
	// wake_token heartbeat refresh (#123/#126 TB6a): every successful non-done
	// ack of a single-entity subscription re-mints the entity-identity
	// assertion for the fenced (generation, wake_id). Done acks never do, so
	// the conformance flow's ack body shape is untouched.
	if wt, ok := rt.mgr.mintWakeTokenOnAck(id, req.Generation, req.WakeID, done, now); ok {
		resp.WakeToken = wt
	}
	writeJSON(w, http.StatusOK, resp)
}

func (rt *Routes) handleClaim(w http.ResponseWriter, r *http.Request, id string) {
	// Authenticate before parsing or store access. In insecure mode an invalid
	// credential remains telemetry-only for protocol compatibility.
	caller, callerErr := rt.authenticateCaller(r)
	if callerErr != nil &&
		!rt.controlDeny(w, "claim", id, http.StatusUnauthorized, ErrCodeUnauthenticated, callerErr.Error()) {
		return
	}
	var req ClaimRequest
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, ErrCodeInvalidRequest)
		return
	}
	sub, ok, err := rt.mgr.store.Get(id)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, ErrCodeNotFound)
		return
	}
	if callerErr == nil {
		reason := claimAuthz(caller, sub)
		rt.recordServiceAuthorization(caller, "claim", reason)
		if reason != "" &&
			!rt.controlDeny(w, "claim", id, http.StatusForbidden, ErrCodeForbidden, reason) {
			return
		}
	}

	wakeID, err := GenerateWakeID(randReader)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	expected := subscriptionExpectationFromSubscription(sub)
	res, err := rt.mgr.store.ClaimAuthorized(
		id, req.Worker, wakeID, expected, time.Now(), sub.Config.LeaseTTLMs,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	switch {
	case res.Busy:
		writeJSON(w, http.StatusConflict, ErrorBody{Error: ErrorDetail{
			Code: ErrCodeAlreadyClaimed, CurrentHolder: res.Holder, Generation: res.Generation,
		}})
	case res.NoSub:
		writeErr(w, http.StatusNotFound, ErrCodeNotFound)
	case res.Forbidden:
		// The owner, incarnation, config, or linked path set changed after the
		// route authorized it. Enforce mode denies; insecure mode records the
		// denial but still returns a conflict because no lease was granted.
		if rt.controlDeny(w, "claim", id, http.StatusForbidden, ErrCodeForbidden,
			"subscription changed after claim authorization") {
			writeErr(w, http.StatusConflict, ErrCodeFenced)
		}
	case res.Claimed:
		// Re-read links and the committed lease for a fresh cursor snapshot.
		fresh, ok, err := rt.mgr.store.Get(id)
		if err != nil || !ok {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		snap, _ := Snapshot(fresh.Links, rt.mgr.tailOf)
		now := time.Now()
		if err := rt.mgr.grantWriteFences(fresh); err != nil {
			rt.mgr.metrics.AppendFenceGrantFailed("claim")
			_, _, _ = rt.mgr.applyRelease(id, ReleaseRequest{
				Generation: res.Generation,
				WakeID:     res.WakeID,
			}, res.Generation)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		token, err := GenerateToken(rt.mgr.tokenKey, id, res.Generation, now, rt.mgr.tokenTTL(fresh), randReader)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		// The claim is where the write capability is born (issue #126): scope it
		// to exactly the claimed streams and bind it to the live claim fence. The
		// marker above is installed before this token can leave Chronicle.
		writeToken, err := GenerateClaimWriteToken(rt.mgr.tokenKey, id, fresh.Incarnation, res.Generation, res.WakeID, res.Holder, 0, writeScope(snap), now, rt.mgr.writeTokenTTL(fresh), randReader)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		resp := ClaimResponse{
			WakeID:     res.WakeID,
			Generation: res.Generation,
			Token:      token,
			WriteToken: writeToken,
			Streams:    snap,
			LeaseTTLMs: sub.Config.LeaseTTLMs,
		}
		// wake_token (#123/#126 TB6a): the woken entity's identity assertion,
		// minted only when the subscription names a single entity.
		wakeSub := sub
		if fresh.ID != "" {
			wakeSub = fresh
		}
		if wt, ok := rt.mgr.mintWakeTokenFor(wakeSub, res.Generation, res.WakeID, now); ok {
			resp.WakeToken = wt
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func (rt *Routes) handleRelease(w http.ResponseWriter, r *http.Request, id string) {
	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, ErrCodeTokenInvalid)
		return
	}
	now := time.Now()
	tv := ValidateToken(rt.mgr.tokenKey, token, id, now)
	if !tv.Valid {
		rt.writeTokenRejected(w, id, tv, now)
		return
	}
	var req ReleaseRequest
	body, err := readJSON(r, &req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, ErrCodeInvalidRequest)
		return
	}
	if missing := missingField(body, "generation", "wake_id"); missing != "" {
		writeErrMsg(w, ErrCodeInvalidRequest, "missing required field: "+missing)
		return
	}
	fenced, gone, err := rt.mgr.applyRelease(id, req, tv.Generation)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if gone {
		writeErr(w, http.StatusGone, ErrCodeSubscriptionGone)
		return
	}
	if fenced {
		writeErr(w, http.StatusConflict, ErrCodeFenced)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeScope maps a claim's stream snapshot to the normalized paths its write
// token is scoped to. A link that fails normalization is dropped: a path we
// cannot normalize can never be granted (fail closed).
func writeScope(snap []StreamSnapshot) []auth.StreamPath {
	out := make([]auth.StreamPath, 0, len(snap))
	for _, s := range snap {
		if p, err := auth.NormalizeStreamPath(s.Path); err == nil {
			out = append(out, p)
		}
	}
	return out
}

func writeScopeFromLinks(links []StreamLink) []auth.StreamPath {
	out := make([]auth.StreamPath, 0, len(links))
	for _, l := range links {
		if p, err := auth.NormalizeStreamPath(l.Path); err == nil {
			out = append(out, p)
		}
	}
	return out
}

// signingViewFor returns the signing block for webhook subscriptions, nil for
// pull-wake.
func (m *Manager) signingViewFor(sub Subscription) *SigningView {
	if sub.Config.Type != DispatchWebhook {
		return nil
	}
	return m.signingView()
}

// ---- small HTTP helpers ----

func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return "", false
	}
	return strings.TrimPrefix(h, "Bearer "), true
}

func decodeJSON(r *http.Request, v any) error {
	_, err := readJSON(r, v)
	return err
}

// readJSON reads the bounded request body and unmarshals it into v, also
// returning the raw bytes so callers can check field presence (an absent fenced
// field is a 400, distinct from a present-but-zero one that fails the fence as a
// 409). An empty body leaves v at its zero value and returns nil bytes.
func readJSON(r *http.Request, v any) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, nil
	}
	return body, json.Unmarshal(body, v)
}

// missingField reports the first of keys absent from the JSON object in body, or
// "" when all are present. Presence — not zero-value — is what separates a
// malformed control-plane request (400 INVALID_REQUEST) from a well-formed but
// stale one (409 FENCED): {"generation":0,"wake_id":""} is present-but-zero.
func missingField(body []byte, keys ...string) string {
	var obj map[string]json.RawMessage
	if len(body) > 0 {
		_ = json.Unmarshal(body, &obj)
	}
	for _, k := range keys {
		if _, ok := obj[k]; !ok {
			return k
		}
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, errBody(code))
}

// writeTokenRejected turns a failed token validation into its 401. A token that
// is ours and well-formed but past expiry (issue #77) gets the distinct,
// retryable TOKEN_EXPIRED plus a freshly minted token in the body, so a
// heartbeating pull-wake worker can retry at once instead of stalling a lease
// window; a genuinely malformed or foreign token stays a bare TOKEN_INVALID.
func (rt *Routes) writeTokenRejected(w http.ResponseWriter, id string, tv TokenValidation, now time.Time) {
	if !tv.Expired {
		writeErr(w, http.StatusUnauthorized, ErrCodeTokenInvalid)
		return
	}
	body := errBody(ErrCodeTokenExpired)
	if fresh, ok := rt.mgr.mintToken(id, tv.Generation, now); ok {
		body.Token = fresh
	}
	writeJSON(w, http.StatusUnauthorized, body)
}

// writeErrMsg writes a 400 error envelope with a human-readable message. Every
// control-plane use of a message-bearing error is a client-request fault, so the
// status is fixed at 400 (bare-code errors with other statuses use writeErr).
func writeErrMsg(w http.ResponseWriter, code, msg string) {
	writeJSON(w, http.StatusBadRequest, ErrorBody{Error: ErrorDetail{Code: code, Message: msg}})
}
