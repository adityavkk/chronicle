package webhook

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
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
			rt.handleDelete(w, id)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case action == "streams" && r.Method == http.MethodPost:
		rt.handleAddStreams(w, r, id)
	case strings.HasPrefix(action, "streams/") && r.Method == http.MethodDelete:
		rt.handleRemoveStream(w, id, strings.TrimPrefix(action, "streams/"))
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
	if cfg.Type == DispatchWebhook {
		if reason := rt.mgr.validateWebhookURL(cfg.WebhookURL); reason != "" {
			writeErrMsg(w, ErrCodeWebhookURLRejected, reason)
			return
		}
	}
	links := rt.mgr.seedLinks(cfg)
	status, err := rt.mgr.store.CreateOrConfirm(id, cfg, links, time.Now())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
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

func (rt *Routes) handleDelete(w http.ResponseWriter, id string) {
	if err := rt.mgr.store.Delete(id); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (rt *Routes) handleAddStreams(w http.ResponseWriter, r *http.Request, id string) {
	var body struct {
		Streams []string `json:"streams"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, ErrCodeInvalidRequest)
		return
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
		if err := rt.mgr.store.Link(id, path, LinkExplicit, off); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (rt *Routes) handleRemoveStream(w http.ResponseWriter, id, path string) {
	path = strings.Trim(path, "/")
	sub, ok, err := rt.mgr.store.Get(id)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	stillGlob := ok && sub.Config.Pattern != "" && GlobMatch(sub.Config.Pattern, path)
	if err := rt.mgr.store.Unlink(id, path, stillGlob); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleAckLike serves both the webhook callback and the pull-wake ack: both are
// Bearer-authenticated, fence on (generation, wake_id), and return
// {ok, next_wake} or 409 FENCED (PROTOCOL §7.1, §7.2).
func (rt *Routes) handleAckLike(w http.ResponseWriter, r *http.Request, id string) {
	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, ErrCodeTokenInvalid)
		return
	}
	tv := ValidateToken(rt.mgr.tokenKey, token, id, time.Now())
	if !tv.Valid {
		writeErr(w, http.StatusUnauthorized, ErrCodeTokenInvalid)
		return
	}
	var req CallbackRequest
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, ErrCodeInvalidRequest)
		return
	}
	selection, fencedShard, err := parseRequestClaimShard(req.Shard, tv)
	if err != nil {
		writeErrMsg(w, ErrCodeInvalidRequest, err.Error())
		return
	}
	if fencedShard {
		rt.mgr.metrics.ClaimContention("fenced", id)
		writeErr(w, http.StatusConflict, ErrCodeFenced)
		return
	}
	fenced, gone, nextWake, err := rt.mgr.applyAck(id, selection, req, tv.Generation)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if fenced || gone {
		writeErr(w, http.StatusConflict, ErrCodeFenced)
		return
	}
	writeJSON(w, http.StatusOK, AckResponse{OK: true, NextWake: nextWake})
}

func (rt *Routes) handleClaim(w http.ResponseWriter, r *http.Request, id string) {
	var req ClaimRequest
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, ErrCodeInvalidRequest)
		return
	}
	selection, err := parseOptionalClaimShard(req.Shard)
	if err != nil {
		writeErrMsg(w, ErrCodeInvalidRequest, err.Error())
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
	wakeID, err := GenerateWakeID(randReader)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	res, err := rt.mgr.store.Claim(id, selection.Mode, selection.Shard, req.Worker, wakeID, time.Now(), sub.Config.LeaseTTLMs)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	switch {
	case res.Busy:
		rt.mgr.metrics.ClaimContention("busy", id)
		writeJSON(w, http.StatusConflict, ErrorBody{Error: ErrorDetail{
			Code: ErrCodeAlreadyClaimed, CurrentHolder: res.Holder, Generation: res.Generation,
		}})
	case res.NoSub:
		rt.mgr.metrics.ClaimContention("nosub", id)
		writeErr(w, http.StatusNotFound, ErrCodeNotFound)
	case res.ModeConflict:
		rt.mgr.metrics.ClaimContention("mode_conflict", id)
		writeJSON(w, http.StatusConflict, ErrorBody{Error: ErrorDetail{
			Code: ErrCodeClaimModeConflict, Message: "subscription already uses " + res.Mode.String() + " claim mode",
		}})
	case res.Claimed:
		rt.mgr.metrics.ClaimContention("claimed", id)
		if res.LeaseLapsed {
			rt.mgr.metrics.ClaimContention("lease_lapse", id)
		}
		// Re-read links for a fresh snapshot (tails may have advanced).
		fresh, _, _ := rt.mgr.store.Get(id)
		snap, _ := Snapshot(fresh.Links, rt.mgr.tailOf)
		var shardView *int
		if selection.Sharded() {
			snap = FilterSnapshotForClaimShard(snap, selection.Shard)
			v := selection.Shard.Int()
			shardView = &v
		}
		token, err := GenerateToken(rt.mgr.tokenKey, id, res.Generation, time.Now(), rt.mgr.tokenTTL(sub), randReader)
		if selection.Sharded() {
			token, err = GenerateTokenForShard(rt.mgr.tokenKey, id, selection.Shard, res.Generation, time.Now(), rt.mgr.tokenTTL(sub), randReader)
		}
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, ClaimResponse{
			WakeID:     res.WakeID,
			Generation: res.Generation,
			Shard:      shardView,
			Token:      token,
			Streams:    snap,
			LeaseTTLMs: sub.Config.LeaseTTLMs,
		})
	}
}

func (rt *Routes) handleRelease(w http.ResponseWriter, r *http.Request, id string) {
	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, ErrCodeTokenInvalid)
		return
	}
	tv := ValidateToken(rt.mgr.tokenKey, token, id, time.Now())
	if !tv.Valid {
		writeErr(w, http.StatusUnauthorized, ErrCodeTokenInvalid)
		return
	}
	var req ReleaseRequest
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, ErrCodeInvalidRequest)
		return
	}
	selection, fencedShard, err := parseRequestClaimShard(req.Shard, tv)
	if err != nil {
		writeErrMsg(w, ErrCodeInvalidRequest, err.Error())
		return
	}
	if fencedShard {
		rt.mgr.metrics.ClaimContention("fenced", id)
		writeErr(w, http.StatusConflict, ErrCodeFenced)
		return
	}
	fenced, gone, err := rt.mgr.applyRelease(id, selection, req, tv.Generation)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if fenced || gone {
		writeErr(w, http.StatusConflict, ErrCodeFenced)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, errBody(code))
}

func writeErrMsg(w http.ResponseWriter, code, msg string) {
	writeJSON(w, http.StatusBadRequest, ErrorBody{Error: ErrorDetail{Code: code, Message: msg}})
}

func parseOptionalClaimShard(raw *int) (ClaimShardSelection, error) {
	return NewClaimShardSelection(raw)
}

func parseRequestClaimShard(raw *int, tv TokenValidation) (selection ClaimShardSelection, fenced bool, err error) {
	selection, err = parseOptionalClaimShard(raw)
	if err != nil {
		return ClaimShardSelection{}, false, err
	}
	if selection.Sharded() != tv.Sharded {
		return selection, true, nil
	}
	if selection.Shard != tv.Shard {
		return selection, true, nil
	}
	return selection, false, nil
}
