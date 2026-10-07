package chronicle

import (
	"net/http"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// authorizeSnapshotPublish authorizes publication separately from every
// ordinary data- and control-plane credential. Snapshot publication is always
// enforced, including in telemetry-only AuthMode, because a publisher can
// replace the state image consumed by every reader. Only an explicit service
// policy snapshot-publish grant is accepted. An explicit trusted_gateway keeps
// its intentional delegation of all actions.
func (h *Handler) authorizeSnapshotPublish(r *http.Request, rawPath string) error {
	path, err := auth.NormalizeStreamPath(rawPath)
	if err != nil {
		return denyError(auth.Deny(auth.ReasonForbidden, "invalid stream path"))
	}
	decision, routed := h.serviceDecision(r, path, auth.ActionSnapshotPublish)
	if !routed {
		decision = auth.Deny(auth.ReasonUnauthenticated, "missing snapshot publisher credential")
	}
	if decision.Allowed() {
		return nil
	}
	h.logger().Warn("snapshot-publish denied",
		"path", rawPath, "reason", decision.Reason().String(), "detail", decision.Detail())
	return denyError(decision)
}
