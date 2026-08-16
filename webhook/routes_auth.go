package webhook

import (
	"errors"
	"net/http"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// Control-plane authorization gates (issue #126). TB2 gates claim — the
// route that mints the callback and write tokens — on an authenticated
// caller; TB3 widens the same seam to create/add-streams with the namespace
// check. Enforcement follows the shared AuthMode: the insecure default only
// logs would-be denials, so a base-protocol client is never broken until an
// operator opts in.

// verifyCaller authenticates a control-plane caller credential against the
// webhook-family verification set in the current key snapshot — the same
// custody-source-fed set the JWKS serves (rotation overlap and the kid
// denylist apply here by construction, #123).
func (m *Manager) verifyCaller(token string, now time.Time) (VerifiedCaller, error) {
	return ValidateCallerToken(token, m.streamRootURL, m.callerKidResolver(now), now)
}

// authenticateCaller extracts and verifies the control-plane caller
// credential from the request. The error is operator-facing and carries no
// token material; the zero VerifiedCaller accompanies every error.
func (rt *Routes) authenticateCaller(r *http.Request) (VerifiedCaller, error) {
	token, ok := bearerToken(r)
	if !ok {
		return VerifiedCaller{}, errors.New("missing caller credential")
	}
	return rt.mgr.verifyCaller(token, time.Now())
}

// controlDeny applies AuthMode to a control-plane denial on op/id: in
// enforce mode it writes the error envelope and reports stop (false); in the
// insecure default it records the would-be denial as telemetry and reports
// proceed (true), keeping the base contract byte-identical.
func (rt *Routes) controlDeny(w http.ResponseWriter, op, id string, status int, code, reason string) bool {
	if rt.mgr.authMode == auth.ModeEnforce {
		rt.mgr.log.Warn(op+" denied", "subscription", id, "reason", reason)
		writeErr(w, status, code)
		return false
	}
	rt.mgr.log.Info("authz telemetry: "+op+" would be denied",
		"subscription", id, "reason", reason)
	return true
}
