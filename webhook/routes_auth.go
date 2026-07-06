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

// authorizeClaim is the TB2 gate: it reports whether handleClaim may
// proceed. It runs before any store access, so a denied claim reads nothing
// and mints nothing. In enforce mode a missing or unverifiable credential
// writes the 401 envelope; in the insecure default the decision is recorded
// as telemetry and the request proceeds. The error is operator-facing and
// never carries token material.
func (rt *Routes) authorizeClaim(w http.ResponseWriter, r *http.Request, id string) bool {
	token, ok := bearerToken(r)
	var err error
	if !ok {
		err = errors.New("missing caller credential")
	} else {
		_, err = rt.mgr.verifyCaller(token, time.Now())
	}
	if err == nil {
		return true
	}
	if rt.mgr.authMode == auth.ModeEnforce {
		rt.mgr.log.Warn("claim denied", "subscription", id, "reason", err.Error())
		writeErr(w, http.StatusUnauthorized, ErrCodeUnauthenticated)
		return false
	}
	rt.mgr.log.Info("authz telemetry: claim would be denied",
		"subscription", id, "reason", err.Error())
	return true
}
