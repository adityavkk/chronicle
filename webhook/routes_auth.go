package webhook

import (
	"errors"
	"net/http"
	"strings"
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

type controlCaller struct {
	caller           VerifiedCaller
	servicePrincipal auth.Principal
	serviceAccess    *auth.ServiceAccess
}

func (c controlCaller) Subject() string {
	if c.servicePrincipal.Kind() == auth.KindService {
		return c.servicePrincipal.Subject()
	}
	return c.caller.Subject()
}

func (c controlCaller) authorize(action auth.Action, paths ...auth.StreamPath) auth.Decision {
	if c.servicePrincipal.Kind() == auth.KindService {
		decision, _ := c.serviceAccess.Authorize(c.servicePrincipal, action, paths...)
		return decision
	}
	for _, path := range paths {
		if !c.caller.MayLink(path) {
			return auth.Deny(auth.ReasonForbidden, "caller namespaces do not cover this stream")
		}
	}
	return auth.Allow()
}

func (c controlCaller) authorizeAction(action auth.Action) auth.Decision {
	if c.servicePrincipal.Kind() == auth.KindService {
		decision, _ := c.serviceAccess.AuthorizeAction(c.servicePrincipal, action)
		return decision
	}
	return auth.Allow()
}

func (c controlCaller) trustedGateway() bool {
	return c.servicePrincipal.Kind() == auth.KindService &&
		c.serviceAccess.TrustedGateway(c.servicePrincipal)
}

func (c controlCaller) isService() bool {
	return c.servicePrincipal.Kind() == auth.KindService
}

// authenticateCaller resolves mesh/static-bearer service identity first, then
// falls back to the Chronicle caller-token family. XFCC rejection is terminal:
// it can never downgrade to a bearer credential.
func (rt *Routes) authenticateCaller(r *http.Request) (controlCaller, error) {
	token, _ := bearerToken(r)
	if access := rt.mgr.serviceAccess; access != nil {
		joinedXFCC := strings.Join(r.Header.Values("X-Forwarded-Client-Cert"), ",")
		marker := ""
		if values := r.Header.Values(access.SidecarMarkerName); len(values) == 1 {
			marker = values[0]
		}
		principal, status := access.Authenticate(token, joinedXFCC, marker)
		switch status {
		case auth.ServiceRejected:
			rt.mgr.metrics.ServiceAuthenticationFailure()
			return controlCaller{}, errors.New("invalid service identity")
		case auth.ServiceAuthenticated:
			return controlCaller{servicePrincipal: principal, serviceAccess: access}, nil
		case auth.ServiceNotAttempted:
			// Fall through to the caller-token credential family.
		}
	}
	if token == "" {
		return controlCaller{}, errors.New("missing caller credential")
	}
	caller, err := rt.mgr.verifyCaller(token, time.Now())
	if err != nil {
		return controlCaller{}, err
	}
	return controlCaller{caller: caller}, nil
}

func (rt *Routes) recordServiceAuthorization(caller controlCaller, op string, reason string) {
	if !caller.isService() {
		return
	}
	if reason != "" {
		rt.mgr.metrics.ServiceAuthorizationFailure()
		return
	}
	if caller.trustedGateway() {
		rt.mgr.metrics.ServiceDelegatedGateway()
		rt.mgr.log.Info("trusted gateway service authorized",
			"operation", op, "subject", caller.Subject())
	}
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
