package webhook

import (
	"crypto/ed25519"
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
// signing keys the JWKS currently publishes — the same trust set webhook
// receivers verify against. A store error fails closed (no keys, no entry).
func (m *Manager) verifyCaller(token string, now time.Time) (VerifiedCaller, error) {
	keys, err := m.store.SigningKeys()
	if err != nil {
		return VerifiedCaller{}, errors.New("caller token: key set unavailable")
	}
	keyFor := func(kid string) (ed25519.PublicKey, bool) {
		for _, k := range keys {
			if k.Kid == kid {
				return k.Public, true
			}
		}
		return nil, false
	}
	return ValidateCallerToken(token, m.streamRootURL, keyFor, now)
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
