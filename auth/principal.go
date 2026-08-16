package auth

import "time"

// PrincipalKind enumerates who can act, aligned with Electric's
// user/agent/service/system vocabulary so the trusted-backend topology maps
// one-to-one (issue #126).
type PrincipalKind int

const (
	// KindAnonymous is the zero value: no verified identity.
	KindAnonymous PrincipalKind = iota
	// KindAgent is a Durable Streams entity authenticated by a wake_token (#123).
	KindAgent
	// KindUser is an enterprise human authenticated by the IdP (PingFed OIDC).
	KindUser
	// KindService is a trusted workload, e.g. the Electric agents-server in
	// trusted-backend mode.
	KindService
	// KindSystem is chronicle's own internal work; it never arrives on the wire.
	KindSystem
)

func (k PrincipalKind) String() string {
	switch k {
	case KindAnonymous:
		return "anonymous"
	case KindAgent:
		return "agent"
	case KindUser:
		return "user"
	case KindService:
		return "service"
	case KindSystem:
		return "system"
	default:
		return "unknown"
	}
}

// Principal is a verified caller identity. The zero value is anonymous;
// non-anonymous values are built only by verifying constructors, which land
// with the first wire credential that carries an identity (TB4: the service
// principal). A non-anonymous Principal in hand is proof its credential
// verified — raw header strings never reach an authorization decision.
type Principal struct {
	kind       PrincipalKind
	subject    string
	namespaces []StreamPath
}

// Kind is the principal's category (anonymous/agent/user/service/system).
func (p Principal) Kind() PrincipalKind { return p.kind }

// Subject is the verified identity string: an entity path for an agent, an
// OIDC sub for a user, a service name for a service. Empty for anonymous.
func (p Principal) Subject() string { return p.subject }

// IsAnonymous reports whether no identity was verified.
func (p Principal) IsAnonymous() bool { return p.kind == KindAnonymous }

// Namespaces returns the normalized namespace prefixes carried by the
// principal itself, as a defensive copy. User principals carry these from the
// IdP. Service namespaces remain in the explicit service policy so identity
// verification cannot imply authority.
func (p Principal) Namespaces() []StreamPath {
	out := make([]StreamPath, len(p.namespaces))
	copy(out, p.namespaces)
	return out
}

// NamespaceCovers reports whether the principal's namespace grant covers
// path (whole-segment prefix semantics, PathWithinPrefixes).
func (p Principal) NamespaceCovers(path StreamPath) bool {
	return PathWithinPrefixes(path, p.namespaces)
}

// Authorizer is the single authorization decision point (issue #126): may p
// perform a on the stream at path? Implementations must fail closed — any
// internal error is a Deny, never an Allow — and stay clock-free (now is
// passed in) so decisions are deterministic under test.
//
// TB1 enforces the thinner claim-token capability check on appends; this
// interface is the seam later bullets widen with real principals (service,
// user, agent) and the remaining actions.
type Authorizer interface {
	Authorize(p Principal, path StreamPath, a Action, now time.Time) Decision
}
