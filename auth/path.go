package auth

import (
	"errors"
	"fmt"
	"strings"
)

// StreamPath is a normalized stream-root-relative path ("events/abc"): no
// leading slash, no empty, ".", or ".." segments. It is the form subscription
// links and token scopes use (PROTOCOL §6 wire paths). Build one only via
// NormalizeStreamPath, so a raw request path can never reach a scope
// comparison unnormalized (§12.2).
type StreamPath struct{ s string }

// NormalizeStreamPath parses a request or store path ("/events/abc" or
// "events/abc") into a StreamPath. At most one leading slash is stripped;
// anything that would make two spellings of different store keys compare
// equal — a trailing slash, a doubled slash, a "." or ".." segment — is
// rejected outright, so traversal never reaches a scope comparison and a
// token scoped to one store key can never authorize another (§12.2).
func NormalizeStreamPath(raw string) (StreamPath, error) {
	p := strings.TrimPrefix(raw, "/")
	if p == "" {
		return StreamPath{}, errors.New("empty stream path")
	}
	for _, seg := range strings.Split(p, "/") {
		switch seg {
		case "", ".", "..":
			return StreamPath{}, fmt.Errorf("invalid stream path segment %q", seg)
		}
	}
	return StreamPath{s: p}, nil
}

func (p StreamPath) String() string { return p.s }

// PathWithinPrefixes reports whether path falls under one of the given
// namespace prefixes. Matching is whole-segment: prefix "events" covers
// "events/a" and "events" itself, never "eventsx". This is the single
// (principal, path) scope predicate — the control-plane link check, the
// read capability, and the OIDC user namespaces all evaluate through it,
// so their semantics can never drift apart.
func PathWithinPrefixes(path StreamPath, prefixes []StreamPath) bool {
	p := path.String()
	for _, pre := range prefixes {
		n := pre.String()
		if p == n || strings.HasPrefix(p, n+"/") {
			return true
		}
	}
	return false
}

// IsZero reports whether p was never built by NormalizeStreamPath.
func (p StreamPath) IsZero() bool { return p.s == "" }
