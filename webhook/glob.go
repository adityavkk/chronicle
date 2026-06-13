package webhook

import "strings"

// GlobMatch matches a stream-root-relative path against a subscription glob
// pattern. Per PROTOCOL §6.2: "*" matches exactly one path segment and "**"
// matches zero or more path segments. Verbatim port of the Caddy plugin's
// webhook/glob.go (the pattern grammar is part of the wire contract).
func GlobMatch(pattern, path string) bool {
	patternParts := splitPath(pattern)
	pathParts := splitPath(path)
	return matchParts(patternParts, 0, pathParts, 0)
}

func splitPath(p string) []string {
	p = strings.TrimLeft(p, "/")
	p = strings.TrimRight(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

func matchParts(pattern []string, pi int, path []string, si int) bool {
	for pi < len(pattern) && si < len(path) {
		seg := pattern[pi]

		if seg == "**" {
			for i := si; i <= len(path); i++ {
				if matchParts(pattern, pi+1, path, i) {
					return true
				}
			}
			return false
		}

		if seg == "*" {
			pi++
			si++
			continue
		}

		// Literal match (handle %2A as *).
		decoded := strings.ReplaceAll(seg, "%2A", "*")
		decoded = strings.ReplaceAll(decoded, "%2a", "*")
		if decoded != path[si] {
			return false
		}
		pi++
		si++
	}

	// Trailing ** matches zero remaining segments.
	for pi < len(pattern) && pattern[pi] == "**" {
		pi++
	}

	return pi == len(pattern) && si == len(path)
}
