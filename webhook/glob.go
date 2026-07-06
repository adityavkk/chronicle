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

// GlobLiteralPrefix returns the pattern's leading literal segments — the
// fixed path prefix every possible match of the pattern lies under — and
// whether the pattern has any. Only the exact segments "*" and "**" are
// wildcards (PROTOCOL §6.2); every other segment matches literally, with
// %2A/%2a decoding to a literal "*". A pattern that opens with a wildcard
// segment ("*", "**/x") has no literal prefix: its matches are unbounded, so
// ok is false. This bound is what makes namespace authorization of a pattern
// sound (issue #126 TB3): covering the prefix covers every match.
func GlobLiteralPrefix(pattern string) (prefix string, ok bool) {
	parts := splitPath(pattern)
	lits := make([]string, 0, len(parts))
	for _, seg := range parts {
		if seg == "*" || seg == "**" {
			break
		}
		decoded := strings.ReplaceAll(seg, "%2A", "*")
		decoded = strings.ReplaceAll(decoded, "%2a", "*")
		lits = append(lits, decoded)
	}
	if len(lits) == 0 {
		return "", false
	}
	return strings.Join(lits, "/"), true
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
