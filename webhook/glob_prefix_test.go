package webhook

import (
	"strings"
	"testing"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

func TestGlobLiteralPrefix(t *testing.T) {
	cases := []struct {
		pattern string
		prefix  string
		ok      bool
	}{
		{"events/*", "events", true},
		{"events/**", "events", true},
		{"a/b/c", "a/b/c", true},
		{"a/*/c", "a", true},
		{"a/**/c", "a", true},
		{"/a/*", "a", true},
		{"*", "", false},
		{"**", "", false},
		{"*/x", "", false},
		{"**/x", "", false},
		{"", "", false},
		{"%2A/x", "*/x", true},   // escaped literal asterisk segment, then a literal
		{"a/%2a", "a/*", true},   // lowercase escape, decoded
		{"a*b/c", "a*b/c", true}, // '*' inside a longer segment is literal
	}
	for _, c := range cases {
		prefix, ok := GlobLiteralPrefix(c.pattern)
		if prefix != c.prefix || ok != c.ok {
			t.Errorf("GlobLiteralPrefix(%q) = (%q, %v), want (%q, %v)",
				c.pattern, prefix, ok, c.prefix, c.ok)
		}
	}
}

// TestMayLinkPatternSoundness is the load-bearing property: if a pattern is
// authorized against a caller's namespaces, then EVERY path the pattern
// matches is itself namespace-authorized. No pattern is ever authorized
// whose matches could escape the namespace — checked against the real
// GlobMatch, not a model of it.
func TestMayLinkPatternSoundness(t *testing.T) {
	seg := rapid.SampledFrom([]string{"a", "b", "ab", "x1"})
	patSeg := rapid.SampledFrom([]string{"a", "b", "ab", "x1", "*", "**"})

	rapid.Check(t, func(t *rapid.T) {
		nsRaw := rapid.SliceOfN(seg, 1, 2).Draw(t, "nsSegs")
		nsPath, err := auth.NormalizeStreamPath(strings.Join(nsRaw, "/"))
		if err != nil {
			t.Fatalf("generator produced bad namespace: %v", err)
		}
		caller := VerifiedCaller{subject: "p", namespaces: []auth.StreamPath{nsPath}}

		pattern := strings.Join(rapid.SliceOfN(patSeg, 1, 4).Draw(t, "patSegs"), "/")
		if !caller.MayLinkPattern(pattern) {
			return // only the authorized case carries the obligation
		}
		// Generate candidate paths and check every match is inside the namespace.
		for i := 0; i < 20; i++ {
			path := strings.Join(rapid.SliceOfN(seg, 1, 5).Draw(t, "path"), "/")
			p, perr := auth.NormalizeStreamPath(path)
			if perr != nil {
				t.Fatalf("generator produced bad path: %v", perr)
			}
			if GlobMatch(pattern, path) && !caller.MayLink(p) {
				t.Fatalf("pattern %q authorized for ns %v but matches %q outside it",
					pattern, nsRaw, path)
			}
		}
	})
}

func TestLinkAuthz(t *testing.T) {
	caller := VerifiedCaller{subject: "u:1", namespaces: []auth.StreamPath{
		mustPath(t, "events"),
	}}

	cases := []struct {
		name string
		cfg  Config
		deny bool
	}{
		{"all inside", Config{Streams: []string{"events/a"}, Pattern: "events/*", WakeStream: "events/wake"}, false},
		{"stream outside", Config{Streams: []string{"victim/a"}}, true},
		{"one of many outside", Config{Streams: []string{"events/a", "victim/b"}}, true},
		{"pattern escapes", Config{Pattern: "victim/*"}, true},
		{"pattern unbounded", Config{Pattern: "*"}, true},
		{"pattern unbounded deep", Config{Pattern: "**/x"}, true},
		{"wake_stream at victim", Config{Streams: []string{"events/a"}, WakeStream: "victim/inbox"}, true},
		{"unnormalizable stream", Config{Streams: []string{".."}}, true},
		{"empty config allowed", Config{}, false}, // structural validity is ValidateConfig's job
	}
	for _, c := range cases {
		reason := linkAuthz(caller, c.cfg)
		if (reason != "") != c.deny {
			t.Errorf("%s: linkAuthz = %q, want deny=%v", c.name, reason, c.deny)
		}
	}
}

func TestOwnershipAuthz(t *testing.T) {
	cases := []struct {
		owner, caller string
		deny          bool
	}{
		{"", "u:1", false},    // ownerless: migration posture
		{"u:1", "u:1", false}, // owner operates
		{"u:1", "u:2", true},  // stranger blocked
		{"u:1", "", true},     // empty subject never matches an owner
	}
	for _, c := range cases {
		reason := ownershipAuthz(c.owner, c.caller)
		if (reason != "") != c.deny {
			t.Errorf("ownershipAuthz(%q, %q) = %q, want deny=%v", c.owner, c.caller, reason, c.deny)
		}
	}
}
