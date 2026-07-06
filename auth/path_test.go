package auth

import (
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func TestNormalizeStreamPath(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"/events/a", "events/a", false},
		{"events/a", "events/a", false},
		{"/a", "a", false},
		{"a-b_c.d/e~f", "a-b_c.d/e~f", false},
		{"...", "...", false}, // three dots is a legal segment; only "." and ".." are traversal
		{"", "", true},
		{"/", "", true},
		// Spellings that would collide distinct store keys are rejected, never
		// silently rewritten: a token scoped to "events/a" must not authorize
		// the distinct store keys "/events/a/" or "/events//a".
		{"/events/a/", "", true},
		{"events//a", "", true},
		{"//events/a", "", true},
		// Traversal segments are rejected outright (§12.2).
		{"../events/a", "", true},
		{"events/../a", "", true},
		{"events/..", "", true},
		{"./events", "", true},
		{"events/./a", "", true},
	}
	for _, c := range cases {
		got, err := NormalizeStreamPath(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("NormalizeStreamPath(%q) err = %v, wantErr %v", c.in, err, c.wantErr)
			continue
		}
		if got.String() != c.want {
			t.Errorf("NormalizeStreamPath(%q) = %q, want %q", c.in, got.String(), c.want)
		}
		if c.wantErr && !got.IsZero() {
			t.Errorf("NormalizeStreamPath(%q) error must return the zero StreamPath", c.in)
		}
	}
}

// TestNormalizeStreamPathProperties: for every input the parser either rejects
// or returns a path that is (a) free of leading slashes and dot segments,
// (b) idempotent under re-normalization, and (c) equal to the input up to the
// single stripped leading slash — normalization never rewrites a path into a
// different store key.
func TestNormalizeStreamPathProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		raw := rapid.String().Draw(t, "raw")
		p, err := NormalizeStreamPath(raw)
		if err != nil {
			return
		}
		s := p.String()
		if s == "" || strings.HasPrefix(s, "/") || strings.HasSuffix(s, "/") {
			t.Fatalf("normalized %q -> %q has boundary slashes", raw, s)
		}
		for _, seg := range strings.Split(s, "/") {
			if seg == "" || seg == "." || seg == ".." {
				t.Fatalf("normalized %q -> %q kept segment %q", raw, s, seg)
			}
		}
		again, err := NormalizeStreamPath(s)
		if err != nil || again != p {
			t.Fatalf("not idempotent: %q -> %q -> (%q, %v)", raw, s, again.String(), err)
		}
		if s != strings.TrimPrefix(raw, "/") {
			t.Fatalf("normalization rewrote %q into %q (must only strip one leading slash)", raw, s)
		}
	})
}
