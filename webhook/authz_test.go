package webhook

import (
	"crypto/rand"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// TestAuthorizeAppendMapping pins the capability→Decision mapping the HTTP
// seam enforces: only a verified, unexpired, in-scope token allows; a missing
// or unverifiable credential is unauthenticated (401); a verified credential
// outside its scope is forbidden (403).
func TestAuthorizeAppendMapping(t *testing.T) {
	key := testTokenKey(t)
	now := time.Unix(1000, 0)
	pathA := mustPath(t, "events/a")
	pathB := mustPath(t, "events/b")

	tok, err := GenerateWriteToken(key, "sub-1", 1, []auth.StreamPath{pathA}, now, time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	az := NewWriteTokenAuthorizer(key)

	cases := []struct {
		name       string
		token      string
		path       auth.StreamPath
		at         time.Time
		wantAllow  bool
		wantReason auth.DenyReason
	}{
		{"valid in scope", tok, pathA, now, true, auth.ReasonNone},
		{"missing token", "", pathA, now, false, auth.ReasonUnauthenticated},
		{"garbage token", "not-a-token", pathA, now, false, auth.ReasonUnauthenticated},
		{"wrong path", tok, pathB, now, false, auth.ReasonForbidden},
		{"expired", tok, pathA, now.Add(2 * time.Minute), false, auth.ReasonUnauthenticated},
	}
	for _, c := range cases {
		d := az.AuthorizeAppend(c.token, c.path, c.at)
		if d.Allowed() != c.wantAllow || d.Reason() != c.wantReason {
			t.Errorf("%s: allowed=%v reason=%v, want allowed=%v reason=%v",
				c.name, d.Allowed(), d.Reason(), c.wantAllow, c.wantReason)
		}
	}
}
