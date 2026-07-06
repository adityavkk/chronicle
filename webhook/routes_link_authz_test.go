package webhook

import (
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// TB3 route matrix (issue #126): create/add-streams require a caller and
// enforce (principal, path) at link time; delete/remove-stream enforce
// ownership. Every deny asserts the store is unmutated.

func callerFor(t *testing.T, mgr *Manager, subject string, ns ...string) string {
	t.Helper()
	tok, err := GenerateCallerToken(mgr.activeSigningKey(), mgr.streamRootURL, subject, ns, time.Now(), time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func pullWakeBody(wakeStream string, streams ...string) string {
	b, _ := json.Marshal(map[string]any{
		"type":        "pull-wake",
		"streams":     streams,
		"wake_stream": wakeStream,
	})
	return string(b)
}

func doCreate(t *testing.T, rt *Routes, id, cred, body string) *httptest.ResponseRecorder {
	t.Helper()
	return doDS(t, rt, http.MethodPut, subsPrefix+id, cred, body)
}

func TestCreateEnforceMatrix(t *testing.T) {
	mgr, store, _ := newAuthTestManager(t, auth.ModeEnforce)
	rt := NewRoutes(mgr)
	owner := callerFor(t, mgr, "u:owner", "events")

	t.Run("in-namespace create is 201 and stamped", func(t *testing.T) {
		rec := doCreate(t, rt, "s-ok", owner, pullWakeBody("events/wake", "events/a"))
		if rec.Code != http.StatusCreated {
			t.Fatalf("create = %d, body %q", rec.Code, rec.Body.String())
		}
		sub, ok, _ := store.Get("s-ok")
		if !ok || sub.OwnerSubject != "u:owner" {
			t.Fatalf("owner stamp = %q ok=%v, want u:owner", sub.OwnerSubject, ok)
		}
	})

	denies := []struct {
		name     string
		id       string
		cred     string
		body     string
		wantCode int
		wantErr  string
	}{
		{"unauthenticated", "s-d1", "", pullWakeBody("events/wake", "events/a"), http.StatusUnauthorized, ErrCodeUnauthenticated},
		{"garbage credential", "s-d2", "nope", pullWakeBody("events/wake", "events/a"), http.StatusUnauthorized, ErrCodeUnauthenticated},
		{"stream outside namespace", "s-d3", "OWNER", pullWakeBody("events/wake", "victim/a"), http.StatusForbidden, ErrCodeForbidden},
		{"pattern escapes namespace", "s-d4", "OWNER", `{"type":"pull-wake","pattern":"victim/*","wake_stream":"events/wake"}`, http.StatusForbidden, ErrCodeForbidden},
		{"pattern unbounded", "s-d5", "OWNER", `{"type":"pull-wake","pattern":"*","wake_stream":"events/wake"}`, http.StatusForbidden, ErrCodeForbidden},
		{"wake_stream at victim", "s-d6", "OWNER", pullWakeBody("victim/inbox", "events/a"), http.StatusForbidden, ErrCodeForbidden},
	}
	for _, c := range denies {
		t.Run(c.name, func(t *testing.T) {
			cred := c.cred
			if cred == "OWNER" {
				cred = owner
			}
			rec := doCreate(t, rt, c.id, cred, c.body)
			if rec.Code != c.wantCode {
				t.Fatalf("status = %d, body %q; want %d", rec.Code, rec.Body.String(), c.wantCode)
			}
			if code := errCodeOf(t, rec); code != c.wantErr {
				t.Fatalf("code = %s, want %s", code, c.wantErr)
			}
			if _, ok, _ := store.Get(c.id); ok {
				t.Fatal("denied create reached the store")
			}
		})
	}

	t.Run("authn precedes parse", func(t *testing.T) {
		rec := doCreate(t, rt, "s-d7", "", `{not json`)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated malformed body = %d, want 401 (no parse info leak)", rec.Code)
		}
	})
	t.Run("parse 400 preserved after authn", func(t *testing.T) {
		rec := doCreate(t, rt, "s-d8", owner, `{not json`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("authenticated malformed body = %d, want 400", rec.Code)
		}
	})
	t.Run("authz precedes SSRF", func(t *testing.T) {
		// Out-of-namespace webhook config with an SSRF-rejected URL: the 403
		// must win — authorization runs before the SSRF classifier.
		body := `{"type":"webhook","streams":["victim/a"],"webhook":{"url":"http://10.0.0.1/hook"}}`
		rec := doCreate(t, rt, "s-d9", owner, body)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403 before SSRF", rec.Code)
		}
		// Control: same URL inside the namespace still gets the SSRF 400.
		body = `{"type":"webhook","streams":["events/a"],"webhook":{"url":"http://10.0.0.1/hook"}}`
		rec = doCreate(t, rt, "s-d10", owner, body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("in-ns SSRF status = %d, want 400", rec.Code)
		}
		if code := errCodeOf(t, rec); code != ErrCodeWebhookURLRejected {
			t.Fatalf("in-ns SSRF code = %s, want %s", code, ErrCodeWebhookURLRejected)
		}
	})
}

func TestCreateOwnershipReconfirm(t *testing.T) {
	mgr, store, _ := newAuthTestManager(t, auth.ModeEnforce)
	rt := NewRoutes(mgr)
	owner := callerFor(t, mgr, "u:owner", "events")
	stranger := callerFor(t, mgr, "u:stranger", "events") // same namespace, different subject

	body := pullWakeBody("events/wake", "events/a")
	if rec := doCreate(t, rt, "s1", owner, body); rec.Code != http.StatusCreated {
		t.Fatalf("create = %d", rec.Code)
	}

	t.Run("owner re-confirm matched", func(t *testing.T) {
		rec := doCreate(t, rt, "s1", owner, body)
		if rec.Code != http.StatusOK {
			t.Fatalf("owner re-confirm = %d, want 200", rec.Code)
		}
	})
	t.Run("stranger re-confirm same config is 403", func(t *testing.T) {
		rec := doCreate(t, rt, "s1", stranger, body)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("stranger matched re-confirm = %d, want 403", rec.Code)
		}
	})
	t.Run("stranger conflicting config is 403 not 409", func(t *testing.T) {
		rec := doCreate(t, rt, "s1", stranger, pullWakeBody("events/wake2", "events/b"))
		if rec.Code != http.StatusForbidden {
			t.Fatalf("stranger conflicting re-confirm = %d, want 403 (no config-match oracle)", rec.Code)
		}
	})
	t.Run("owner conflicting config keeps 409", func(t *testing.T) {
		rec := doCreate(t, rt, "s1", owner, pullWakeBody("events/wake2", "events/b"))
		if rec.Code != http.StatusConflict {
			t.Fatalf("owner conflicting re-confirm = %d, want 409", rec.Code)
		}
	})
	t.Run("ownerless legacy sub accepts authorized caller", func(t *testing.T) {
		// Simulate a pre-enforcement subscription: created with no credential.
		if _, err := store.CreateOrConfirm("legacy", pullWakeCfg(), nil, time.Now()); err != nil {
			t.Fatal(err)
		}
		sub, _, _ := store.Get("legacy")
		if sub.OwnerSubject != "" {
			t.Fatalf("legacy owner = %q, want empty", sub.OwnerSubject)
		}
		cfg := pullWakeCfg()
		b, _ := json.Marshal(map[string]any{
			"type": "pull-wake", "pattern": cfg.Pattern, "wake_stream": cfg.WakeStream,
			"lease_ttl_ms": cfg.LeaseTTLMs,
		})
		// Namespace must cover the legacy pattern's prefix for the 200 path.
		legacyCaller := callerFor(t, mgr, "u:migrator", strings.SplitN(cfg.Pattern, "/", 2)[0], firstSeg(cfg.WakeStream))
		rec := doCreate(t, rt, "legacy", legacyCaller, string(b))
		if rec.Code != http.StatusOK {
			t.Fatalf("ownerless re-confirm = %d, body %q; want 200 (migration posture)", rec.Code, rec.Body.String())
		}
	})
}

func firstSeg(p string) string { return strings.SplitN(strings.Trim(p, "/"), "/", 2)[0] }

func TestAddStreamsEnforceMatrix(t *testing.T) {
	mgr, store, _ := newAuthTestManager(t, auth.ModeEnforce)
	rt := NewRoutes(mgr)
	owner := callerFor(t, mgr, "u:owner", "events")
	if rec := doCreate(t, rt, "s1", owner, pullWakeBody("events/wake", "events/a")); rec.Code != http.StatusCreated {
		t.Fatalf("setup create = %d", rec.Code)
	}
	linkCount := func() int {
		sub, _, _ := store.Get("s1")
		return len(sub.Links)
	}
	base := linkCount()

	t.Run("unauthenticated is 401", func(t *testing.T) {
		rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/streams", "", `{"streams":["events/b"]}`)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("= %d, want 401", rec.Code)
		}
		if linkCount() != base {
			t.Fatal("denied add-streams linked")
		}
	})
	t.Run("out-of-namespace path is 403", func(t *testing.T) {
		rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/streams", owner, `{"streams":["victim/x"]}`)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("= %d, want 403", rec.Code)
		}
		if linkCount() != base {
			t.Fatal("denied add-streams linked")
		}
	})
	t.Run("stranger on owned subscription is 403", func(t *testing.T) {
		stranger := callerFor(t, mgr, "u:stranger", "events")
		rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/streams", stranger, `{"streams":["events/c"]}`)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("= %d, want 403", rec.Code)
		}
		if linkCount() != base {
			t.Fatal("stranger's add-streams linked")
		}
	})
	t.Run("missing subscription is 404", func(t *testing.T) {
		rec := doDS(t, rt, http.MethodPost, subsPrefix+"nope/streams", owner, `{"streams":["events/b"]}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("= %d, want 404", rec.Code)
		}
	})
	t.Run("owner adds in-namespace path", func(t *testing.T) {
		rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/streams", owner, `{"streams":["events/b"]}`)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("= %d, body %q; want 204", rec.Code, rec.Body.String())
		}
		if linkCount() != base+1 {
			t.Fatalf("links = %d, want %d", linkCount(), base+1)
		}
	})
}

func TestDeleteAndRemoveStreamOwnership(t *testing.T) {
	mgr, store, _ := newAuthTestManager(t, auth.ModeEnforce)
	rt := NewRoutes(mgr)
	owner := callerFor(t, mgr, "u:owner", "events")
	stranger := callerFor(t, mgr, "u:stranger", "events")
	if rec := doCreate(t, rt, "s1", owner, pullWakeBody("events/wake", "events/a")); rec.Code != http.StatusCreated {
		t.Fatalf("setup create = %d", rec.Code)
	}

	t.Run("unauthenticated delete is 401", func(t *testing.T) {
		rec := doDS(t, rt, http.MethodDelete, subsPrefix+"s1", "", "")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("= %d, want 401", rec.Code)
		}
	})
	t.Run("stranger remove-stream is 403", func(t *testing.T) {
		rec := doDS(t, rt, http.MethodDelete, subsPrefix+"s1/streams/events/a", stranger, "")
		if rec.Code != http.StatusForbidden {
			t.Fatalf("= %d, want 403", rec.Code)
		}
		sub, _, _ := store.Get("s1")
		if len(sub.Links) == 0 {
			t.Fatal("stranger pruned the link")
		}
	})
	t.Run("stranger delete is 403", func(t *testing.T) {
		rec := doDS(t, rt, http.MethodDelete, subsPrefix+"s1", stranger, "")
		if rec.Code != http.StatusForbidden {
			t.Fatalf("= %d, want 403", rec.Code)
		}
		if _, ok, _ := store.Get("s1"); !ok {
			t.Fatal("stranger deleted the subscription")
		}
	})
	t.Run("owner remove-stream then delete", func(t *testing.T) {
		rec := doDS(t, rt, http.MethodDelete, subsPrefix+"s1/streams/events/a", owner, "")
		if rec.Code != http.StatusNoContent {
			t.Fatalf("remove = %d, want 204", rec.Code)
		}
		rec = doDS(t, rt, http.MethodDelete, subsPrefix+"s1", owner, "")
		if rec.Code != http.StatusNoContent {
			t.Fatalf("delete = %d, want 204", rec.Code)
		}
		if _, ok, _ := store.Get("s1"); ok {
			t.Fatal("subscription survived owner delete")
		}
	})
	t.Run("missing id stays idempotent 204", func(t *testing.T) {
		rec := doDS(t, rt, http.MethodDelete, subsPrefix+"never-existed", owner, "")
		if rec.Code != http.StatusNoContent {
			t.Fatalf("= %d, want 204", rec.Code)
		}
	})
}

// TestControlPlaneInsecureDefaultUnchanged pins the migration posture: with
// no AuthMode configured, credential-less create/add-streams/delete behave
// exactly as before TB3, and subscriptions stay ownerless.
func TestControlPlaneInsecureDefaultUnchanged(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)

	rec := doCreate(t, rt, "s1", "", pullWakeBody("events/wake", "events/a"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("insecure create = %d, body %q", rec.Code, rec.Body.String())
	}
	sub, ok, _ := store.Get("s1")
	if !ok || sub.OwnerSubject != "" {
		t.Fatalf("insecure create owner = %q ok=%v, want ownerless", sub.OwnerSubject, ok)
	}

	// Blind add-streams to a missing subscription keeps today's 204.
	rec = doDS(t, rt, http.MethodPost, subsPrefix+"ghost/streams", "", `{"streams":["events/b"]}`)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("insecure blind add-streams = %d, want 204", rec.Code)
	}

	rec = doDS(t, rt, http.MethodDelete, subsPrefix+"s1", "", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("insecure delete = %d, want 204", rec.Code)
	}
}
