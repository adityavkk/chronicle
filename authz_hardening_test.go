package chronicle

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// Regression tests for the #126 post-merge security review (adversarial PR
// review, four PRs). Each pins one confirmed-live finding so it cannot regress.

// rawRequest builds a request whose URL.Path is set verbatim, bypassing the
// client-side path cleaning httptest.NewRequest would apply — so a
// key-colliding spelling like "//events/a" reaches the handler intact.
func rawRequest(method, path string, headers map[string]string, body []byte) *http.Request {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, "/ignored", r)
	req.URL.Path = path
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

// TestAppendKeyCollidingPathDenied is finding F1: the append gate must
// normalize the EXACT store path, not subStreamPath(rawPath). "//events/a"
// double-stripped would authorize as "events/a" while the store operates on
// the distinct key "//events/a" — an authorize-A-operate-on-B bypass. The
// gate must deny it (403 invalid path), leave the store unmutated, and fire
// no wake.
func TestAppendKeyCollidingPathDenied(t *testing.T) {
	key := testAuthKey(t)
	h, hooks := enforcedHandler(t, key)
	createDirect(t, h, "/events/a", "application/json")
	before := tailOf(t, h, "/events/a")

	// A token perfectly scoped to events/a, presented against the colliding
	// spelling //events/a.
	tok := mintWriteToken(t, key, "events/a")
	req := rawRequest(http.MethodPost, "//events/a", map[string]string{
		"Content-Type":   "application/json",
		ClaimTokenHeader: tok,
	}, []byte(`{"n":1}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("append to //events/a = %d, want 403 (key-colliding path must not be authorized)", rec.Code)
	}
	if after := tailOf(t, h, "/events/a"); !after.Equal(before) {
		t.Fatalf("intended stream mutated: tail %s -> %s", before, after)
	}
	if _, err := h.Store.Get("//events/a"); !errors.Is(err, store.ErrStreamNotFound) {
		t.Fatalf("colliding key //events/a must not exist, got err=%v", err)
	}
	if hooks.appendCount() != 0 {
		t.Fatal("append hook fired on a denied key-colliding append")
	}
}

// TestReadKeyCollidingPathDenied is F1 on the read gate: the same colliding
// spelling must be denied (not leak existence, not serve).
func TestReadKeyCollidingPathDenied(t *testing.T) {
	key := testAuthKey(t)
	h, _ := enforcedHandler(t, key)
	createDirect(t, h, "/events/a", "application/json")

	req := rawRequest(http.MethodGet, "//events/a", nil, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("read of //events/a = 200; a key-colliding path must be denied")
	}
}

// TestXFCCDuplicateHeaderLastElementWins is finding F6: with two
// X-Forwarded-Client-Cert header lines, the LAST element across all values
// decides — because Envoy appends its attested element last, so a client that
// injects its own leading XFCC line can never win. http.Header.Get (first
// value only) would let the injected leading line authenticate.
func TestXFCCDuplicateHeaderLastElementWins(t *testing.T) {
	key := testAuthKey(t)

	// Attack: attacker forges a trusted element as the FIRST header line; the
	// real sidecar element (untrusted peer, for this test) is appended last.
	// The last element governs, so the request is denied.
	t.Run("forged leading trusted element loses to real last element", func(t *testing.T) {
		h, _ := serviceHandler(t, key)
		createDirect(t, h, "/events/a", "application/json")
		req := rawRequest(http.MethodPost, "/events/a", map[string]string{"Content-Type": "application/json"}, []byte(`{"n":1}`))
		req.Header.Add(tb4XFCCHdr, "URI="+tb4AgentsID) // forged, first
		req.Header.Add(tb4XFCCHdr, "URI="+tb4OtherID)  // real sidecar element, last
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("append = %d, want 401 (untrusted last element must govern)", rec.Code)
		}
	})

	// Legit: an attacker prepends a line, the sidecar appends the trusted
	// element last — it governs, request served.
	t.Run("trusted last element governs over injected leading line", func(t *testing.T) {
		h, hooks := serviceHandler(t, key)
		createDirect(t, h, "/events/a", "application/json")
		req := rawRequest(http.MethodPost, "/events/a", map[string]string{"Content-Type": "application/json"}, []byte(`{"n":1}`))
		req.Header.Add(tb4XFCCHdr, "URI="+tb4OtherID)  // injected, first
		req.Header.Add(tb4XFCCHdr, "URI="+tb4AgentsID) // trusted sidecar element, last
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("append = %d, want 204 (trusted last element must govern)", rec.Code)
		}
		if hooks.appendCount() != 1 {
			t.Fatalf("append hook fired %d times, want 1", hooks.appendCount())
		}
	})
}

// TestXFCCSidecarMarkerGate is finding F5: when a sidecar marker is
// configured, an XFCC mesh identity is honored only if the request also
// carries the marker header with its exact value — a header the sidecar
// injects and strips from inbound requests, so an external peer that forges
// XFCC cannot also produce it.
func TestXFCCSidecarMarkerGate(t *testing.T) {
	const markerName, markerValue = "X-Chronicle-Sidecar", "verified"
	key := testAuthKey(t)

	// allowNoMarker flips ServiceAuth.AllowXFCCWithoutMarker so each case runs
	// both ways: a configured marker must take precedence and behave
	// identically whether or not the marker-less opt-in is also set (#130). A
	// set marker is REQUIRED even when the opt-in is on.
	newHandler := func(t *testing.T, allowNoMarker bool) (*Handler, *hookRecorder) {
		h, rec := serviceHandler(t, key)
		h.ServiceAuth.SidecarMarkerName = markerName
		h.ServiceAuth.SidecarMarkerValue = markerValue
		h.ServiceAuth.AllowXFCCWithoutMarker = allowNoMarker
		return h, rec
	}

	cases := []struct {
		name       string
		marker     string
		setMarker  bool
		wantStatus int
	}{
		{"correct marker → served", markerValue, true, http.StatusNoContent},
		{"missing marker → denied", "", false, http.StatusUnauthorized},
		{"wrong marker → denied", "nope", true, http.StatusUnauthorized},
	}
	for _, c := range cases {
		for _, allowNoMarker := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s (opt-in=%v)", c.name, allowNoMarker), func(t *testing.T) {
				h, _ := newHandler(t, allowNoMarker)
				createDirect(t, h, "/events/a", "application/json")
				headers := map[string]string{
					"Content-Type": "application/json",
					tb4XFCCHdr:     "URI=" + tb4AgentsID, // a trusted mesh identity
				}
				if c.setMarker {
					headers[markerName] = c.marker
				}
				rec := do(h, http.MethodPost, "/events/a", headers, []byte(`{"n":1}`))
				if rec.Code != c.wantStatus {
					t.Fatalf("status = %d, want %d (opt-in=%v: a set marker must govern)", rec.Code, c.wantStatus, allowNoMarker)
				}
			})
		}
	}
}
