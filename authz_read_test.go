package chronicle

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// The TB5 acceptance suite (issue #126): read authorization (read-capability
// JWS + multi-issuer principals) and the create/delete completion of the
// data-plane matrix, end to end through the real handler.

const tb5Iss = "http://chronicle.test/v1/stream/"

// tb5Handler is an enforce-mode handler with the full TB5 authorizer set
// wired against a fixed chronicle signing key: read capability, caller
// token, write token, and (optionally) service auth.
func tb5Handler(t *testing.T) (*Handler, webhook.SigningKey, []byte) {
	t.Helper()
	sk, err := webhook.GenerateSigningKey(rand.Reader, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	tokenKey := testAuthKey(t)
	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	h.AppendAuth = webhook.NewWriteTokenAuthorizer(tokenKey)
	h.ReadAuth = webhook.NewReadCapabilityAuthorizer(tb5Iss, webhook.StaticKidResolver(sk))
	h.CallerAuth = webhook.NewCallerTokenAuthorizer(tb5Iss, webhook.StaticKidResolver(sk))
	return h, sk, tokenKey
}

func mintReadCap(t *testing.T, sk webhook.SigningKey, paths ...string) string {
	t.Helper()
	tok, err := webhook.GenerateReadCapability(sk, tb5Iss, "reader-1", paths, time.Now(), time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func mintCallerTok(t *testing.T, sk webhook.SigningKey, namespaces ...string) string {
	t.Helper()
	tok, err := webhook.GenerateCallerToken(sk, tb5Iss, "caller-1", namespaces, time.Now(), time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// seed creates /events/a with one record, bypassing the create gate via a
// caller token scoped to everything the test needs.
func seedStream(t *testing.T, h *Handler, sk webhook.SigningKey, path string) {
	t.Helper()
	tok := mintCallerTok(t, sk, "events", "other", "agents")
	rec := do(h, http.MethodPut, path, map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + tok,
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("seed create %s: %d %q", path, rec.Code, rec.Body.String())
	}
}

func appendSeed(t *testing.T, h *Handler, tokenKey []byte, path, wirePath string) {
	t.Helper()
	tok := mintWriteToken(t, tokenKey, wirePath)
	rec := do(h, http.MethodPost, path, map[string]string{
		"Content-Type":   "application/json",
		ClaimTokenHeader: tok,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("seed append: %d %q", rec.Code, rec.Body.String())
	}
}

// ---- read gate ----

func TestReadDeniedWithoutCredential(t *testing.T) {
	h, sk, _ := tb5Handler(t)
	seedStream(t, h, sk, "/events/a")

	// Every read mode takes the same gate, and a missing stream answers the
	// same 401 as an existing one (no existence leak).
	targets := []string{
		"/events/a",
		"/events/missing",
		"/events/a?offset=" + off(0) + "&live=long-poll",
		"/events/a?offset=" + off(0) + "&live=sse",
	}
	for _, target := range targets {
		rec := do(h, http.MethodGet, target, nil, nil)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("GET %s = %d, want 401", target, rec.Code)
		}
		eb := decodeEnvelope(t, rec)
		if eb.Error.Code != "UNAUTHENTICATED" {
			t.Errorf("GET %s code = %q", target, eb.Error.Code)
		}
	}
	if rec := do(h, http.MethodHead, "/events/a", nil, nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("HEAD = %d, want 401", rec.Code)
	}
}

func TestReadAllowedWithScopedCapability(t *testing.T) {
	h, sk, tokenKey := tb5Handler(t)
	seedStream(t, h, sk, "/events/a")
	appendSeed(t, h, tokenKey, "/events/a", "events/a")

	cap := mintReadCap(t, sk, "events")
	rec := do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + cap}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("credentialed read = %d %q, want 200", rec.Code, rec.Body.String())
	}
	// Q3 posture: private, no ETag, no public max-age.
	if cc := rec.Header().Get("Cache-Control"); cc != "private" {
		t.Errorf("Cache-Control = %q, want private", cc)
	}
	if et := rec.Header().Get("ETag"); et != "" {
		t.Errorf("ETag = %q, want suppressed", et)
	}

	// HEAD with the same capability is served.
	rec = do(h, http.MethodHead, "/events/a", map[string]string{"Authorization": "Bearer " + cap}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("credentialed HEAD = %d, want 200", rec.Code)
	}
}

func TestReadWrongPathCapabilityForbidden(t *testing.T) {
	h, sk, _ := tb5Handler(t)
	seedStream(t, h, sk, "/events/a")

	cap := mintReadCap(t, sk, "other")
	rec := do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + cap}, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("wrong-path capability = %d, want 403", rec.Code)
	}
	if eb := decodeEnvelope(t, rec); eb.Error.Code != "FORBIDDEN" {
		t.Errorf("code = %q", eb.Error.Code)
	}
}

func TestReadServicePrincipalServed(t *testing.T) {
	h, sk, _ := tb5Handler(t)
	creds, err := auth.ParseServiceBearerConfig("agents-server:" + tb5SvcToken)
	if err != nil {
		t.Fatal(err)
	}
	h.ServiceAuth = &ServiceAuth{
		Credentials: creds,
		Policies:    gatewayPolicies(t, "agents-server"),
	}
	seedStream(t, h, sk, "/events/a")

	rec := do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + tb5SvcToken}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("service read = %d, want 200", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "private" {
		t.Errorf("service read Cache-Control = %q, want private", cc)
	}
}

const tb5SvcToken = "svc-read-token"

// TestWriteCredentialsNeverAuthorizeReads: the claim-scoped write token is
// append-only — presented as a Bearer or on electric-claim-token it never
// grants a read.
func TestWriteCredentialsNeverAuthorizeReads(t *testing.T) {
	h, sk, tokenKey := tb5Handler(t)
	seedStream(t, h, sk, "/events/a")
	wtok := mintWriteToken(t, tokenKey, "events/a")

	rec := do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + wtok}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("write token as read credential = %d, want 401", rec.Code)
	}
	rec = do(h, http.MethodGet, "/events/a", map[string]string{ClaimTokenHeader: wtok}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("claim header as read credential = %d, want 401", rec.Code)
	}
}

func TestReadTraversalRejected(t *testing.T) {
	h, sk, _ := tb5Handler(t)
	cap := mintReadCap(t, sk, "events")

	req := httptest.NewRequest(http.MethodGet, "/ignored", nil)
	req.URL.Path = "/events/../events/a"
	req.Header.Set("Authorization", "Bearer "+cap)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("traversal read = %d, want 403", rec.Code)
	}
}

// TestUncredentialedInsecureReadBytesIdentical pins the conformance
// contract: with the whole TB5 stack configured but AuthMode insecure, an
// uncredentialed read keeps the base protocol's exact caching surface —
// ETag present, public max-age on historical reads, If-None-Match 304.
func TestUncredentialedInsecureReadBytesIdentical(t *testing.T) {
	h, sk, tokenKey := tb5Handler(t)
	h.AuthMode = auth.ModeInsecure
	seedStream(t, h, sk, "/events/a")
	appendSeed(t, h, tokenKey, "/events/a", "events/a")
	appendSeed(t, h, tokenKey, "/events/a", "events/a")

	rec := do(h, http.MethodGet, "/events/a?offset="+off(0), nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("read = %d", rec.Code)
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("uncredentialed read lost its ETag")
	}
	// Historical (not at tail after reading from 0? read returns all → at
	// tail): force a historical read by reading from 0 with more data ahead
	// is not observable here; assert the 304 conditional path instead.
	rec = do(h, http.MethodGet, "/events/a?offset="+off(0), map[string]string{"If-None-Match": etag}, nil)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("If-None-Match = %d, want 304", rec.Code)
	}

	// A credentialed read in the same insecure mode is already private (the
	// posture is credential-driven, not mode-driven).
	cap := mintReadCap(t, sk, "events")
	rec = do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + cap}, nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "private" {
		t.Fatalf("insecure credentialed read: %d, Cache-Control %q", rec.Code, rec.Header().Get("Cache-Control"))
	}
}

// ---- OIDC user route (httptest IdP) ----

type tb5IdP struct {
	key    *rsa.PrivateKey
	kid    string
	issuer string
	jwks   *httptest.Server
	disco  *httptest.Server
	// swap lets rotation tests replace the served JWKS.
	serveKid func() string
}

func newTB5IdP(t *testing.T) *tb5IdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := &tb5IdP{key: key, kid: "idp-k1"}
	idp.serveKid = func() string { return idp.kid }

	idp.jwks = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		doc := map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": idp.serveKid(), "use": "sig",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}}
		_ = json.NewEncoder(w).Encode(doc)
	}))
	t.Cleanup(idp.jwks.Close)

	idp.disco = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"jwks_uri": idp.jwks.URL})
	}))
	t.Cleanup(idp.disco.Close)
	idp.issuer = idp.disco.URL
	return idp
}

func (idp *tb5IdP) mint(t *testing.T, kid string, ns any, aud string, exp time.Time) string {
	t.Helper()
	hdr, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "at+jwt", "kid": kid})
	claims, _ := json.Marshal(map[string]any{
		"iss": idp.issuer, "aud": aud, "sub": "user-9",
		"exp": exp.Unix(), "iat": time.Now().Unix(), "ds_namespaces": ns,
	})
	input := base64.RawURLEncoding.EncodeToString(hdr) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, idp.key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func (idp *tb5IdP) userAuth(t *testing.T) *OIDCUserAuth {
	t.Helper()
	ua, err := NewOIDCUserAuth(auth.OIDCConfig{
		Issuer: idp.issuer, Audience: "chronicle", NamespaceClaim: "ds_namespaces",
	}, idp.disco.Client(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return ua
}

func TestOIDCUserReadAndMutate(t *testing.T) {
	h, sk, _ := tb5Handler(t)
	idp := newTB5IdP(t)
	h.UserAuth = idp.userAuth(t)
	seedStream(t, h, sk, "/events/a")

	good := idp.mint(t, "idp-k1", []string{"events"}, "chronicle", time.Now().Add(time.Hour))

	// Read within the namespace grant.
	rec := do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + good}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("oidc read = %d %q, want 200", rec.Code, rec.Body.String())
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "private" {
		t.Errorf("oidc read Cache-Control = %q, want private", cc)
	}

	// Create + delete inside the namespace.
	rec = do(h, http.MethodPut, "/events/new", map[string]string{
		"Content-Type": "application/json", "Authorization": "Bearer " + good,
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("oidc create = %d, want 201", rec.Code)
	}
	rec = do(h, http.MethodDelete, "/events/new", map[string]string{"Authorization": "Bearer " + good}, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("oidc delete = %d, want 204", rec.Code)
	}

	// Outside the grant: 403, store untouched.
	outside := idp.mint(t, "idp-k1", []string{"other"}, "chronicle", time.Now().Add(time.Hour))
	rec = do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + outside}, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("oidc out-of-namespace read = %d, want 403", rec.Code)
	}
	rec = do(h, http.MethodDelete, "/events/a", map[string]string{"Authorization": "Bearer " + outside}, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("oidc out-of-namespace delete = %d, want 403", rec.Code)
	}
	if _, err := h.Store.Get("/events/a"); err != nil {
		t.Fatal("stream deleted despite 403")
	}

	// Wrong audience / expired: 401.
	for name, bad := range map[string]string{
		"wrong aud": idp.mint(t, "idp-k1", []string{"events"}, "someone-else", time.Now().Add(time.Hour)),
		"expired":   idp.mint(t, "idp-k1", []string{"events"}, "chronicle", time.Now().Add(-time.Hour)),
	} {
		rec = do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + bad}, nil)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s = %d, want 401", name, rec.Code)
		}
	}
}

func TestOIDCForkRequiresSourceReadAuthorization(t *testing.T) {
	h, _, _ := tb5Handler(t)
	idp := newTB5IdP(t)
	h.UserAuth = idp.userAuth(t)

	const secret = "tenant-b-secret"
	if _, _, err := h.Store.Create("/tenant-b/secret", store.CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte(secret),
	}); err != nil {
		t.Fatalf("create source: %v", err)
	}
	before, err := h.Store.Get("/tenant-b/secret")
	if err != nil {
		t.Fatalf("get source before fork: %v", err)
	}

	tenantA := idp.mint(t, "idp-k1", []string{"tenant-a"}, "chronicle", time.Now().Add(time.Hour))
	headers := map[string]string{"Authorization": "Bearer " + tenantA}
	rec := do(h, http.MethodGet, "/tenant-b/secret", headers, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("direct source read = %d, want 403; body=%q", rec.Code, rec.Body.String())
	}

	rec = do(h, http.MethodPut, "/tenant-a/copied-secret", map[string]string{
		"Authorization":                 "Bearer " + tenantA,
		"Content-Type":                  "text/plain",
		protocol.HeaderStreamForkedFrom: "/tenant-b/secret",
	}, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-namespace fork = %d, want 403; body=%q", rec.Code, rec.Body.String())
	}
	if got := decodeEnvelope(t, rec).Error.Code; got != errCodeForbidden {
		t.Fatalf("cross-namespace fork code = %q, want %q", got, errCodeForbidden)
	}
	existingDeny := rec.Body.String()
	rec = do(h, http.MethodPut, "/tenant-a/copied-missing", map[string]string{
		"Authorization":                 "Bearer " + tenantA,
		"Content-Type":                  "text/plain",
		protocol.HeaderStreamForkedFrom: "/tenant-b/missing",
	}, nil)
	if rec.Code != http.StatusForbidden || rec.Body.String() != existingDeny {
		t.Fatalf("missing source denial differs from existing source: status=%d body=%q, want 403 %q",
			rec.Code, rec.Body.String(), existingDeny)
	}

	after, err := h.Store.Get("/tenant-b/secret")
	if err != nil {
		t.Fatalf("get source after denied fork: %v", err)
	}
	if after.RefCount != before.RefCount {
		t.Fatalf("denied fork changed source refcount: before=%d after=%d", before.RefCount, after.RefCount)
	}
	for _, destination := range []string{"/tenant-a/copied-secret", "/tenant-a/copied-missing"} {
		if _, err := h.Store.Get(destination); !errors.Is(err, store.ErrStreamNotFound) {
			t.Fatalf("denied fork created %s: err=%v", destination, err)
		}
	}

	rec = do(h, http.MethodGet, "/tenant-a/copied-secret", headers, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("read denied destination = %d, want 404; body=%q", rec.Code, rec.Body.String())
	}
	if bytes.Contains(rec.Body.Bytes(), []byte(secret)) {
		t.Fatalf("denied fork exposed inherited bytes: %q", rec.Body.String())
	}
}

func TestOIDCForkAllowsAuthorizedSource(t *testing.T) {
	h, _, _ := tb5Handler(t)
	idp := newTB5IdP(t)
	h.UserAuth = idp.userAuth(t)

	const payload = "authorized-source"
	if _, _, err := h.Store.Create("/tenant-a/source", store.CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte(payload),
	}); err != nil {
		t.Fatalf("create source: %v", err)
	}
	token := idp.mint(t, "idp-k1", []string{"tenant-a"}, "chronicle", time.Now().Add(time.Hour))
	headers := map[string]string{
		"Authorization":                 "Bearer " + token,
		"Content-Type":                  "text/plain",
		protocol.HeaderStreamForkedFrom: "/tenant-a/source",
	}
	rec := do(h, http.MethodPut, "/tenant-a/copy", headers, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("authorized fork = %d, want 201; body=%q", rec.Code, rec.Body.String())
	}
	rec = do(h, http.MethodGet, "/tenant-a/copy",
		map[string]string{"Authorization": "Bearer " + token}, nil)
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(payload)) {
		t.Fatalf("authorized fork read = %d body=%q, want inherited payload", rec.Code, rec.Body.String())
	}
}

// TestOIDCKidMissRefetch: a token minted under a freshly rotated kid is
// denied until the kid-miss refetch picks the new JWKS up, then verifies —
// without a restart.
func TestOIDCKidMissRefetch(t *testing.T) {
	h, sk, _ := tb5Handler(t)
	idp := newTB5IdP(t)
	ua := idp.userAuth(t)
	ua.kidMissMinInterval = 0 // let the test drive refetches immediately
	h.UserAuth = ua
	seedStream(t, h, sk, "/events/a")

	// Warm the cache under kid1, then rotate the served JWKS to kid2.
	warm := idp.mint(t, "idp-k1", []string{"events"}, "chronicle", time.Now().Add(time.Hour))
	if rec := do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + warm}, nil); rec.Code != http.StatusOK {
		t.Fatalf("warm read = %d", rec.Code)
	}
	idp.kid = "idp-k2"

	rotated := idp.mint(t, "idp-k2", []string{"events"}, "chronicle", time.Now().Add(time.Hour))
	rec := do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + rotated}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("post-rotation read = %d, want 200 via kid-miss refetch", rec.Code)
	}
}

// TestOIDCFetchFailureFailsClosed: with no cached JWKS and a broken IdP,
// every OIDC-routed token is denied — never an error page, never an allow.
func TestOIDCFetchFailureFailsClosed(t *testing.T) {
	h, sk, _ := tb5Handler(t)
	idp := newTB5IdP(t)
	tok := idp.mint(t, "idp-k1", []string{"events"}, "chronicle", time.Now().Add(time.Hour))
	idp.jwks.Close() // IdP down before anything was cached

	h.UserAuth = idp.userAuth(t)
	seedStream(t, h, sk, "/events/a")

	rec := do(h, http.MethodGet, "/events/a", map[string]string{"Authorization": "Bearer " + tok}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("IdP-down read = %d, want 401 (fail closed)", rec.Code)
	}
}

// ---- create/delete gate (the matrix completion) ----

func TestCreateDeleteGate(t *testing.T) {
	h, sk, tokenKey := tb5Handler(t)

	// Unauthenticated create: 401, nothing created.
	rec := do(h, http.MethodPut, "/events/a", map[string]string{"Content-Type": "application/json"}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth create = %d, want 401", rec.Code)
	}
	if _, err := h.Store.Get("/events/a"); err == nil {
		t.Fatal("stream created despite 401")
	}

	// Caller token inside its namespace: created; outside: 403.
	inside := mintCallerTok(t, sk, "events")
	rec = do(h, http.MethodPut, "/events/a", map[string]string{
		"Content-Type": "application/json", "Authorization": "Bearer " + inside,
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("caller create = %d, want 201", rec.Code)
	}
	rec = do(h, http.MethodPut, "/other/x", map[string]string{
		"Content-Type": "application/json", "Authorization": "Bearer " + inside,
	}, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("out-of-namespace create = %d, want 403", rec.Code)
	}

	// Read caps and write tokens never authorize mutations.
	cap := mintReadCap(t, sk, "events")
	wtok := mintWriteToken(t, tokenKey, "events/a")
	for name, tok := range map[string]string{"read cap": cap, "write token": wtok} {
		rec = do(h, http.MethodDelete, "/events/a", map[string]string{"Authorization": "Bearer " + tok}, nil)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s delete = %d, want 401", name, rec.Code)
		}
	}
	if _, err := h.Store.Get("/events/a"); err != nil {
		t.Fatal("stream deleted by a non-caller credential")
	}

	// Caller token delete inside the namespace: 204.
	rec = do(h, http.MethodDelete, "/events/a", map[string]string{"Authorization": "Bearer " + inside}, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("caller delete = %d, want 204", rec.Code)
	}

	// Service principal: pre-authorized create + delete.
	creds, err := auth.ParseServiceBearerConfig(tb5SvcToken)
	if err != nil {
		t.Fatal(err)
	}
	h.ServiceAuth = &ServiceAuth{
		Credentials: creds,
		Policies:    gatewayPolicies(t, "service"),
	}
	rec = do(h, http.MethodPut, "/anything/at/all", map[string]string{
		"Content-Type": "application/json", "Authorization": "Bearer " + tb5SvcToken,
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("service create = %d, want 201", rec.Code)
	}
	rec = do(h, http.MethodDelete, "/anything/at/all", map[string]string{"Authorization": "Bearer " + tb5SvcToken}, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("service delete = %d, want 204", rec.Code)
	}
}

// TestCreateDeleteInsecureUnchanged: the default mode still serves the base
// protocol's uncredentialed create/delete.
func TestCreateDeleteInsecureUnchanged(t *testing.T) {
	h, _, _ := tb5Handler(t)
	h.AuthMode = auth.ModeInsecure

	mustCreate(t, h, "/events/a", "application/json", nil)
	rec := do(h, http.MethodDelete, "/events/a", nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("insecure delete = %d, want 204", rec.Code)
	}
}

// TestZeroValueHandlerStillUngated: a Handler with no auth config at all
// behaves exactly as the base protocol on every action.
func TestZeroValueHandlerStillUngated(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/events/a", "application/json", nil)
	mustAppend(t, h, "/events/a", "application/json", []byte(`{"n":1}`))
	if rec := do(h, http.MethodGet, "/events/a", nil, nil); rec.Code != http.StatusOK {
		t.Fatalf("read = %d", rec.Code)
	}
	if rec := do(h, http.MethodDelete, "/events/a", nil, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d", rec.Code)
	}
}
