package webhook

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

const (
	serviceMarkerName  = "X-Chronicle-Sidecar"
	serviceMarkerValue = "verified"
	readerSPIFFE       = "spiffe://cluster.local/ns/electric/sa/reader"
	operatorSPIFFE     = "spiffe://cluster.local/ns/electric/sa/operator"
	gatewaySPIFFE      = "spiffe://cluster.local/ns/electric/sa/gateway"
)

type serviceRecordingMetrics struct {
	NopMetrics
	spiffeAuthentications  int
	bearerAuthentications  int
	authenticationFailures int
	authorizationFailures  int
	delegatedGateways      int
}

func (m *serviceRecordingMetrics) ServiceSPIFFEAuthentication()  { m.spiffeAuthentications++ }
func (m *serviceRecordingMetrics) ServiceBearerAuthentication()  { m.bearerAuthentications++ }
func (m *serviceRecordingMetrics) ServiceAuthenticationFailure() { m.authenticationFailures++ }
func (m *serviceRecordingMetrics) ServiceAuthorizationFailure()  { m.authorizationFailures++ }
func (m *serviceRecordingMetrics) ServiceDelegatedGateway()      { m.delegatedGateways++ }

func doMeshDS(t *testing.T, rt *Routes, method, path, identity, body string, marker bool) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Add("X-Forwarded-Client-Cert", "URI="+identity)
	if marker {
		req.Header.Set(serviceMarkerName, serviceMarkerValue)
	}
	rec := httptest.NewRecorder()
	if !rt.HandleRequest(rec, req) {
		t.Fatalf("route %s was not handled", path)
	}
	return rec
}

func TestServicePolicyAppliesToSubscriptionRoutes(t *testing.T) {
	mgr, store, _ := newAuthTestManager(t, auth.ModeEnforce)
	policies, err := auth.NewServicePolicies([]auth.ServicePolicyConfig{
		{Identity: readerSPIFFE, Actions: []auth.Action{auth.ActionRead}, Namespaces: []string{"tenant-a"}},
		{Identity: operatorSPIFFE, Actions: []auth.Action{auth.ActionSubscribe, auth.ActionLink, auth.ActionClaim}, Namespaces: []string{"tenant-a"}},
		{Identity: gatewaySPIFFE, TrustedGateway: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics := &serviceRecordingMetrics{}
	mgr.metrics = metrics
	mgr.serviceAccess = &auth.ServiceAccess{
		TrustedSPIFFEIDs:   []string{readerSPIFFE, operatorSPIFFE, gatewaySPIFFE},
		Policies:           policies,
		SidecarMarkerName:  serviceMarkerName,
		SidecarMarkerValue: serviceMarkerValue,
	}
	rt := NewRoutes(mgr)

	t.Run("forged mesh header without marker is rejected", func(t *testing.T) {
		rec := doMeshDS(t, rt, http.MethodPut, subsPrefix+"forged", readerSPIFFE,
			pullWakeBody("tenant-a/wake", "tenant-a/events"), false)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("forged XFCC create = %d, want 401", rec.Code)
		}
		if _, ok, _ := store.Get("forged"); ok {
			t.Fatal("forged XFCC mutated the subscription store")
		}
	})

	t.Run("read-only service cannot mutate control plane", func(t *testing.T) {
		rec := doMeshDS(t, rt, http.MethodPut, subsPrefix+"reader", readerSPIFFE,
			pullWakeBody("tenant-a/wake", "tenant-a/events"), true)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("reader subscription create = %d, want 403", rec.Code)
		}
		if _, ok, _ := store.Get("reader"); ok {
			t.Fatal("read-only service created a subscription")
		}
	})

	t.Run("operator is action and namespace scoped", func(t *testing.T) {
		rec := doMeshDS(t, rt, http.MethodPut, subsPrefix+"owned", operatorSPIFFE,
			pullWakeBody("tenant-a/wake", "tenant-a/events"), true)
		if rec.Code != http.StatusCreated {
			t.Fatalf("operator subscription create = %d, body=%q", rec.Code, rec.Body.String())
		}
		sub, ok, err := store.Get("owned")
		if err != nil || !ok || sub.OwnerSubject != operatorSPIFFE {
			t.Fatalf("owned subscription = ok %v err %v owner %q", ok, err, sub.OwnerSubject)
		}

		rec = doMeshDS(t, rt, http.MethodPut, subsPrefix+"cross-tenant", operatorSPIFFE,
			pullWakeBody("tenant-a/wake", "tenant-ab/events"), true)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("cross-namespace subscription create = %d, want 403", rec.Code)
		}

		rec = doMeshDS(t, rt, http.MethodPost, subsPrefix+"owned/streams", readerSPIFFE,
			`{"streams":["tenant-a/other"]}`, true)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("reader link = %d, want 403", rec.Code)
		}

		rec = doMeshDS(t, rt, http.MethodPost, subsPrefix+"owned/claim", readerSPIFFE,
			`{"worker":"reader"}`, true)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("reader claim = %d, want 403", rec.Code)
		}
		rec = doMeshDS(t, rt, http.MethodPost, subsPrefix+"owned/claim", operatorSPIFFE,
			`{"worker":"operator"}`, true)
		if rec.Code != http.StatusOK {
			t.Fatalf("operator claim = %d, body=%q", rec.Code, rec.Body.String())
		}
	})

	t.Run("trusted gateway applies only to exact subject", func(t *testing.T) {
		rec := doMeshDS(t, rt, http.MethodPut, subsPrefix+"gateway-target", operatorSPIFFE,
			pullWakeBody("tenant-a/wake", "tenant-a/events"), true)
		if rec.Code != http.StatusCreated {
			t.Fatalf("gateway target create = %d, body=%q", rec.Code, rec.Body.String())
		}
		rec = doMeshDS(t, rt, http.MethodPost, subsPrefix+"gateway-target/claim", gatewaySPIFFE,
			`{"worker":"gateway"}`, true)
		if rec.Code != http.StatusOK {
			t.Fatalf("gateway claim = %d, body=%q", rec.Code, rec.Body.String())
		}
		rec = doMeshDS(t, rt, http.MethodPost, subsPrefix+"gateway-target/claim",
			"spiffe://cluster.local/ns/electric/sa/not-gateway", `{"worker":"attacker"}`, true)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unlisted near gateway = %d, want 401", rec.Code)
		}
	})

	if metrics.spiffeAuthentications == 0 {
		t.Fatal("mesh-authenticated service requests were not recorded")
	}
	if metrics.bearerAuthentications != 0 {
		t.Fatalf("bearer authentications = %d, want 0 in mesh-only test", metrics.bearerAuthentications)
	}
	if metrics.authenticationFailures < 2 {
		t.Fatalf("service authentication failures = %d, want at least 2", metrics.authenticationFailures)
	}
	if metrics.authorizationFailures < 3 {
		t.Fatalf("service authorization failures = %d, want at least 3", metrics.authorizationFailures)
	}
	if metrics.delegatedGateways != 1 {
		t.Fatalf("delegated gateway decisions = %d, want 1", metrics.delegatedGateways)
	}
}

// TestMalformedXFCCRefusedOnControlPlane: the subscription control plane
// shares auth's XFCC parser, so a client prefix with an unbalanced quote that
// used to run across the comma before the sidecar's appended element is
// refused here too — 401 UNAUTHENTICATED on the wire, the distinct reason in
// the denial log line (the control-plane envelope carries codes only), no
// store mutation, one authentication failure — while a well-formed prefix
// leaves the appended element governing.
func TestMalformedXFCCRefusedOnControlPlane(t *testing.T) {
	mgr, store, _ := newAuthTestManager(t, auth.ModeEnforce)
	policies, err := auth.NewServicePolicies([]auth.ServicePolicyConfig{
		{Identity: operatorSPIFFE, Actions: []auth.Action{auth.ActionSubscribe, auth.ActionLink, auth.ActionClaim}, Namespaces: []string{"tenant-a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics := &serviceRecordingMetrics{}
	mgr.metrics = metrics
	var logs bytes.Buffer
	mgr.log = slog.New(slog.NewTextHandler(&logs, nil))
	mgr.serviceAccess = &auth.ServiceAccess{
		TrustedSPIFFEIDs:   []string{operatorSPIFFE},
		Policies:           policies,
		SidecarMarkerName:  serviceMarkerName,
		SidecarMarkerValue: serviceMarkerValue,
	}
	rt := NewRoutes(mgr)

	create := func(t *testing.T, id string, xfcc string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPut, subsPrefix+id, strings.NewReader(pullWakeBody("tenant-a/wake", "tenant-a/events")))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Add("X-Forwarded-Client-Cert", xfcc)
		req.Header.Set(serviceMarkerName, serviceMarkerValue)
		rec := httptest.NewRecorder()
		if !rt.HandleRequest(rec, req) {
			t.Fatalf("route %s was not handled", subsPrefix+id)
		}
		return rec
	}

	// The client's prefix plants the trusted operator and opens a quote it
	// never closes; the sidecar appends an untrusted peer.
	rec := create(t, "forged-quote", "URI="+operatorSPIFFE+`;Subject="oops,URI=`+readerSPIFFE)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("forged-quote create = %d, want 401; body %q", rec.Code, rec.Body.String())
	}
	if body := strings.TrimSpace(rec.Body.String()); body != `{"error":{"code":"UNAUTHENTICATED"}}` {
		t.Fatalf("body %q, want the bare UNAUTHENTICATED envelope", body)
	}
	if !strings.Contains(logs.String(), "malformed X-Forwarded-Client-Cert: unterminated quoted value") {
		t.Fatalf("denial log %q lacks the distinct malformed-header reason", logs.String())
	}
	if strings.Contains(logs.String(), "spiffe://") {
		t.Fatalf("denial log %q echoes header content", logs.String())
	}
	if _, ok, _ := store.Get("forged-quote"); ok {
		t.Fatal("a refused header mutated the subscription store")
	}
	if metrics.authenticationFailures != 1 || metrics.spiffeAuthentications != 0 {
		t.Fatalf("metrics = %d failures, %d spiffe authentications; want 1 and 0", metrics.authenticationFailures, metrics.spiffeAuthentications)
	}

	// Well-formed prefix, trusted appended element: the appended element governs.
	rec = create(t, "well-formed", `Subject="a,URI=`+readerSPIFFE+`";URI=`+readerSPIFFE+",URI="+operatorSPIFFE)
	if rec.Code != http.StatusCreated {
		t.Fatalf("well-formed create = %d, want 201; body %q", rec.Code, rec.Body.String())
	}
	if sub, ok, err := store.Get("well-formed"); err != nil || !ok || sub.OwnerSubject != operatorSPIFFE {
		t.Fatalf("well-formed subscription = ok %v err %v owner %q", ok, err, sub.OwnerSubject)
	}
}
