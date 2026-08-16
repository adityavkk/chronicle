package webhook

import (
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
	authenticationFailures int
	authorizationFailures  int
	delegatedGateways      int
}

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
