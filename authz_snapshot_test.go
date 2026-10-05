package chronicle

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

func TestAuthorizeSnapshotPublishAlwaysEnforcesDistinctServiceGrant(t *testing.T) {
	creds, err := auth.ParseServiceBearerConfig("publisher:publish-secret,writer:write-secret,gateway:gateway-secret")
	if err != nil {
		t.Fatal(err)
	}
	policies, err := auth.NewServicePolicies([]auth.ServicePolicyConfig{
		{Identity: "publisher", Actions: []auth.Action{auth.ActionSnapshotPublish}, Namespaces: []string{"tenant-a"}},
		{Identity: "writer", Actions: []auth.Action{auth.ActionRead, auth.ActionAppend, auth.ActionCreate, auth.ActionLink, auth.ActionClaim}, Namespaces: []string{"tenant-a"}},
		{Identity: "gateway", TrustedGateway: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, mode := range []auth.Mode{auth.ModeInsecure, auth.ModeEnforce} {
		h := &Handler{AuthMode: mode, ServiceAuth: &ServiceAuth{Credentials: creds, Policies: policies}}
		tests := []struct {
			name, token, path string
			wantStatus        int
		}{
			{"explicit grant", "publish-secret", "tenant-a/events", 0},
			{"namespace denied", "publish-secret", "tenant-b/events", http.StatusForbidden},
			{"ordinary grants denied", "write-secret", "tenant-a/events", http.StatusForbidden},
			{"trusted gateway delegates", "gateway-secret", "tenant-b/events", 0},
			{"missing credential", "", "tenant-a/events", http.StatusUnauthorized},
			{"unrecognized credential", "unknown", "tenant-a/events", http.StatusUnauthorized},
			{"invalid path", "publish-secret", "tenant-a//events", http.StatusForbidden},
		}
		for _, tc := range tests {
			t.Run(mode.String()+"/"+tc.name, func(t *testing.T) {
				r := httptest.NewRequest(http.MethodPut, "/", nil)
				if tc.token != "" {
					r.Header.Set("Authorization", "Bearer "+tc.token)
				}
				err := h.authorizeSnapshotPublish(r, tc.path)
				if tc.wantStatus == 0 {
					if err != nil {
						t.Fatalf("unexpected denial: %v", err)
					}
					return
				}
				authErr := &authError{}
				ok := errors.As(err, &authErr)
				if !ok || authErr.status != tc.wantStatus {
					t.Fatalf("error = %#v, want status %d", err, tc.wantStatus)
				}
			})
		}
	}
}
