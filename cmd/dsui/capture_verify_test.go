package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

func TestDefaultJWKSURL(t *testing.T) {
	if got := defaultJWKSURL("https://example.test/"); got != "https://example.test/v1/stream/__ds/jwks.json" {
		t.Fatalf("defaultJWKSURL() = %q", got)
	}
	if got := defaultJWKSURL("  "); got != "" {
		t.Fatalf("blank defaultJWKSURL() = %q", got)
	}
}

func TestJWKSCaptureVerifierCachesKeys(t *testing.T) {
	now := time.Unix(1_778_324_210, 0)
	key, err := webhook.GenerateSigningKey(rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	jwks := webhook.BuildJWKS([]webhook.SigningKey{key})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	defer server.Close()

	verifier := newJWKSCaptureVerifier(server.URL)
	verifier.now = func() time.Time { return now }
	body := []byte(`{"subscription_id":"sub-1"}`)
	header := webhook.SignWebhookPayload(key, body, now)
	for i := 0; i < 2; i++ {
		if err := verifier.Verify(header, body); err != nil {
			t.Fatalf("verification %d failed: %v", i+1, err)
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("JWKS requests = %d, want 1", got)
	}
}

func TestJWKSCaptureVerifierRefreshesUnknownKid(t *testing.T) {
	now := time.Unix(1_778_324_210, 0)
	oldKey, err := webhook.GenerateSigningKey(rand.Reader, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := webhook.GenerateSigningKey(rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		request := requests.Add(1)
		keys := []webhook.SigningKey{oldKey}
		if request > 1 {
			keys = []webhook.SigningKey{newKey, oldKey}
		}
		_ = json.NewEncoder(w).Encode(webhook.BuildJWKS(keys))
	}))
	defer server.Close()

	verifier := newJWKSCaptureVerifier(server.URL)
	verifier.now = func() time.Time { return now }
	oldBody := []byte(`{"wake_id":"old"}`)
	if err := verifier.Verify(webhook.SignWebhookPayload(oldKey, oldBody, now), oldBody); err != nil {
		t.Fatalf("prime old key: %v", err)
	}
	newBody := []byte(`{"wake_id":"new"}`)
	if err := verifier.Verify(webhook.SignWebhookPayload(newKey, newBody, now), newBody); err != nil {
		t.Fatalf("verify rotated key: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("JWKS requests = %d, want 2", got)
	}
}

// TestJWKSCaptureVerifierThrottlesForcedRefreshes pins that an unknown kid
// forces at most one JWKS fetch per forcedRefreshInterval: an unauthenticated
// POST must not be able to make this binary fetch on demand.
func TestJWKSCaptureVerifierThrottlesForcedRefreshes(t *testing.T) {
	now := time.Unix(1_778_324_210, 0)
	knownKey, err := webhook.GenerateSigningKey(rand.Reader, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	unknownKey, err := webhook.GenerateSigningKey(rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(webhook.BuildJWKS([]webhook.SigningKey{knownKey}))
	}))
	defer server.Close()

	verifier := newJWKSCaptureVerifier(server.URL)
	verifier.now = func() time.Time { return now }
	body := []byte(`{"wake_id":"forged"}`)
	for i := 0; i < 3; i++ {
		if err := verifier.Verify(webhook.SignWebhookPayload(unknownKey, body, now), body); !errors.Is(err, webhook.ErrWebhookSignatureKey) {
			t.Fatalf("verify %d with an unknown kid = %v, want %v", i, err, webhook.ErrWebhookSignatureKey)
		}
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("JWKS requests = %d after three unknown kids, want 2 (the first fetch and one forced refresh)", got)
	}
	now = now.Add(forcedRefreshInterval + time.Second)
	if err := verifier.Verify(webhook.SignWebhookPayload(unknownKey, body, now), body); !errors.Is(err, webhook.ErrWebhookSignatureKey) {
		t.Fatalf("verify after the interval = %v, want %v", err, webhook.ErrWebhookSignatureKey)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("JWKS requests = %d after the interval, want 3 (one more forced refresh)", got)
	}
}

func TestCaptureVerifierForRequiresAnExplicitUnverifiedMode(t *testing.T) {
	cases := []struct {
		name, server, jwks string
		insecure           bool
		wantURL            string
		wantErr, wantNone  bool
	}{
		{name: "jwks url wins", server: "https://a.example", jwks: "https://k.example/jwks.json", wantURL: "https://k.example/jwks.json"},
		{name: "derived from the server", server: "https://a.example/", wantURL: "https://a.example/v1/stream/__ds/jwks.json"},
		{name: "neither, without the opt-in", wantErr: true},
		{name: "neither, with the opt-in", insecure: true, wantNone: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			verifier, url, err := captureVerifierFor(tc.server, tc.jwks, tc.insecure)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantNone && verifier != nil {
				t.Fatal("unverified mode must hand out no verifier")
			}
			if url != tc.wantURL {
				t.Fatalf("jwks url = %q, want %q", url, tc.wantURL)
			}
			if tc.wantURL != "" && verifier == nil {
				t.Fatal("a resolved JWKS URL must produce a verifier")
			}
		})
	}
}

func TestPlainHTTPToRemote(t *testing.T) {
	for raw, want := range map[string]bool{
		"http://localhost:4437/v1/stream/__ds/jwks.json": false,
		"http://127.0.0.1:4437/jwks.json":                false,
		"http://[::1]:4437/jwks.json":                    false,
		"https://chronicle.example/jwks.json":            false,
		"http://chronicle.example/jwks.json":             true,
		"not a url":                                      false,
	} {
		if got := plainHTTPToRemote(raw); got != want {
			t.Errorf("plainHTTPToRemote(%q) = %v, want %v", raw, got, want)
		}
	}
}
