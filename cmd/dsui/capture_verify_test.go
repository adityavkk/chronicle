package main

import (
	"crypto/rand"
	"encoding/json"
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
