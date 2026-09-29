package webhook

import (
	"crypto/rand"
	"errors"
	"testing"
	"time"
)

func TestVerifyWebhookPayload(t *testing.T) {
	now := time.Unix(1_778_324_210, 0)
	key, err := GenerateSigningKey(rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"subscription_id":"sub-1"}`)
	header := SignWebhookPayload(key, body, now)
	jwks := BuildJWKS([]SigningKey{key})

	if err := VerifyWebhookPayload(jwks, header, body, now, 5*time.Minute); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	tests := []struct {
		name   string
		header string
		body   []byte
		now    time.Time
		jwks   JWKS
		want   error
	}{
		{name: "missing", body: body, now: now, jwks: jwks, want: ErrWebhookSignatureMissing},
		{name: "malformed", header: "garbage", body: body, now: now, jwks: jwks, want: ErrWebhookSignatureMalformed},
		{name: "duplicate parameter", header: "t=1,kid=a,kid=b", body: body, now: now, jwks: jwks, want: ErrWebhookSignatureMalformed},
		{name: "stale", header: header, body: body, now: now.Add(5*time.Minute + time.Second), jwks: jwks, want: ErrWebhookSignatureTimestamp},
		{name: "future", header: header, body: body, now: now.Add(-5*time.Minute - time.Second), jwks: jwks, want: ErrWebhookSignatureTimestamp},
		{name: "unknown key", header: header, body: body, now: now, jwks: JWKS{}, want: ErrWebhookSignatureKey},
		{name: "tampered body", header: header, body: append(body, 'x'), now: now, jwks: jwks, want: ErrWebhookSignatureInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyWebhookPayload(tc.jwks, tc.header, tc.body, tc.now, 5*time.Minute)
			if !errors.Is(err, tc.want) {
				t.Fatalf("VerifyWebhookPayload() error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestVerifyWebhookPayloadAcceptsRotationKeys(t *testing.T) {
	now := time.Unix(1_778_324_210, 0)
	oldKey, err := GenerateSigningKey(rand.Reader, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := GenerateSigningKey(rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"wake_id":"w_1"}`)
	header := SignWebhookPayload(oldKey, body, now)
	jwks := BuildJWKS([]SigningKey{newKey, oldKey})

	if err := VerifyWebhookPayload(jwks, header, body, now, 5*time.Minute); err != nil {
		t.Fatalf("retiring key signature rejected: %v", err)
	}
}
