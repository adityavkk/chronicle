package webhook

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrWebhookSignatureMissing means no Webhook-Signature was presented.
	ErrWebhookSignatureMissing = errors.New("webhook signature is missing")
	// ErrWebhookSignatureMalformed means the signature header failed strict parsing.
	ErrWebhookSignatureMalformed = errors.New("webhook signature is malformed")
	// ErrWebhookSignatureTimestamp means the signature is stale or future-dated.
	ErrWebhookSignatureTimestamp = errors.New("webhook signature timestamp is outside the accepted window")
	// ErrWebhookSignatureKey means the named public key is absent or unusable.
	ErrWebhookSignatureKey = errors.New("webhook signature key is unavailable")
	// ErrWebhookSignatureInvalid means Ed25519 verification failed.
	ErrWebhookSignatureInvalid = errors.New("webhook signature is invalid")
)

// VerifyWebhookPayload verifies a Webhook-Signature against public JWKS keys.
// It is pure: callers own JWKS retrieval/caching and supply the verification
// clock. maxAge applies symmetrically so stale and implausibly future-dated
// deliveries are both rejected.
func VerifyWebhookPayload(jwks JWKS, header string, body []byte, now time.Time, maxAge time.Duration) error {
	timestamp, kid, signature, err := parseWebhookSignature(header)
	if err != nil {
		return err
	}
	if maxAge <= 0 || timestamp.Before(now.Add(-maxAge)) || timestamp.After(now.Add(maxAge)) {
		return ErrWebhookSignatureTimestamp
	}

	publicKey, ok := webhookPublicKey(jwks, kid)
	if !ok {
		return ErrWebhookSignatureKey
	}
	signed := []byte(strconv.FormatInt(timestamp.Unix(), 10) + "." + string(body))
	if !ed25519.Verify(publicKey, signed, signature) {
		return ErrWebhookSignatureInvalid
	}
	return nil
}

func parseWebhookSignature(header string) (time.Time, string, []byte, error) {
	if header == "" {
		return time.Time{}, "", nil, ErrWebhookSignatureMissing
	}
	parts := strings.Split(header, ",")
	if len(parts) != 3 {
		return time.Time{}, "", nil, ErrWebhookSignatureMalformed
	}
	values := make(map[string]string, len(parts))
	for _, part := range parts {
		name, value, ok := strings.Cut(part, "=")
		if !ok || name == "" || value == "" {
			return time.Time{}, "", nil, ErrWebhookSignatureMalformed
		}
		if _, duplicate := values[name]; duplicate {
			return time.Time{}, "", nil, ErrWebhookSignatureMalformed
		}
		values[name] = value
	}
	if len(values) != 3 || values["t"] == "" || values["kid"] == "" || values["ed25519"] == "" {
		return time.Time{}, "", nil, ErrWebhookSignatureMalformed
	}
	seconds, err := strconv.ParseInt(values["t"], 10, 64)
	if err != nil {
		return time.Time{}, "", nil, ErrWebhookSignatureMalformed
	}
	signature, err := base64.RawURLEncoding.DecodeString(values["ed25519"])
	if err != nil || len(signature) != ed25519.SignatureSize {
		return time.Time{}, "", nil, ErrWebhookSignatureMalformed
	}
	return time.Unix(seconds, 0), values["kid"], signature, nil
}

func webhookPublicKey(jwks JWKS, kid string) (ed25519.PublicKey, bool) {
	for _, key := range jwks.Keys {
		if key.Kid != kid || key.Kty != "OKP" || key.Crv != "Ed25519" || key.Alg != "EdDSA" || key.Use != "sig" {
			continue
		}
		decoded, err := base64.RawURLEncoding.DecodeString(key.X)
		if err != nil || len(decoded) != ed25519.PublicKeySize {
			return nil, false
		}
		return ed25519.PublicKey(decoded), true
	}
	return nil, false
}
