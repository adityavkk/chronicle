package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

const (
	captureSignatureMaxAge = 5 * time.Minute
	jwksCacheTTL           = 5 * time.Minute
	jwksFetchTimeout       = 5 * time.Second
	maxJWKSBody            = 1 << 20
)

var errJWKSUnavailable = errors.New("webhook verification keys unavailable")

type captureVerifier interface {
	Verify(header string, body []byte) error
}

type jwksCaptureVerifier struct {
	url    string
	client *http.Client
	now    func() time.Time

	mu        sync.Mutex
	keys      webhook.JWKS
	refreshAt time.Time
}

func newJWKSCaptureVerifier(url string) *jwksCaptureVerifier {
	return &jwksCaptureVerifier{
		url:    url,
		client: &http.Client{Timeout: jwksFetchTimeout},
		now:    time.Now,
	}
}

func defaultJWKSURL(serverURL string) string {
	if strings.TrimSpace(serverURL) == "" {
		return ""
	}
	return strings.TrimRight(serverURL, "/") + "/v1/stream/__ds/jwks.json"
}

func (v *jwksCaptureVerifier) Verify(header string, body []byte) error {
	keys, err := v.cachedKeys(false)
	if err != nil {
		return err
	}
	err = webhook.VerifyWebhookPayload(keys, header, body, v.now(), captureSignatureMaxAge)
	if !errors.Is(err, webhook.ErrWebhookSignatureKey) {
		return err
	}
	// A previously unseen kid is expected during rotation. Refresh once and retry;
	// every other verification failure is final and must not trigger network I/O.
	keys, refreshErr := v.cachedKeys(true)
	if refreshErr != nil {
		return refreshErr
	}
	return webhook.VerifyWebhookPayload(keys, header, body, v.now(), captureSignatureMaxAge)
}

func (v *jwksCaptureVerifier) cachedKeys(force bool) (webhook.JWKS, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	now := v.now()
	if !force && len(v.keys.Keys) > 0 && now.Before(v.refreshAt) {
		return v.keys, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), jwksFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.url, nil)
	if err != nil {
		return webhook.JWKS{}, fmt.Errorf("%w: build JWKS request", errJWKSUnavailable)
	}
	res, err := v.client.Do(req)
	if err != nil {
		return webhook.JWKS{}, fmt.Errorf("%w: fetch JWKS", errJWKSUnavailable)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return webhook.JWKS{}, fmt.Errorf("%w: JWKS returned HTTP %d", errJWKSUnavailable, res.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, maxJWKSBody+1))
	if err != nil || len(raw) > maxJWKSBody {
		return webhook.JWKS{}, fmt.Errorf("%w: invalid JWKS body", errJWKSUnavailable)
	}
	var keys webhook.JWKS
	if err := json.Unmarshal(raw, &keys); err != nil || len(keys.Keys) == 0 {
		return webhook.JWKS{}, fmt.Errorf("%w: invalid JWKS", errJWKSUnavailable)
	}
	v.keys = keys
	v.refreshAt = now.Add(jwksCacheTTL)
	return keys, nil
}
