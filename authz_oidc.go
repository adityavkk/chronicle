package chronicle

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// OIDCUserAuth is the imperative shell around auth.VerifyOIDCUser (issue
// #126 TB5): it discovers and caches the IdP's JWKS and hands the pure
// verifier a kid resolver. Fail-closed throughout — a discovery or fetch
// error, a stale cache with a missing kid, or any parse failure yields a
// deny, never a panic and never a weaker check.
type OIDCUserAuth struct {
	cfg    auth.OIDCConfig
	client *http.Client
	log    *slog.Logger

	// refreshInterval bounds how stale the cached JWKS may grow before a use
	// refreshes it; kidMissMinInterval rate-limits the refetch a rotation
	// (unknown kid) triggers, so a flood of forged kids cannot hammer the IdP.
	refreshInterval    time.Duration
	kidMissMinInterval time.Duration

	mu        sync.Mutex
	jwksURI   string
	keys      map[string]auth.OIDCKey
	fetchedAt time.Time
	lastMiss  time.Time
}

// NewOIDCUserAuth builds the verifier shell for a complete OIDCConfig.
// client may be nil (a timeout-bounded default is used); logger may be nil.
func NewOIDCUserAuth(cfg auth.OIDCConfig, client *http.Client, logger *slog.Logger) (*OIDCUserAuth, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &OIDCUserAuth{
		cfg:                cfg,
		client:             client,
		log:                logger,
		refreshInterval:    5 * time.Minute,
		kidMissMinInterval: 30 * time.Second,
	}, nil
}

// Issuer returns the configured issuer, the routing key for multi-issuer
// verification.
func (o *OIDCUserAuth) Issuer() string { return o.cfg.Issuer }

// VerifyUser verifies an IdP access token into a user Principal. The JWKS
// cache is refreshed opportunistically when stale and once (rate-limited) on
// an unknown kid, so a key rotation converges without a restart.
func (o *OIDCUserAuth) VerifyUser(token string, now time.Time) (auth.Principal, error) {
	keys := o.currentKeys(now, "")
	p, err := auth.VerifyOIDCUser(token, o.cfg, func(kid string) (auth.OIDCKey, bool) {
		if k, ok := keys[kid]; ok {
			return k, true
		}
		// Unknown kid: one rate-limited refetch, then retry the lookup — the
		// standard rotation path (a fresh key published between refreshes).
		keys = o.currentKeys(now, kid)
		k, ok := keys[kid]
		return k, ok
	}, now)
	if err != nil {
		return auth.Principal{}, err
	}
	return p, nil
}

// currentKeys returns the cached JWKS, refreshing it when the cache is
// empty, older than refreshInterval, or a kid miss warrants a rate-limited
// refetch. On fetch failure the previous cache (if any) stays in use: those
// keys were authentic when fetched, and denying every request because of a
// transient IdP blip would turn an availability problem into an outage. With
// no cache at all, verification fails closed on the empty set.
func (o *OIDCUserAuth) currentKeys(now time.Time, missedKid string) map[string]auth.OIDCKey {
	o.mu.Lock()
	defer o.mu.Unlock()

	stale := o.keys == nil || now.Sub(o.fetchedAt) > o.refreshInterval
	missRefetch := missedKid != "" && now.Sub(o.lastMiss) > o.kidMissMinInterval
	if !stale && !missRefetch {
		return o.keys
	}
	if missedKid != "" {
		o.lastMiss = now
	}
	if err := o.refreshLocked(); err != nil {
		o.log.Warn("oidc jwks refresh failed; verification continues on cached keys",
			"issuer", o.cfg.Issuer, "cached_keys", len(o.keys), "error", err)
	}
	return o.keys
}

// refreshLocked re-discovers (when needed) and refetches the JWKS. Caller
// holds o.mu.
func (o *OIDCUserAuth) refreshLocked() error {
	if o.jwksURI == "" {
		uri, err := o.discoverJWKSURI()
		if err != nil {
			return err
		}
		o.jwksURI = uri
	}
	data, err := o.get(o.jwksURI)
	if err != nil {
		// Force re-discovery next time: the jwks_uri itself may have moved.
		o.jwksURI = ""
		return err
	}
	keys, err := auth.ParseOIDCJWKS(data)
	if err != nil {
		return err
	}
	o.keys = keys
	o.fetchedAt = time.Now()
	return nil
}

// discoverJWKSURI resolves the issuer's jwks_uri via OIDC discovery.
func (o *OIDCUserAuth) discoverJWKSURI() (string, error) {
	data, err := o.get(strings.TrimSuffix(o.cfg.Issuer, "/") + "/.well-known/openid-configuration")
	if err != nil {
		return "", err
	}
	var doc struct {
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return "", fmt.Errorf("oidc discovery: %w", err)
	}
	if doc.JWKSURI == "" {
		return "", fmt.Errorf("oidc discovery: no jwks_uri")
	}
	return doc.JWKSURI, nil
}

func (o *OIDCUserAuth) get(url string) ([]byte, error) {
	resp, err := o.client.Get(url) //nolint:noctx // bounded by the client timeout; no request-scoped context exists here
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // read-only response body
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}
