package chronicle

import (
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

func TestLoadEnvReadPageBytes(t *testing.T) {
	lookup := func(value string) func(string) (string, bool) {
		return func(key string) (string, bool) {
			if key == EnvReadPageBytes {
				return value, true
			}
			return "", false
		}
	}

	if got := DefaultConfig().ReadPageBytes; got != 1<<20 {
		t.Fatalf("default ReadPageBytes = %d, want %d", got, 1<<20)
	}
	c := DefaultConfig()
	if err := c.LoadEnv(lookup("262144")); err != nil {
		t.Fatal(err)
	}
	if c.ReadPageBytes != 256<<10 {
		t.Fatalf("ReadPageBytes = %d, want %d", c.ReadPageBytes, 256<<10)
	}
	for _, value := range []string{"0", "-1", "nope"} {
		c = DefaultConfig()
		if err := c.LoadEnv(lookup(value)); err == nil {
			t.Fatalf("ReadPageBytes %q must fail", value)
		}
	}
}

func TestLoadEnvSSEHubBounds(t *testing.T) {
	env := func(vars map[string]string) func(string) (string, bool) {
		return func(key string) (string, bool) {
			value, ok := vars[key]
			return value, ok
		}
	}

	cfg := DefaultConfig()
	if cfg.SSEHubReplayBytes != defaultSSEHubReplayBytes {
		t.Fatalf("default replay bytes = %d, want %d", cfg.SSEHubReplayBytes, defaultSSEHubReplayBytes)
	}
	if cfg.SSEHubBatchBytes != defaultSSEHubBatchBytes {
		t.Fatalf("default batch bytes = %d, want %d", cfg.SSEHubBatchBytes, defaultSSEHubBatchBytes)
	}
	if cfg.SSEClientWriteTimeout != defaultSSEWriteTimeout {
		t.Fatalf(
			"default client write timeout = %s, want %s",
			cfg.SSEClientWriteTimeout,
			defaultSSEWriteTimeout,
		)
	}
	if cfg.SSENotificationGroups != 1 {
		t.Fatalf("default notification connections = %d, want 1", cfg.SSENotificationGroups)
	}

	if err := cfg.LoadEnv(env(map[string]string{
		EnvSSEHubReplayBytes:     "2097152",
		EnvSSEHubBatchBytes:      "131072",
		EnvSSENotificationGroups: "4",
		EnvSSEClientWriteTimeout: "3s",
	})); err != nil {
		t.Fatal(err)
	}
	if cfg.SSEHubReplayBytes != 2097152 || cfg.SSEHubBatchBytes != 131072 {
		t.Fatalf("SSE hub bounds = replay %d batch %d", cfg.SSEHubReplayBytes, cfg.SSEHubBatchBytes)
	}
	if cfg.SSEClientWriteTimeout != 3*time.Second {
		t.Fatalf("client write timeout = %s", cfg.SSEClientWriteTimeout)
	}
	if cfg.SSENotificationGroups != 4 {
		t.Fatalf("notification connections = %d, want 4", cfg.SSENotificationGroups)
	}

	for _, key := range []string{
		EnvSSEHubReplayBytes,
		EnvSSEHubBatchBytes,
		EnvSSENotificationGroups,
	} {
		cfg = DefaultConfig()
		if err := cfg.LoadEnv(env(map[string]string{key: "0"})); err == nil {
			t.Fatalf("%s=0 must fail startup", key)
		}
		cfg = DefaultConfig()
		if err := cfg.LoadEnv(env(map[string]string{key: "not-an-int"})); err == nil {
			t.Fatalf("%s with invalid integer must fail startup", key)
		}
	}

	cfg = DefaultConfig()
	if err := cfg.LoadEnv(env(map[string]string{
		EnvSSEClientWriteTimeout: "0s",
	})); err == nil {
		t.Fatal("zero client write timeout must fail startup")
	}
}

func TestLoadEnvMetricsPprof(t *testing.T) {
	lookup := func(value string) func(string) (string, bool) {
		return func(key string) (string, bool) {
			if key == EnvMetricsPprof {
				return value, true
			}
			return "", false
		}
	}

	cfg := DefaultConfig()
	if cfg.MetricsPprof {
		t.Fatal("pprof must be disabled by default")
	}
	if err := cfg.LoadEnv(lookup("true")); err != nil {
		t.Fatal(err)
	}
	if !cfg.MetricsPprof {
		t.Fatal("CHRONICLE_METRICS_PPROF=true did not enable pprof")
	}

	cfg = DefaultConfig()
	if err := cfg.LoadEnv(lookup("not-a-bool")); err == nil {
		t.Fatal("invalid pprof boolean was accepted")
	}
}

// TestLoadEnvAuthMode pins the enforcement toggle's env boundary: unset stays
// insecure (telemetry default — a deploy sync can never auto-enforce),
// "enforce" opts in, and garbage refuses to start rather than guessing.
func TestLoadEnvAuthMode(t *testing.T) {
	env := func(vars map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) {
			v, ok := vars[k]
			return v, ok
		}
	}

	c := DefaultConfig()
	if err := c.LoadEnv(env(nil)); err != nil {
		t.Fatal(err)
	}
	if c.AuthMode != auth.ModeInsecure {
		t.Fatalf("default AuthMode = %v, want insecure", c.AuthMode)
	}

	c = DefaultConfig()
	if err := c.LoadEnv(env(map[string]string{EnvAuthMode: "enforce"})); err != nil {
		t.Fatal(err)
	}
	if c.AuthMode != auth.ModeEnforce {
		t.Fatalf("AuthMode = %v, want enforce", c.AuthMode)
	}

	c = DefaultConfig()
	if err := c.LoadEnv(env(map[string]string{EnvAuthMode: "yolo"})); err == nil {
		t.Fatal("invalid CHRONICLE_AUTH_MODE must fail startup, not default")
	}
}

// TestLoadEnvOIDC pins the all-or-nothing OIDC surface: a complete triple
// configures the issuer route; a partial one refuses startup rather than
// verifying more weakly than the operator intended.
func TestLoadEnvOIDC(t *testing.T) {
	env := func(vars map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) {
			v, ok := vars[k]
			return v, ok
		}
	}

	c := DefaultConfig()
	if err := c.LoadEnv(env(map[string]string{
		EnvOIDCIssuer:   "https://idp.example.com",
		EnvOIDCAudience: "chronicle",
		EnvOIDCNSClaim:  "ds_namespaces",
	})); err != nil {
		t.Fatal(err)
	}
	if c.OIDC.Issuer != "https://idp.example.com" {
		t.Fatalf("issuer = %q", c.OIDC.Issuer)
	}

	c = DefaultConfig()
	if err := c.LoadEnv(env(map[string]string{EnvOIDCIssuer: "https://idp.example.com"})); err == nil {
		t.Fatal("partial OIDC config must fail startup")
	}

	c = DefaultConfig()
	if err := c.LoadEnv(env(nil)); err != nil || c.OIDC.Issuer != "" {
		t.Fatalf("unset OIDC must stay disabled: err=%v issuer=%q", err, c.OIDC.Issuer)
	}
}

// TestLoadEnvXFCCFailsClosed pins the #130 re-review fix at the config
// boundary: a SPIFFE allowlist with neither a marker nor the explicit
// trust-without-marker opt-in refuses startup, rather than silently defaulting
// to trusting raw client XFCC. A marker OR the opt-in resolves it.
func TestLoadEnvXFCCFailsClosed(t *testing.T) {
	env := func(vars map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
	}
	const spiffe = "spiffe://cluster.local/ns/electric/sa/agents-server"

	// Allowlist alone → refuse.
	c := DefaultConfig()
	if err := c.LoadEnv(env(map[string]string{EnvTrustedSPIFFE: spiffe})); err == nil {
		t.Fatal("TRUSTED_SPIFFE_IDS without a marker or opt-in must fail startup")
	}

	// Allowlist + marker → OK, opt-in stays false.
	c = DefaultConfig()
	if err := c.LoadEnv(env(map[string]string{
		EnvTrustedSPIFFE:      spiffe,
		EnvXFCCRequiredHeader: "X-Chronicle-Sidecar: verified",
	})); err != nil {
		t.Fatalf("allowlist + marker must load: %v", err)
	}
	if c.AllowXFCCWithoutMarker {
		t.Fatal("marker path must not set AllowXFCCWithoutMarker")
	}

	// Allowlist + explicit opt-in → OK.
	c = DefaultConfig()
	if err := c.LoadEnv(env(map[string]string{
		EnvTrustedSPIFFE:          spiffe,
		EnvXFCCTrustWithoutMarker: "true",
	})); err != nil {
		t.Fatalf("allowlist + explicit opt-in must load: %v", err)
	}
	if !c.AllowXFCCWithoutMarker {
		t.Fatal("opt-in must set AllowXFCCWithoutMarker")
	}

	// No allowlist → the invariant does not apply; unset stays clean.
	c = DefaultConfig()
	if err := c.LoadEnv(env(nil)); err != nil {
		t.Fatalf("no service config must load: %v", err)
	}

	// An empty marker VALUE is a fail-open trap (a header the gate can never
	// distinguish from absent) and must be refused, not accepted as a gate.
	c = DefaultConfig()
	if err := c.LoadEnv(env(map[string]string{
		EnvTrustedSPIFFE:      spiffe,
		EnvXFCCRequiredHeader: "X-Chronicle-Sidecar:",
	})); err == nil {
		t.Fatal("an empty XFCC marker value must fail startup (fail-open trap)")
	}
}

// TestLoadEnvKeysFileAllowGroupRead pins the #131 opt-in parse: unset stays
// fail-closed (false), "true"/"1" set it.
func TestLoadEnvKeysFileAllowGroupRead(t *testing.T) {
	env := func(vars map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
	}

	c := DefaultConfig()
	if err := c.LoadEnv(env(nil)); err != nil {
		t.Fatal(err)
	}
	if c.KeysFileAllowGroupRead {
		t.Fatal("default KeysFileAllowGroupRead must be false (fail closed)")
	}

	c = DefaultConfig()
	if err := c.LoadEnv(env(map[string]string{EnvKeysFileAllowGroupRead: "true"})); err != nil {
		t.Fatal(err)
	}
	if !c.KeysFileAllowGroupRead {
		t.Fatal("CHRONICLE_KEYS_FILE_ALLOW_GROUP_READ=true must set the flag")
	}
}

func TestLoadEnvImmutableSegments(t *testing.T) {
	env := func(vars map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
	}
	c := DefaultConfig()
	if c.SegmentMode != "off" || c.SegmentAutoSealRead || c.SegmentInitialState != "shadow" {
		t.Fatalf("unsafe segment defaults: mode=%q autoSeal=%v state=%q",
			c.SegmentMode, c.SegmentAutoSealRead, c.SegmentInitialState)
	}
	if err := c.LoadEnv(env(map[string]string{
		EnvSegmentMode:         "object-cache",
		EnvSegmentDir:          "/var/lib/chronicle-segments",
		EnvSegmentTargetBytes:  "1048576",
		EnvSegmentIndexStride:  "64",
		EnvSegmentCacheBytes:   "33554432",
		EnvSegmentAutoSealRead: "false",
		EnvSegmentInitialState: "shadow",
	})); err != nil {
		t.Fatal(err)
	}
	if c.SegmentMode != "object-cache" || c.SegmentDir == "" ||
		c.SegmentTargetBytes != 1048576 || c.SegmentIndexStride != 64 ||
		c.SegmentCacheBytes != 33554432 || c.SegmentAutoSealRead ||
		c.SegmentInitialState != "shadow" {
		t.Fatalf("segment env not loaded: %+v", c)
	}
	for _, tc := range []struct {
		key, value string
	}{
		{EnvSegmentTargetBytes, "0"},
		{EnvSegmentIndexStride, "-1"},
		{EnvSegmentCacheBytes, "not-a-number"},
	} {
		c = DefaultConfig()
		if err := c.LoadEnv(env(map[string]string{tc.key: tc.value})); err == nil {
			t.Fatalf("%s=%q must fail", tc.key, tc.value)
		}
	}
}
