package chronicle

import (
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

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
