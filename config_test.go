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
