package chronicle

import "testing"

func TestLoadEnvRedisCredentials(t *testing.T) {
	env := map[string]string{
		"CHRONICLE_REDIS_URL":             "rediss+cluster://redis.example:6379",
		"REDIS_USERNAME":                  "appuser",
		"CHRONICLE_REDIS_CREDENTIAL_FILE": "/etc/secrets/secrets",
	}
	cfg := DefaultConfig()
	if err := cfg.LoadEnv(func(key string) (string, bool) { value, ok := env[key]; return value, ok }); err != nil {
		t.Fatal(err)
	}
	if cfg.RedisURL != env["CHRONICLE_REDIS_URL"] || cfg.RedisUsername != "appuser" || cfg.RedisCredentialFile != "/etc/secrets/secrets" {
		t.Fatal("managed Redis configuration was not loaded")
	}
}

func TestLoadEnvRedisTLS(t *testing.T) {
	lookup := func(env map[string]string) func(string) (string, bool) {
		return func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	}
	cfg := DefaultConfig()
	if cfg.RedisTLSInsecureSkipVerify {
		t.Fatal("Redis TLS verification must be on by default")
	}
	if err := cfg.LoadEnv(lookup(map[string]string{
		"CHRONICLE_REDIS_CA_FILE":                  "/etc/redis-ca/ca.pem",
		"CHRONICLE_REDIS_TLS_INSECURE_SKIP_VERIFY": "true",
	})); err != nil {
		t.Fatal(err)
	}
	if cfg.RedisCAFile != "/etc/redis-ca/ca.pem" || !cfg.RedisTLSInsecureSkipVerify {
		t.Fatal("Redis TLS configuration was not loaded")
	}
	cfg = DefaultConfig()
	if err := cfg.LoadEnv(lookup(map[string]string{"CHRONICLE_REDIS_TLS_INSECURE_SKIP_VERIFY": "yes please"})); err == nil {
		t.Fatal("an unparseable TLS opt-out was accepted")
	}
}
