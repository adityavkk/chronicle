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
