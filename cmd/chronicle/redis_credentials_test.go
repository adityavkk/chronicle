package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
)

func TestRedisConfigurationErrorsDoNotExposeCredentials(t *testing.T) {
	credential := strings.Repeat("private", 7)
	for _, rawURL := range []string{
		"redis://user:" + credential + "@localhost:6379/0",
		"rediss+cluster://user:" + credential + "@redis-a.example:6379,redis-b.example:6379",
		// A malformed URL: net/url echoes it whole in its parse error.
		"redis://localhost:6379/" + credential + "%zz",
	} {
		_, err := newRedisClient(chronicle.Config{RedisURL: rawURL}, nil)
		if err == nil {
			t.Fatal("invalid Redis URL was accepted")
		}
		if strings.Contains(err.Error(), credential) {
			t.Fatalf("Redis configuration error exposed credential material: %v", err)
		}
	}
}

func TestRedisCredentialsLoadFromFileIntoBothClientModes(t *testing.T) {
	username := "appuser"
	password := strings.Repeat("p", 47)
	path := writeCredentialFile(t, "UNRELATED_LEGACY_KEY=ignored\nREDIS_PASSWORD="+password+"\n", 0o400)

	for _, rawURL := range []string{
		"rediss://redis.example:6379/0",
		"rediss+cluster://redis-a.example:6379,redis-b.example:6379",
	} {
		client, err := newRedisClient(chronicle.Config{
			RedisURL:            rawURL,
			RedisUsername:       username,
			RedisCredentialFile: path,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		var gotUsername, gotPassword string
		switch c := client.(type) {
		case *goredis.Client:
			gotUsername, gotPassword = c.Options().Username, c.Options().Password
		case *goredis.ClusterClient:
			gotUsername, gotPassword = c.Options().Username, c.Options().Password
		}
		if gotUsername != username || gotPassword != password {
			t.Fatalf("%T did not receive file-backed credentials", client)
		}
	}
}

func TestRedisCredentialFileUsername(t *testing.T) {
	password := strings.Repeat("q", 29)
	path := writeCredentialFile(t, "REDIS_USERNAME=appuser\nREDIS_PASSWORD="+password+"\n", 0o400)
	for _, configured := range []string{"", "appuser"} {
		credentials, err := loadRedisCredentials(path, configured, false)
		if err != nil {
			t.Fatal(err)
		}
		if credentials.username != "appuser" || credentials.password != password {
			t.Fatalf("configured username %q: file-backed username was not used", configured)
		}
	}
	credentials, err := loadRedisCredentials("", "appuser", false)
	if err != nil || credentials != (redisCredentials{username: "appuser"}) {
		t.Fatalf("username without a credential file = %+v, %v", credentials, err)
	}
}

func TestRedisCredentialFileFailsClosedWithoutExposingValues(t *testing.T) {
	password := strings.Repeat("s", 53)
	tests := []struct {
		name        string
		contents    string
		permissions os.FileMode
	}{
		{name: "missing password", contents: "REDIS_USERNAME=appuser\n", permissions: 0o400},
		{name: "empty password", contents: "REDIS_PASSWORD=\n", permissions: 0o400},
		{name: "duplicate password", contents: "REDIS_PASSWORD=" + password + "\nREDIS_PASSWORD=" + password + "\n", permissions: 0o400},
		{name: "carriage return", contents: "REDIS_PASSWORD=" + password + "\r\n", permissions: 0o400},
		{name: "nul byte", contents: "REDIS_PASSWORD=" + password + "\x00\n", permissions: 0o400},
		{name: "malformed password", contents: "REDIS_PASSWORD " + password + "\n", permissions: 0o400},
		{name: "shell export password", contents: "export REDIS_PASSWORD=" + password + "\n", permissions: 0o400},
		{name: "username mismatch", contents: "REDIS_USERNAME=other\nREDIS_PASSWORD=" + password + "\n", permissions: 0o400},
		// Custody is the keys-file rule: no world access, no group write, no
		// execute bit, and group read only by explicit opt-in.
		{name: "world readable", contents: "REDIS_PASSWORD=" + password + "\n", permissions: 0o444},
		{name: "group readable without the opt-in", contents: "REDIS_PASSWORD=" + password + "\n", permissions: 0o440},
		{name: "group writable", contents: "REDIS_PASSWORD=" + password + "\n", permissions: 0o660},
		{name: "executable", contents: "REDIS_PASSWORD=" + password + "\n", permissions: 0o500},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeCredentialFile(t, test.contents, test.permissions)
			_, err := loadRedisCredentials(path, "appuser", false)
			if err == nil {
				t.Fatal("invalid Redis credential file was accepted")
			}
			if strings.Contains(err.Error(), password) {
				t.Fatal("Redis credential validation error exposed credential material")
			}
		})
	}
}

func TestRedisCredentialFileRejectsUnsafePaths(t *testing.T) {
	if _, err := loadRedisCredentials("relative/secrets", "", false); err == nil {
		t.Fatal("relative credential file path was accepted")
	}
	if _, err := loadRedisCredentials(t.TempDir(), "", false); err == nil {
		t.Fatal("directory credential file path was accepted")
	}
	if _, err := loadRedisCredentials(filepath.Join(t.TempDir(), "missing"), "", false); err == nil {
		t.Fatal("missing credential file was accepted")
	}
}

func TestRedisCredentialFileAcceptsKubernetesProjectedSymlink(t *testing.T) {
	password := strings.Repeat("z", 31)
	target := writeCredentialFile(t, "REDIS_PASSWORD="+password+"\n", 0o400)
	link := filepath.Join(t.TempDir(), "secrets")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	credentials, err := loadRedisCredentials(link, "appuser", false)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.username != "appuser" || credentials.password != password {
		t.Fatal("projected Redis credentials were not loaded through their symlink")
	}
}

// TestRedisCredentialFileGroupReadOptIn pins the one documented exception, the
// same as the keys file's: a non-root container reading a root-owned secret
// through a dedicated fsGroup needs the group-read bit (0440). The opt-in
// permits exactly that bit and nothing else.
func TestRedisCredentialFileGroupReadOptIn(t *testing.T) {
	password := strings.Repeat("g", 37)
	contents := "REDIS_PASSWORD=" + password + "\n"
	credentials, err := loadRedisCredentials(writeCredentialFile(t, contents, 0o440), "appuser", true)
	if err != nil || credentials.password != password {
		t.Fatalf("group-readable file with the opt-in = %+v, %v; want it loaded", credentials, err)
	}
	for _, perm := range []os.FileMode{0o444, 0o460, 0o550} {
		if _, err := loadRedisCredentials(writeCredentialFile(t, contents, perm), "appuser", true); err == nil {
			t.Errorf("mode %04o was accepted under the group-read opt-in; only the group-read bit is permitted", perm)
		}
	}
}

func TestRedisCredentialFileRejectsOversizeFiles(t *testing.T) {
	oversizeValue := strings.Repeat("x", int(maximumRedisCredentialFileBytes))
	path := writeCredentialFile(t, "REDIS_PASSWORD="+oversizeValue+"\n", 0o400)
	if _, err := loadRedisCredentials(path, "appuser", false); err == nil {
		t.Fatal("oversize Redis credential file was accepted")
	} else if strings.Contains(err.Error(), oversizeValue) {
		t.Fatal("Redis credential size error exposed credential material")
	}
}

func writeCredentialFile(t *testing.T, contents string, permissions os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secrets")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, permissions); err != nil {
		t.Fatal(err)
	}
	return path
}
