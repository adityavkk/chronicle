package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
)

const (
	maximumRedisCredentialFileBytes int64 = 64 << 10

	redisUsernameKey = "REDIS_USERNAME"
	redisPasswordKey = "REDIS_PASSWORD"
)

type redisCredentials struct {
	username string
	password string
}

// newRedisClient builds the Redis client from a credential-free URL. Two
// scheme families are supported:
//   - redis://host:port/db and rediss://… — a standalone client;
//   - redis+cluster://h1:port,h2:port and rediss+cluster://… — a Redis Cluster
//     client (required for sharded managed Redis) seeded with every listed node.
//
// Credentials come only from cfg.RedisUsername and cfg.RedisCredentialFile, so
// a URL is never secret and may be logged; no returned error carries the URL.
func newRedisClient(cfg chronicle.Config, redisEvents *redisEventSink) (goredis.UniversalClient, error) {
	if strings.Contains(cfg.RedisURL, "@") {
		return nil, errors.New("redis URL must not contain credentials; set REDIS_USERNAME and CHRONICLE_REDIS_CREDENTIAL_FILE instead")
	}
	credentials, err := loadRedisCredentials(cfg.RedisCredentialFile, cfg.RedisUsername, cfg.RedisCredentialFileAllowGroupRead)
	if err != nil {
		return nil, err
	}
	// rediss+cluster:// = Redis Cluster over TLS; redis+cluster:// = plaintext.
	useTLS := strings.HasPrefix(cfg.RedisURL, "rediss+cluster://")
	if useTLS || strings.HasPrefix(cfg.RedisURL, "redis+cluster://") {
		rest := strings.TrimPrefix(strings.TrimPrefix(cfg.RedisURL, "rediss+cluster://"), "redis+cluster://")
		// Strip any /db suffix — cluster mode ignores DB selection.
		if i := strings.LastIndex(rest, "/"); i >= 0 {
			rest = rest[:i]
		}
		seeds := strings.Split(rest, ",")
		for i := range seeds {
			seeds[i] = strings.TrimSpace(seeds[i])
		}
		tlsConfig, err := redisTLSConfig(useTLS, redisHosts(seeds...), cfg)
		if err != nil {
			return nil, err
		}
		opts := &goredis.ClusterOptions{
			Addrs:     seeds,
			Username:  credentials.username,
			Password:  credentials.password,
			TLSConfig: tlsConfig,
		}
		if cfg.RedisPoolSize > 0 {
			opts.PoolSize = cfg.RedisPoolSize
		}
		if redisEvents != nil {
			opts.OnConnect = redisEvents.OnConnect
		}
		return goredis.NewClusterClient(opts), nil
	}
	opt, err := goredis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", withoutURL(err))
	}
	if opt.TLSConfig != nil && opt.TLSConfig.InsecureSkipVerify {
		return nil, errors.New("redis URL skip_verify is not supported; set CHRONICLE_REDIS_TLS_INSECURE_SKIP_VERIFY=true to disable verification deliberately")
	}
	if opt.TLSConfig, err = redisTLSConfig(opt.TLSConfig != nil, redisHosts(opt.Addr), cfg); err != nil {
		return nil, err
	}
	opt.Username = credentials.username
	opt.Password = credentials.password
	if redisEvents != nil {
		opt.OnConnect = redisEvents.OnConnect
	}
	if cfg.RedisPoolSize > 0 {
		opt.PoolSize = cfg.RedisPoolSize
	}
	return goredis.NewClient(opt), nil
}

// withoutURL drops the raw URL that net/url embeds in its parse errors.
func withoutURL(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Err
	}
	return err
}

// loadRedisCredentials returns the Redis username and password. Without a
// credential file only the configured username is used. With one, the file
// must hold exactly one REDIS_PASSWORD and at most one REDIS_USERNAME, which
// must agree with a configured username. The file may carry unrelated
// KEY=VALUE entries (a shared mounted secret); they are ignored. Every failure
// refuses startup, and no error echoes file content.
func loadRedisCredentials(path, configuredUsername string, allowGroupRead bool) (redisCredentials, error) {
	if path == "" {
		return redisCredentials{username: configuredUsername}, nil
	}
	raw, err := readRedisCredentialFile(path, allowGroupRead)
	if err != nil {
		return redisCredentials{}, err
	}
	defer clear(raw)
	fileUsername, password, err := parseRedisCredentials(raw)
	if err != nil {
		return redisCredentials{}, err
	}
	credentials := redisCredentials{username: configuredUsername, password: password}
	if fileUsername != "" {
		if configuredUsername != "" && fileUsername != configuredUsername {
			return redisCredentials{}, errors.New("redis credential file username does not match REDIS_USERNAME")
		}
		credentials.username = fileUsername
	}
	return credentials, nil
}

// readRedisCredentialFile enforces custody on the mounted secret before
// reading it: an absolute path to a regular file (a Kubernetes projected
// symlink is followed) whose mode passes checkRedisCredentialFileMode. The
// opened file must be the one that was checked.
func readRedisCredentialFile(path string, allowGroupRead bool) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("redis credential file path must be absolute")
	}
	before, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("redis credential file: %w", err)
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("redis credential file must be a regular file")
	}
	if err := checkRedisCredentialFileMode(before.Mode().Perm(), allowGroupRead); err != nil {
		return nil, err
	}
	if before.Size() > maximumRedisCredentialFileBytes {
		return nil, errors.New("redis credential file is too large")
	}
	file, err := os.Open(path) // #nosec G304 -- the operator-configured secret mount path
	if err != nil {
		return nil, fmt.Errorf("redis credential file: %w", err)
	}
	defer file.Close() //nolint:errcheck // read-only; the read error is the one that matters
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, errors.New("redis credential file changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximumRedisCredentialFileBytes+1))
	if err != nil {
		clear(raw)
		return nil, fmt.Errorf("redis credential file: %w", err)
	}
	if int64(len(raw)) > maximumRedisCredentialFileBytes {
		clear(raw)
		return nil, errors.New("redis credential file is too large")
	}
	return raw, nil
}

// checkRedisCredentialFileMode is the keys file's custody rule applied to the
// Redis password, which guards every stream and, without CHRONICLE_KEYS_FILE,
// the signing keys stored in Redis. World access and group write are never
// defensible, an execute bit marks the wrong file, and group read on a shared
// group is read-to-connect, so it needs the explicit fsGroup opt-in: mount
// the file 0400, or 0440 with CHRONICLE_REDIS_CREDENTIAL_FILE_ALLOW_GROUP_READ.
func checkRedisCredentialFileMode(perm os.FileMode, allowGroupRead bool) error {
	switch {
	case perm&0o007 != 0:
		return fmt.Errorf("redis credential file permissions %04o are unsafe: it must not be readable, writable or executable by other; set the mount to 0400 or 0600", perm)
	case perm&0o020 != 0:
		return fmt.Errorf("redis credential file permissions %04o are unsafe: it must not be group-writable; set the mount to 0400 or 0600", perm)
	case perm&0o111 != 0:
		return fmt.Errorf("redis credential file permissions %04o are unsafe: it must not be executable", perm)
	case perm&0o040 != 0 && !allowGroupRead:
		return fmt.Errorf("redis credential file permissions %04o are unsafe: it must not be group-readable; set the mount to 0400 or 0600, or set %s=true only if a non-root container reads it through a dedicated fsGroup", perm, chronicle.EnvRedisCredentialFileAllowGroupRead)
	}
	return nil
}

// parseRedisCredentials reads REDIS_USERNAME and REDIS_PASSWORD from literal
// KEY=VALUE lines. A line that names either key in any other form (such as
// "export REDIS_PASSWORD=…" or "REDIS_PASSWORD …") is refused rather than
// skipped, so a typo cannot silently drop the password.
func parseRedisCredentials(raw []byte) (username, password string, err error) {
	if bytes.IndexByte(raw, 0) >= 0 {
		return "", "", errors.New("redis credential file is malformed")
	}
	values := make(map[string]string, 2)
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		key, value, ok := bytes.Cut(line, []byte{'='})
		name := string(key)
		if !ok || (name != redisUsernameKey && name != redisPasswordKey) {
			if namesRedisCredential(line) {
				return "", "", errors.New("redis credential file contains a malformed Redis entry")
			}
			continue
		}
		if _, duplicate := values[name]; duplicate || len(value) == 0 || bytes.IndexByte(value, '\r') >= 0 {
			return "", "", fmt.Errorf("redis credential file contains an invalid %s entry", name)
		}
		values[name] = string(value)
	}
	password, ok := values[redisPasswordKey]
	if !ok {
		return "", "", errors.New("redis credential file does not contain REDIS_PASSWORD")
	}
	return values[redisUsernameKey], password, nil
}

func namesRedisCredential(line []byte) bool {
	line = bytes.TrimPrefix(bytes.TrimSpace(line), []byte("export "))
	for _, key := range []string{redisUsernameKey, redisPasswordKey} {
		rest, ok := bytes.CutPrefix(line, []byte(key))
		if ok && (len(rest) == 0 || rest[0] == '=' || rest[0] == ' ' || rest[0] == '\t') {
			return true
		}
	}
	return false
}
