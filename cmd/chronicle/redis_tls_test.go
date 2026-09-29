package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
)

// seedHost is the configured Redis host. Handshakes dial 127.0.0.1, as a
// cluster client dials the node addresses CLUSTER SLOTS reports.
const seedHost = "redis.example.test"

func TestRedisClusterTLSVerifiesByDefault(t *testing.T) {
	untrusted := newTestCA(t)
	client := redisClientTLS(t, chronicle.Config{RedisURL: "rediss+cluster://" + seedHost + ":6379"})
	if err := handshake(t, client, untrusted.leaf(t, seedHost)); err == nil {
		t.Fatal("certificate from an unknown authority was accepted")
	}
}

func TestRedisTLSVerification(t *testing.T) {
	trusted := newTestCA(t)
	caFile := writeFile(t, trusted.pem, 0o644)
	unknown := newTestCA(t)
	cluster := "rediss+cluster://" + seedHost + ":6379"
	tests := []struct {
		name   string
		cfg    chronicle.Config
		cert   tls.Certificate
		accept bool
	}{
		{
			name:   "cluster node at an address the certificate does not name",
			cfg:    chronicle.Config{RedisURL: cluster, RedisCAFile: caFile},
			cert:   trusted.leaf(t, seedHost),
			accept: true,
		},
		{
			name:   "cluster certificate for any configured seed",
			cfg:    chronicle.Config{RedisURL: "rediss+cluster://seed-a.example.test:6379," + seedHost + ":6379", RedisCAFile: caFile},
			cert:   trusted.leaf(t, seedHost),
			accept: true,
		},
		{
			name: "cluster certificate for another host",
			cfg:  chronicle.Config{RedisURL: cluster, RedisCAFile: caFile},
			cert: trusted.leaf(t, "elsewhere.example.test"),
		},
		{
			name: "cluster certificate from another authority",
			cfg:  chronicle.Config{RedisURL: cluster, RedisCAFile: caFile},
			cert: unknown.leaf(t, seedHost),
		},
		{
			name: "system roots do not trust a private authority",
			cfg:  chronicle.Config{RedisURL: cluster},
			cert: trusted.leaf(t, seedHost),
		},
		{
			name:   "standalone certificate for the configured host",
			cfg:    chronicle.Config{RedisURL: "rediss://" + seedHost + ":6380/0", RedisCAFile: caFile},
			cert:   trusted.leaf(t, seedHost),
			accept: true,
		},
		{
			name: "standalone certificate for another host",
			cfg:  chronicle.Config{RedisURL: "rediss://" + seedHost + ":6380/0", RedisCAFile: caFile},
			cert: trusted.leaf(t, "elsewhere.example.test"),
		},
		{
			name:   "explicit opt-out accepts an unverifiable certificate",
			cfg:    chronicle.Config{RedisURL: cluster, RedisTLSInsecureSkipVerify: true},
			cert:   unknown.leaf(t, "elsewhere.example.test"),
			accept: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := handshake(t, redisClientTLS(t, tc.cfg), tc.cert)
			if tc.accept && err != nil {
				t.Fatalf("handshake refused: %v", err)
			}
			if !tc.accept && err == nil {
				t.Fatal("handshake accepted a certificate it must refuse")
			}
		})
	}
}

func TestRedisTLSConfigurationErrors(t *testing.T) {
	caFile := writeFile(t, newTestCA(t).pem, 0o644)
	for _, tc := range []struct {
		name string
		cfg  chronicle.Config
	}{
		{name: "CA file on a plaintext URL", cfg: chronicle.Config{RedisURL: "redis://localhost:6379/0", RedisCAFile: caFile}},
		{name: "opt-out on a plaintext cluster URL", cfg: chronicle.Config{RedisURL: "redis+cluster://localhost:6379", RedisTLSInsecureSkipVerify: true}},
		{name: "CA file with the opt-out", cfg: chronicle.Config{RedisURL: "rediss+cluster://localhost:6379", RedisCAFile: caFile, RedisTLSInsecureSkipVerify: true}},
		{name: "skip_verify in the URL", cfg: chronicle.Config{RedisURL: "rediss://localhost:6380/0?skip_verify=true"}},
		{name: "missing CA file", cfg: chronicle.Config{RedisURL: "rediss://localhost:6380/0", RedisCAFile: filepath.Join(t.TempDir(), "missing.pem")}},
		{name: "CA file without certificates", cfg: chronicle.Config{RedisURL: "rediss://localhost:6380/0", RedisCAFile: writeFile(t, []byte("not a certificate"), 0o644)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if client, err := newRedisClient(tc.cfg, nil); err == nil {
				_ = client.Close()
				t.Fatal("invalid Redis TLS configuration was accepted")
			}
		})
	}
}

func TestRedisTLSOptOutIsLoggedLoudly(t *testing.T) {
	cfg := chronicle.DefaultConfig()
	cfg.RedisURL = "rediss://127.0.0.1:1/0"
	cfg.RedisTLSInsecureSkipVerify = true
	var logs bytes.Buffer
	if st, _, _, err := newStore(cfg, slog.New(slog.NewTextHandler(&logs, nil)), nil); err == nil {
		_ = st.Close()
		t.Fatal("unreachable Redis was accepted")
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "CHRONICLE_REDIS_TLS_INSECURE_SKIP_VERIFY") {
		t.Fatalf("TLS verification opt-out was not logged as a warning: %s", logs.String())
	}
}

// handshake runs one TLS handshake against a loopback server presenting cert,
// dialing the way go-redis does, and returns the client's error.
func handshake(t *testing.T, client *tls.Config, cert tls.Certificate) error {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	served := make(chan struct{})
	go func() {
		defer close(served)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.(*tls.Conn).Handshake()
	}()
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", ln.Addr().String(), client)
	if err == nil {
		_ = conn.Close()
	}
	<-served
	return err
}

func redisClientTLS(t *testing.T, cfg chronicle.Config) *tls.Config {
	t.Helper()
	client, err := newRedisClient(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	var tlsConfig *tls.Config
	switch c := client.(type) {
	case *goredis.Client:
		tlsConfig = c.Options().TLSConfig
	case *goredis.ClusterClient:
		tlsConfig = c.Options().TLSConfig
	}
	if tlsConfig == nil {
		t.Fatal("TLS Redis URL produced a client without TLS")
	}
	return tlsConfig
}

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) testCA {
	t.Helper()
	key := newTestKey(t)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "chronicle test Redis CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// leaf issues a server certificate for the given DNS names.
func (ca testCA) leaf(t *testing.T, dnsNames ...string) tls.Certificate {
	t.Helper()
	key := newTestKey(t)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsNames[0]},
		DNSNames:     dnsNames,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func newTestKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func writeFile(t *testing.T, contents []byte, permissions os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, contents, permissions); err != nil {
		t.Fatal(err)
	}
	return path
}
