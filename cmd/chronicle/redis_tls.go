package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
)

// redisTLSConfig returns the TLS configuration for a Redis client, or nil for
// a plaintext URL. hosts are the configured Redis hosts: the standalone host or
// every cluster seed.
//
// By default the server chain is verified against the system roots, or against
// cfg.RedisCAFile alone when it is set, and the leaf must be valid for one of
// hosts. cfg.RedisTLSInsecureSkipVerify is the one explicit opt-out; newStore
// logs a warning whenever it is set.
func redisTLSConfig(useTLS bool, hosts []string, cfg chronicle.Config) (*tls.Config, error) {
	switch {
	case !useTLS && (cfg.RedisCAFile != "" || cfg.RedisTLSInsecureSkipVerify):
		return nil, errors.New("CHRONICLE_REDIS_CA_FILE and CHRONICLE_REDIS_TLS_INSECURE_SKIP_VERIFY need a rediss:// or rediss+cluster:// URL")
	case !useTLS:
		return nil, nil
	case cfg.RedisTLSInsecureSkipVerify && cfg.RedisCAFile != "":
		return nil, errors.New("CHRONICLE_REDIS_CA_FILE and CHRONICLE_REDIS_TLS_INSECURE_SKIP_VERIFY are mutually exclusive")
	case cfg.RedisTLSInsecureSkipVerify:
		return &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}, nil // #nosec G402 -- the explicit, logged operator opt-out
	}
	roots, err := loadRedisRootCAs(cfg.RedisCAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		// A cluster client dials nodes at the addresses CLUSTER SLOTS reports,
		// which a certificate issued for the cluster's hostname does not name.
		// The standard check against the dialed address is replaced, not
		// dropped: verifyRedisPeer runs on every handshake.
		InsecureSkipVerify: true, // #nosec G402 -- verification is done by VerifyConnection
		VerifyConnection:   verifyRedisPeer(roots, hosts),
	}, nil
}

// verifyRedisPeer verifies the server chain against roots (nil means the
// system roots) and requires the leaf to be valid for one of the configured
// hosts. The dialed address is never consulted, which is the only mismatch
// this tolerates: a cluster node reached at the address CLUSTER SLOTS gave.
func verifyRedisPeer(roots *x509.CertPool, hosts []string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return errors.New("redis TLS: server presented no certificate")
		}
		leaf := cs.PeerCertificates[0]
		intermediates := x509.NewCertPool()
		for _, cert := range cs.PeerCertificates[1:] {
			intermediates.AddCert(cert)
		}
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates}); err != nil {
			return fmt.Errorf("redis TLS: %w", err)
		}
		for _, host := range hosts {
			if leaf.VerifyHostname(host) == nil {
				return nil
			}
		}
		return fmt.Errorf("redis TLS: certificate is not valid for %s", strings.Join(hosts, ", "))
	}
}

// loadRedisRootCAs reads a PEM CA bundle. An empty path selects the system
// roots, which x509 represents as a nil pool.
func loadRedisRootCAs(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, nil
	}
	bundle, err := os.ReadFile(path) // #nosec G304 -- the operator-configured CA bundle path
	if err != nil {
		return nil, fmt.Errorf("redis CA file: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle) {
		return nil, fmt.Errorf("redis CA file %s contains no PEM certificates", path)
	}
	return roots, nil
}

// redisHosts strips the ports from host:port addresses.
func redisHosts(addrs ...string) []string {
	hosts := make([]string, len(addrs))
	for i, addr := range addrs {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
		}
		hosts[i] = host
	}
	return hosts
}
