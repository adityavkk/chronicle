package main

import (
	"log/slog"
	"sync/atomic"

	goredis "github.com/redis/go-redis/v9"
)

// logRedisConnected reports the endpoint from the constructed client's
// options, never from the configured URL.
func logRedisConnected(logger *slog.Logger, client goredis.UniversalClient) {
	switch c := client.(type) {
	case *goredis.Client:
		opts := c.Options()
		logger.Info("redis connected", "mode", "standalone", "address", opts.Addr, "tls", opts.TLSConfig != nil)
	case *goredis.ClusterClient:
		opts := c.Options()
		logger.Info("redis connected", "mode", "cluster", "seeds", opts.Addrs, "tls", opts.TLSConfig != nil)
	}
}

// redisReadiness wraps a readiness check so that only transitions are logged,
// not every probe, and never with the raw error, which may carry backend detail.
// Probes ping concurrently; the one whose swap observes the transition logs it.
func redisReadiness(logger *slog.Logger, check func() error) func() error {
	var failed atomic.Bool
	return func() error {
		err := check()
		if now := err != nil; failed.Swap(now) != now {
			if now {
				logger.Warn("redis readiness failed", "check", "ping")
			} else {
				logger.Info("redis readiness recovered", "check", "ping")
			}
		}
		return err
	}
}
