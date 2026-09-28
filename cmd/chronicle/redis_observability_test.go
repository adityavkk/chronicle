package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
)

func TestRedisStartupReportsActualConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis integration test in -short mode")
	}
	cfg := chronicle.DefaultConfig()
	if rawURL := os.Getenv("REDIS_URL"); rawURL != "" {
		cfg.RedisURL = rawURL
	}
	if strings.Contains(cfg.RedisURL, "+cluster://") {
		t.Skip("startup logging integration test uses standalone Redis")
	}
	endpoint, err := url.Parse(cfg.RedisURL)
	if err != nil {
		t.Fatal("invalid Redis test URL")
	}
	// Fixture availability is optional; startup behavior after a successful
	// preflight is not. Do not turn newStore failures into skips.
	probe, err := newRedisClient(cfg, nil)
	if err != nil {
		t.Fatalf("create Redis test client: %v", err)
	}
	t.Cleanup(func() {
		if err := probe.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := probe.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis integration fixture unavailable at %s: %v", endpoint.Host, err)
	}
	var output bytes.Buffer
	st, _, _, err := newStore(cfg, slog.New(slog.NewTextHandler(&output, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, expected := range []string{`msg="redis connected"`, `mode=standalone`, `address=` + endpoint.Host, `tls=` + strconv.FormatBool(endpoint.Scheme == "rediss")} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("missing %q in %s", expected, output.String())
		}
	}
}

func TestRedisReadinessLogsTransitionsOnly(t *testing.T) {
	var output bytes.Buffer
	failure := errors.New("private backend detail")
	var current error
	probe := redisReadiness(slog.New(slog.NewTextHandler(&output, nil)), func() error { return current })
	if err := probe(); err != nil {
		t.Fatal(err)
	}
	current = failure
	for range 2 {
		if !errors.Is(probe(), failure) {
			t.Fatal("failure not returned")
		}
	}
	current = nil
	for range 2 {
		if err := probe(); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Count(output.String(), "redis readiness failed") != 1 || strings.Count(output.String(), "redis readiness recovered") != 1 || strings.Contains(output.String(), failure.Error()) {
		t.Fatalf("unexpected transition logs: %s", output.String())
	}
}
