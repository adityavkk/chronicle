// Package telemetry is Chronicle's opt-in distributed tracing: W3C trace
// context accepted on every request and passed to the webhook a stream append
// causes, exported over OTLP/HTTP. Spans carry shapes (method, status, byte
// counts) and Chronicle's own safe identifiers, never a URL path or query, a
// body, a header value or a Redis statement.
//
// Tracing is the one Chronicle subsystem that fails open: when the configured
// destination is unusable because a credential or CA file is missing or
// unreadable, Start disables tracing, says so once at Warn, exposes the
// reason for a metric, and lets the server start. A malformed configuration
// value is still a startup error, like every other flag.
package telemetry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

// Environment variables. Tracing is on when EnvEndpoint is set; nothing else
// is read while it is not.
const (
	// EnvEndpoint is the OTLP/HTTP traces URL, https (plain http only to a
	// loopback address), with no credentials, query or fragment.
	EnvEndpoint = "CHRONICLE_OTLP_ENDPOINT"
	// EnvUsernameFile and EnvPasswordFile name files holding the two halves
	// of an HTTP basic-auth credential the exporter sends. Set both or
	// neither; a file that cannot be read fails open.
	EnvUsernameFile = "CHRONICLE_OTLP_USERNAME_FILE"
	EnvPasswordFile = "CHRONICLE_OTLP_PASSWORD_FILE"
	// EnvCAFile names a PEM bundle of roots that verify the destination in
	// place of the system roots. A file that cannot be read fails open.
	EnvCAFile = "CHRONICLE_OTLP_CA_FILE"
	// EnvSampleRatio is the fraction, 0 to 1, of Chronicle's root spans kept;
	// a caller's sampled flag always wins over it. Default 1.
	EnvSampleRatio = "CHRONICLE_TRACE_SAMPLE_RATIO"
	// EnvSampleAlways lists operations (correlation.Operations, comma
	// separated) whose root spans are kept regardless of the ratio.
	EnvSampleAlways = "CHRONICLE_TRACE_SAMPLE_ALWAYS"
)

// Fail-open reasons: the closed vocabulary FailedOpen reports and the metric
// labels by.
const (
	ReasonCredentialsUnavailable = "credentials_unavailable"
	ReasonCAUnavailable          = "ca_unavailable"
	ReasonExporterUnavailable    = "exporter_unavailable"
)

// scopeName identifies the spans this package starts.
const scopeName = "gecgithub01.walmart.com/auk000v/chronicle/telemetry"

// maxCredentialBytes bounds a credential file so a wrong path (a log, a
// certificate bundle) cannot become an Authorization header.
const maxCredentialBytes = 4096

// Tracing is the process's tracing state: a live provider, or none because
// tracing is off or failed open. The zero value is off.
type Tracing struct {
	provider *sdktrace.TracerProvider
	exports  *observedExporter
	failure  string
	logger   *slog.Logger
}

type config struct {
	endpoint     *url.URL
	usernameFile string
	passwordFile string
	caFile       string
	ratio        float64
	always       []string
}

// Start reads the configuration through lookup (os.LookupEnv in the binary)
// and returns the process's Tracing. The error is reserved for malformed
// configuration; an unusable credential or CA file fails open instead.
func Start(ctx context.Context, lookup func(string) (string, bool), logger *slog.Logger) (*Tracing, error) {
	if logger == nil {
		logger = slog.Default()
	}
	cfg, enabled, err := loadConfig(lookup)
	if err != nil {
		return nil, err
	}
	t := &Tracing{logger: logger}
	if !enabled {
		return t, nil
	}
	options := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(cfg.endpoint.String()),
		otlptracehttp.WithTimeout(5 * time.Second),
	}
	authorization, err := basicAuthorization(cfg)
	if err != nil {
		return t.failOpen(ReasonCredentialsUnavailable, err), nil
	}
	if authorization != "" {
		options = append(options, otlptracehttp.WithHeaders(map[string]string{"Authorization": authorization}))
	}
	if cfg.caFile != "" {
		roots, err := rootCAs(cfg.caFile)
		if err != nil {
			return t.failOpen(ReasonCAUnavailable, err), nil
		}
		options = append(options, otlptracehttp.WithTLSClientConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}))
	}
	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return t.failOpen(ReasonExporterUnavailable, err), nil
	}
	t.exports = observeExports(exporter, logger)
	t.provider = sdktrace.NewTracerProvider(
		sdktrace.WithSampler(newSampler(cfg.ratio, cfg.always...)),
		sdktrace.WithResource(serviceResource()),
		// The queue is the memory bound; a slow destination drops spans, never
		// blocks a request.
		sdktrace.WithBatcher(t.exports,
			sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithMaxExportBatchSize(256),
			sdktrace.WithBatchTimeout(time.Second),
			sdktrace.WithExportTimeout(5*time.Second)),
	)
	// The SDK reports every export error to its global handler as well;
	// observedExporter already counts and logs them on transitions, so the
	// handler only keeps the SDK's default stderr printer quiet and leaves
	// the detail at Debug.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Debug("tracing sdk error", "event", "tracing_sdk_error", "error", err)
	}))
	logger.Info("tracing enabled",
		"event", "tracing_enabled",
		"destination", cfg.endpoint.Host,
		"sample_ratio", cfg.ratio,
		"sample_always", strings.Join(cfg.always, ","))
	return t, nil
}

// newTracing wraps an existing provider; tests use it with an in-memory
// exporter.
func newTracing(provider *sdktrace.TracerProvider, logger *slog.Logger) *Tracing {
	return &Tracing{provider: provider, logger: logger}
}

func (t *Tracing) failOpen(reason string, err error) *Tracing {
	t.failure = reason
	t.logger.Warn("tracing disabled: the configured destination is unusable; serving without traces",
		"event", "tracing_disabled",
		"reason", reason,
		"error", err)
	return t
}

// Enabled reports whether spans are recorded and exported.
func (t *Tracing) Enabled() bool { return t.provider != nil }

// FailedOpen reports the reason tracing was configured but could not start,
// for the metric that makes the fail-open visible.
func (t *Tracing) FailedOpen() (reason string, failed bool) {
	return t.failure, t.failure != ""
}

// ExportFailures is the number of span batches the destination has refused
// since startup, for chronicle_tracing_export_failures_total; 0 while tracing
// is off.
func (t *Tracing) ExportFailures() uint64 {
	if t.exports == nil {
		return 0
	}
	return t.exports.Failures()
}

// Tracer starts Chronicle's own spans, or is nil when tracing is off so that
// callers start none and propagate nothing.
func (t *Tracing) Tracer() trace.Tracer {
	if t.provider == nil {
		return nil
	}
	return t.provider.Tracer(scopeName)
}

// InstrumentRedis traces every Redis command as a child of the request that
// issued it, without the statement (no keys, so no stream paths). Commands
// issued outside a traced request are dropped by the sampler.
func (t *Tracing) InstrumentRedis(client goredis.UniversalClient) error {
	if t.provider == nil {
		return nil
	}
	return redisotel.InstrumentTracing(client,
		redisotel.WithTracerProvider(t.provider),
		redisotel.WithDBStatement(false))
}

// Shutdown flushes and stops the exporter.
func (t *Tracing) Shutdown(ctx context.Context) error {
	if t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

func loadConfig(lookup func(string) (string, bool)) (config, bool, error) {
	get := func(key string) string {
		value, _ := lookup(key)
		return strings.TrimSpace(value)
	}
	raw := get(EnvEndpoint)
	if raw == "" {
		return config{}, false, nil
	}
	endpoint, err := parseEndpoint(raw)
	if err != nil {
		return config{}, false, fmt.Errorf("%s: %w", EnvEndpoint, err)
	}
	cfg := config{endpoint: endpoint, usernameFile: get(EnvUsernameFile), passwordFile: get(EnvPasswordFile), caFile: get(EnvCAFile), ratio: 1}
	if (cfg.usernameFile == "") != (cfg.passwordFile == "") {
		return config{}, false, fmt.Errorf("%s and %s must be set together", EnvUsernameFile, EnvPasswordFile)
	}
	if raw := get(EnvSampleRatio); raw != "" {
		ratio, err := strconv.ParseFloat(raw, 64)
		if err != nil || ratio < 0 || ratio > 1 {
			return config{}, false, fmt.Errorf("%s: want a number from 0 to 1, got %q", EnvSampleRatio, raw)
		}
		cfg.ratio = ratio
	}
	for _, operation := range strings.Split(get(EnvSampleAlways), ",") {
		operation = strings.TrimSpace(operation)
		if operation == "" {
			continue
		}
		if !correlation.IsOperation(operation) {
			return config{}, false, fmt.Errorf("%s: unknown operation %q, want one of %s", EnvSampleAlways, operation, strings.Join(correlation.Operations, ", "))
		}
		cfg.always = append(cfg.always, operation)
	}
	return cfg, true, nil
}

// parseEndpoint accepts an https URL, or an http URL to a loopback address
// for a local collector, with no user information, query or fragment.
func parseEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("want a URL, got %q", raw)
	}
	switch {
	case u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "":
		return nil, fmt.Errorf("want a URL with a host and no credentials, query or fragment, got %q", raw)
	case u.Scheme == "https":
		return u, nil
	case u.Scheme == "http" && isLoopback(u.Hostname()):
		return u, nil
	default:
		return nil, fmt.Errorf("want an https URL (http only to a loopback address), got %q", raw)
	}
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// basicAuthorization builds the Authorization header value from the two
// credential files, or "" when none are configured.
func basicAuthorization(cfg config) (string, error) {
	if cfg.usernameFile == "" {
		return "", nil
	}
	username, err := readCredential(EnvUsernameFile, cfg.usernameFile)
	if err != nil {
		return "", err
	}
	password, err := readCredential(EnvPasswordFile, cfg.passwordFile)
	if err != nil {
		return "", err
	}
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password)), nil
}

// readCredential reads one trimmed, non-empty credential, bounded before it
// is read rather than after, as the Redis credential loader does. Errors name
// the variable and the path, never the content.
func readCredential(env, path string) (string, error) {
	file, err := os.Open(path) // #nosec G304 -- the operator-configured secret mount path
	if err != nil {
		return "", fmt.Errorf("%s: %w", env, err)
	}
	defer file.Close() //nolint:errcheck // read-only; the read error is the one that matters
	data, err := io.ReadAll(io.LimitReader(file, maxCredentialBytes+1))
	if err != nil {
		return "", fmt.Errorf("%s: %w", env, err)
	}
	if len(data) > maxCredentialBytes {
		return "", fmt.Errorf("%s: %s exceeds %d bytes", env, path, maxCredentialBytes)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", fmt.Errorf("%s: %s is empty", env, path)
	}
	return value, nil
}

func rootCAs(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", EnvCAFile, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s: %s holds no PEM certificate", EnvCAFile, path)
	}
	return roots, nil
}

// serviceResource names the service "chronicle" unless the standard
// OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES variables say otherwise.
func serviceResource() *resource.Resource {
	base := resource.NewSchemaless(attribute.String("service.name", "chronicle"))
	merged, err := resource.Merge(base, resource.Environment())
	if err != nil || merged == nil {
		return base
	}
	return merged
}
