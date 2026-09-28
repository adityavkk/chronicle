// Package correlation carries a bounded, log-safe request identifier across
// Chronicle's HTTP, append fan-out, webhook delivery and acknowledgement
// boundaries.
//
// The identifier is a hint for joining log lines and nothing more: it is
// unsigned, it never participates in authentication or authorization, and a
// value the grammar cannot carry is replaced rather than rejected, so a
// caller can never make a request fail by sending a strange id.
package correlation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// DefaultHeader is the HTTP header Chronicle reads and echoes when a
// deployment configures no other name.
const DefaultHeader = "X-Request-ID"

// MaxLength bounds a request id in bytes so one id is always one short,
// single-line log field.
const MaxLength = 128

type requestIDKey struct{}

// Valid reports whether value satisfies the request-id grammar: 1 to
// MaxLength bytes, the first alphanumeric, the rest alphanumeric or one of
// "._:-". The grammar is a strict subset of an HTTP token, so a valid id is
// safe to echo as a header value and to emit as one JSON field.
func Valid(value string) bool {
	if len(value) == 0 || len(value) > MaxLength || !alphanumeric(value[0]) {
		return false
	}
	for i := 1; i < len(value); i++ {
		if !alphanumeric(value[i]) && !strings.ContainsRune("._:-", rune(value[i])) {
			return false
		}
	}
	return true
}

func alphanumeric(b byte) bool {
	return b >= 'a' && b <= 'z' ||
		b >= 'A' && b <= 'Z' ||
		b >= '0' && b <= '9'
}

// Normalize preserves a caller value that satisfies the grammar, ignoring
// surrounding whitespace, and otherwise mints a fresh opaque id.
func Normalize(value string) string {
	value = strings.TrimSpace(value)
	if Valid(value) {
		return value
	}
	return newRequestID()
}

// newRequestID mints a random UUIDv4 string, which the grammar accepts.
// Since Go 1.24 crypto/rand.Read never returns an error (it terminates the
// process if the platform's random source is unusable), so there is no
// fallback to take.
func newRequestID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	return hex.EncodeToString(raw[0:4]) + "-" +
		hex.EncodeToString(raw[4:6]) + "-" +
		hex.EncodeToString(raw[6:8]) + "-" +
		hex.EncodeToString(raw[8:10]) + "-" +
		hex.EncodeToString(raw[10:16])
}

// WithRequestID returns a context carrying Normalize(requestID), so a context
// never carries an id that fails the grammar.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, Normalize(requestID))
}

// RequestID returns the id the context carries, or "" outside HTTP work.
func RequestID(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDKey{}).(string)
	return requestID
}

// WakeRequestID is the stable fallback id for a durable wake whose originating
// request id is not known to this process: after a restart, on another
// replica, or once the process-local memory of it has expired. It is derived
// from the wake id alone, so every replica computes the same value.
func WakeRequestID(wakeID string) string {
	if candidate := "wake-" + wakeID; Valid(candidate) {
		return candidate
	}
	return newRequestID()
}

// CheckHeaderName is the check a configured correlation header name must
// pass before Chronicle will read, echo or send it: an RFC 9110 field-name
// token that Chronicle does not already interpret. The middleware overwrites
// the configured header on every request, so a credential, framing, trace
// context or caller-identity header, or one of the protocol's own families,
// would stop meaning what it means; startup refuses the name instead.
func CheckHeaderName(name string) error {
	if !validHeaderName(name) {
		return fmt.Errorf("%q is not an HTTP header field name", name)
	}
	if reservedHeader(name) {
		return fmt.Errorf("%q already has a meaning to Chronicle and cannot carry the request id", name)
	}
	return nil
}

// validHeaderName reports whether name is an RFC 9110 field-name token.
func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !tokenByte(name[i]) {
			return false
		}
	}
	return true
}

// reservedHeader reports whether Chronicle or HTTP already gives name a
// meaning. Header names are case-insensitive, so the comparison is too.
func reservedHeader(name string) bool {
	lower := strings.ToLower(name)
	for _, family := range []string{"stream-", "producer-", "write-", "webhook-"} {
		if strings.HasPrefix(lower, family) {
			return true
		}
	}
	switch lower {
	case "authorization", "cookie", "content-type", "content-length", "host",
		"traceparent", "tracestate", "x-forwarded-client-cert", "electric-claim-token":
		return true
	}
	return false
}

// tokenByte reports whether b is an RFC 9110 tchar.
func tokenByte(b byte) bool {
	return alphanumeric(b) || strings.IndexByte("!#$%&'*+-.^_`|~", b) >= 0
}
