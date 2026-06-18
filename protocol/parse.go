// Parsing and validation helpers ported from the Durable Streams reference
// Caddy plugin (packages/caddy-plugin/handler.go @ 82f9963). All functions
// are pure: strings in, typed values or errors out.
package protocol

import (
	"fmt"
	"regexp"
	"strconv"
)

// nonNegativeIntegerRegex matches valid non-negative integer strings
// (no floats, no negatives, no signs).
var nonNegativeIntegerRegex = regexp.MustCompile(`^[0-9]+$`)

// IsValidIntegerString reports whether s is a non-negative decimal integer.
// Used for Producer-Epoch and Producer-Seq header validation.
func IsValidIntegerString(s string) bool {
	return nonNegativeIntegerRegex.MatchString(s)
}

// ttlRegex enforces the Stream-TTL grammar: a non-negative integer without
// leading zeros (except "0" itself), signs, decimal points, or exponents.
var ttlRegex = regexp.MustCompile(`^[1-9][0-9]*$|^0$`)

// ParseTTL parses and validates a Stream-TTL header value (PROTOCOL.md §5.1).
func ParseTTL(s string) (int64, error) {
	if !ttlRegex.MatchString(s) {
		return 0, fmt.Errorf("invalid TTL format: must be a non-negative integer without leading zeros")
	}

	ttl, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid TTL: %w", err)
	}

	if ttl < 0 {
		return 0, fmt.Errorf("TTL must be non-negative")
	}

	return ttl, nil
}

// subOffsetRegex matches the same digit-only grammar as Stream-TTL.
var subOffsetRegex = regexp.MustCompile(`^[1-9][0-9]*$|^0$`)

// ParseSubOffset parses a Stream-Fork-Sub-Offset value: a non-negative
// integer without leading zeros, sign, or whitespace.
func ParseSubOffset(s string) (uint64, error) {
	if !subOffsetRegex.MatchString(s) {
		return 0, fmt.Errorf("invalid Stream-Fork-Sub-Offset format: must be a non-negative integer without leading zeros")
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Stream-Fork-Sub-Offset: %w", err)
	}
	return v, nil
}
