// Package auth is the pure core of chronicle's stream authentication and
// authorization model (issue #126): the Action and Decision vocabulary, the
// enforcement Mode toggle, the normalized StreamPath, and the
// Principal/Authorizer seam the HTTP layer enforces at.
//
// Everything here is pure — no I/O, no clock (verifiers take now as an
// argument) — so every security decision is table- and property-testable in
// isolation. The imperative shell (handler.go, webhook/routes.go) only wires
// requests into these types and maps a Decision to HTTP.
package auth

import (
	"fmt"
	"strings"
)

// Mode selects how authorization decisions are applied. The zero value is
// ModeInsecure — evaluation-only telemetry — so turning the auth stack on can
// never break a base-protocol client until an operator explicitly opts into
// enforcement (issue #126's telemetry-first rollout: a deploy sync must never
// auto-enforce).
type Mode int

const (
	// ModeInsecure evaluates authorization decisions for telemetry but allows
	// every request. The default.
	ModeInsecure Mode = iota
	// ModeEnforce fails closed: a Deny decision is returned to the client as a
	// 401/403 before any store access.
	ModeEnforce
)

// ParseMode parses a CHRONICLE_AUTH_MODE value. Unset or empty means
// ModeInsecure; anything other than "insecure"/"enforce" is an error, so a
// typo can neither silently enforce nor silently disable an intended boundary
// (the process refuses to start instead).
func ParseMode(s string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "insecure":
		return ModeInsecure, nil
	case "enforce":
		return ModeEnforce, nil
	default:
		return ModeInsecure, fmt.Errorf("invalid auth mode %q (want %q or %q)", s, "insecure", "enforce")
	}
}

func (m Mode) String() string {
	switch m {
	case ModeInsecure:
		return "insecure"
	case ModeEnforce:
		return "enforce"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}
