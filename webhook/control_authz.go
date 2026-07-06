package webhook

import (
	"fmt"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// Pure control-plane authorization deciders (issue #126 TB3). Values in,
// deny-reason out, no I/O: the route shell maps a non-empty reason to the
// 403 envelope (or telemetry in insecure mode). The reasons name paths from
// the caller's own request — never credential material.

// linkAuthz reports whether caller may link everything cfg can ever reach:
// each explicit stream, every possible match of the pattern (bounded by its
// literal prefix — see MayLinkPattern), and the wake_stream chronicle itself
// appends wake events into. The wake_stream check is what stops a caller
// pointing a subscription's wake output at a victim's stream. Empty reason
// means allowed; a path that cannot be normalized can never be granted.
func linkAuthz(caller VerifiedCaller, cfg Config) string {
	if reason := linkPathsAuthz(caller, cfg.Streams); reason != "" {
		return reason
	}
	if cfg.Pattern != "" && !caller.MayLinkPattern(cfg.Pattern) {
		return fmt.Sprintf("pattern %q is not bounded by the caller's namespaces", cfg.Pattern)
	}
	if cfg.WakeStream != "" {
		p, err := auth.NormalizeStreamPath(cfg.WakeStream)
		if err != nil || !caller.MayLink(p) {
			return fmt.Sprintf("wake_stream %q is outside the caller's namespaces", cfg.WakeStream)
		}
	}
	return ""
}

// linkPathsAuthz checks explicit stream paths against the caller's
// namespaces (create seeds and add-streams).
func linkPathsAuthz(caller VerifiedCaller, paths []string) string {
	for _, s := range paths {
		p, err := auth.NormalizeStreamPath(s)
		if err != nil || !caller.MayLink(p) {
			return fmt.Sprintf("stream %q is outside the caller's namespaces", s)
		}
	}
	return ""
}

// ownershipAuthz gates operations on an existing subscription: once a
// subscription records an owner, only that owner may re-confirm, extend,
// prune, or delete it. An ownerless subscription (created without a
// credential — insecure mode or pre-enforcement) accepts any authenticated,
// otherwise-authorized caller: the documented migration posture. Ownership
// begins at the first create performed under a credential and is never
// backfilled onto older records.
func ownershipAuthz(storedOwner, callerSubject string) string {
	if storedOwner != "" && storedOwner != callerSubject {
		return "subscription is owned by another caller"
	}
	return ""
}
