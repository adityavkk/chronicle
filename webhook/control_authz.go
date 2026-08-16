package webhook

import (
	"fmt"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// Pure control-plane authorization deciders (issue #126 TB3). Values in,
// deny-reason out, no I/O: the route shell maps a non-empty reason to the
// 403 envelope (or telemetry in insecure mode). The reasons name paths from
// the caller's own request — never credential material.

// linkAuthz reports whether caller may perform action on everything cfg can
// reach: each explicit stream, every possible pattern match, and wake_stream.
func linkAuthz(caller controlCaller, action auth.Action, cfg Config) string {
	if reason := linkPathsAuthz(caller, action, cfg.Streams); reason != "" {
		return reason
	}
	if cfg.Pattern != "" && !caller.trustedGateway() {
		prefix, ok := GlobLiteralPrefix(cfg.Pattern)
		if !ok {
			return fmt.Sprintf("pattern %q is not bounded by a namespace", cfg.Pattern)
		}
		path, err := auth.NormalizeStreamPath(prefix)
		if err != nil {
			return fmt.Sprintf("pattern %q is not bounded by a namespace", cfg.Pattern)
		}
		if decision := caller.authorize(action, path); !decision.Allowed() {
			return decision.Detail()
		}
	}
	if cfg.WakeStream != "" {
		path, err := auth.NormalizeStreamPath(cfg.WakeStream)
		if err != nil {
			return fmt.Sprintf("wake_stream %q is invalid", cfg.WakeStream)
		}
		if decision := caller.authorize(action, path); !decision.Allowed() {
			return decision.Detail()
		}
	}
	return ""
}

// linkPathsAuthz checks explicit stream paths for action.
func linkPathsAuthz(caller controlCaller, action auth.Action, paths []string) string {
	for _, raw := range paths {
		path, err := auth.NormalizeStreamPath(raw)
		if err != nil {
			return fmt.Sprintf("stream %q is invalid", raw)
		}
		if decision := caller.authorize(action, path); !decision.Allowed() {
			return decision.Detail()
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

func controlOwnershipAuthz(storedOwner string, caller controlCaller) string {
	if caller.trustedGateway() {
		return ""
	}
	return ownershipAuthz(storedOwner, caller.Subject())
}

// claimAuthz authorizes the complete subscription snapshot a successful claim
// will expose and turn into callback, write, and wake capabilities.
func claimAuthz(caller controlCaller, sub Subscription) string {
	if reason := controlOwnershipAuthz(sub.OwnerSubject, caller); reason != "" {
		return reason
	}
	return linkAuthz(caller, auth.ActionClaim, sub.Config)
}
