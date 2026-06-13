package webhook

import (
	"embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/*.lua
var scriptFS embed.FS

// loadScript builds a redis.Script from the shared common.lua prelude plus the
// named script body, mirroring store/redis/scripts.go. Script.Run handles
// NOSCRIPT reloads transparently so EVALSHA survives a flushed script cache.
func loadScript(name string) *redis.Script {
	prelude, err := scriptFS.ReadFile("scripts/common.lua")
	if err != nil {
		panic(fmt.Sprintf("webhook: embedded common.lua missing: %v", err))
	}
	body, err := scriptFS.ReadFile("scripts/" + name)
	if err != nil {
		panic(fmt.Sprintf("webhook: embedded script %s missing: %v", name, err))
	}
	return redis.NewScript(string(prelude) + "\n" + string(body))
}

var (
	createSubScript      = loadScript("create_sub.lua")
	linkStreamScript     = loadScript("link_stream.lua")
	unlinkStreamScript   = loadScript("unlink_stream.lua")
	armWakeScript        = loadScript("arm_wake.lua")
	claimScript          = loadScript("claim.lua")
	ackScript            = loadScript("ack.lua")
	releaseScript        = loadScript("release.lua")
	expireLeaseScript    = loadScript("expire_lease.lua")
	claimDueScript       = loadScript("claim_due.lua")
	scheduleRetryScript  = loadScript("schedule_retry.lua")
	recordSuccessScript  = loadScript("record_success.lua")
	recordWakeSentScript = loadScript("record_wake_sent.lua")
	deleteSubScript      = loadScript("delete_sub.lua")
	getOrCreateKeyScript = loadScript("get_or_create_key.lua")
)
