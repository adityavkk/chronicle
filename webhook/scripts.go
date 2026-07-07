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
//
// Always invoke these via Script.Run/RunRO: the NOSCRIPT->EVAL self-heal does
// NOT fire inside a pipeline/MULTI (go-redis #3228), so a bare EVALSHA there can
// fail NOSCRIPT after a cache flush/failover. A forbidigo rule (.golangci.yml)
// forbids bare EVAL/EVALSHA; SCRIPT LOAD + a justified //nolint if ever batching.
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
	createSubScript = newTypedScript[createSubKeys, createSubReply](scriptABI{
		Name: "create_sub", File: "create_sub.lua",
		Keys: []scriptKeySchema{keys("sub", "subs_set", "links", "incarnation_counter")},
		Args: variadicArgs(
			[]scriptArg{arg("id", argString), arg("cfg_hash", argString), arg("now_ns", argUnixNS), arg("type", argString), arg("pattern", argString), arg("webhook_url", argString), arg("wake_stream", argString), arg("lease_ttl_ms", argInt), arg("description", argString), arg("owner", argString), arg("num_links", argInt)},
			[]scriptArg{arg("path", argString), arg("link_type", argString), arg("offset", argString)}, nil,
		),
	}, createSubDecoder)
	linkStreamScript = newTypedScript[linkStreamKeys, linkStreamReply](scriptABI{
		Name: "link_stream", File: "link_stream.lua",
		Keys: []scriptKeySchema{keys("links")},
		Args: exactArgs(arg("path", argString), arg("link_type", argString), arg("offset", argString)),
	}, linkStreamDecoder)
	unlinkStreamScript = newTypedScript[unlinkStreamKeys, unlinkStreamReply](scriptABI{
		Name: "unlink_stream", File: "unlink_stream.lua",
		Keys: []scriptKeySchema{keys("links")},
		Args: exactArgs(arg("path", argString), arg("still_glob", argBool01)),
	}, unlinkStreamDecoder)
	armWakeScript = newTypedScript[armWakeKeyVec, armWakeReply](scriptABI{
		Name: "arm_wake", File: "arm_wake.lua",
		Keys: []scriptKeySchema{keys("sub", "lease_zset", "due_zset"), keys("sub", "lease_zset", "due_zset", "slot")},
		Args: exactArgs(arg("id", argString), arg("now_ns", argUnixNS), arg("lease_ttl_ms", argInt), arg("arm_lease", argBool01), arg("new_wake_id", argString), arg("replica_id", argString), arg("expected_epoch", argString)),
	}, armWakeDecoder)
	claimScript = newTypedScript[claimKeyVec, claimReply](scriptABI{
		Name: "claim", File: "claim.lua",
		Keys: []scriptKeySchema{keys("sub_config", "shardstate", "lease_zset", "incarnation_counter", "shard_registry")},
		Args: exactArgs(arg("member", argString), arg("worker", argString), arg("now_ns", argUnixNS), arg("lease_ttl_ms", argInt), arg("new_wake_id", argString), arg("shard_index", argInt)),
	}, claimDecoder)
	writeFenceScript = newTypedScript[writeFenceKeyVec, writeFenceReply](scriptABI{
		Name: "check_write_fence", File: "check_write_fence.lua",
		Keys: []scriptKeySchema{keys("shardstate", "sub_config")},
		Args: exactArgs(arg("now_ns", argUnixNS), arg("generation", argInt), arg("wake_id", argString), arg("holder", argString)),
	}, writeFenceDecoder)
	ackScript = newTypedScript[ackKeyVec, ackReply](scriptABI{
		Name: "ack", File: "ack.lua",
		Keys: []scriptKeySchema{keys("shardstate", "links", "lease_zset", "retry_zset", "due_zset", "sub_config"), keys("shardstate", "links", "lease_zset", "retry_zset", "due_zset", "sub_config", "slot")},
		Args: variadicArgs(
			[]scriptArg{arg("member", argString), arg("req_gen", argInt), arg("req_wake", argString), arg("token_gen", argInt), arg("done", argBool01), arg("now_ns", argUnixNS), arg("lease_ttl_ms", argInt), arg("num_acks", argInt)},
			[]scriptArg{arg("path", argString), arg("offset", argString)},
			[]scriptArg{arg("replica_id", argString), arg("expected_epoch", argString)},
		),
	}, ackDecoder)
	releaseScript = newTypedScript[releaseKeyVec, releaseReply](scriptABI{
		Name: "release", File: "release.lua",
		Keys: []scriptKeySchema{keys("sub", "lease_zset", "retry_zset", "due_zset"), keys("sub", "lease_zset", "retry_zset", "due_zset", "slot")},
		Args: exactArgs(arg("id", argString), arg("req_gen", argInt), arg("req_wake", argString), arg("token_gen", argInt), arg("replica_id", argString), arg("expected_epoch", argString)),
	}, releaseDecoder)
	expireLeaseScript = newTypedScript[expireLeaseKeyVec, expireLeaseReply](scriptABI{
		Name: "expire_lease", File: "expire_lease.lua",
		Keys: []scriptKeySchema{keys("sub", "lease_zset", "due_zset"), keys("sub", "lease_zset", "due_zset", "slot")},
		Args: exactArgs(arg("id", argString), arg("now_ns", argUnixNS), arg("replica_id", argString), arg("expected_epoch", argString)),
	}, expireLeaseDecoder)
	restoreLeaseScript = newTypedScript[restoreLeaseKeys, restoreLeaseReply](scriptABI{
		Name: "restore_lease", File: "restore_lease.lua",
		Keys: []scriptKeySchema{keys("sub", "lease_zset", "due_zset")},
		Args: exactArgs(arg("id", argString), arg("now_ns", argUnixNS), arg("owed", argBool01)),
	}, restoreLeaseDecoder)
	claimDueScript = newTypedScript[claimDueKeys, stringListReply](scriptABI{
		Name: "claim_due", File: "claim_due.lua",
		Keys: []scriptKeySchema{keys("zset")},
		Args: exactArgs(arg("now_ns", argUnixNS), arg("limit", argInt), arg("visibility_ns", argDuration)),
	}, stringListDecoder)
	scheduleRetryScript = newTypedScript[scheduleRetryKeyVec, scheduleRetryReply](scriptABI{
		Name: "schedule_retry", File: "schedule_retry.lua",
		Keys: []scriptKeySchema{keys("sub", "retry_zset"), keys("sub", "retry_zset", "slot")},
		Args: exactArgs(arg("id", argString), arg("now_ns", argUnixNS), arg("next_attempt_ns", argUnixNS), arg("generation", argInt), arg("wake_id", argString), arg("replica_id", argString), arg("expected_epoch", argString)),
	}, scheduleRetryDecoder)
	recordSuccessScript = newTypedScript[recordSuccessKeyVec, recordSuccessReply](scriptABI{
		Name: "record_success", File: "record_success.lua",
		Keys: []scriptKeySchema{keys("sub", "retry_zset"), keys("sub", "retry_zset", "slot")},
		Args: exactArgs(arg("id", argString), arg("generation", argInt), arg("wake_id", argString), arg("replica_id", argString), arg("expected_epoch", argString)),
	}, recordSuccessDecoder)
	recordWakeSentScript = newTypedScript[recordWakeSentKeys, recordWakeSentReply](scriptABI{
		Name: "record_wake_sent", File: "record_wake_sent.lua",
		Keys: []scriptKeySchema{keys("sub")},
		Args: exactArgs(arg("now_ns", argUnixNS), arg("generation", argInt), arg("wake_id", argString)),
	}, recordWakeSentDecoder)
	deleteSubScript = newTypedScript[deleteSubKeys, deleteSubReply](scriptABI{
		Name: "delete_sub", File: "delete_sub.lua",
		Keys: []scriptKeySchema{keys("sub", "subs_set", "links", "lease_zset", "retry_zset", "due_zset", "shard_registry")},
		Args: exactArgs(arg("id", argString)),
	}, deleteSubDecoder)
	getOrCreateKeyScript = newTypedScript[getOrCreateKeyKeys, keyMaterialReply](scriptABI{
		Name: "get_or_create_key", File: "get_or_create_key.lua",
		Keys: []scriptKeySchema{keys("jwks_hash", "active_kid")},
		Args: exactArgs(arg("candidate_kid", argString), arg("candidate_material", argString)),
	}, keyMaterialDecoder)
	// Work-sharded leased slot ownership (issue #14): the co-homed slot CAS and its
	// owner-epoch fence. Orthogonal to the per-(subId,g) claim granularity above
	// (#11) — slot ownership shards which replica runs background work.
	claimShardScript = newTypedScript[claimShardKeys, slotClaimReply](scriptABI{
		Name: "claim_shard", File: "claim_shard.lua",
		Keys: []scriptKeySchema{keys("slot")},
		Args: exactArgs(arg("replica_id", argString), arg("now_ns", argUnixNS), arg("slot_lease_ttl_ms", argDuration)),
	}, slotClaimDecoder)
	checkOwnerScript = newTypedScript[checkOwnerKeys, ownerCheckReply](scriptABI{
		Name: "check_owner", File: "check_owner.lua",
		Keys: []scriptKeySchema{keys("slot")},
		Args: exactArgs(arg("replica_id", argString), arg("expected_epoch", argString)),
	}, ownerCheckDecoder)
	// Rollout guard for the pre-#146 owner key. It keeps mixed old/new pods from
	// owning the same logical slot through different hashes during deployment.
	reserveLegacySlotScript = newTypedScript[reserveLegacySlotKeys, reserveLegacySlotReply](scriptABI{
		Name: "reserve_legacy_slot", File: "reserve_legacy_slot.lua",
		Keys: []scriptKeySchema{keys("legacy_slot")},
		Args: exactArgs(arg("replica_id", argString), arg("now_ns", argUnixNS), arg("slot_lease_ttl_ms", argDuration)),
	}, reserveLegacySlotDecoder)
	// Key rotation (#123/#126 TBrot): the atomic successor-mint + active_kid
	// CAS in the {__ds} slot.
	rotateKeyScript = newTypedScript[rotateKeyKeys, rotateKeyReply](scriptABI{
		Name: "rotate_key", File: "rotate_key.lua",
		Keys: []scriptKeySchema{keys("family_hash", "active_kid")},
		Args: exactArgs(arg("expected_active_kid", argString), arg("new_kid", argString), arg("new_material", argString), arg("retire_after_unix", argInt)),
	}, rotateKeyDecoder)
)

var registeredScripts = []registeredScript{
	createSubScript.registration(), linkStreamScript.registration(), unlinkStreamScript.registration(), armWakeScript.registration(),
	claimScript.registration(), writeFenceScript.registration(), ackScript.registration(), releaseScript.registration(), expireLeaseScript.registration(),
	restoreLeaseScript.registration(), claimDueScript.registration(), scheduleRetryScript.registration(),
	recordSuccessScript.registration(), recordWakeSentScript.registration(), deleteSubScript.registration(),
	getOrCreateKeyScript.registration(), claimShardScript.registration(), checkOwnerScript.registration(),
	reserveLegacySlotScript.registration(), rotateKeyScript.registration(),
}
