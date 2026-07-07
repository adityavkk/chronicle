package webhook

import "fmt"

// ownerScriptKeys appends the co-homed owner slot key only for explicit Owned
// calls. Unscoped calls declare no owner key, so Redis Cluster validates only the
// subscription's {__ds:h} keys before running Lua. Owned calls are strict: the
// presented scope must be for the same h as the subscription/schedule keys, or the
// caller has crossed ownership slots and the script would be unsafe to compose.
func ownerScriptKeys(keys []string, h int, owner ownerScriptArgs) ([]string, error) {
	if owner.epoch == "" {
		return keys, nil
	}
	want := slotKey(h)
	if owner.slotKey != want {
		return nil, fmt.Errorf("webhook: owner scope key %q does not match slot %d (%q)", owner.slotKey, h, want)
	}
	return append(keys, owner.slotKey), nil
}

type (
	createSubKeys         struct{ scriptKeys }
	linkStreamKeys        struct{ scriptKeys }
	unlinkStreamKeys      struct{ scriptKeys }
	armWakeKeyVec         struct{ scriptKeys }
	claimKeyVec           struct{ scriptKeys }
	ackKeyVec             struct{ scriptKeys }
	releaseKeyVec         struct{ scriptKeys }
	expireLeaseKeyVec     struct{ scriptKeys }
	restoreLeaseKeys      struct{ scriptKeys }
	claimDueKeys          struct{ scriptKeys }
	scheduleRetryKeyVec   struct{ scriptKeys }
	recordSuccessKeyVec   struct{ scriptKeys }
	recordWakeSentKeys    struct{ scriptKeys }
	deleteSubKeys         struct{ scriptKeys }
	getOrCreateKeyKeys    struct{ scriptKeys }
	claimShardKeys        struct{ scriptKeys }
	checkOwnerKeys        struct{ scriptKeys }
	reserveLegacySlotKeys struct{ scriptKeys }
	rotateKeyKeys         struct{ scriptKeys }
)

func newScriptKeys(values []string, roles ...scriptKeyRole) scriptKeys {
	return scriptKeys{values: values, roles: roles}
}

func newCreateSubKeys(id string) createSubKeys {
	h := slotOf(id)
	return createSubKeys{newScriptKeys([]string{subKey(id), subsKey(h), linksKey(id), subIncarnationKey(id)}, "sub", "subs_set", "links", "incarnation_counter")}
}

func newLinkStreamKeys(id string) linkStreamKeys {
	return linkStreamKeys{newScriptKeys([]string{linksKey(id)}, "links")}
}

func newUnlinkStreamKeys(id string) unlinkStreamKeys {
	return unlinkStreamKeys{newScriptKeys([]string{linksKey(id)}, "links")}
}

func armWakeKeys(id string, owner ownerScriptArgs) (armWakeKeyVec, error) {
	h := slotOf(id)
	keys, err := ownerScriptKeys([]string{subKey(id), leaseZKey(h), dueZKey(h)}, h, owner)
	if err != nil {
		return armWakeKeyVec{}, err
	}
	roles := []scriptKeyRole{"sub", "lease_zset", "due_zset"}
	if owner.epoch != "" {
		roles = append(roles, "slot")
	}
	return armWakeKeyVec{scriptKeys{values: keys, roles: roles}}, nil
}

func claimKeys(id string, g int) claimKeyVec {
	h := slotOf(id)
	return claimKeyVec{newScriptKeys([]string{subKey(id), subShardKey(id, g), leaseZKey(h), subIncarnationKey(id), subShardRegistryKey(id)}, "sub_config", "shardstate", "lease_zset", "incarnation_counter", "shard_registry")}
}

func ackKeys(id string, g int, owner ownerScriptArgs) (ackKeyVec, error) {
	h := slotOf(id)
	keys, err := ownerScriptKeys([]string{subShardKey(id, g), linksKey(id), leaseZKey(h), retryZKey(h), dueZKey(h), subKey(id)}, h, owner)
	if err != nil {
		return ackKeyVec{}, err
	}
	roles := []scriptKeyRole{"shardstate", "links", "lease_zset", "retry_zset", "due_zset", "sub_config"}
	if owner.epoch != "" {
		roles = append(roles, "slot")
	}
	return ackKeyVec{scriptKeys{values: keys, roles: roles}}, nil
}

func releaseKeys(id string, owner ownerScriptArgs) (releaseKeyVec, error) {
	h := slotOf(id)
	keys, err := ownerScriptKeys([]string{subKey(id), leaseZKey(h), retryZKey(h), dueZKey(h)}, h, owner)
	if err != nil {
		return releaseKeyVec{}, err
	}
	roles := []scriptKeyRole{"sub", "lease_zset", "retry_zset", "due_zset"}
	if owner.epoch != "" {
		roles = append(roles, "slot")
	}
	return releaseKeyVec{scriptKeys{values: keys, roles: roles}}, nil
}

func expireLeaseKeys(id string, owner ownerScriptArgs) (expireLeaseKeyVec, error) {
	h := slotOf(id)
	keys, err := ownerScriptKeys([]string{subKey(id), leaseZKey(h), dueZKey(h)}, h, owner)
	if err != nil {
		return expireLeaseKeyVec{}, err
	}
	roles := []scriptKeyRole{"sub", "lease_zset", "due_zset"}
	if owner.epoch != "" {
		roles = append(roles, "slot")
	}
	return expireLeaseKeyVec{scriptKeys{values: keys, roles: roles}}, nil
}

func newRestoreLeaseKeys(id string) restoreLeaseKeys {
	h := slotOf(id)
	return restoreLeaseKeys{newScriptKeys([]string{subKey(id), leaseZKey(h), dueZKey(h)}, "sub", "lease_zset", "due_zset")}
}

func newClaimDueKeys(zkey string) claimDueKeys {
	return claimDueKeys{newScriptKeys([]string{zkey}, "zset")}
}

func scheduleRetryKeys(id string, owner ownerScriptArgs) (scheduleRetryKeyVec, error) {
	h := slotOf(id)
	keys, err := ownerScriptKeys([]string{subKey(id), retryZKey(h)}, h, owner)
	if err != nil {
		return scheduleRetryKeyVec{}, err
	}
	roles := []scriptKeyRole{"sub", "retry_zset"}
	if owner.epoch != "" {
		roles = append(roles, "slot")
	}
	return scheduleRetryKeyVec{scriptKeys{values: keys, roles: roles}}, nil
}

func recordSuccessKeys(id string, owner ownerScriptArgs) (recordSuccessKeyVec, error) {
	h := slotOf(id)
	keys, err := ownerScriptKeys([]string{subKey(id), retryZKey(h)}, h, owner)
	if err != nil {
		return recordSuccessKeyVec{}, err
	}
	roles := []scriptKeyRole{"sub", "retry_zset"}
	if owner.epoch != "" {
		roles = append(roles, "slot")
	}
	return recordSuccessKeyVec{scriptKeys{values: keys, roles: roles}}, nil
}

func newRecordWakeSentKeys(id string) recordWakeSentKeys {
	return recordWakeSentKeys{newScriptKeys([]string{subKey(id)}, "sub")}
}

func newDeleteSubKeys(id string) deleteSubKeys {
	h := slotOf(id)
	return deleteSubKeys{newScriptKeys([]string{subKey(id), subsKey(h), linksKey(id), leaseZKey(h), retryZKey(h), dueZKey(h), subShardRegistryKey(id)}, "sub", "subs_set", "links", "lease_zset", "retry_zset", "due_zset", "shard_registry")}
}

func newGetOrCreateKeyKeys(hashKey, activeKey string) getOrCreateKeyKeys {
	return getOrCreateKeyKeys{newScriptKeys([]string{hashKey, activeKey}, "jwks_hash", "active_kid")}
}

func newClaimShardKeys(slotKey string) claimShardKeys {
	return claimShardKeys{newScriptKeys([]string{slotKey}, "slot")}
}

func newCheckOwnerKeys(slotKey string) checkOwnerKeys {
	return checkOwnerKeys{newScriptKeys([]string{slotKey}, "slot")}
}

func newReserveLegacySlotKeys(key string) reserveLegacySlotKeys {
	return reserveLegacySlotKeys{newScriptKeys([]string{key}, "legacy_slot")}
}

func newRotateKeyKeys(hashKey, activeKey string) rotateKeyKeys {
	return rotateKeyKeys{newScriptKeys([]string{hashKey, activeKey}, "family_hash", "active_kid")}
}
