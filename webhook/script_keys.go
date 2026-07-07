package webhook

import "fmt"

// ownerScriptKey validates the co-homed owner slot key only for explicit Owned
// calls. Unscoped calls declare no owner key, so Redis Cluster validates only the
// subscription's {__ds:h} keys before running Lua. Owned calls are strict: the
// presented scope must be for the same h as the subscription/schedule keys, or the
// caller has crossed ownership slots and the script would be unsafe to compose.
func ownerScriptKey(h int, owner ownerScriptArgs) (string, error) {
	if owner.epoch == "" {
		return "", nil
	}
	want := slotKey(h)
	if owner.slotKey != want {
		return "", fmt.Errorf("webhook: owner scope key %q does not match slot %d (%q)", owner.slotKey, h, want)
	}
	return owner.slotKey, nil
}

type createSubKeys struct {
	Sub                string
	SubsSet            string
	Links              string
	IncarnationCounter string
}

func (k createSubKeys) redisKeys() []string {
	return []string{k.Sub, k.SubsSet, k.Links, k.IncarnationCounter}
}

func (k createSubKeys) keyRoles() []scriptKeyRole {
	return []scriptKeyRole{"sub", "subs_set", "links", "incarnation_counter"}
}

func newCreateSubKeys(id string) createSubKeys {
	h := slotOf(id)
	return createSubKeys{Sub: subKey(id), SubsSet: subsKey(h), Links: linksKey(id), IncarnationCounter: subIncarnationKey(id)}
}

type linkStreamKeys struct{ Links string }

func (k linkStreamKeys) redisKeys() []string       { return []string{k.Links} }
func (k linkStreamKeys) keyRoles() []scriptKeyRole { return []scriptKeyRole{"links"} }

func newLinkStreamKeys(id string) linkStreamKeys { return linkStreamKeys{Links: linksKey(id)} }

type unlinkStreamKeys struct{ Links string }

func (k unlinkStreamKeys) redisKeys() []string       { return []string{k.Links} }
func (k unlinkStreamKeys) keyRoles() []scriptKeyRole { return []scriptKeyRole{"links"} }

func newUnlinkStreamKeys(id string) unlinkStreamKeys { return unlinkStreamKeys{Links: linksKey(id)} }

type armWakeKeyVec struct {
	Sub       string
	LeaseZSet string
	DueZSet   string
	Slot      string
}

func (k armWakeKeyVec) redisKeys() []string {
	keys := []string{k.Sub, k.LeaseZSet, k.DueZSet}
	if k.Slot != "" {
		keys = append(keys, k.Slot)
	}
	return keys
}

func (k armWakeKeyVec) keyRoles() []scriptKeyRole {
	roles := []scriptKeyRole{"sub", "lease_zset", "due_zset"}
	if k.Slot != "" {
		roles = append(roles, "slot")
	}
	return roles
}

func armWakeKeys(id string, owner ownerScriptArgs) (armWakeKeyVec, error) {
	h := slotOf(id)
	slot, err := ownerScriptKey(h, owner)
	if err != nil {
		return armWakeKeyVec{}, err
	}
	return armWakeKeyVec{Sub: subKey(id), LeaseZSet: leaseZKey(h), DueZSet: dueZKey(h), Slot: slot}, nil
}

type claimKeyVec struct {
	SubConfig          string
	ShardState         string
	LeaseZSet          string
	IncarnationCounter string
	ShardRegistry      string
}

func (k claimKeyVec) redisKeys() []string {
	return []string{k.SubConfig, k.ShardState, k.LeaseZSet, k.IncarnationCounter, k.ShardRegistry}
}

func (k claimKeyVec) keyRoles() []scriptKeyRole {
	return []scriptKeyRole{"sub_config", "shardstate", "lease_zset", "incarnation_counter", "shard_registry"}
}

func claimKeys(id string, g int) claimKeyVec {
	h := slotOf(id)
	return claimKeyVec{SubConfig: subKey(id), ShardState: subShardKey(id, g), LeaseZSet: leaseZKey(h), IncarnationCounter: subIncarnationKey(id), ShardRegistry: subShardRegistryKey(id)}
}

type writeFenceKeyVec struct {
	ShardState string
	SubConfig  string
}

func (k writeFenceKeyVec) redisKeys() []string { return []string{k.ShardState, k.SubConfig} }
func (k writeFenceKeyVec) keyRoles() []scriptKeyRole {
	return []scriptKeyRole{"shardstate", "sub_config"}
}

func newWriteFenceKeys(id string, g int) writeFenceKeyVec {
	return writeFenceKeyVec{ShardState: subShardKey(id, g), SubConfig: subKey(id)}
}

type ackKeyVec struct {
	ShardState string
	Links      string
	LeaseZSet  string
	RetryZSet  string
	DueZSet    string
	SubConfig  string
	Slot       string
}

func (k ackKeyVec) redisKeys() []string {
	keys := []string{k.ShardState, k.Links, k.LeaseZSet, k.RetryZSet, k.DueZSet, k.SubConfig}
	if k.Slot != "" {
		keys = append(keys, k.Slot)
	}
	return keys
}

func (k ackKeyVec) keyRoles() []scriptKeyRole {
	roles := []scriptKeyRole{"shardstate", "links", "lease_zset", "retry_zset", "due_zset", "sub_config"}
	if k.Slot != "" {
		roles = append(roles, "slot")
	}
	return roles
}

func ackKeys(id string, g int, owner ownerScriptArgs) (ackKeyVec, error) {
	h := slotOf(id)
	slot, err := ownerScriptKey(h, owner)
	if err != nil {
		return ackKeyVec{}, err
	}
	return ackKeyVec{ShardState: subShardKey(id, g), Links: linksKey(id), LeaseZSet: leaseZKey(h), RetryZSet: retryZKey(h), DueZSet: dueZKey(h), SubConfig: subKey(id), Slot: slot}, nil
}

type releaseKeyVec struct {
	Sub       string
	LeaseZSet string
	RetryZSet string
	DueZSet   string
	Slot      string
}

func (k releaseKeyVec) redisKeys() []string {
	keys := []string{k.Sub, k.LeaseZSet, k.RetryZSet, k.DueZSet}
	if k.Slot != "" {
		keys = append(keys, k.Slot)
	}
	return keys
}

func (k releaseKeyVec) keyRoles() []scriptKeyRole {
	roles := []scriptKeyRole{"sub", "lease_zset", "retry_zset", "due_zset"}
	if k.Slot != "" {
		roles = append(roles, "slot")
	}
	return roles
}

func releaseKeys(id string, owner ownerScriptArgs) (releaseKeyVec, error) {
	h := slotOf(id)
	slot, err := ownerScriptKey(h, owner)
	if err != nil {
		return releaseKeyVec{}, err
	}
	return releaseKeyVec{Sub: subKey(id), LeaseZSet: leaseZKey(h), RetryZSet: retryZKey(h), DueZSet: dueZKey(h), Slot: slot}, nil
}

type expireLeaseKeyVec struct {
	Sub       string
	LeaseZSet string
	DueZSet   string
	Slot      string
}

func (k expireLeaseKeyVec) redisKeys() []string {
	keys := []string{k.Sub, k.LeaseZSet, k.DueZSet}
	if k.Slot != "" {
		keys = append(keys, k.Slot)
	}
	return keys
}

func (k expireLeaseKeyVec) keyRoles() []scriptKeyRole {
	roles := []scriptKeyRole{"sub", "lease_zset", "due_zset"}
	if k.Slot != "" {
		roles = append(roles, "slot")
	}
	return roles
}

func expireLeaseKeys(id string, owner ownerScriptArgs) (expireLeaseKeyVec, error) {
	h := slotOf(id)
	slot, err := ownerScriptKey(h, owner)
	if err != nil {
		return expireLeaseKeyVec{}, err
	}
	return expireLeaseKeyVec{Sub: subKey(id), LeaseZSet: leaseZKey(h), DueZSet: dueZKey(h), Slot: slot}, nil
}

type restoreLeaseKeys struct {
	Sub       string
	LeaseZSet string
	DueZSet   string
}

func (k restoreLeaseKeys) redisKeys() []string { return []string{k.Sub, k.LeaseZSet, k.DueZSet} }
func (k restoreLeaseKeys) keyRoles() []scriptKeyRole {
	return []scriptKeyRole{"sub", "lease_zset", "due_zset"}
}

func newRestoreLeaseKeys(id string) restoreLeaseKeys {
	h := slotOf(id)
	return restoreLeaseKeys{Sub: subKey(id), LeaseZSet: leaseZKey(h), DueZSet: dueZKey(h)}
}

type claimDueKeys struct{ ZSet string }

func (k claimDueKeys) redisKeys() []string       { return []string{k.ZSet} }
func (k claimDueKeys) keyRoles() []scriptKeyRole { return []scriptKeyRole{"zset"} }

func newClaimDueKeys(zkey string) claimDueKeys { return claimDueKeys{ZSet: zkey} }

type scheduleRetryKeyVec struct {
	Sub       string
	RetryZSet string
	Slot      string
}

func (k scheduleRetryKeyVec) redisKeys() []string {
	keys := []string{k.Sub, k.RetryZSet}
	if k.Slot != "" {
		keys = append(keys, k.Slot)
	}
	return keys
}

func (k scheduleRetryKeyVec) keyRoles() []scriptKeyRole {
	roles := []scriptKeyRole{"sub", "retry_zset"}
	if k.Slot != "" {
		roles = append(roles, "slot")
	}
	return roles
}

func scheduleRetryKeys(id string, owner ownerScriptArgs) (scheduleRetryKeyVec, error) {
	h := slotOf(id)
	slot, err := ownerScriptKey(h, owner)
	if err != nil {
		return scheduleRetryKeyVec{}, err
	}
	return scheduleRetryKeyVec{Sub: subKey(id), RetryZSet: retryZKey(h), Slot: slot}, nil
}

type recordSuccessKeyVec struct {
	Sub       string
	RetryZSet string
	Slot      string
}

func (k recordSuccessKeyVec) redisKeys() []string {
	keys := []string{k.Sub, k.RetryZSet}
	if k.Slot != "" {
		keys = append(keys, k.Slot)
	}
	return keys
}

func (k recordSuccessKeyVec) keyRoles() []scriptKeyRole {
	roles := []scriptKeyRole{"sub", "retry_zset"}
	if k.Slot != "" {
		roles = append(roles, "slot")
	}
	return roles
}

func recordSuccessKeys(id string, owner ownerScriptArgs) (recordSuccessKeyVec, error) {
	h := slotOf(id)
	slot, err := ownerScriptKey(h, owner)
	if err != nil {
		return recordSuccessKeyVec{}, err
	}
	return recordSuccessKeyVec{Sub: subKey(id), RetryZSet: retryZKey(h), Slot: slot}, nil
}

type recordWakeSentKeys struct{ Sub string }

func (k recordWakeSentKeys) redisKeys() []string       { return []string{k.Sub} }
func (k recordWakeSentKeys) keyRoles() []scriptKeyRole { return []scriptKeyRole{"sub"} }

func newRecordWakeSentKeys(id string) recordWakeSentKeys { return recordWakeSentKeys{Sub: subKey(id)} }

type deleteSubKeys struct {
	Sub           string
	SubsSet       string
	Links         string
	LeaseZSet     string
	RetryZSet     string
	DueZSet       string
	ShardRegistry string
}

func (k deleteSubKeys) redisKeys() []string {
	return []string{k.Sub, k.SubsSet, k.Links, k.LeaseZSet, k.RetryZSet, k.DueZSet, k.ShardRegistry}
}

func (k deleteSubKeys) keyRoles() []scriptKeyRole {
	return []scriptKeyRole{"sub", "subs_set", "links", "lease_zset", "retry_zset", "due_zset", "shard_registry"}
}

func newDeleteSubKeys(id string) deleteSubKeys {
	h := slotOf(id)
	return deleteSubKeys{Sub: subKey(id), SubsSet: subsKey(h), Links: linksKey(id), LeaseZSet: leaseZKey(h), RetryZSet: retryZKey(h), DueZSet: dueZKey(h), ShardRegistry: subShardRegistryKey(id)}
}

type getOrCreateKeyKeys struct {
	JWKSHash  string
	ActiveKid string
}

func (k getOrCreateKeyKeys) redisKeys() []string { return []string{k.JWKSHash, k.ActiveKid} }
func (k getOrCreateKeyKeys) keyRoles() []scriptKeyRole {
	return []scriptKeyRole{"jwks_hash", "active_kid"}
}

func newGetOrCreateKeyKeys(hashKey, activeKey string) getOrCreateKeyKeys {
	return getOrCreateKeyKeys{JWKSHash: hashKey, ActiveKid: activeKey}
}

type claimShardKeys struct{ Slot string }

func (k claimShardKeys) redisKeys() []string       { return []string{k.Slot} }
func (k claimShardKeys) keyRoles() []scriptKeyRole { return []scriptKeyRole{"slot"} }

func newClaimShardKeys(slotKey string) claimShardKeys { return claimShardKeys{Slot: slotKey} }

type checkOwnerKeys struct{ Slot string }

func (k checkOwnerKeys) redisKeys() []string       { return []string{k.Slot} }
func (k checkOwnerKeys) keyRoles() []scriptKeyRole { return []scriptKeyRole{"slot"} }

func newCheckOwnerKeys(slotKey string) checkOwnerKeys { return checkOwnerKeys{Slot: slotKey} }

type reserveLegacySlotKeys struct{ LegacySlot string }

func (k reserveLegacySlotKeys) redisKeys() []string       { return []string{k.LegacySlot} }
func (k reserveLegacySlotKeys) keyRoles() []scriptKeyRole { return []scriptKeyRole{"legacy_slot"} }

func newReserveLegacySlotKeys(key string) reserveLegacySlotKeys {
	return reserveLegacySlotKeys{LegacySlot: key}
}

type rotateKeyKeys struct {
	FamilyHash string
	ActiveKid  string
}

func (k rotateKeyKeys) redisKeys() []string { return []string{k.FamilyHash, k.ActiveKid} }
func (k rotateKeyKeys) keyRoles() []scriptKeyRole {
	return []scriptKeyRole{"family_hash", "active_kid"}
}

func newRotateKeyKeys(hashKey, activeKey string) rotateKeyKeys {
	return rotateKeyKeys{FamilyHash: hashKey, ActiveKid: activeKey}
}
