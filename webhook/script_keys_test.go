package webhook

import (
	"strings"
	"testing"
)

func assertScriptKeysSingleSlot(t *testing.T, name string, keys []string) {
	t.Helper()
	if len(keys) == 0 {
		t.Fatalf("%s declared no keys", name)
	}
	want := clusterSlot(keys[0])
	for i, k := range keys[1:] {
		if got := clusterSlot(k); got != want {
			t.Fatalf("%s KEYS[%d]=%q cluster slot %d, want %d from %q", name, i+2, k, got, want, keys[0])
		}
	}
}

func mustScriptKeys(t *testing.T, name string, build func() ([]string, error)) []string {
	t.Helper()
	keys, err := build()
	if err != nil {
		t.Fatalf("%s keys: %v", name, err)
	}
	return keys
}

// TestScriptKeyVectorsAreClusterSingleSlot is the executable slot policy for the
// Lua registry: every EVAL key vector is internally single-slot, and the explicit
// Unscoped schedule/due calls declare only {__ds:h} keys. Redis Cluster validates
// the declared KEYS before Lua can short-circuit owner_fenced, so no-scope paths
// must not smuggle an ownership key.
func TestScriptKeyVectorsAreClusterSingleSlot(t *testing.T) {
	id := "sub-with-{braces}-and-:colons"
	g := 7
	h := slotOf(id)
	owned := ownerScriptArgs{slotKey: slotKey(h), replicaID: "replica", epoch: "1"}

	noOwnerCases := map[string][]string{
		"arm_wake/unscoped": mustScriptKeys(t, "arm_wake/unscoped", func() ([]string, error) { v, err := armWakeKeys(id, unscopedOwnerArgs()); return v.redisKeys(), err }),
		"ack/unscoped":      mustScriptKeys(t, "ack/unscoped", func() ([]string, error) { v, err := ackKeys(id, g, unscopedOwnerArgs()); return v.redisKeys(), err }),
		"release/unscoped":  mustScriptKeys(t, "release/unscoped", func() ([]string, error) { v, err := releaseKeys(id, unscopedOwnerArgs()); return v.redisKeys(), err }),
		"expire_lease/unscoped": mustScriptKeys(t, "expire_lease/unscoped", func() ([]string, error) {
			v, err := expireLeaseKeys(id, unscopedOwnerArgs())
			return v.redisKeys(), err
		}),
		"schedule_retry/unscoped": mustScriptKeys(t, "schedule_retry/unscoped", func() ([]string, error) {
			v, err := scheduleRetryKeys(id, unscopedOwnerArgs())
			return v.redisKeys(), err
		}),
		"record_success/unscoped": mustScriptKeys(t, "record_success/unscoped", func() ([]string, error) {
			v, err := recordSuccessKeys(id, unscopedOwnerArgs())
			return v.redisKeys(), err
		}),
	}
	for name, keys := range noOwnerCases {
		assertScriptKeysSingleSlot(t, name, keys)
		for _, k := range keys {
			if strings.Contains(k, ":ownership:slot:") || strings.Contains(k, "{ownership}") {
				t.Fatalf("%s unscoped key vector declares owner key %q", name, k)
			}
		}
	}
	if got := len(noOwnerCases["arm_wake/unscoped"]); got != 3 {
		t.Fatalf("arm_wake/unscoped declares %d keys, want 3", got)
	}
	if got := len(noOwnerCases["ack/unscoped"]); got != 6 {
		t.Fatalf("ack/unscoped declares %d keys, want 6", got)
	}

	cases := map[string][]string{
		"create_sub":            {subKey(id), subsKey(h), linksKey(id), subIncarnationKey(id)},
		"delete_sub":            {subKey(id), subsKey(h), linksKey(id), leaseZKey(h), retryZKey(h), dueZKey(h), subShardRegistryKey(id)},
		"link_stream":           {linksKey(id)},
		"unlink_stream":         {linksKey(id)},
		"arm_wake/owned":        mustScriptKeys(t, "arm_wake/owned", func() ([]string, error) { v, err := armWakeKeys(id, owned); return v.redisKeys(), err }),
		"claim/g0":              {subKey(id), subShardKey(id, 0), leaseZKey(h), subIncarnationKey(id), subShardRegistryKey(id), linksKey(id)},
		"claim/g7":              {subKey(id), subShardKey(id, g), leaseZKey(h), subIncarnationKey(id), subShardRegistryKey(id), linksKey(id)},
		"ack/owned":             mustScriptKeys(t, "ack/owned", func() ([]string, error) { v, err := ackKeys(id, g, owned); return v.redisKeys(), err }),
		"release/owned":         mustScriptKeys(t, "release/owned", func() ([]string, error) { v, err := releaseKeys(id, owned); return v.redisKeys(), err }),
		"expire_lease/owned":    mustScriptKeys(t, "expire_lease/owned", func() ([]string, error) { v, err := expireLeaseKeys(id, owned); return v.redisKeys(), err }),
		"restore_lease":         {subKey(id), leaseZKey(h), dueZKey(h)},
		"claim_due/lease":       {leaseZKey(h)},
		"claim_due/retry":       {retryZKey(h)},
		"claim_due/due":         {dueZKey(h)},
		"schedule_retry/owned":  mustScriptKeys(t, "schedule_retry/owned", func() ([]string, error) { v, err := scheduleRetryKeys(id, owned); return v.redisKeys(), err }),
		"record_success/owned":  mustScriptKeys(t, "record_success/owned", func() ([]string, error) { v, err := recordSuccessKeys(id, owned); return v.redisKeys(), err }),
		"record_wake_sent":      {subKey(id)},
		"claim_shard":           {slotKey(h)},
		"check_owner":           {slotKey(h)},
		"reserve_legacy_slot":   {legacyOwnershipSlotKey(h)},
		"get_or_create_webhook": {jwksKey, activeKidKey},
		"get_or_create_wake":    {wakeKeysKey, wakeActiveKidKey},
		"rotate_key_webhook":    {jwksKey, activeKidKey},
		"rotate_key_wake":       {wakeKeysKey, wakeActiveKidKey},
	}
	for name, keys := range cases {
		assertScriptKeysSingleSlot(t, name, keys)
	}

	bad := (h + 1) % subSlots
	if _, err := armWakeKeys(id, ownerScriptArgs{slotKey: slotKey(bad), replicaID: "replica", epoch: "1"}); err == nil {
		t.Fatalf("owned script key builder accepted slotKey(%d) for subscription slot %d", bad, h)
	}
}
