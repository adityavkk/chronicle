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

func armWakeKeys(id string, owner ownerScriptArgs) ([]string, error) {
	h := slotOf(id)
	return ownerScriptKeys([]string{subKey(id), leaseZKey(h), dueZKey(h)}, h, owner)
}

func ackKeys(id string, g int, owner ownerScriptArgs) ([]string, error) {
	h := slotOf(id)
	return ownerScriptKeys([]string{subShardKey(id, g), linksKey(id), leaseZKey(h), retryZKey(h), dueZKey(h), subKey(id)}, h, owner)
}

func releaseKeys(id string, owner ownerScriptArgs) ([]string, error) {
	h := slotOf(id)
	return ownerScriptKeys([]string{subKey(id), leaseZKey(h), retryZKey(h), dueZKey(h)}, h, owner)
}

func expireLeaseKeys(id string, owner ownerScriptArgs) ([]string, error) {
	h := slotOf(id)
	return ownerScriptKeys([]string{subKey(id), leaseZKey(h), dueZKey(h)}, h, owner)
}

func scheduleRetryKeys(id string, owner ownerScriptArgs) ([]string, error) {
	h := slotOf(id)
	return ownerScriptKeys([]string{subKey(id), retryZKey(h)}, h, owner)
}

func recordSuccessKeys(id string, owner ownerScriptArgs) ([]string, error) {
	h := slotOf(id)
	return ownerScriptKeys([]string{subKey(id), retryZKey(h)}, h, owner)
}
