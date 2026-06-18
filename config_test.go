package chronicle

import (
	"testing"
	"time"
)

func TestDefaultConfigSweepIntervalIsCoarseFloor(t *testing.T) {
	got := DefaultConfig().SweepInterval
	if got == 2*time.Second {
		t.Fatal("default config sweep interval must not remain the old 2s recovery sweep")
	}
	if got < 5*time.Second || got > 5*time.Minute {
		t.Fatalf("default config sweep interval %s outside seconds-to-minutes floor band", got)
	}
	if got != DefaultConfig().ReconcileInterval {
		t.Fatalf("default config sweep interval = %s, want reconcile interval %s",
			got, DefaultConfig().ReconcileInterval)
	}
}

func TestDefaultConfigOwnershipTiming(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MemberLeaseTTL != 9*time.Second ||
		cfg.HeartbeatInterval != 3*time.Second ||
		cfg.SlotLeaseTTL != 9*time.Second ||
		cfg.SlotReconcileInterval != 3*time.Second {
		t.Fatalf("ownership defaults = member %s heartbeat %s slot %s reconcile %s",
			cfg.MemberLeaseTTL, cfg.HeartbeatInterval, cfg.SlotLeaseTTL, cfg.SlotReconcileInterval)
	}
}

func TestConfigLoadEnvOwnership(t *testing.T) {
	cfg := DefaultConfig()
	values := map[string]string{
		EnvReplicaID:         "replica-test",
		EnvMemberLeaseTTL:    "11s",
		EnvHeartbeatInterval: "4s",
		EnvSlotLeaseTTL:      "12s",
		EnvSlotReconcile:     "2s",
	}
	if err := cfg.LoadEnv(func(k string) (string, bool) {
		v, ok := values[k]
		return v, ok
	}); err != nil {
		t.Fatal(err)
	}
	if cfg.ReplicaID != "replica-test" ||
		cfg.MemberLeaseTTL != 11*time.Second ||
		cfg.HeartbeatInterval != 4*time.Second ||
		cfg.SlotLeaseTTL != 12*time.Second ||
		cfg.SlotReconcileInterval != 2*time.Second {
		t.Fatalf("loaded ownership env = %+v", cfg)
	}
}
