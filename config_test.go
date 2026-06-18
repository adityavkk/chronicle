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
