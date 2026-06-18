package main

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGCPauseDurationHoldsPastLeaseTTL(t *testing.T) {
	got := gcPauseDuration(1000)
	if got <= time.Second {
		t.Fatalf("gcPauseDuration = %s, want > lease ttl", got)
	}
	if got != 1250*time.Millisecond {
		t.Fatalf("gcPauseDuration = %s, want 1.25s", got)
	}
}

func TestNormalizeNemesisWindow(t *testing.T) {
	minWindow, maxWindow := normalizeNemesisWindow(8*time.Second, 2*time.Second)
	if minWindow != 2*time.Second || maxWindow != 8*time.Second {
		t.Fatalf("normalize swapped = %s/%s, want 2s/8s", minWindow, maxWindow)
	}
	minWindow, maxWindow = normalizeNemesisWindow(0, 0)
	if minWindow != 2*time.Second || maxWindow != 2*time.Second {
		t.Fatalf("normalize defaults = %s/%s, want 2s/2s", minWindow, maxWindow)
	}
}

func TestUnsupportedNemesisPrimitivesFailClearly(t *testing.T) {
	n := &nemesis{}
	for name, fn := range map[string]func() error{
		"killSlotOwner":      func() error { return n.killSlotOwner(3) },
		"toxiproxyPartition": n.toxiproxyPartition,
		"clockSkew":          n.clockSkew,
	} {
		if err := fn(); err == nil || !strings.Contains(err.Error(), "requires") {
			t.Fatalf("%s error = %v, want explicit requires error", name, err)
		}
	}
}

func TestUnsupportedNemesisPrimitivesDryRun(t *testing.T) {
	n := &nemesis{dryRun: true}
	if err := n.killSlotOwner(3); err != nil {
		t.Fatal(err)
	}
	if err := n.toxiproxyPartition(); err != nil {
		t.Fatal(err)
	}
	if err := n.clockSkew(); err != nil {
		t.Fatal(err)
	}
	if got := join(n.log); !strings.Contains(got, "dry-run-kill-slot-owner-3") ||
		!strings.Contains(got, "dry-run-toxiproxy-partition") ||
		!strings.Contains(got, "dry-run-clock-skew") {
		t.Fatalf("dry-run log = %q", got)
	}
}

func TestDropLeaseTailCommandOnlyZREMsLeaseSchedule(t *testing.T) {
	want := []string{"zrem", "ds:{__ds}:sched:lease", "sub-1"}
	if got := dropLeaseTailCommand("sub-1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("dropLeaseTailCommand = %#v, want %#v", got, want)
	}
}
