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

func TestKillSlotOwnerReadsSlotHomedOwnerKey(t *testing.T) {
	slot := 3
	redisLookup := append([]string{"exec", "deploy/redis", "--", "redis-cli", "-n", "0"}, slotOwnerCommand(slot)...)
	listPods := []string{"get", "pods", "-l", "app=chronicle", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"}
	deletePod := []string{"delete", "pod", "chronicle-0", "--grace-period=0", "--force"}
	var calls [][]string
	n := &nemesis{kubectlFn: func(args ...string) ([]byte, error) {
		call := append([]string(nil), args...)
		calls = append(calls, call)
		switch {
		case reflect.DeepEqual(call, redisLookup):
			return []byte("chronicle-0-abcd\n"), nil
		case reflect.DeepEqual(call, listPods):
			return []byte("chronicle-0\nchronicle-1\n"), nil
		case reflect.DeepEqual(call, deletePod):
			return []byte("pod deleted\n"), nil
		default:
			t.Fatalf("unexpected kubectl call: %#v", call)
			return nil, nil
		}
	}}

	if err := n.killSlotOwner(slot); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls[0], redisLookup) {
		t.Fatalf("owner lookup command = %#v, want %#v", calls[0], redisLookup)
	}
	// This legacy key string is intentionally kept only as a regression guard.
	if strings.Contains(strings.Join(calls[0], " "), "ds:{ownership}:slot") {
		t.Fatalf("owner lookup used legacy ownership key: %#v", calls[0])
	}
	if got := join(n.log); !strings.Contains(got, "kill-slot-owner-3:chronicle-0") {
		t.Fatalf("killSlotOwner log = %q, want chronicle-0 kill", got)
	}
}

func TestDropLeaseTailCommandOnlyZREMsLeaseSchedule(t *testing.T) {
	want := []string{"zrem", "ds:{__ds:91}:sched:lease", "sub-1"}
	if got := dropLeaseTailCommand("sub-1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("dropLeaseTailCommand = %#v, want %#v", got, want)
	}
}

func TestLeaseTailDropRecoveryProbeCommands(t *testing.T) {
	if got, want := leaseScheduleScoreCommand("sub-1"), []string{"--raw", "zscore", "ds:{__ds:91}:sched:lease", "sub-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("leaseScheduleScoreCommand = %#v, want %#v", got, want)
	}
	if got, want := subscriptionFieldCommand("sub-1", "phase"), []string{"--raw", "hget", "ds:{__ds:91}:sub:sub-1", "phase"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("subscriptionFieldCommand = %#v, want %#v", got, want)
	}
}

func TestAcksFromClaimSnapshotOnlyPendingTails(t *testing.T) {
	got := acksFromClaimSnapshot([]claimStreamSnap{
		{Path: "events/a", TailOffset: "0000000000000001_0000000000000005", HasPending: true},
		{Path: "events/b", TailOffset: "0000000000000001_0000000000000007", HasPending: false},
	})
	want := []ackBody{{Stream: "events/a", Offset: "0000000000000001_0000000000000005"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("acksFromClaimSnapshot = %#v, want %#v", got, want)
	}
}
