package chaos

import (
	"strings"
	"testing"
)

func TestPodKillCmd(t *testing.T) {
	got := strings.Join(PodKillCmd("chronicle-loadtest", "app=chronicle"), " ")
	want := "delete pods -n chronicle-loadtest -l app=chronicle --grace-period=0 --force"
	if got != want {
		t.Fatalf("PodKillCmd = %q, want %q", got, want)
	}
}

func TestKillPods_UsesInjectedRunnerWithKubectl(t *testing.T) {
	var name string
	var args []string
	k := Killer{Namespace: "ns", Selector: "app=chronicle", Run: func(n string, a ...string) ([]byte, error) {
		name, args = n, a
		return []byte("pod deleted"), nil
	}}
	if _, err := k.KillPods(); err != nil {
		t.Fatal(err)
	}
	if name != "kubectl" {
		t.Fatalf("expected kubectl, got %q", name)
	}
	if joined := strings.Join(args, " "); !strings.Contains(joined, "delete pods") || !strings.Contains(joined, "app=chronicle") {
		t.Fatalf("unexpected kubectl args: %q", joined)
	}
}

func TestKillPods_RefusesEmptySelector(t *testing.T) {
	// An empty selector would match every pod in the namespace — refuse it.
	if _, err := (Killer{Namespace: "ns"}).KillPods(); err == nil {
		t.Fatal("expected an error for an empty selector")
	}
}
