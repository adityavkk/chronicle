// Package chaos is the rig's pod-kill step (docs/specs/horizontal-scale/research/05
// Migration slice 0: "extend the loadgen rig — chaos/pod-kill"). It is the load-
// rig analogue of the jepsen nemesis: a coarse kubectl force-delete of SUT pods
// during a measured run, to observe the membership churn window (gate #4 / 07
// L2/L4) at scale. The command builder is pure and unit-tested; KillPod is the
// thin shell that runs it through an injectable runner.
package chaos

import (
	"fmt"
	"os/exec"
)

// PodKillCmd returns the kubectl arguments that force-delete every pod matching
// selector in namespace (e.g. selector "app=chronicle"). Force + grace-period=0 is
// a hard kill — the SUT loses its in-memory wake/lease state, the case the rig is
// there to exercise.
func PodKillCmd(namespace, selector string) []string {
	return []string{
		"delete", "pods",
		"-n", namespace,
		"-l", selector,
		"--grace-period=0", "--force",
	}
}

// Killer force-deletes SUT pods. Run is injectable (nil => exec.Command), so the
// command is unit-testable without a cluster.
type Killer struct {
	Namespace string
	Selector  string
	Run       func(name string, args ...string) ([]byte, error)
}

// KillPods force-deletes the pods matching the killer's selector, returning the
// kubectl output.
func (k Killer) KillPods() ([]byte, error) {
	if k.Selector == "" {
		return nil, fmt.Errorf("chaos: empty selector would match every pod")
	}
	return k.run("kubectl", PodKillCmd(k.Namespace, k.Selector)...)
}

func (k Killer) run(name string, args ...string) ([]byte, error) {
	if k.Run != nil {
		return k.Run(name, args...)
	}
	return exec.Command(name, args...).CombinedOutput()
}
