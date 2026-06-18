package webhook

// failpoint is the in-process fault-injection seam for the surgical windows the
// host-driven Jepsen nemesis cannot hit precisely (doc 07 honest-gap #2). The
// canonical example is the few-µs window inside issueWake between arming a wake
// (the generation HINCRBY mints the fence + sets wake_event_sent_ns=0) and
// durably emitting it (the wake-stream append / webhook POST): a crash there
// strands a pull-wake that only the recovery sweep can re-emit, but `kubectl
// delete pod` from the host can only approximate it (runPullWakeArmCrash drives
// the strictly-stronger end-to-end property instead).
//
// etcd's gofail is the production-grade tool: `// gofail:` comments compiled by a
// codegen pass into runtime-toggled failpoints. Adopting it is a build-system
// change (the codegen step + a trigger surface). Until that lands, this is the
// dependency-free equivalent and is faithful to gofail's runtime model — a
// nil-by-default hook a test installs at a named site, with zero production
// overhead beyond a single nil check on the hot path. The trade-off and the
// pod-kill+many-seeds approximation it replaces are documented in
// docs/jepsen/results.md.

// FailpointHook, when non-nil, is invoked with the failpoint name at each wired
// injection site. Production leaves it nil; only tests set it, and must reset it
// (t.Cleanup). It is process-global and not concurrency-guarded — matching
// gofail — so a test that installs it drives the injected path single-threaded.
var FailpointHook func(name string)

// Wired failpoint sites. Keep this list small and named so the injection points
// are auditable.
const (
	// fpArmedBeforeEmit is the arm→emit window inside issueWake: ARMED has minted
	// the fence (phase waking, wake_event_sent_ns=0) but the wake event is not yet
	// appended. Firing here strands the wake exactly as a crash in that window
	// would, which the host nemesis cannot land on (07 honest-gap #2).
	fpArmedBeforeEmit = "issueWake.armedBeforeEmit"
)

// failpoint fires the named injection site if a hook is installed. With no hook
// (production) it is a single nil check the compiler can inline.
func failpoint(name string) {
	if FailpointHook != nil {
		FailpointHook(name)
	}
}
