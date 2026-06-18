package webhook

import (
	"testing"
	"time"
)

func TestNopMetricsImplementsExpandedInterface(t *testing.T) {
	var m Metrics = NopMetrics{}
	m.SweepTick(time.Millisecond, 1, 2, 3)
	m.WakeDelivery(time.Millisecond, "ok")
	m.WakeEvent(time.Millisecond, "ok")
	m.WorkerTick("lease", 1)
	m.FanOut(time.Millisecond, 4, 5)
	m.DueSetMutation("arm")
	m.DueWorkerTick(time.Millisecond, 6)
	m.SlotOwnership("claimed", 7)
	m.CoverageGap(time.Millisecond)
	m.OwnerFenced("due")
	m.ClaimContention("busy", "sub-1")
}
