package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// scenario_cursor.go is the IMPERATIVE SHELL for T2 (cursor monotonicity,
// docs/specs/horizontal-scale/research/07): it drives the existing webhook
// delivery workload under origin churn — so the retry worker and the recovery
// sweep both re-fire, and restarts replay the AOF — while a poller samples each
// subscription cursor over time, then checks the samples with the pure
// CheckCursorMonotonic (check_cursor.go). The property: a replayed or stale ack
// is a no-op, so an acked offset never regresses and never phantom-advances.

// runCursorMonotonic appends a known message set under origin churn while
// sampling cursors, then asserts every cursor advanced forward-only.
func runCursorMonotonic(c config, r *receiver) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	ctx := fmt.Sprintf("k3d-%s", c.cluster)
	subID := fmt.Sprintf("jepsen-cursor-%d", time.Now().UnixNano())
	webhookURL := fmt.Sprintf("http://%s:%d/webhook", c.recvHost, c.recvPort)

	// A webhook subscription whose receiver auto-acks ({done:true}), so cursors
	// advance as wakes are delivered and re-delivered under churn.
	if err := createSubscription(c.base, subID, webhookURL, "events/*"); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)
	fmt.Printf("created webhook subscription %s; sampling cursors under origin churn\n", subID)

	sampler := &cursorSampler{start: time.Now()}
	stopPoll := make(chan struct{})
	var pwg sync.WaitGroup
	pwg.Add(1)
	go func() { defer pwg.Done(); sampler.poll(c.base, subID, 150*time.Millisecond, stopPoll) }()

	// Continuous origin churn: the sweep and the retry worker both re-fire wakes,
	// the exact condition under which a naive ack could replay an offset backward.
	nem := c.newNemesis(ctx)
	nem.scenario = "origin-restart"
	stopNem := make(chan struct{})
	var nwg sync.WaitGroup
	nwg.Add(1)
	go func() { defer nwg.Done(); nem.run(stopNem) }()

	for i := 0; i < c.streams; i++ {
		stream := fmt.Sprintf("events/e-%d", i)
		if _, err := appendStream(c.base, stream, c.msgs); err != nil {
			close(stopNem)
			nwg.Wait()
			close(stopPoll)
			pwg.Wait()
			return fmt.Errorf("workload %s: %w", stream, err)
		}
	}
	close(stopNem)
	nwg.Wait()

	if err := waitReady(c.base, 90*time.Second); err != nil {
		close(stopPoll)
		pwg.Wait()
		return fmt.Errorf("chronicle did not recover: %w", err)
	}
	fmt.Printf("settling %s while the sweep re-fires and the poller samples...\n", c.settle)
	sleep(c.settle)
	close(stopPoll)
	pwg.Wait()

	samples := sampler.snapshot()
	violations := CheckCursorMonotonic(samples)

	delivered, dupFactor := r.stats()
	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("nemesis actions:   %d (%s)\n", len(nem.log), join(nem.log))
	fmt.Printf("wakes delivered:   %d (dup factor %.2f)\n", delivered, dupFactor)
	fmt.Printf("cursor samples:    %d across %d streams\n", len(samples), c.streams)
	if len(violations) > 0 {
		for _, v := range violations {
			fmt.Printf("  VIOLATION %s\n", v)
		}
		return fmt.Errorf("%d cursor regression(s): a replayed or stale ack moved a cursor backward — forward-only violated", len(violations))
	}
	fmt.Println("PASS: every subscription cursor advanced forward-only under churn (no regression, no phantom advance)")
	return nil
}

// cursorSampler polls a subscription's acked offsets over time into a history for
// the pure checker. The clock is the driver host's monotonic clock.
type cursorSampler struct {
	mu      sync.Mutex
	start   time.Time
	samples []cursorSample
}

// poll samples on a ticker until stop is closed, then takes one final sample so
// the post-settle cursor state is always recorded.
func (s *cursorSampler) poll(base, subID string, every time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			s.sampleOnce(base, subID)
			return
		case <-t.C:
			s.sampleOnce(base, subID)
		}
	}
}

// sampleOnce reads the subscription once WITHOUT retrying — a sample missed
// during origin churn is simply skipped, keeping the poller cadence steady rather
// than blocking on a down origin.
func (s *cursorSampler) sampleOnce(base, subID string) {
	url := fmt.Sprintf("%s/v1/stream/__ds/subscriptions/%s", base, subID)
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	var v subscriptionView
	if json.NewDecoder(resp.Body).Decode(&v) != nil {
		return
	}
	now := int64(time.Since(s.start))
	s.mu.Lock()
	for _, st := range v.Streams {
		s.samples = append(s.samples, cursorSample{sub: subID, path: st.Path, offset: st.AckedOffset, atNs: now})
	}
	s.mu.Unlock()
}

// snapshot returns a copy of the samples in observation order.
func (s *cursorSampler) snapshot() []cursorSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cursorSample, len(s.samples))
	copy(out, s.samples)
	return out
}
