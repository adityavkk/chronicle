package main

import "fmt"

// check_delivery.go is the PURE CORE of the at-least-once delivery checker (L1 in
// docs/specs/horizontal-scale/research/07): every durably-appended message is
// eventually delivered. The original verify() (main.go) inlined a final
// cursor==tail comparison and printed as it went; this extracts the decision into
// a pure, per-(sub,stream) function so it is unit-testable without a cluster and
// reusable across every liveness scenario.
//
// Why cursor==tail is the per-message L1 proof: one wake's snapshot covers every
// pending offset up to the current tail and a {done:true} acks the whole snapshot
// (PROTOCOL §7), and the cursor is forward-only (T2). So a final acked_offset
// equal to the stream tail proves every message at or below that tail was
// delivered at-least-once — there is no offset gap a monotonic cursor could have
// skipped. The checker therefore asserts coverage of each linked stream's whole
// appended range, and reports the shortfall (a stream stuck below its tail is a
// lost, un-recovered wake).

// deliveryExpectation is one linked stream's appended range: the tail it must
// reach and how many messages were appended into it (for the report).
type deliveryExpectation struct {
	path string
	tail string // the stream's final tail offset after the workload
	msgs int    // messages appended (for a human-readable shortfall)
}

// deliveryGap records a stream whose cursor never reached its tail — a message
// that was appended but never delivered.
type deliveryGap struct {
	path string
	want string // the tail the cursor should have reached
	got  string // the acked_offset actually observed
	msgs int
}

func (g deliveryGap) String() string {
	return fmt.Sprintf("%s acked=%s want=%s (%d msgs)", g.path, short(g.got), short(g.want), g.msgs)
}

// CheckAtLeastOnce asserts L1 over the keyspace: for every expected stream, the
// final acked offset has reached the tail (>= tail). acked maps stream path ->
// acked_offset (a missing path is treated as the beginning sentinel, i.e. nothing
// delivered). It returns every gap found, deterministically ordered by the input;
// an empty result means every appended message was delivered.
func CheckAtLeastOnce(expected []deliveryExpectation, acked map[string]string) []deliveryGap {
	var gaps []deliveryGap
	for _, e := range expected {
		got := acked[e.path]
		// Delivered iff the cursor reached (or passed) the tail. offsetGreater is
		// strict, so "got == tail" or "got > tail" both clear the bar.
		if got == e.tail || offsetGreater(got, e.tail) {
			continue
		}
		gaps = append(gaps, deliveryGap{path: e.path, want: e.tail, got: got, msgs: e.msgs})
	}
	return gaps
}
