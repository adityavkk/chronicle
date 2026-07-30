package main

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"
)

func TestCheckPagedCatchupAcceptsInterruptedResumesAndGrowingSnapshots(t *testing.T) {
	t.Parallel()

	oracle := testPagedOracle(7)
	attempts := []pagedAttempt{
		{Name: "cancel", StartOffset: "-1", SnapshotTail: oracle[3].Offset, Frames: oracle[:2]},
		{Name: "restart", StartOffset: oracle[1].Offset, SnapshotTail: oracle[3].Offset, Frames: oracle[2:4], Complete: true},
		{Name: "close", StartOffset: oracle[3].Offset, SnapshotTail: oracle[6].Offset, Frames: oracle[4:], Complete: true, Closed: true},
	}
	if err := CheckPagedCatchup(oracle, attempts); err != nil {
		t.Fatalf("CheckPagedCatchup: %v", err)
	}
}

func TestCheckPagedCatchupRejectsGapDuplicateAndSnapshotLeak(t *testing.T) {
	t.Parallel()

	oracle := testPagedOracle(4)
	tests := map[string][]pagedAttempt{
		"gap": {
			{StartOffset: "-1", SnapshotTail: oracle[3].Offset, Frames: []pagedFrame{oracle[0], oracle[2]}},
		},
		"duplicate": {
			{StartOffset: "-1", SnapshotTail: oracle[3].Offset, Frames: []pagedFrame{oracle[0], oracle[0]}},
		},
		"snapshot leak": {
			{StartOffset: "-1", SnapshotTail: oracle[1].Offset, Frames: oracle[:3]},
		},
		"false complete": {
			{StartOffset: "-1", SnapshotTail: oracle[3].Offset, Frames: oracle[:2], Complete: true},
		},
	}
	for name, attempts := range tests {
		t.Run(name, func(t *testing.T) {
			if err := CheckPagedCatchup(oracle, attempts); err == nil {
				t.Fatal("CheckPagedCatchup accepted an invalid history")
			}
		})
	}
}

func TestCheckPagedCatchupPropertyPartitionsRemainExact(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 80).Draw(rt, "frames")
		oracle := testPagedOracle(n)

		var attempts []pagedAttempt
		cursor := "-1"
		next := 0
		for next < n {
			snapshotEnd := rapid.IntRange(next, n-1).Draw(rt, fmt.Sprintf("snapshot-%d", next))
			take := rapid.IntRange(1, snapshotEnd-next+1).Draw(rt, fmt.Sprintf("take-%d", next))
			end := next + take
			complete := end == snapshotEnd+1
			attempts = append(attempts, pagedAttempt{
				StartOffset:  cursor,
				SnapshotTail: oracle[snapshotEnd].Offset,
				Frames:       oracle[next:end],
				Complete:     complete,
				Closed:       complete && end == n,
			})
			cursor = oracle[end-1].Offset
			next = end
		}

		if err := CheckPagedCatchup(oracle, attempts); err != nil {
			rt.Fatalf("valid partition rejected: %v", err)
		}
	})
}

func testPagedOracle(n int) []pagedFrame {
	frames := make([]pagedFrame, n)
	for i := range frames {
		frames[i] = pagedFrame{
			Offset: fmt.Sprintf("%016d_%016d", i+1, i/3),
			Data:   []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}
	}
	return frames
}
