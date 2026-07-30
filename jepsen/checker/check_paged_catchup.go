package main

import (
	"bytes"
	"fmt"
)

// pagedFrame is the independent Redis-oracle identity of one stream frame.
// Offset is the opaque protocol cursor; Data is compared byte-for-byte.
type pagedFrame struct {
	Offset string
	Data   []byte
}

// pagedAttempt records one HTTP catch-up response. Complete means the JSON
// envelope reached its closing bracket. An interrupted response may still
// contain a prefix of complete frames, which is safe to resume after the last
// such frame.
type pagedAttempt struct {
	Name         string
	StartOffset  string
	SnapshotTail string
	Frames       []pagedFrame
	Complete     bool
	Closed       bool
}

// CheckPagedCatchup proves that a sequence of resumed HTTP reads covers the
// direct Redis oracle once, in order, without a gap or a frame beyond any
// response's captured snapshot. Interrupted attempts may end at any complete
// frame. A complete attempt must end exactly at its advertised snapshot tail.
func CheckPagedCatchup(oracle []pagedFrame, attempts []pagedAttempt) error {
	indexByOffset := make(map[string]int, len(oracle))
	for i, frame := range oracle {
		if frame.Offset == "" {
			return fmt.Errorf("oracle frame %d has an empty offset", i)
		}
		if _, duplicate := indexByOffset[frame.Offset]; duplicate {
			return fmt.Errorf("oracle offset %q is duplicated", frame.Offset)
		}
		indexByOffset[frame.Offset] = i
	}

	cursor := "-1"
	nextIndex := 0
	for attemptIndex, attempt := range attempts {
		label := attempt.Name
		if label == "" {
			label = fmt.Sprintf("attempt-%d", attemptIndex)
		}
		if attempt.StartOffset != cursor {
			return fmt.Errorf("%s starts at %q, want resume cursor %q", label, attempt.StartOffset, cursor)
		}

		snapshotIndex, err := pagedSnapshotIndex(attempt.SnapshotTail, oracle, indexByOffset)
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		if snapshotIndex+1 < nextIndex {
			return fmt.Errorf("%s snapshot %q regressed behind resume cursor %q", label, attempt.SnapshotTail, cursor)
		}

		for frameIndex, got := range attempt.Frames {
			if nextIndex >= len(oracle) {
				return fmt.Errorf("%s frame %d (%q) is beyond the Redis oracle", label, frameIndex, got.Offset)
			}
			if nextIndex > snapshotIndex {
				return fmt.Errorf("%s returned frame %q beyond captured snapshot %q", label, got.Offset, attempt.SnapshotTail)
			}
			want := oracle[nextIndex]
			if got.Offset != want.Offset {
				return fmt.Errorf("%s frame %d offset = %q, want contiguous %q", label, frameIndex, got.Offset, want.Offset)
			}
			if !bytes.Equal(got.Data, want.Data) {
				return fmt.Errorf("%s frame %q data = %q, want Redis oracle %q", label, got.Offset, got.Data, want.Data)
			}
			cursor = got.Offset
			nextIndex++
		}

		if attempt.Complete && nextIndex != snapshotIndex+1 {
			return fmt.Errorf("%s completed at %q after %d oracle frame(s), want snapshot %q after %d",
				label, cursor, nextIndex, attempt.SnapshotTail, snapshotIndex+1)
		}
	}

	if nextIndex != len(oracle) {
		return fmt.Errorf("catch-up ended at %q after %d/%d Redis-oracle frames", cursor, nextIndex, len(oracle))
	}
	if len(attempts) == 0 || !attempts[len(attempts)-1].Complete {
		return fmt.Errorf("final catch-up response was interrupted")
	}
	if !attempts[len(attempts)-1].Closed {
		return fmt.Errorf("final catch-up response did not signal Stream-Closed")
	}
	return nil
}

func pagedSnapshotIndex(tail string, oracle []pagedFrame, indexByOffset map[string]int) (int, error) {
	if tail == "0000000000000000_0000000000000000" && len(oracle) == 0 {
		return -1, nil
	}
	index, ok := indexByOffset[tail]
	if !ok {
		return 0, fmt.Errorf("snapshot tail %q is absent from the Redis oracle", tail)
	}
	return index, nil
}
