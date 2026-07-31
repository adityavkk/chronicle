package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestRootReadRangeDecisionDifferential(t *testing.T) {
	subject := newTestStore(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		offset     store.Offset
		tail       store.Offset
		forkedFrom string
		forkOffset store.Offset
	}{
		{name: "unforked", offset: store.Offset{ReadSeq: 2, ByteOffset: 10}, tail: store.Offset{ReadSeq: 2, ByteOffset: 100}},
		{name: "fork before", offset: store.Offset{ReadSeq: 2, ByteOffset: 10}, tail: store.Offset{ReadSeq: 2, ByteOffset: 100}, forkedFrom: "/source", forkOffset: store.Offset{ReadSeq: 2, ByteOffset: 40}},
		{name: "fork at", offset: store.Offset{ReadSeq: 2, ByteOffset: 40}, tail: store.Offset{ReadSeq: 2, ByteOffset: 100}, forkedFrom: "/source", forkOffset: store.Offset{ReadSeq: 2, ByteOffset: 40}},
		{name: "fork above", offset: store.Offset{ReadSeq: 2, ByteOffset: 50}, tail: store.Offset{ReadSeq: 2, ByteOffset: 100}, forkedFrom: "/source", forkOffset: store.Offset{ReadSeq: 2, ByteOffset: 40}},
		{name: "zero tail", offset: store.ZeroOffset, tail: store.ZeroOffset},
		{name: "now", offset: store.NowOffset, tail: store.Offset{ByteOffset: 100}},
		{name: "at tail", offset: store.Offset{ByteOffset: 100}, tail: store.Offset{ByteOffset: 100}},
		{name: "beyond tail", offset: store.Offset{ByteOffset: 101}, tail: store.Offset{ByteOffset: 100}},
		{name: "older read sequence", offset: store.Offset{ReadSeq: 1, ByteOffset: 999}, tail: store.Offset{ReadSeq: 2, ByteOffset: 100}},
		{name: "newer read sequence", offset: store.Offset{ReadSeq: 3}, tail: store.Offset{ReadSeq: 2, ByteOffset: 100}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := testPath("root-range-" + tc.name)
			meta := &store.StreamMetadata{
				Path:           path,
				Incarnation:    "root-range-incarnation",
				ContentType:    "application/octet-stream",
				CurrentOffset:  tc.tail,
				CreatedAt:      time.Unix(100, 0),
				LastAccessedAt: time.Unix(100, 0),
				ForkedFrom:     tc.forkedFrom,
				ForkOffset:     tc.forkOffset,
			}
			if err := subject.client.HSet(ctx, metaKey(path), metaToFields(meta)).Err(); err != nil {
				t.Fatal(err)
			}

			want := store.ClassifyRootReadRange(tc.offset, tc.tail, tc.forkedFrom, tc.forkOffset)
			if want == store.RootReadRangeOwned {
				if err := subject.client.ZAdd(ctx, msgKey(path),
					goredis.Z{Score: 0, Member: encodeFrame(tc.tail, []byte("x"))},
				).Err(); err != nil {
					t.Fatal(err)
				}
			}

			result, err := subject.runReadPageScript(
				ctx,
				path,
				tc.offset,
				store.ZeroOffset,
				store.DefaultReadPageBytes,
				store.DefaultReadPageFrames,
				"",
				true,
				true,
				false,
				false,
				nil,
				tc.offset.IsNow(),
			)
			if err != nil {
				t.Fatal(err)
			}
			if result.rootRange == nil || *result.rootRange != want {
				t.Fatalf("Lua root range = %v, Go oracle = %s", result.rootRange, want)
			}
			if got := len(result.messages); (want == store.RootReadRangeOwned) != (got == 1) {
				t.Fatalf("Lua returned %d messages for %s range", got, want)
			}
		})
	}
}
