package store

import (
	"context"
	"errors"
	"time"
)

const (
	// DefaultReadPageBytes is the returned payload target used when the caller
	// does not provide one. A page may exceed it only when its first valid frame
	// is larger than the target.
	DefaultReadPageBytes = 1 << 20

	// DefaultReadPageFrames bounds work for streams with many small frames.
	DefaultReadPageFrames = 1024
)

var (
	// ErrReadSnapshotChanged means the stream path now names a different stream
	// incarnation than the one captured for this response.
	ErrReadSnapshotChanged = errors.New("read snapshot stream incarnation changed")

	// ErrReadDataMissing means metadata selected a nonempty stream range whose
	// acknowledged frame data is absent. Treating it as an empty page would
	// advance a client past missing durable bytes.
	ErrReadDataMissing = errors.New("read snapshot data missing")
)

// ReadSnapshot fixes the upper boundary and response metadata for one catch-up
// response. Incarnation is opaque to callers and is used only to reject a
// delete and recreate between pages.
type ReadSnapshot struct {
	Tail                Offset
	ContentType         string
	Closed              bool
	Incarnation         string
	TTLSeconds          *int64
	ExpiresAt           *time.Time
	CreatedAt           time.Time
	ForkedFrom          string
	ForkOffset          Offset
	ForkOffsetRequested *Offset
	ForkSubOffset       uint64
	WriteFence          bool
	SealedGeneration    int64
	SealedOffset        *Offset
	// StoreToken is an opaque, process-local storage generation identifier.
	// Handlers pass it back only through ReadPageOptions.Snapshot; it is never
	// serialized into the Durable Streams wire protocol.
	StoreToken string
}

// RootReadRange identifies which stream owns the first chronological range of
// a root read. Redis mirrors this decision inside read.lua after atomically
// loading root metadata.
type RootReadRange uint8

const (
	// RootReadRangeEmpty means the requested range has no bytes inside the fixed
	// response tail, including offset=now, at-tail, and beyond-tail reads.
	RootReadRangeEmpty RootReadRange = iota
	// RootReadRangeInherited means a fork request begins below its divergence
	// point and must keep traversing the source chain.
	RootReadRangeInherited
	// RootReadRangeOwned means the root's own message ZSET owns the complete
	// nonempty requested range.
	RootReadRangeOwned
)

// ClassifyRootReadRange is the pure oracle for first-page root ownership. It
// compares the complete typed offset, including ReadSeq.
func ClassifyRootReadRange(
	offset Offset,
	tail Offset,
	forkedFrom string,
	forkOffset Offset,
) RootReadRange {
	if offset.IsNow() || !offset.LessThan(tail) {
		return RootReadRangeEmpty
	}
	if forkedFrom != "" && offset.LessThan(forkOffset) {
		return RootReadRangeInherited
	}
	return RootReadRangeOwned
}

func (r RootReadRange) String() string {
	switch r {
	case RootReadRangeEmpty:
		return "empty"
	case RootReadRangeInherited:
		return "inherited"
	case RootReadRangeOwned:
		return "root"
	default:
		return "unknown"
	}
}

// ReadSnapshotFromMetadata projects the immutable state needed by readers.
// Producer and writer-sequence state deliberately do not cross this boundary.
func ReadSnapshotFromMetadata(meta *StreamMetadata) ReadSnapshot {
	if meta == nil {
		return ReadSnapshot{}
	}
	return ReadSnapshot{
		Tail:                meta.CurrentOffset,
		ContentType:         meta.ContentType,
		Closed:              meta.Closed,
		Incarnation:         meta.Incarnation,
		TTLSeconds:          cloneInt64(meta.TTLSeconds),
		ExpiresAt:           cloneTime(meta.ExpiresAt),
		CreatedAt:           meta.CreatedAt,
		ForkedFrom:          meta.ForkedFrom,
		ForkOffset:          meta.ForkOffset,
		ForkOffsetRequested: cloneOffset(meta.ForkOffsetRequested),
		ForkSubOffset:       meta.ForkSubOffset,
		WriteFence:          meta.WriteFence,
		SealedGeneration:    meta.SealedGeneration,
		SealedOffset:        cloneOffset(meta.SealedOffset),
	}
}

// SameReadStream reports whether two snapshots belong to the same created
// stream. Content type is part of the fence because it controls wire framing.
func SameReadStream(left, right ReadSnapshot) bool {
	return left.Incarnation != "" &&
		left.Incarnation == right.Incarnation &&
		ContentTypeMatches(left.ContentType, right.ContentType)
}

// ShouldRenewReadAccess is the pure access decision mirrored by
// should_touch_read in Redis common.lua. Absolute expiry never slides.
func ShouldRenewReadAccess(meta *StreamMetadata) bool {
	return meta != nil && meta.TTLSeconds != nil
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneOffset(value *Offset) *Offset {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// ReadPageOptions controls one bounded storage read. Snapshot is nil on the
// first call and must be the snapshot returned by that call thereafter.
type ReadPageOptions struct {
	TargetBytes int
	MaxFrames   int
	Snapshot    *ReadSnapshot
	// NoTouch is reserved for Chronicle's internal sealing and repair work.
	// It preserves lazy expiry checks but does not renew a sliding TTL.
	NoTouch bool
}

// Normalize fills safe defaults for omitted page bounds.
func (o ReadPageOptions) Normalize() ReadPageOptions {
	if o.TargetBytes <= 0 {
		o.TargetBytes = DefaultReadPageBytes
	}
	if o.MaxFrames <= 0 {
		o.MaxFrames = DefaultReadPageFrames
	}
	return o
}

// ReadPageStats describes storage work for one page. Byte counts refer to
// message payload bytes. ResponseBytes is recorded by the HTTP layer because it
// includes JSON framing, envelopes, base64, and SSE control records.
type ReadPageStats struct {
	RequestedBytes     int
	FetchedBytes       int
	ReturnedBytes      int
	DiscardedBytes     int
	RedisScriptTime    time.Duration
	RedisScriptInvokes int
}

// ReadPage is one frame-aligned part of a captured stream suffix.
type ReadPage struct {
	Messages   []Message
	NextOffset Offset
	Snapshot   ReadSnapshot
	UpToDate   bool
	Stats      ReadPageStats
}

// PageReader is the bounded read capability implemented by Chronicle's
// maintained stores. It remains separate from Store so existing third-party
// Store implementations continue to compile.
type PageReader interface {
	ReadPage(ctx context.Context, path string, offset Offset, opts ReadPageOptions) (ReadPage, error)
}

// ReadWaitResult is the durable state observed when a long-poll wakes or
// times out. Page is authoritative for response headers in either case.
type ReadWaitResult struct {
	Page     ReadPage
	TimedOut bool
}

// PageWaiter is the race-closing long-poll capability implemented by
// Chronicle's maintained stores. The caller has already performed the logical
// read access represented by Initial; wait rechecks must therefore be no-touch.
type PageWaiter interface {
	WaitForPage(
		ctx context.Context,
		path string,
		offset Offset,
		initial ReadSnapshot,
		timeout time.Duration,
		opts ReadPageOptions,
	) (ReadWaitResult, error)
}

// PageSnapshotReleaser is implemented by stores that retain resources for a
// multi-page snapshot. ReleaseReadSnapshot must be safe to call more than once.
type PageSnapshotReleaser interface {
	ReleaseReadSnapshot(path string, snapshot ReadSnapshot)
}
