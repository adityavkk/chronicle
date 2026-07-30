package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

// NewReadIncarnation returns a random 128-bit stream identity for snapshot
// validation across delete and recreate.
func NewReadIncarnation() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

// ReadSnapshot fixes the upper boundary and response metadata for one catch-up
// response. Incarnation is opaque to callers and is used only to reject a
// delete and recreate between pages.
type ReadSnapshot struct {
	Tail        Offset
	ContentType string
	Closed      bool
	Incarnation string
}

// ReadPageOptions controls one bounded storage read. Snapshot is nil on the
// first call and must be the snapshot returned by that call thereafter.
type ReadPageOptions struct {
	TargetBytes int
	MaxFrames   int
	Snapshot    *ReadSnapshot
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
