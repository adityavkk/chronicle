// Package checkpoint is a store-level snapshot-plus-tail experiment, not an
// implementation of the proposed HTTP extension. It never truncates the log,
// acknowledges a subscription, or runs reducers inside Redis.
package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// MaxImageBytes bounds the prototype's serialized checkpoint, including metadata.
const MaxImageBytes = 1 << 20

// ErrInvalidImage denotes an incompatible, malformed, or corrupt checkpoint.
var ErrInvalidImage = errors.New("invalid checkpoint image")

// Image binds a complete JSON state image to a consumed stream boundary.
// Projection must change whenever the reducer, schema, or serialization changes.
// SHA256 detects accidental corruption, not a malicious publisher.
type Image struct {
	Format      int             `json:"format"`
	Path        string          `json:"path"`
	Incarnation string          `json:"incarnation"`
	Projection  string          `json:"projection"`
	Offset      string          `json:"offset"`
	State       json.RawMessage `json:"state"`
	SHA256      string          `json:"sha256"`
}

// Projection supplies a pure, deterministic fold. Initial must return fresh
// state; Apply must not perform external effects. State must round-trip through
// encoding/json without loss. The caller must not mutate it during Capture.
type Projection[S any] struct {
	Version string
	Initial func() S
	Apply   func(S, store.Message) (S, error)
}

// Stats measures actual suffix work; a cache hit need not imply a small image.
type Stats struct {
	Restored bool
	Frames   int
	Bytes    int
}

// Capture restores a compatible image and folds through a fixed, current tail.
// Missing, incompatible, or corrupt images fall back to full replay. Source read
// and reducer errors fail the operation: they must never become empty state.
// A successful result can be published with RedisCache.Save. Capture and Save
// are separate so callers can choose cadence and treat cache writes as optional.
func Capture[S any](ctx context.Context, reader store.PageReader, path string, p Projection[S], prior *Image) (S, Image, Stats, error) {
	var zero S
	var stats Stats
	if p.Version == "" || p.Initial == nil || p.Apply == nil {
		return zero, Image{}, stats, errors.New("checkpoint projection requires version, initial state, and reducer")
	}
	// Capturing via PageReader preserves the store's incarnation fence and its
	// fixed-tail semantics, including for forked streams. No offset arithmetic.
	head, err := reader.ReadPage(ctx, path, store.NowOffset, store.ReadPageOptions{})
	if err != nil {
		return zero, Image{}, stats, err
	}
	snapshot := head.Snapshot
	if releaser, ok := reader.(store.PageSnapshotReleaser); ok {
		defer releaser.ReleaseReadSnapshot(path, snapshot)
	}
	state := p.Initial()
	offset := store.ZeroOffset
	if prior != nil && prior.Path == path && prior.Incarnation == snapshot.Incarnation && prior.Projection == p.Version {
		if off, err := prior.validate(); err == nil && off.LessThanOrEqual(snapshot.Tail) {
			var restored S
			if json.Unmarshal(prior.State, &restored) == nil {
				state, offset, stats.Restored = restored, off, true
			}
		}
	}
	for {
		// Read even at tail: the source might have been deleted/recreated while
		// the image was decoded. A successful empty suffix must be fenced too.
		page, err := reader.ReadPage(ctx, path, offset, store.ReadPageOptions{Snapshot: &snapshot})
		if err != nil {
			return zero, Image{}, stats, err
		}
		for _, msg := range page.Messages {
			state, err = p.Apply(state, msg)
			if err != nil {
				return zero, Image{}, stats, fmt.Errorf("fold at %s: %w", msg.Offset, err)
			}
			stats.Frames++
			stats.Bytes += len(msg.Data)
		}
		offset = page.NextOffset // only after every frame successfully applied
		if page.UpToDate {
			break
		}
	}
	data, err := json.Marshal(state)
	if err != nil {
		return zero, Image{}, stats, err
	}
	image := Image{Format: 1, Path: path, Incarnation: snapshot.Incarnation, Projection: p.Version, Offset: offset.String(), State: data}
	image.SHA256 = image.digest()
	encoded, err := json.Marshal(image)
	if err != nil {
		return zero, Image{}, stats, err
	}
	if len(encoded) > MaxImageBytes {
		return zero, Image{}, stats, fmt.Errorf("checkpoint exceeds %d bytes", MaxImageBytes)
	}
	return state, image, stats, nil
}

func (i Image) digest() string {
	// Bind metadata as well as state, so an accidentally changed cursor cannot
	// turn a valid body checksum into skipped events. RawMessage whitespace is
	// normalized by Marshal for a stable JSON round trip.
	i.SHA256 = ""
	data, _ := json.Marshal(i) // all fields are serializable after JSON validation
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (i Image) validate() (store.Offset, error) {
	off, err := store.ParseOffset(i.Offset)
	if err != nil || off.IsNow() || i.Offset != off.String() || i.Format != 1 ||
		i.Path == "" || i.Incarnation == "" || i.Projection == "" ||
		!json.Valid(i.State) || i.SHA256 != i.digest() {
		return store.Offset{}, ErrInvalidImage
	}
	return off, nil
}
