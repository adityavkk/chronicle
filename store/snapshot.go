package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"unicode/utf8"
)

// MaxSnapshotBytes bounds a complete projection image, excluding its descriptor.
const (
	MaxSnapshotBytes       = 1 << 20
	MaxSnapshotTotalBytes  = 4 << 20
	MaxSnapshotVersions    = 8
	MaxSnapshotContentType = 1024
)

var (
	// ErrSnapshotNotFound means the live source has no image for this projection.
	ErrSnapshotNotFound = errors.New("projection snapshot not found")
	// ErrSnapshotPrecondition means a conditional publication lost its CAS.
	ErrSnapshotPrecondition = errors.New("projection snapshot precondition failed")
	// ErrSnapshotConflict rejects invalid boundaries, regressions, and ambiguous state.
	ErrSnapshotConflict = errors.New("projection snapshot conflicts with published state")
	// ErrInvalidSnapshot means the descriptor or publication condition is malformed.
	ErrInvalidSnapshot = errors.New("invalid projection snapshot")
	// ErrSnapshotTooLarge means the image exceeds MaxSnapshotBytes.
	ErrSnapshotTooLarge = errors.New("projection snapshot exceeds maximum size")
	// ErrSnapshotQuota means the source's aggregate image count or byte budget is exhausted.
	ErrSnapshotQuota = errors.New("projection snapshot source quota exhausted")
)

// ProjectionSnapshot is an opaque state image at an exactly consumed boundary.
// ETag is computed by the store; caller-supplied values are never trusted.
type ProjectionSnapshot struct {
	Projection  string
	Incarnation string
	Offset      Offset
	ContentType string
	Body        []byte
	ETag        string
}

// SnapshotCondition requires either a previously observed strong ETag or absence.
type SnapshotCondition struct {
	IfMatch     string
	IfNoneMatch bool
}

// ProjectionSnapshotStore is an optional capability; Store remains frozen.
type ProjectionSnapshotStore interface {
	GetSnapshot(context.Context, string, string) (ProjectionSnapshot, error)
	PutSnapshot(context.Context, string, ProjectionSnapshot, SnapshotCondition) (ProjectionSnapshot, bool, error)
	DeleteSnapshot(context.Context, string, string, string, string) error
}

// ValidateSnapshotProjection checks the versioned application identifier.
func ValidateSnapshotProjection(s string) error {
	if len(s) == 0 || len(s) > 128 || !utf8.ValidString(s) {
		return ErrInvalidSnapshot
	}
	for _, r := range s {
		if r < 0x20 || (r >= 0x7f && r <= 0x9f) {
			return ErrInvalidSnapshot
		}
	}
	return nil
}

// Validate checks the descriptor and condition before either storage backend
// examines source state. Offset-boundary and CAS checks happen under its lock.
func (v ProjectionSnapshot) Validate(cond SnapshotCondition) error {
	if ValidateSnapshotProjection(v.Projection) != nil || v.Incarnation == "" || v.ContentType == "" || len(v.ContentType) > MaxSnapshotContentType || v.Offset.IsNow() {
		return ErrInvalidSnapshot
	}
	if len(v.Body) > MaxSnapshotBytes {
		return ErrSnapshotTooLarge
	}
	if cond.IfNoneMatch == (cond.IfMatch != "") {
		return ErrInvalidSnapshot
	}
	return nil
}

// SnapshotETag binds the source, projection, consumed cut, media type and bytes.
// Length prefixes prevent distinct field partitions from hashing identically.
func SnapshotETag(path string, s ProjectionSnapshot) string {
	h := sha256.New()
	for _, value := range [][]byte{[]byte(path), []byte(s.Incarnation), []byte(s.Projection), []byte(s.Offset.String()), []byte(s.ContentType), s.Body} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(value)))
		_, _ = h.Write(n[:])
		_, _ = h.Write(value)
	}
	return `"` + hex.EncodeToString(h.Sum(nil)) + `"`
}

func cloneSnapshot(v ProjectionSnapshot) ProjectionSnapshot {
	v.Body = append([]byte(nil), v.Body...)
	return v
}

// GetSnapshot returns a detached image only while the source is live.
func (s *MemoryStore) GetSnapshot(ctx context.Context, path, projection string) (ProjectionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ProjectionSnapshot{}, err
	}
	if err := ValidateSnapshotProjection(projection); err != nil {
		return ProjectionSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.streams[path]
	if !ok {
		return ProjectionSnapshot{}, ErrStreamNotFound
	}
	if s.isExpired(&stream.metadata) {
		stream.snapshots = nil
		return ProjectionSnapshot{}, ErrStreamNotFound
	}
	if stream.metadata.SoftDeleted {
		return ProjectionSnapshot{}, ErrStreamSoftDeleted
	}
	v, ok := stream.snapshots[projection]
	if !ok {
		return ProjectionSnapshot{}, ErrSnapshotNotFound
	}
	return cloneSnapshot(v), nil
}

// PutSnapshot atomically validates and publishes a detached image. It does not
// touch source access time, producer state, or notifications.
func (s *MemoryStore) PutSnapshot(ctx context.Context, path string, v ProjectionSnapshot, cond SnapshotCondition) (ProjectionSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return ProjectionSnapshot{}, false, err
	}
	if err := v.Validate(cond); err != nil {
		return ProjectionSnapshot{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.streams[path]
	if !ok {
		return ProjectionSnapshot{}, false, ErrStreamNotFound
	}
	if s.isExpired(&stream.metadata) {
		stream.snapshots = nil
		return ProjectionSnapshot{}, false, ErrStreamNotFound
	}
	if stream.metadata.SoftDeleted {
		return ProjectionSnapshot{}, false, ErrStreamSoftDeleted
	}
	if v.Incarnation != memoryReadIncarnation(&stream.metadata) {
		return ProjectionSnapshot{}, false, ErrReadSnapshotChanged
	}
	validBoundary := stream.metadata.ForkedFrom == "" && v.Offset.IsZero()
	for _, m := range stream.messages {
		if m.Offset == v.Offset {
			validBoundary = true
			break
		}
	}
	if !validBoundary || stream.metadata.CurrentOffset.LessThan(v.Offset) {
		return ProjectionSnapshot{}, false, ErrSnapshotConflict
	}
	old, exists := stream.snapshots[v.Projection]
	if (cond.IfNoneMatch && exists) || (!cond.IfNoneMatch && (!exists || cond.IfMatch != old.ETag)) {
		return ProjectionSnapshot{}, false, ErrSnapshotPrecondition
	}
	if exists {
		if v.Offset.LessThan(old.Offset) {
			return ProjectionSnapshot{}, false, ErrSnapshotConflict
		}
		v.ETag = SnapshotETag(path, v)
		if v.Offset == old.Offset && v.ETag != old.ETag {
			return ProjectionSnapshot{}, false, ErrSnapshotConflict
		}
		if v.Offset == old.Offset {
			return cloneSnapshot(old), false, nil
		}
	} else {
		v.ETag = SnapshotETag(path, v)
	}
	if !exists && len(stream.snapshots) >= MaxSnapshotVersions {
		return ProjectionSnapshot{}, false, ErrSnapshotQuota
	}
	total := len(v.Body)
	for projection, image := range stream.snapshots {
		if projection != v.Projection {
			total += len(image.Body)
		}
	}
	if total > MaxSnapshotTotalBytes {
		return ProjectionSnapshot{}, false, ErrSnapshotQuota
	}
	v.Body = append([]byte(nil), v.Body...)
	if stream.snapshots == nil {
		stream.snapshots = make(map[string]ProjectionSnapshot)
	}
	stream.snapshots[v.Projection] = v
	return cloneSnapshot(v), !exists, nil
}

// DeleteSnapshot conditionally retires one image without touching its source.
func (s *MemoryStore) DeleteSnapshot(ctx context.Context, path, projection, incarnation, etag string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ValidateSnapshotProjection(projection) != nil || incarnation == "" || etag == "" {
		return ErrInvalidSnapshot
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.streams[path]
	if !ok || s.isExpired(&stream.metadata) {
		if ok {
			stream.snapshots = nil
		}
		return ErrStreamNotFound
	}
	if stream.metadata.SoftDeleted {
		return ErrStreamSoftDeleted
	}
	if incarnation != memoryReadIncarnation(&stream.metadata) {
		return ErrReadSnapshotChanged
	}
	old, ok := stream.snapshots[projection]
	if !ok || old.ETag != etag {
		return ErrSnapshotPrecondition
	}
	delete(stream.snapshots, projection)
	return nil
}
