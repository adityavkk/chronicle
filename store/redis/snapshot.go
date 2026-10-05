package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// Offset stays a string: Lua JSON numbers cannot preserve uint64 positions.
type snapshotRecord struct {
	Incarnation string
	Offset      string
	ContentType string
	ETag        string
}

func snapshotEnvelope(v store.ProjectionSnapshot) string {
	b, _ := json.Marshal(snapshotRecord{v.Incarnation, v.Offset.String(), v.ContentType, v.ETag})
	return string(b)
}

func decodeSnapshotEnvelope(path, projection, incarnation, value string, body []byte) (store.ProjectionSnapshot, error) {
	var record snapshotRecord
	if err := json.Unmarshal([]byte(value), &record); err != nil {
		return store.ProjectionSnapshot{}, fmt.Errorf("corrupt snapshot envelope: %w", err)
	}
	offset, err := store.ParseOffset(record.Offset)
	if err != nil || offset.IsNow() || offset.String() != record.Offset || record.Incarnation != incarnation {
		return store.ProjectionSnapshot{}, fmt.Errorf("corrupt snapshot descriptor")
	}
	v := store.ProjectionSnapshot{
		Projection: projection, Incarnation: incarnation, Offset: offset,
		ContentType: record.ContentType, ETag: record.ETag, Body: body,
	}
	if len(v.Body) > store.MaxSnapshotBytes || v.ContentType == "" || len(v.ContentType) > store.MaxSnapshotContentType || v.ETag != store.SnapshotETag(path, v) {
		return store.ProjectionSnapshot{}, fmt.Errorf("corrupt snapshot image")
	}
	return v, nil
}

// GetSnapshot atomically checks source liveness and loads an image without
// renewing its source's sliding expiry. The descriptor and bytes are indivisible.
func (s *Store) GetSnapshot(ctx context.Context, path, projection string) (store.ProjectionSnapshot, error) {
	if err := store.ValidateSnapshotProjection(projection); err != nil {
		return store.ProjectionSnapshot{}, err
	}
	raw, err := snapshotGetScript.Run(ctx, s.client, keysFor(path), s.nowNsArg(), projection).Result()
	if err != nil {
		return store.ProjectionSnapshot{}, err
	}
	status, rest, err := decodeStatusReply(raw)
	if err != nil {
		return store.ProjectionSnapshot{}, err
	}
	switch status {
	case stNotFound:
		return store.ProjectionSnapshot{}, store.ErrStreamNotFound
	case stSoftDel:
		return store.ProjectionSnapshot{}, store.ErrStreamSoftDeleted
	case stMissing:
		return store.ProjectionSnapshot{}, store.ErrSnapshotNotFound
	case stOK:
		if len(rest) != 4 {
			return store.ProjectionSnapshot{}, fmt.Errorf("malformed snapshot get reply")
		}
		inc, iok := rest[0].(string)
		tailText, tok := rest[1].(string)
		env, eok := rest[2].(string)
		body, bok := rest[3].(string)
		if !iok || !tok || !eok || !bok {
			return store.ProjectionSnapshot{}, fmt.Errorf("malformed snapshot get reply")
		}
		tail, err := store.ParseOffset(tailText)
		if err != nil {
			return store.ProjectionSnapshot{}, err
		}
		image, err := decodeSnapshotEnvelope(path, projection, inc, env, []byte(body))
		if err != nil {
			return store.ProjectionSnapshot{}, err
		}
		if tail.LessThan(image.Offset) {
			return store.ProjectionSnapshot{}, store.ErrReadSnapshotChanged
		}
		return image, nil
	default:
		return store.ProjectionSnapshot{}, fmt.Errorf("snapshot_get.lua: unexpected status %q", status)
	}
}

// PutSnapshot validates the source, boundary and CAS in the same slot and Lua
// invocation as publication. It never appends source events or emits a wake.
func (s *Store) PutSnapshot(ctx context.Context, path string, v store.ProjectionSnapshot, cond store.SnapshotCondition) (store.ProjectionSnapshot, bool, error) {
	if err := v.Validate(cond); err != nil {
		return store.ProjectionSnapshot{}, false, err
	}
	v.ETag = store.SnapshotETag(path, v)
	env, kind := snapshotEnvelope(v), "match"
	if cond.IfNoneMatch {
		kind = "none"
	}
	raw, err := snapshotPutScript.Run(ctx, s.client, keysFor(path), s.nowNsArg(), v.Projection, v.Incarnation, v.Offset.String(), v.ETag, env, v.Body, kind, cond.IfMatch, store.MaxSnapshotVersions, store.MaxSnapshotTotalBytes).Result()
	if err != nil {
		return store.ProjectionSnapshot{}, false, err
	}
	status, rest, err := decodeStatusReply(raw)
	if err != nil {
		return store.ProjectionSnapshot{}, false, err
	}
	switch status {
	case stNotFound:
		return store.ProjectionSnapshot{}, false, store.ErrStreamNotFound
	case stSoftDel:
		return store.ProjectionSnapshot{}, false, store.ErrStreamSoftDeleted
	case stSnapshot:
		return store.ProjectionSnapshot{}, false, store.ErrReadSnapshotChanged
	case "PRECONDITION":
		return store.ProjectionSnapshot{}, false, store.ErrSnapshotPrecondition
	case "CONFLICT":
		return store.ProjectionSnapshot{}, false, store.ErrSnapshotConflict
	case "QUOTA":
		return store.ProjectionSnapshot{}, false, store.ErrSnapshotQuota
	case "CORRUPT":
		return store.ProjectionSnapshot{}, false, fmt.Errorf("corrupt snapshot storage")
	case stOK:
		if len(rest) != 1 {
			return store.ProjectionSnapshot{}, false, fmt.Errorf("malformed snapshot put reply")
		}
		created, ok := rest[0].(string)
		if !ok {
			return store.ProjectionSnapshot{}, false, fmt.Errorf("malformed snapshot put reply")
		}
		v.Body = append([]byte(nil), v.Body...)
		return v, created == "1", nil
	default:
		return store.ProjectionSnapshot{}, false, fmt.Errorf("snapshot_put.lua: unexpected status %q", status)
	}
}

// DeleteSnapshot atomically retires the matching image and frees its quota.
func (s *Store) DeleteSnapshot(ctx context.Context, path, projection, incarnation, etag string) error {
	if store.ValidateSnapshotProjection(projection) != nil || incarnation == "" || etag == "" {
		return store.ErrInvalidSnapshot
	}
	raw, err := snapshotDeleteScript.Run(ctx, s.client, keysFor(path), s.nowNsArg(), projection, incarnation, etag).Result()
	if err != nil {
		return err
	}
	status, _, err := decodeStatusReply(raw)
	if err != nil {
		return err
	}
	switch status {
	case stOK:
		return nil
	case stNotFound:
		return store.ErrStreamNotFound
	case stSoftDel:
		return store.ErrStreamSoftDeleted
	case stSnapshot:
		return store.ErrReadSnapshotChanged
	case "PRECONDITION":
		return store.ErrSnapshotPrecondition
	case "CORRUPT":
		return fmt.Errorf("corrupt snapshot storage")
	default:
		return fmt.Errorf("snapshot_delete.lua: unexpected status %q", status)
	}
}
