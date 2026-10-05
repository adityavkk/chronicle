package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestProjectionSnapshotRedisReloadsScript(t *testing.T) {
	r := newTestStore(t)
	path := testPath("snapshot")
	m := mustCreate(t, r, path, store.CreateOptions{})
	a := mustAppend(t, r, path, []byte("abc"), store.AppendOptions{})
	v := store.ProjectionSnapshot{Projection: "p1", Incarnation: m.Incarnation, Offset: a.Offset, ContentType: "application/octet-stream", Body: []byte{0, '\n', 255}}
	got, created, err := r.PutSnapshot(context.Background(), path, v, store.SnapshotCondition{IfNoneMatch: true})
	if err != nil || !created {
		t.Fatalf("put: %+v %v %v", got, created, err)
	}
	got.Body[0] = 9
	read, err := r.GetSnapshot(context.Background(), path, "p1")
	if err != nil || read.Body[0] != 0 {
		t.Fatalf("get: %+v %v", read, err)
	}
	if _, _, err := r.PutSnapshot(context.Background(), path, v, store.SnapshotCondition{IfNoneMatch: true}); !errors.Is(err, store.ErrSnapshotPrecondition) {
		t.Fatalf("CAS: %v", err)
	}
	bad := v
	bad.Offset = store.Offset{ByteOffset: 2}
	if _, _, err := r.PutSnapshot(context.Background(), path, bad, store.SnapshotCondition{IfMatch: read.ETag}); !errors.Is(err, store.ErrSnapshotConflict) {
		t.Fatalf("boundary: %v", err)
	}
	if err := testClient.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.GetSnapshot(context.Background(), path, "p1"); err != nil {
		t.Fatalf("NOSCRIPT reload: %v", err)
	}
}

func TestProjectionSnapshotForkBoundaries(t *testing.T) {
	for name, s := range snapshotBackends(t, store.RealClock()) {
		t.Run(name, func(t *testing.T) {
			src := testPath("snapshot-source")
			meta, _, err := s.Create(src, store.CreateOptions{InitialData: []byte("abc")})
			if err != nil {
				t.Fatal(err)
			}
			image := store.ProjectionSnapshot{Projection: "p", Incarnation: meta.Incarnation, Offset: meta.CurrentOffset, ContentType: "text/plain", Body: []byte("source")}
			if _, _, err := s.PutSnapshot(t.Context(), src, image, store.SnapshotCondition{IfNoneMatch: true}); err != nil {
				t.Fatal(err)
			}
			fork := testPath("snapshot-fork")
			fm, _, err := s.Create(fork, store.CreateOptions{ForkedFrom: src})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.GetSnapshot(t.Context(), fork, "p"); !errors.Is(err, store.ErrSnapshotNotFound) {
				t.Fatalf("fork inherited source image: %v", err)
			}
			image.Incarnation = fm.Incarnation
			if _, _, err := s.PutSnapshot(t.Context(), fork, image, store.SnapshotCondition{IfNoneMatch: true}); !errors.Is(err, store.ErrSnapshotConflict) {
				t.Fatalf("inherited cut: %v", err)
			}
			own, err := s.Append(fork, []byte("defgh"), store.AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			image.Offset, image.Body = own.Offset, []byte("fork")
			if _, created, err := s.PutSnapshot(t.Context(), fork, image, store.SnapshotCondition{IfNoneMatch: true}); err != nil || !created {
				t.Fatalf("root-owned fork boundary: created=%v err=%v", created, err)
			}
			got, err := s.GetSnapshot(t.Context(), src, "p")
			if err != nil || string(got.Body) != "source" || got.Incarnation != meta.Incarnation {
				t.Fatalf("fork publication changed source image: %+v %v", got, err)
			}
		})
	}
}

// Models an operator-controlled rollback drill, not automatic failover
// detection. The quiesced recovery transaction matches docs/spec/SNAPSHOTS.md.
func TestProjectionSnapshotRollbackRecoveryProcedure(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	path := testPath("snapshot-rollback-drill")
	meta := mustCreate(t, s, path, store.CreateOptions{InitialData: []byte("abc")})
	oldRead := store.ReadSnapshotFromMetadata(meta)
	image, _, err := s.PutSnapshot(ctx, path, store.ProjectionSnapshot{
		Projection: "identity-v1", Incarnation: meta.Incarnation, Offset: meta.CurrentOffset,
		ContentType: "application/octet-stream", Body: []byte("abc"),
	}, store.SnapshotCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a lost acknowledged prefix followed by offset reuse. The tail,
	// image digest and source identity still look valid: none detect this case.
	_, err = testClient.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
		pipe.Del(ctx, msgKey(path))
		pipe.ZAdd(ctx, msgKey(path), goredis.Z{Score: 0, Member: encodeFrame(meta.CurrentOffset, []byte("xyz"))})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetSnapshot(ctx, path, image.Projection); err != nil || string(got.Body) != "abc" {
		t.Fatalf("drill must demonstrate undetectable same-incarnation rollback: %+v %v", got, err)
	}
	// With ALL traffic/materializers quiesced, rotate incarnation and invalidate
	// all projections together. The two keys share the source's cluster slot.
	const newIncarnation = "rollback-drill-new-lifetime"
	err = testClient.Watch(ctx, func(tx *goredis.Tx) error {
		exists, err := tx.Exists(ctx, metaKey(path)).Result()
		if err != nil {
			return err
		}
		if exists != 1 {
			return store.ErrStreamNotFound
		}
		_, err = tx.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
			pipe.HSet(ctx, metaKey(path), "incarnation", newIncarnation)
			pipe.Del(ctx, snapshotKey(path))
			return nil
		})
		return err
	}, metaKey(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetSnapshot(ctx, path, image.Projection); !errors.Is(err, store.ErrSnapshotNotFound) {
		t.Fatalf("old image survived recovery: %v", err)
	}
	if _, _, err := s.PutSnapshot(ctx, path, image, store.SnapshotCondition{IfNoneMatch: true}); !errors.Is(err, store.ErrReadSnapshotChanged) {
		t.Fatalf("old materializer republished after recovery: %v", err)
	}
	if err := s.DeleteSnapshot(ctx, path, image.Projection, image.Incarnation, image.ETag); !errors.Is(err, store.ErrReadSnapshotChanged) {
		t.Fatalf("old retirement crossed recovery fence: %v", err)
	}
	if _, err := s.ReadPage(ctx, path, image.Offset, store.ReadPageOptions{Snapshot: &oldRead}); !errors.Is(err, store.ErrReadSnapshotChanged) {
		t.Fatalf("old reader crossed recovery fence: %v", err)
	}
	fresh, err := s.ReadPage(ctx, path, store.ZeroOffset, store.ReadPageOptions{})
	if err != nil || fresh.Snapshot.Incarnation != newIncarnation || len(fresh.Messages) != 1 || string(fresh.Messages[0].Data) != "xyz" {
		t.Fatalf("fresh replay did not use recovered source: %+v %v", fresh, err)
	}
	image.Incarnation, image.Body = newIncarnation, fresh.Messages[0].Data
	if _, created, err := s.PutSnapshot(ctx, path, image, store.SnapshotCondition{IfNoneMatch: true}); err != nil || !created {
		t.Fatalf("fresh fold could not publish after recovery: %v %v", created, err)
	}
}

func TestProjectionSnapshotRedisBinaryLayout(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	path := testPath("snapshot-binary-layout")
	meta := mustCreate(t, s, path, store.CreateOptions{})
	body := bytes.Repeat([]byte{0, 255, 128, '\n'}, 808*1024/4)
	image, _, err := s.PutSnapshot(ctx, path, store.ProjectionSnapshot{
		Projection: "p", Incarnation: meta.Incarnation, Offset: meta.CurrentOffset,
		ContentType: "application/octet-stream", Body: body,
	}, store.SnapshotCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := testClient.HGet(ctx, snapshotKey(path), "b:p").Bytes()
	if err != nil || !bytes.Equal(stored, body) {
		t.Fatalf("body was encoded or changed in Redis: %v", err)
	}
	descriptor, err := testClient.HGet(ctx, snapshotKey(path), "d:p").Result()
	if err != nil || len(descriptor) > 512 {
		t.Fatalf("body leaked into CAS descriptor: size=%d err=%v", len(descriptor), err)
	}
	// Measure the same body in the previous unpublished representation without
	// involving stream history, HTTP, compression, or process-memory estimates.
	legacy, err := json.Marshal(struct {
		Incarnation string
		Offset      string
		ContentType string
		ETag        string
		Body        []byte
	}{image.Incarnation, image.Offset.String(), image.ContentType, image.ETag, body})
	if err != nil {
		t.Fatal(err)
	}
	legacyKey := snapshotKey(path) + ":legacy-layout-measurement"
	t.Cleanup(func() { _ = testClient.Del(context.Background(), legacyKey).Err() })
	if err := testClient.HSet(ctx, legacyKey, "p", legacy).Err(); err != nil {
		t.Fatal(err)
	}
	oldMemory, err := testClient.MemoryUsage(ctx, legacyKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	newMemory, err := testClient.MemoryUsage(ctx, snapshotKey(path)).Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("body=%d descriptor=%d legacy-envelope=%d Redis MEMORY USAGE: legacy=%d binary=%d (allocator/version dependent)", len(body), len(descriptor), len(legacy), oldMemory, newMemory)
}
