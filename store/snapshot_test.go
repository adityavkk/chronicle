package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestMemoryProjectionSnapshots(t *testing.T) {
	s := NewMemoryStore()
	m, _, err := s.Create("/s", CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.Append("/s", []byte("abc"), AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	v := ProjectionSnapshot{Projection: "entity-v1", Incarnation: m.Incarnation, Offset: a.Offset, ContentType: "application/json", Body: []byte(`{"n":1}`), ETag: "ignored"}
	got, created, err := s.PutSnapshot(context.Background(), "/s", v, SnapshotCondition{IfNoneMatch: true})
	if err != nil || !created || got.ETag == "ignored" {
		t.Fatalf("put = %+v, %v, %v", got, created, err)
	}
	v.Body[0] = 'x'
	got.Body[0] = 'y'
	read, err := s.GetSnapshot(context.Background(), "/s", "entity-v1")
	if err != nil || string(read.Body) != `{"n":1}` {
		t.Fatalf("get = %+v, %v", read, err)
	}
	if _, _, err := s.PutSnapshot(context.Background(), "/s", ProjectionSnapshot{Projection: "entity-v1", Incarnation: m.Incarnation, Offset: Offset{ByteOffset: 2}, ContentType: "x", Body: []byte("x")}, SnapshotCondition{IfMatch: read.ETag}); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("interior offset: %v", err)
	}
	changed := read
	changed.Body = []byte("different")
	if _, _, err := s.PutSnapshot(context.Background(), "/s", changed, SnapshotCondition{IfMatch: read.ETag}); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("equal cut: %v", err)
	}
	if err := s.Delete("/s"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetSnapshot(context.Background(), "/s", "entity-v1"); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("deleted get: %v", err)
	}
}

func TestSnapshotProjectionValidation(t *testing.T) {
	for _, bad := range []string{"", "a\n", string([]byte{0xff})} {
		if !errors.Is(ValidateSnapshotProjection(bad), ErrInvalidSnapshot) {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestSnapshotVersionQuotaAndConditionalRetirement(t *testing.T) {
	s := NewMemoryStore()
	t.Cleanup(func() { _ = s.Close() })
	m, _, err := s.Create("/quota", CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var first ProjectionSnapshot
	for i := 0; i < MaxSnapshotVersions; i++ {
		v := ProjectionSnapshot{Projection: fmt.Sprintf("p-%d", i), Incarnation: m.Incarnation, Offset: m.CurrentOffset, ContentType: "application/octet-stream", Body: []byte{byte(i)}}
		got, _, err := s.PutSnapshot(t.Context(), "/quota", v, SnapshotCondition{IfNoneMatch: true})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = got
		}
	}
	extra := ProjectionSnapshot{Projection: "extra", Incarnation: m.Incarnation, Offset: m.CurrentOffset, ContentType: "text/plain", Body: []byte("x")}
	if _, _, err := s.PutSnapshot(t.Context(), "/quota", extra, SnapshotCondition{IfNoneMatch: true}); !errors.Is(err, ErrSnapshotQuota) {
		t.Fatalf("ninth image: %v", err)
	}
	if err := s.DeleteSnapshot(t.Context(), "/quota", first.Projection, m.Incarnation, `"stale"`); !errors.Is(err, ErrSnapshotPrecondition) {
		t.Fatalf("stale retirement: %v", err)
	}
	if err := s.DeleteSnapshot(t.Context(), "/quota", first.Projection, m.Incarnation, first.ETag); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PutSnapshot(t.Context(), "/quota", extra, SnapshotCondition{IfNoneMatch: true}); err != nil {
		t.Fatalf("quota not freed: %v", err)
	}
	if err := s.DeleteSnapshot(t.Context(), "/quota", first.Projection, m.Incarnation, first.ETag); !errors.Is(err, ErrSnapshotPrecondition) {
		t.Fatalf("missing retirement: %v", err)
	}
}
