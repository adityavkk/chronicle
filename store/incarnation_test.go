package store

import (
	"encoding/hex"
	"testing"
	"time"
)

func TestNewIncarnationIDIsOpaqueAndDistinct(t *testing.T) {
	first, err := NewIncarnationID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewIncarnationID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("incarnation IDs were equal: %q", first)
	}
	raw, err := hex.DecodeString(first)
	if err != nil {
		t.Fatalf("decode incarnation ID: %v", err)
	}
	if len(raw) != incarnationBytes {
		t.Fatalf("incarnation ID bytes = %d, want %d", len(raw), incarnationBytes)
	}
}

func TestMemoryStoreDeleteRecreateChangesIncarnationWithFrozenClock(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_765_000_000, 123))
	st := NewMemoryStore(WithClock(clock))

	first, created, err := st.Create("/stream", CreateOptions{ContentType: "text/plain"})
	if err != nil || !created {
		t.Fatalf("first create: created=%t err=%v", created, err)
	}
	if err := st.Delete("/stream"); err != nil {
		t.Fatal(err)
	}
	second, created, err := st.Create("/stream", CreateOptions{ContentType: "text/plain"})
	if err != nil || !created {
		t.Fatalf("second create: created=%t err=%v", created, err)
	}

	if !first.CreatedAt.Equal(second.CreatedAt) {
		t.Fatalf("frozen CreatedAt changed: first=%s second=%s", first.CreatedAt, second.CreatedAt)
	}
	if first.Incarnation == "" || second.Incarnation == "" {
		t.Fatalf("empty incarnation: first=%q second=%q", first.Incarnation, second.Incarnation)
	}
	if first.SameIncarnation(second) {
		t.Fatalf("delete/recreate reused incarnation %q", first.Incarnation)
	}
}

func TestSameIncarnationLegacyFallback(t *testing.T) {
	created := time.Unix(1_765_000_000, 123)
	legacyA := &StreamMetadata{CreatedAt: created}
	legacyB := &StreamMetadata{CreatedAt: created}
	if !legacyA.SameIncarnation(legacyB) {
		t.Fatal("legacy metadata at the same creation timestamp did not match")
	}
	current := &StreamMetadata{CreatedAt: created, Incarnation: "new"}
	if legacyA.SameIncarnation(current) {
		t.Fatal("legacy and identified metadata matched")
	}
}
