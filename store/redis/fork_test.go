package redis

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestIntegrationForkBasics(t *testing.T) {
	s := newTestStore(t)
	src := testPath("fork-src")
	mustCreate(t, s, src, store.CreateOptions{})
	mustAppend(t, s, src, []byte("hello"), store.AppendOptions{})                // tail 5
	tail := mustAppend(t, s, src, []byte("world"), store.AppendOptions{}).Offset // tail 10

	// Fork at source tail (nil ForkOffset).
	fork := testPath("fork-dst")
	meta := mustCreate(t, s, fork, store.CreateOptions{ForkedFrom: src})
	if meta.ForkedFrom != src || !meta.ForkOffset.Equal(tail) || !meta.CurrentOffset.Equal(tail) {
		t.Fatalf("fork meta: %+v", meta)
	}
	if srcMeta, _ := s.Get(src); srcMeta == nil || srcMeta.RefCount != 1 {
		t.Fatalf("source refCount: %+v", srcMeta)
	}

	// Stitched read returns inherited source data.
	msgs, upToDate, err := s.Read(fork, store.ZeroOffset)
	if err != nil || len(msgs) != 2 || !upToDate {
		t.Fatalf("fork read: %d msgs upToDate=%v err=%v", len(msgs), upToDate, err)
	}
	if string(msgs[0].Data) != "hello" || string(msgs[1].Data) != "world" {
		t.Errorf("inherited data: %q %q", msgs[0].Data, msgs[1].Data)
	}

	// Fork appends extend the shared offset space.
	fr := mustAppend(t, s, fork, []byte("abc"), store.AppendOptions{})
	if fr.Offset.ByteOffset != 13 {
		t.Errorf("fork append offset: %v", fr.Offset)
	}

	// Source appends after the fork stay invisible to the fork.
	mustAppend(t, s, src, []byte("ZZZZ"), store.AppendOptions{})
	msgs, upToDate, err = s.Read(fork, store.ZeroOffset)
	if err != nil || len(msgs) != 3 || !upToDate {
		t.Fatalf("fork read after source append: %d upToDate=%v err=%v", len(msgs), upToDate, err)
	}
	if string(msgs[2].Data) != "abc" {
		t.Errorf("fork own data: %q", msgs[2].Data)
	}

	// Mid-source-range fork: explicit offset.
	fork2 := testPath("fork-mid")
	off5 := store.Offset{ByteOffset: 5}
	m2 := mustCreate(t, s, fork2, store.CreateOptions{ForkedFrom: src, ForkOffset: &off5})
	if !m2.ForkOffset.Equal(off5) || m2.ForkOffsetRequested == nil || !m2.ForkOffsetRequested.Equal(off5) {
		t.Fatalf("fork2 meta: %+v", m2)
	}
	msgs, _, err = s.Read(fork2, store.ZeroOffset)
	if err != nil || len(msgs) != 1 || string(msgs[0].Data) != "hello" {
		t.Fatalf("fork2 read: %v err=%v", msgs, err)
	}

	// Fork idempotent re-create and config mismatch.
	if _, created, err := s.Create(fork2, store.CreateOptions{ForkedFrom: src, ForkOffset: &off5}); err != nil || created {
		t.Errorf("fork idempotent recreate: created=%v err=%v", created, err)
	}
	off3 := store.Offset{ByteOffset: 3}
	if _, _, err := s.Create(fork2, store.CreateOptions{ForkedFrom: src, ForkOffset: &off3}); !errors.Is(err, store.ErrConfigMismatch) {
		t.Errorf("fork offset mismatch: %v", err)
	}

	// Fork offset beyond source tail.
	off999 := store.Offset{ByteOffset: 999}
	if _, _, err := s.Create(testPath("fork-bad"), store.CreateOptions{ForkedFrom: src, ForkOffset: &off999}); !errors.Is(err, store.ErrInvalidForkOffset) {
		t.Errorf("fork beyond tail: %v", err)
	}

	// Fork of missing source.
	if _, _, err := s.Create(testPath("fork-bad2"), store.CreateOptions{ForkedFrom: testPath("nope")}); !errors.Is(err, store.ErrStreamNotFound) {
		t.Errorf("fork of missing source: %v", err)
	}

	// Content-type mismatch rejected before taking a reference.
	if _, _, err := s.Create(testPath("fork-bad3"), store.CreateOptions{ForkedFrom: src, ContentType: "application/json"}); !errors.Is(err, store.ErrContentTypeMismatch) {
		t.Errorf("fork ct mismatch: %v", err)
	}
	if srcMeta, _ := s.Get(src); srcMeta.RefCount != 2 {
		t.Errorf("source refCount after failed forks: %d (leaked reference?)", srcMeta.RefCount)
	}
}

func TestIntegrationForkSubOffsets(t *testing.T) {
	s := newTestStore(t)

	// JSON: sub-offset counts flattened messages past the fork point.
	src := testPath("subj-src")
	mustCreate(t, s, src, store.CreateOptions{ContentType: "application/json"})
	mustAppend(t, s, src, []byte(`[10,20,30]`), store.AppendOptions{}) // offsets 2,4,6

	fork := testPath("subj-fork")
	zero := store.ZeroOffset
	sub2 := uint64(2)
	meta := mustCreate(t, s, fork, store.CreateOptions{ForkedFrom: src, ForkOffset: &zero, ForkSubOffset: &sub2})
	if meta.ForkOffset.ByteOffset != 4 { // advanced to 2nd message boundary
		t.Errorf("JSON fork ForkOffset: %v", meta.ForkOffset)
	}
	if meta.ForkOffsetRequested == nil || !meta.ForkOffsetRequested.IsZero() || meta.ForkSubOffset != 2 {
		t.Errorf("requested fields: %+v", meta)
	}
	msgs, _, err := s.Read(fork, store.ZeroOffset)
	if err != nil || len(msgs) != 2 || string(msgs[0].Data) != "10" || string(msgs[1].Data) != "20" {
		t.Fatalf("JSON sub-offset read: %v err=%v", msgs, err)
	}
	// Re-create with identical opts is idempotent (requested offset matching).
	// ContentType must be explicit: ConfigMatches normalizes an empty request
	// content type to application/octet-stream (mirrors upstream), so an
	// empty-CT re-create of a JSON fork is a config mismatch.
	if _, created, err := s.Create(fork, store.CreateOptions{ContentType: "application/json", ForkedFrom: src, ForkOffset: &zero, ForkSubOffset: &sub2}); err != nil || created {
		t.Errorf("JSON sub-offset idempotent recreate: created=%v err=%v", created, err)
	}
	if _, _, err := s.Create(fork, store.CreateOptions{ForkedFrom: src, ForkOffset: &zero, ForkSubOffset: &sub2}); !errors.Is(err, store.ErrConfigMismatch) {
		t.Errorf("empty-CT recreate of JSON fork should mismatch (upstream parity): %v", err)
	}

	// Overshoot.
	sub9 := uint64(9)
	if _, _, err := s.Create(testPath("subj-bad"), store.CreateOptions{ForkedFrom: src, ForkOffset: &zero, ForkSubOffset: &sub9}); !errors.Is(err, store.ErrInvalidForkSubOffset) {
		t.Errorf("JSON sub-offset overshoot: %v", err)
	}

	// Binary: sub-offset is a byte prefix of the first following message,
	// materialized as the fork's first own frame.
	bsrc := testPath("subb-src")
	mustCreate(t, s, bsrc, store.CreateOptions{})
	mustAppend(t, s, bsrc, []byte("hello"), store.AppendOptions{})                    // 0..5
	mustAppend(t, s, bsrc, []byte{0x00, 0xff, 'w', 0x00, 'r'}, store.AppendOptions{}) // 5..10

	bfork := testPath("subb-fork")
	off5 := store.Offset{ByteOffset: 5}
	sub3 := uint64(3)
	bmeta := mustCreate(t, s, bfork, store.CreateOptions{ForkedFrom: bsrc, ForkOffset: &off5, ForkSubOffset: &sub3})
	if !bmeta.ForkOffset.Equal(off5) || bmeta.CurrentOffset.ByteOffset != 8 {
		t.Fatalf("binary sub-offset meta: %+v", bmeta)
	}
	msgs, upToDate, err := s.Read(bfork, store.ZeroOffset)
	if err != nil || len(msgs) != 2 || !upToDate {
		t.Fatalf("binary sub-offset read: %v upToDate=%v err=%v", msgs, upToDate, err)
	}
	if string(msgs[0].Data) != "hello" || !bytes.Equal(msgs[1].Data, []byte{0x00, 0xff, 'w'}) {
		t.Errorf("binary prefix frame: %q %q", msgs[0].Data, msgs[1].Data)
	}
	if msgs[1].Offset.ByteOffset != 8 {
		t.Errorf("prefix frame offset: %v", msgs[1].Offset)
	}

	// Prefix longer than the following message.
	sub99 := uint64(99)
	if _, _, err := s.Create(testPath("subb-bad"), store.CreateOptions{ForkedFrom: bsrc, ForkOffset: &off5, ForkSubOffset: &sub99}); !errors.Is(err, store.ErrInvalidForkSubOffset) {
		t.Errorf("binary sub-offset overshoot: %v", err)
	}
}

func TestIntegrationForkSoftDeleteAndCascade(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// 3-level chain: A <- B <- C.
	a := testPath("gc-a")
	mustCreate(t, s, a, store.CreateOptions{})
	mustAppend(t, s, a, []byte("aa"), store.AppendOptions{})
	b := testPath("gc-b")
	mustCreate(t, s, b, store.CreateOptions{ForkedFrom: a})
	mustAppend(t, s, b, []byte("bb"), store.AppendOptions{})
	c := testPath("gc-c")
	mustCreate(t, s, c, store.CreateOptions{ForkedFrom: b})

	// Deleting referenced sources soft-deletes them.
	if err := s.Delete(a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(a); !errors.Is(err, store.ErrStreamSoftDeleted) {
		t.Errorf("Get soft-deleted: %v", err)
	}
	if s.Has(a) {
		t.Error("Has soft-deleted")
	}
	if _, _, err := s.Read(a, store.ZeroOffset); !errors.Is(err, store.ErrStreamNotFound) {
		t.Errorf("Read soft-deleted: %v", err)
	}
	if _, err := s.Append(a, []byte("x"), store.AppendOptions{}); !errors.Is(err, store.ErrStreamSoftDeleted) {
		t.Errorf("Append soft-deleted: %v", err)
	}
	if err := s.Delete(a); !errors.Is(err, store.ErrStreamSoftDeleted) {
		t.Errorf("Delete soft-deleted: %v", err)
	}
	if _, _, err := s.Create(a, store.CreateOptions{}); !errors.Is(err, store.ErrStreamExists) {
		t.Errorf("Create over soft-deleted: %v", err)
	}

	// Forks read through soft-deleted parents.
	msgs, _, err := s.Read(c, store.ZeroOffset)
	if err != nil || len(msgs) != 2 || string(msgs[0].Data) != "aa" || string(msgs[1].Data) != "bb" {
		t.Fatalf("read through soft-deleted chain: %v err=%v", msgs, err)
	}

	// Delete B: soft (C references it). Delete C: hard, cascades B then A.
	if err := s.Delete(b); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(c); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{a, b, c} {
		if s.Has(p) {
			t.Errorf("%s should be gone after cascade", p)
		}
		n, err := testClient.Exists(ctx, metaKey(p), msgKey(p), prodKey(p), forksKey(p)).Result()
		if err != nil || n != 0 {
			t.Errorf("%s keys remain after cascade: n=%d err=%v", p, n, err)
		}
	}

	// A is recreatable after the cascade wiped it.
	mustCreate(t, s, a, store.CreateOptions{})
}

func TestIntegrationForkRefCountLifecycle(t *testing.T) {
	s := newTestStore(t)
	src := testPath("rc-src")
	mustCreate(t, s, src, store.CreateOptions{})
	mustAppend(t, s, src, []byte("data"), store.AppendOptions{})

	f1 := testPath("rc-f1")
	f2 := testPath("rc-f2")
	mustCreate(t, s, f1, store.CreateOptions{ForkedFrom: src})
	mustCreate(t, s, f2, store.CreateOptions{ForkedFrom: src})
	if m, _ := s.Get(src); m.RefCount != 2 {
		t.Fatalf("refCount: %d", m.RefCount)
	}

	// Deleting a fork releases its reference; source stays live.
	if err := s.Delete(f1); err != nil {
		t.Fatal(err)
	}
	if m, _ := s.Get(src); m.RefCount != 1 {
		t.Fatalf("refCount after fork delete: %d", m.RefCount)
	}
	// Source not soft-deleted: deleting last fork must NOT delete it.
	if err := s.Delete(f2); err != nil {
		t.Fatal(err)
	}
	m, err := s.Get(src)
	if err != nil || m.RefCount != 0 {
		t.Fatalf("source after all forks gone: %+v err=%v", m, err)
	}
	// Now unreferenced: a delete is a hard delete.
	if err := s.Delete(src); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(src); !errors.Is(err, store.ErrStreamNotFound) {
		t.Errorf("source after hard delete: %v", err)
	}
}

// TestIntegrationExpiredSourceWithForksFlipsSoftDeleted: lazy expiry on a
// stream with refCount > 0 must soft-delete it (fork readers keep working)
// instead of reaping the data.
func TestIntegrationExpiredSourceWithForksFlipsSoftDeleted(t *testing.T) {
	s := newTestStore(t)
	src := testPath("expflip-src")
	ttl := int64(1)
	mustCreate(t, s, src, store.CreateOptions{TTLSeconds: &ttl})
	mustAppend(t, s, src, []byte("keepme"), store.AppendOptions{})

	fork := testPath("expflip-fork")
	noTTL := int64(3600)
	mustCreate(t, s, fork, store.CreateOptions{ForkedFrom: src, TTLSeconds: &noTTL})

	time.Sleep(1200 * time.Millisecond)

	// Access discovers expiry: NOTFOUND, and the source flips to soft-deleted.
	if _, _, err := s.Read(src, store.ZeroOffset); !errors.Is(err, store.ErrStreamNotFound) {
		t.Fatalf("Read expired source: %v", err)
	}
	if _, err := s.Get(src); !errors.Is(err, store.ErrStreamSoftDeleted) {
		t.Fatalf("expired source with forks should be soft-deleted: %v", err)
	}
	if _, _, err := s.Create(src, store.CreateOptions{}); !errors.Is(err, store.ErrStreamExists) {
		t.Errorf("Create over expired-flipped source: %v", err)
	}

	// The fork still reads the inherited data.
	msgs, _, err := s.Read(fork, store.ZeroOffset)
	if err != nil || len(msgs) != 1 || string(msgs[0].Data) != "keepme" {
		t.Fatalf("fork read through expired source: %v err=%v", msgs, err)
	}

	// Deleting the fork cascades the soft-deleted source away.
	if err := s.Delete(fork); err != nil {
		t.Fatal(err)
	}
	if n, _ := testClient.Exists(context.Background(), metaKey(src), msgKey(src)).Result(); n != 0 {
		t.Error("source keys should be gone after cascade")
	}
}

func TestIntegrationForkExpiryInheritance(t *testing.T) {
	s := newTestStore(t)
	src := testPath("inherit-src")
	ttl := int64(3600)
	mustCreate(t, s, src, store.CreateOptions{TTLSeconds: &ttl})

	// No explicit expiry: inherit source TTL.
	f1 := mustCreate(t, s, testPath("inherit-f1"), store.CreateOptions{ForkedFrom: src})
	if f1.TTLSeconds == nil || *f1.TTLSeconds != 3600 || f1.ExpiresAt != nil {
		t.Errorf("inherited TTL: %+v", f1)
	}
	// Explicit TTL wins.
	ttl2 := int64(60)
	f2 := mustCreate(t, s, testPath("inherit-f2"), store.CreateOptions{ForkedFrom: src, TTLSeconds: &ttl2})
	if f2.TTLSeconds == nil || *f2.TTLSeconds != 60 {
		t.Errorf("explicit TTL: %+v", f2)
	}
}
