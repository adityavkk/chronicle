package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// TestDifferentialProducerTable runs the exact validation table from
// store/producer_test.go (PROTOCOL.md §5.2.1) through Store.Append against live
// Redis and asserts the Lua mirror produces an identical full reply tuple,
// error class, persist decision, and tail-advances-on-accept to the pure-Go
// oracle store.ValidateProducer (and, under -tags leanoracle, the proven Lean
// model via checkLeanProducer).
//
// As of issue #32 this fixed table is RETAINED as a named, covered subset /
// seed corpus of the generalized rapid property TestDifferentialProducerProperty
// (differential_producer_test.go): both share assertProducerDifferential, so the
// boundary generator strictly subsumes these rungs while these stay readable,
// deterministic regression rows. Every case here uses tiny values well inside
// the proven-exact < 10^14 domain, so the three oracles must agree exactly.
// [INV-PROD-08]
func TestDifferentialProducerTable(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Unix()
	ctx := context.Background()

	st := func(epoch, lastSeq int64) *store.ProducerState {
		return &store.ProducerState{Epoch: epoch, LastSeq: lastSeq, LastUpdated: now - 100}
	}

	tests := []struct {
		name string
		producerCase
	}{
		{"new producer seq 0 accepted", producerCase{state: nil, epoch: 0, seq: 0}},
		{"new producer any epoch accepted at seq 0", producerCase{state: nil, epoch: 7, seq: 0}},
		{"new producer nonzero seq is a gap with expected 0", producerCase{state: nil, epoch: 0, seq: 3}},
		{"stale epoch fenced", producerCase{state: st(5, 9), epoch: 4, seq: 0}},
		{"epoch bump must start at seq 0", producerCase{state: st(5, 9), epoch: 6, seq: 1}},
		{"epoch bump at seq 0 accepted, lastSeq resets", producerCase{state: st(5, 9), epoch: 6, seq: 0}},
		{"duplicate seq returns highest accepted seq", producerCase{state: st(2, 4), epoch: 2, seq: 4}},
		{"old duplicate seq still reports highest accepted seq", producerCase{state: st(2, 4), epoch: 2, seq: 1}},
		{"next seq accepted", producerCase{state: st(2, 4), epoch: 2, seq: 5}},
		{"seq gap rejected with expected and received", producerCase{state: st(2, 4), epoch: 2, seq: 7}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if luaUnsafe(tt.state, tt.epoch, tt.seq) {
				t.Fatalf("corpus row %q unexpectedly outside the < 10^14 reply-exact safe domain", tt.name)
			}
			assertProducerDifferential(t, s, ctx, now, tt.producerCase)
		})
	}
}

// TestDifferentialOffsetCompare pins the proven Lean Offset.compare against the
// Go core store.Compare over a table of boundary pairs, including the LB-1
// boundary (10^16) and the top of the uint64 domain. It does NOT touch Redis
// (offset order is a pure-core property); the live-Lua side of the offset order
// is exercised by the read path's ZRANGEBYLEX usage elsewhere. The check is a
// no-op without -tags leanoracle. [INV-OFF-01, INV-OFF-02]
func TestDifferentialOffsetCompare(t *testing.T) {
	const safe = uint64(1e16)
	off := func(rs, bo uint64) store.Offset { return store.Offset{ReadSeq: rs, ByteOffset: bo} }

	pairs := []struct {
		name string
		a, b store.Offset
	}{
		{"equal zero", off(0, 0), off(0, 0)},
		{"byteOffset less", off(0, 5), off(0, 9)},
		{"byteOffset greater", off(0, 9), off(0, 5)},
		{"readSeq dominates", off(2, 0), off(1, ^uint64(0))},
		{"tie", off(7, 7), off(7, 7)},
		{"just below LB-1 boundary", off(0, safe-1), off(0, safe-2)},
		{"straddle LB-1 boundary", off(0, safe-1), off(0, safe)},     // labeled LB-1 region
		{"both above LB-1 boundary", off(0, safe+1), off(0, safe+2)}, // labeled LB-1 region
		{"max uint64 byteOffset", off(0, ^uint64(0)), off(0, ^uint64(0)-1)},
		{"max uint64 readSeq", off(^uint64(0), 0), off(^uint64(0)-1, ^uint64(0))},
	}

	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			// Sanity: Go core's sign is in {-1,0,1} (the order contract).
			if s := store.Compare(p.a, p.b); s < -1 || s > 1 {
				t.Fatalf("Go core Compare out of range: %d", s)
			}
			checkLeanOffsetCompare(t, p.a, p.b)
		})
	}
}

func TestIntegrationTTLSliding(t *testing.T) {
	s := newTestStore(t)
	ttl := int64(2)

	// Read and Append renew the window; Get does not.
	path := testPath("ttl-renew")
	mustCreate(t, s, path, store.CreateOptions{TTLSeconds: &ttl})
	mustAppend(t, s, path, []byte("x"), store.AppendOptions{})

	time.Sleep(1200 * time.Millisecond)
	if _, _, err := s.Read(path, store.ZeroOffset); err != nil { // renews
		t.Fatalf("read at 1.2s: %v", err)
	}
	time.Sleep(1200 * time.Millisecond) // 2.4s since create, 1.2s since Read
	if !s.Has(path) {
		t.Fatal("stream expired despite Read renewal")
	}
	mustAppend(t, s, path, []byte("y"), store.AppendOptions{}) // renews again
	time.Sleep(1200 * time.Millisecond)
	if !s.Has(path) {
		t.Fatal("stream expired despite Append renewal")
	}
	time.Sleep(1200 * time.Millisecond) // ~2.4s since last touch: expired
	if s.Has(path) {
		t.Error("Has after TTL expiry")
	}
	if _, err := s.Get(path); !errors.Is(err, store.ErrStreamNotFound) {
		t.Errorf("Get after expiry: %v", err)
	}
	if _, _, err := s.Read(path, store.ZeroOffset); !errors.Is(err, store.ErrStreamNotFound) {
		t.Errorf("Read after expiry: %v", err)
	}
	if _, err := s.Append(path, []byte("z"), store.AppendOptions{}); !errors.Is(err, store.ErrStreamNotFound) {
		t.Errorf("Append after expiry: %v", err)
	}

	// Get must NOT renew the sliding window.
	path2 := testPath("ttl-get-no-renew")
	mustCreate(t, s, path2, store.CreateOptions{TTLSeconds: &ttl})
	time.Sleep(1500 * time.Millisecond)
	if _, err := s.Get(path2); err != nil { // alive at 1.5s, but no touch
		t.Fatalf("Get at 1.5s: %v", err)
	}
	time.Sleep(1000 * time.Millisecond) // 2.5s since create
	if s.Has(path2) {
		t.Error("Get must not refresh the sliding TTL")
	}

	// Expired streams are recreatable (created=true, fresh).
	meta, created, err := s.Create(path2, store.CreateOptions{TTLSeconds: &ttl})
	if err != nil || !created {
		t.Fatalf("recreate expired: created=%v err=%v", created, err)
	}
	if !meta.CurrentOffset.IsZero() {
		t.Error("recreated stream should be fresh")
	}
}

func TestIntegrationExpiresAtAbsolute(t *testing.T) {
	s := newTestStore(t)
	path := testPath("expires-at")
	expAt := time.Now().Add(1 * time.Second)
	mustCreate(t, s, path, store.CreateOptions{ExpiresAt: &expAt})
	mustAppend(t, s, path, []byte("x"), store.AppendOptions{})

	// Touches do not extend an absolute expiry.
	time.Sleep(600 * time.Millisecond)
	if _, _, err := s.Read(path, store.ZeroOffset); err != nil {
		t.Fatalf("read before expiry: %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	if s.Has(path) {
		t.Error("Has after ExpiresAt")
	}
	if _, err := s.Get(path); !errors.Is(err, store.ErrStreamNotFound) {
		t.Errorf("Get after ExpiresAt: %v", err)
	}

	// Streams without expiry never expire (smoke: still alive after sleeps).
	path2 := testPath("no-expiry")
	mustCreate(t, s, path2, store.CreateOptions{})
	if !s.Has(path2) {
		t.Error("no-expiry stream should persist")
	}
}
