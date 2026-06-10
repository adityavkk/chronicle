package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// TestDifferentialProducerTable runs the exact validation table from
// store/producer_test.go (PROTOCOL.md §5.2.1) through RedisStore.Append
// against live Redis and asserts the Lua mirror produces identical
// (result, error) to the pure-Go oracle store.ValidateProducer.
func TestDifferentialProducerTable(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Unix()

	st := func(epoch, lastSeq int64) *store.ProducerState {
		return &store.ProducerState{Epoch: epoch, LastSeq: lastSeq, LastUpdated: now - 100}
	}

	tests := []struct {
		name       string
		state      *store.ProducerState
		epoch, seq int64
	}{
		{name: "new producer seq 0 accepted", state: nil, epoch: 0, seq: 0},
		{name: "new producer any epoch accepted at seq 0", state: nil, epoch: 7, seq: 0},
		{name: "new producer nonzero seq is a gap with expected 0", state: nil, epoch: 0, seq: 3},
		{name: "stale epoch fenced", state: st(5, 9), epoch: 4, seq: 0},
		{name: "epoch bump must start at seq 0", state: st(5, 9), epoch: 6, seq: 1},
		{name: "epoch bump at seq 0 accepted, lastSeq resets", state: st(5, 9), epoch: 6, seq: 0},
		{name: "duplicate seq returns highest accepted seq", state: st(2, 4), epoch: 2, seq: 4},
		{name: "old duplicate seq still reports highest accepted seq", state: st(2, 4), epoch: 2, seq: 1},
		{name: "next seq accepted", state: st(2, 4), epoch: 2, seq: 5},
		{name: "seq gap rejected with expected and received", state: st(2, 4), epoch: 2, seq: 7},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Oracle: the pure-Go state machine.
			wantResult, wantNewState, wantErr := store.ValidateProducer(tt.state, tt.epoch, tt.seq, now)

			// Subject: a fresh stream with the producer state seeded directly.
			path := testPath("diff")
			mustCreate(t, s, path, store.CreateOptions{})
			if tt.state != nil {
				if err := testClient.HSet(ctx, prodKey(path), "p", encodeProducerState(tt.state)).Err(); err != nil {
					t.Fatal(err)
				}
			}
			epoch, seq := tt.epoch, tt.seq
			gotResult, gotErr := s.Append(path, []byte("payload"), store.AppendOptions{
				ProducerId: "p", ProducerEpoch: &epoch, ProducerSeq: &seq,
			})

			if !errors.Is(gotErr, wantErr) {
				t.Fatalf("err = %v, oracle %v", gotErr, wantErr)
			}
			if gotResult.ProducerResult != wantResult.ProducerResult {
				t.Errorf("ProducerResult = %v, oracle %v", gotResult.ProducerResult, wantResult.ProducerResult)
			}
			if gotResult.CurrentEpoch != wantResult.CurrentEpoch {
				t.Errorf("CurrentEpoch = %d, oracle %d", gotResult.CurrentEpoch, wantResult.CurrentEpoch)
			}
			if gotResult.ExpectedSeq != wantResult.ExpectedSeq {
				t.Errorf("ExpectedSeq = %d, oracle %d", gotResult.ExpectedSeq, wantResult.ExpectedSeq)
			}
			if gotResult.ReceivedSeq != wantResult.ReceivedSeq {
				t.Errorf("ReceivedSeq = %d, oracle %d", gotResult.ReceivedSeq, wantResult.ReceivedSeq)
			}
			if gotResult.LastSeq != wantResult.LastSeq {
				t.Errorf("LastSeq = %d, oracle %d", gotResult.LastSeq, wantResult.LastSeq)
			}

			// Persisted state must match the oracle's newState (or stay
			// unchanged when the oracle returns none).
			raw, err := testClient.HGet(ctx, prodKey(path), "p").Result()
			var persisted *store.ProducerState
			if err == nil {
				if persisted, err = decodeProducerState(raw); err != nil {
					t.Fatal(err)
				}
			}
			switch {
			case wantNewState != nil:
				if persisted == nil || persisted.Epoch != wantNewState.Epoch || persisted.LastSeq != wantNewState.LastSeq {
					t.Errorf("persisted state = %+v, oracle newState %+v", persisted, wantNewState)
				}
			case tt.state != nil:
				if persisted == nil || persisted.Epoch != tt.state.Epoch || persisted.LastSeq != tt.state.LastSeq {
					t.Errorf("persisted state = %+v, want untouched %+v", persisted, tt.state)
				}
			default:
				if persisted != nil {
					t.Errorf("persisted state = %+v, want none", persisted)
				}
			}

			// Writes must happen exactly on accepted results.
			tail, err := s.GetCurrentOffset(path)
			if err != nil {
				t.Fatal(err)
			}
			wrote := tail.ByteOffset == uint64(len("payload"))
			if wantResult.ProducerResult == store.ProducerResultAccepted && wantErr == nil {
				if !wrote {
					t.Error("accepted append did not write")
				}
			} else if wrote {
				t.Error("non-accepted append wrote data")
			}
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
	if _, err := s.Get(path); err != store.ErrStreamNotFound {
		t.Errorf("Get after expiry: %v", err)
	}
	if _, _, err := s.Read(path, store.ZeroOffset); err != store.ErrStreamNotFound {
		t.Errorf("Read after expiry: %v", err)
	}
	if _, err := s.Append(path, []byte("z"), store.AppendOptions{}); err != store.ErrStreamNotFound {
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
	if _, err := s.Get(path); err != store.ErrStreamNotFound {
		t.Errorf("Get after ExpiresAt: %v", err)
	}

	// Streams without expiry never expire (smoke: still alive after sleeps).
	path2 := testPath("no-expiry")
	mustCreate(t, s, path2, store.CreateOptions{})
	if !s.Has(path2) {
		t.Error("no-expiry stream should persist")
	}
}
