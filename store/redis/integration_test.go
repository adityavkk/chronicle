package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// Integration tests run against live Redis (REDIS_URL or
// redis://localhost:6379/15) and are skipped under -short. The database is
// flushed once at start; every test uses a unique path prefix.

var (
	testClient   *goredis.Client
	testStore    *Store
	setupOnce    sync.Once
	setupErr     error
	pathCounter  atomic.Int64
	testRunStamp = time.Now().UnixNano()
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	return testStoreFor(t)
}

// testStoreFor is the live-Redis setup for the integration / property tests,
// generalized to any testing.TB (it no longer relies on *testing.T.Context, so
// the one-time client/flush/handshake works from any test entry point). The
// coverage-guided fuzz target uses its OWN non-flushing variant (fuzzStore in
// equivalence_fuzz_test.go) because `go test -fuzz` runs many worker processes
// that share one DB and a FlushDB would wipe a peer's streams. This skips under
// -short and when Redis is unreachable, exactly as the original newTestStore did.
func testStoreFor(tb testing.TB) *Store {
	tb.Helper()
	if testing.Short() {
		tb.Skip("skipping Redis integration test in -short mode")
	}
	setupOnce.Do(func() {
		url := os.Getenv("REDIS_URL")
		if url == "" {
			url = "redis://localhost:6379/15"
		}
		opts, err := goredis.ParseURL(url)
		if err != nil {
			setupErr = err
			return
		}
		testClient = goredis.NewClient(opts)
		ctx := context.Background()
		if err := testClient.Ping(ctx).Err(); err != nil {
			setupErr = fmt.Errorf("redis not reachable at %s: %w (run `docker compose up -d --wait redis`)", url, err)
			return
		}
		if err := testClient.FlushDB(ctx).Err(); err != nil {
			setupErr = err
			return
		}
		testStore = New(testClient, Options{})
	})
	if setupErr != nil {
		tb.Fatal(setupErr)
	}
	return testStore
}

// testPath returns a unique stream path for this test run.
func testPath(name string) string {
	return fmt.Sprintf("/t%d/%d/%s", testRunStamp, pathCounter.Add(1), name)
}

func mustCreate(t *testing.T, s *Store, path string, opts store.CreateOptions) *store.StreamMetadata {
	t.Helper()
	meta, created, err := s.Create(path, opts)
	if err != nil {
		t.Fatalf("Create(%s): %v", path, err)
	}
	if !created {
		t.Fatalf("Create(%s): expected newly created", path)
	}
	return meta
}

func mustAppend(t *testing.T, s *Store, path string, data []byte, opts store.AppendOptions) store.AppendResult {
	t.Helper()
	res, err := s.Append(path, data, opts)
	if err != nil {
		t.Fatalf("Append(%s): %v", path, err)
	}
	return res
}

func TestIntegrationCreateGetHas(t *testing.T) {
	s := newTestStore(t)
	path := testPath("basic")

	meta := mustCreate(t, s, path, store.CreateOptions{ContentType: "text/plain"})
	if meta.ContentType != "text/plain" || !meta.CurrentOffset.IsZero() {
		t.Errorf("unexpected meta: %+v", meta)
	}

	got, err := s.Get(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.ContentType != "text/plain" || got.Closed || got.Path != path {
		t.Errorf("Get: %+v", got)
	}
	if !s.Has(path) {
		t.Error("Has should be true")
	}
	if s.Has(path + "-nope") {
		t.Error("Has on missing path should be false")
	}
	if _, err := s.Get(path + "-nope"); !errors.Is(err, store.ErrStreamNotFound) {
		t.Errorf("Get missing: %v", err)
	}
}

func TestIntegrationCreateIdempotency(t *testing.T) {
	s := newTestStore(t)
	path := testPath("idem")
	ttl := int64(3600)
	opts := store.CreateOptions{ContentType: "application/json", TTLSeconds: &ttl}

	mustCreate(t, s, path, opts)

	// Same config: idempotent, created=false.
	meta, created, err := s.Create(path, opts)
	if err != nil || created {
		t.Fatalf("idempotent create: created=%v err=%v", created, err)
	}
	if meta.ContentType != "application/json" {
		t.Errorf("matched meta: %+v", meta)
	}

	// Different config: mismatch.
	if _, _, err := s.Create(path, store.CreateOptions{ContentType: "text/plain", TTLSeconds: &ttl}); !errors.Is(err, store.ErrConfigMismatch) {
		t.Errorf("config mismatch: %v", err)
	}
	ttl2 := int64(60)
	if _, _, err := s.Create(path, store.CreateOptions{ContentType: "application/json", TTLSeconds: &ttl2}); !errors.Is(err, store.ErrConfigMismatch) {
		t.Errorf("ttl mismatch: %v", err)
	}
	if _, _, err := s.Create(path, store.CreateOptions{ContentType: "application/json"}); !errors.Is(err, store.ErrConfigMismatch) {
		t.Errorf("ttl-nil mismatch: %v", err)
	}

	// Case-insensitive content type matches.
	if _, created, err := s.Create(path, store.CreateOptions{ContentType: "Application/JSON; charset=utf-8", TTLSeconds: &ttl}); err != nil || created {
		t.Errorf("case-insensitive match: created=%v err=%v", created, err)
	}
}

func TestIntegrationCreateClosedStatusMatching(t *testing.T) {
	s := newTestStore(t)
	path := testPath("closed-create")

	meta := mustCreate(t, s, path, store.CreateOptions{ContentType: "text/plain", Closed: true})
	if !meta.Closed {
		t.Error("meta should be closed")
	}

	// Closed-status must participate in config matching.
	if _, _, err := s.Create(path, store.CreateOptions{ContentType: "text/plain"}); !errors.Is(err, store.ErrConfigMismatch) {
		t.Errorf("open-vs-closed mismatch: %v", err)
	}
	if _, created, err := s.Create(path, store.CreateOptions{ContentType: "text/plain", Closed: true}); err != nil || created {
		t.Errorf("closed idempotent: created=%v err=%v", created, err)
	}

	// Appends to a closed-created stream fail.
	if _, err := s.Append(path, []byte("x"), store.AppendOptions{}); !errors.Is(err, store.ErrStreamClosed) {
		t.Errorf("append to closed: %v", err)
	}
}

func TestIntegrationAppendReadBinary(t *testing.T) {
	s := newTestStore(t)
	path := testPath("binary")
	mustCreate(t, s, path, store.CreateOptions{})

	// Binary data with 0x00 and 0xff and the frame separator byte.
	data1 := []byte{0x00, 0xff, '|', 'a', 0x00}
	data2 := bytes.Repeat([]byte{0xff, 0x00, '_'}, 100)

	r1 := mustAppend(t, s, path, data1, store.AppendOptions{})
	if r1.Offset.ByteOffset != uint64(len(data1)) {
		t.Errorf("offset after first append: %v", r1.Offset)
	}
	r2 := mustAppend(t, s, path, data2, store.AppendOptions{})
	if r2.Offset.ByteOffset != uint64(len(data1)+len(data2)) {
		t.Errorf("offset after second append: %v", r2.Offset)
	}

	msgs, upToDate, err := s.Read(path, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 || !upToDate {
		t.Fatalf("read: %d msgs upToDate=%v", len(msgs), upToDate)
	}
	if !bytes.Equal(msgs[0].Data, data1) || !bytes.Equal(msgs[1].Data, data2) {
		t.Error("binary payload corrupted")
	}
	if !msgs[0].Offset.Equal(r1.Offset) || !msgs[1].Offset.Equal(r2.Offset) {
		t.Error("message offsets mismatch")
	}

	// Resume from the first message's offset: only the second comes back.
	msgs, upToDate, err = s.Read(path, r1.Offset)
	if err != nil || len(msgs) != 1 || !upToDate {
		t.Fatalf("resume read: %d msgs upToDate=%v err=%v", len(msgs), upToDate, err)
	}
	if !bytes.Equal(msgs[0].Data, data2) {
		t.Error("resume payload mismatch")
	}

	// Read at tail: empty, up to date.
	msgs, upToDate, err = s.Read(path, r2.Offset)
	if err != nil || len(msgs) != 0 || !upToDate {
		t.Fatalf("tail read: %d msgs upToDate=%v err=%v", len(msgs), upToDate, err)
	}

	cur, err := s.GetCurrentOffset(path)
	if err != nil || !cur.Equal(r2.Offset) {
		t.Errorf("GetCurrentOffset: %v err=%v", cur, err)
	}
}

func TestIntegrationJSONModeFlattening(t *testing.T) {
	s := newTestStore(t)
	path := testPath("json")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json"})

	res := mustAppend(t, s, path, []byte(`[{"id":1},{"id":2},"three"]`), store.AppendOptions{})
	msgs, upToDate, err := s.Read(path, store.ZeroOffset)
	if err != nil || len(msgs) != 3 || !upToDate {
		t.Fatalf("json read: %d msgs upToDate=%v err=%v", len(msgs), upToDate, err)
	}
	if string(msgs[0].Data) != `{"id":1}` || string(msgs[2].Data) != `"three"` {
		t.Errorf("flattened messages: %q %q %q", msgs[0].Data, msgs[1].Data, msgs[2].Data)
	}
	if !msgs[2].Offset.Equal(res.Offset) {
		t.Error("last message offset should equal append result offset")
	}

	out, err := s.FormatResponse(path, msgs)
	if err != nil || string(out) != `[{"id":1},{"id":2},"three"]` {
		t.Errorf("FormatResponse: %s err=%v", out, err)
	}

	// Single value append: whitespace-trimmed, one message.
	mustAppend(t, s, path, []byte("  42 "), store.AppendOptions{})
	msgs, _, _ = s.Read(path, res.Offset)
	if len(msgs) != 1 || string(msgs[0].Data) != "42" {
		t.Errorf("single JSON value: %q", msgs[0].Data)
	}

	// Errors: invalid JSON, empty array on append.
	if _, err := s.Append(path, []byte("{oops"), store.AppendOptions{}); !errors.Is(err, store.ErrInvalidJSON) {
		t.Errorf("invalid json: %v", err)
	}
	if _, err := s.Append(path, []byte("[]"), store.AppendOptions{}); !errors.Is(err, store.ErrEmptyJSONArray) {
		t.Errorf("empty array: %v", err)
	}

	// Create with InitialData []: zero messages, allowed.
	path2 := testPath("json-empty-init")
	meta := mustCreate(t, s, path2, store.CreateOptions{ContentType: "application/json", InitialData: []byte("[]")})
	if !meta.CurrentOffset.IsZero() {
		t.Errorf("empty initial data should leave offset zero: %v", meta.CurrentOffset)
	}
}

func TestIntegrationEmptyBodyGuard(t *testing.T) {
	s := newTestStore(t)
	path := testPath("emptybody")
	mustCreate(t, s, path, store.CreateOptions{})
	if _, err := s.Append(path, nil, store.AppendOptions{}); !errors.Is(err, store.ErrEmptyBody) {
		t.Errorf("empty body: %v", err)
	}
	// Empty body with Close is allowed (close-only append).
	res, err := s.Append(path, nil, store.AppendOptions{Close: true})
	if err != nil || !res.StreamClosed {
		t.Errorf("close-only append: %+v err=%v", res, err)
	}
}

func TestIntegrationStreamSeqLexRegression(t *testing.T) {
	s := newTestStore(t)
	path := testPath("seq")
	mustCreate(t, s, path, store.CreateOptions{})

	mustAppend(t, s, path, []byte("a"), store.AppendOptions{Seq: "2"})
	// "10" < "2" bytewise: REJECTED even though numerically larger.
	if _, err := s.Append(path, []byte("b"), store.AppendOptions{Seq: "10"}); !errors.Is(err, store.ErrSequenceConflict) {
		t.Errorf(`seq "10" after "2": %v`, err)
	}
	// Same seq again: rejected.
	if _, err := s.Append(path, []byte("b"), store.AppendOptions{Seq: "2"}); !errors.Is(err, store.ErrSequenceConflict) {
		t.Errorf(`duplicate seq: %v`, err)
	}

	path2 := testPath("seq-padded")
	mustCreate(t, s, path2, store.CreateOptions{})
	mustAppend(t, s, path2, []byte("a"), store.AppendOptions{Seq: "09"})
	// "09" < "10" bytewise: accepted.
	if _, err := s.Append(path2, []byte("b"), store.AppendOptions{Seq: "10"}); err != nil {
		t.Errorf(`seq "10" after "09": %v`, err)
	}

	meta, err := s.Get(path2)
	if err != nil || meta.LastSeq != "10" {
		t.Errorf("LastSeq round trip: %+v err=%v", meta, err)
	}
}

func TestIntegrationClosurePaths(t *testing.T) {
	s := newTestStore(t)

	// Close-only, idempotent.
	path := testPath("close-idem")
	mustCreate(t, s, path, store.CreateOptions{})
	tail := mustAppend(t, s, path, []byte("data"), store.AppendOptions{}).Offset
	res, err := s.CloseStream(path)
	if err != nil || res.AlreadyClosed || !res.FinalOffset.Equal(tail) {
		t.Fatalf("first close: %+v err=%v", res, err)
	}
	res, err = s.CloseStream(path)
	if err != nil || !res.AlreadyClosed || !res.FinalOffset.Equal(tail) {
		t.Fatalf("second close: %+v err=%v", res, err)
	}

	// Append+close in one shot.
	path2 := testPath("append-close")
	mustCreate(t, s, path2, store.CreateOptions{})
	ar := mustAppend(t, s, path2, []byte("final"), store.AppendOptions{Close: true})
	if !ar.StreamClosed {
		t.Error("append+close should report StreamClosed")
	}
	meta, _ := s.Get(path2)
	if meta == nil || !meta.Closed {
		t.Error("stream should be closed")
	}

	// Append to closed carries the final offset.
	ar2, err := s.Append(path2, []byte("more"), store.AppendOptions{})
	if !errors.Is(err, store.ErrStreamClosed) {
		t.Fatalf("append to closed: %v", err)
	}
	if !ar2.Offset.Equal(ar.Offset) || !ar2.StreamClosed {
		t.Errorf("closed append result: %+v want offset %v", ar2, ar.Offset)
	}

	// CloseStream on missing stream.
	if _, err := s.CloseStream(testPath("nope")); !errors.Is(err, store.ErrStreamNotFound) {
		t.Errorf("close missing: %v", err)
	}
}

func TestIntegrationClosedByProducerDedup(t *testing.T) {
	s := newTestStore(t)
	path := testPath("closedby")
	mustCreate(t, s, path, store.CreateOptions{})
	epoch, seq := int64(1), int64(0)
	mustAppend(t, s, path, []byte("d"), store.AppendOptions{ProducerId: "p1", ProducerEpoch: &epoch, ProducerSeq: &seq})

	// Close with producer.
	cres, err := s.CloseStreamWithProducer(path, store.CloseProducerOptions{ProducerId: "p1", ProducerEpoch: 1, ProducerSeq: 1})
	if err != nil || cres.ProducerResult != store.ProducerResultAccepted || !cres.StreamClosed || cres.AlreadyClosed {
		t.Fatalf("producer close: %+v err=%v", cres, err)
	}

	// Exact same tuple again: duplicate success.
	cres, err = s.CloseStreamWithProducer(path, store.CloseProducerOptions{ProducerId: "p1", ProducerEpoch: 1, ProducerSeq: 1})
	if err != nil || cres.ProducerResult != store.ProducerResultDuplicate || !cres.AlreadyClosed || cres.LastSeq != 1 {
		t.Fatalf("duplicate close: %+v err=%v", cres, err)
	}

	// Different tuple: ErrStreamClosed with final offset.
	cres, err = s.CloseStreamWithProducer(path, store.CloseProducerOptions{ProducerId: "p2", ProducerEpoch: 1, ProducerSeq: 0})
	if !errors.Is(err, store.ErrStreamClosed) || !cres.AlreadyClosed || !cres.StreamClosed {
		t.Fatalf("other-producer close: %+v err=%v", cres, err)
	}

	// Append duplicate of the closing tuple: idempotent 204-style success.
	seq1 := int64(1)
	ares, err := s.Append(path, []byte("retry"), store.AppendOptions{ProducerId: "p1", ProducerEpoch: &epoch, ProducerSeq: &seq1})
	if err != nil || ares.ProducerResult != store.ProducerResultDuplicate || !ares.StreamClosed || ares.LastSeq != 1 {
		t.Fatalf("append dup of closing tuple: %+v err=%v", ares, err)
	}
}

func TestIntegrationDeleteAndRecreate(t *testing.T) {
	s := newTestStore(t)
	path := testPath("delete")
	mustCreate(t, s, path, store.CreateOptions{})
	mustAppend(t, s, path, []byte("x"), store.AppendOptions{Seq: "5"})

	if err := s.Delete(path); err != nil {
		t.Fatal(err)
	}
	if s.Has(path) {
		t.Error("Has after delete")
	}
	if err := s.Delete(path); !errors.Is(err, store.ErrStreamNotFound) {
		t.Errorf("double delete: %v", err)
	}

	// Recreate: brand new stream, everything reset.
	meta := mustCreate(t, s, path, store.CreateOptions{})
	if !meta.CurrentOffset.IsZero() || meta.LastSeq != "" || meta.Closed {
		t.Errorf("recreate not fresh: %+v", meta)
	}
	mustAppend(t, s, path, []byte("y"), store.AppendOptions{Seq: "1"}) // "1" < "5": fresh LastSeq
}

func TestIntegrationProducerSequencing(t *testing.T) {
	s := newTestStore(t)
	path := testPath("producer")
	mustCreate(t, s, path, store.CreateOptions{})

	pe, ps := int64(1), int64(0)
	popts := func(epoch, seq int64) store.AppendOptions {
		e, q := epoch, seq
		return store.AppendOptions{ProducerId: "p", ProducerEpoch: &e, ProducerSeq: &q}
	}

	// Partial headers.
	if _, err := s.Append(path, []byte("x"), store.AppendOptions{ProducerId: "p"}); !errors.Is(err, store.ErrPartialProducer) {
		t.Errorf("partial producer: %v", err)
	}
	_ = pe
	_ = ps

	// First contact must be seq 0.
	r, err := s.Append(path, []byte("x"), popts(5, 3))
	if !errors.Is(err, store.ErrProducerSeqGap) || r.ExpectedSeq != 0 || r.ReceivedSeq != 3 {
		t.Fatalf("first-contact gap: %+v err=%v", r, err)
	}
	// First contact seq 0 accepted with any epoch.
	r = mustAppend(t, s, path, []byte("a"), popts(5, 0))
	if r.ProducerResult != store.ProducerResultAccepted || r.LastSeq != 0 {
		t.Fatalf("first contact: %+v", r)
	}
	// In-order accepted.
	r = mustAppend(t, s, path, []byte("b"), popts(5, 1))
	if r.ProducerResult != store.ProducerResultAccepted || r.LastSeq != 1 {
		t.Fatalf("in-order: %+v", r)
	}
	// Duplicate: no write, highest seq back.
	tailBefore, _ := s.GetCurrentOffset(path)
	r, err = s.Append(path, []byte("b"), popts(5, 1))
	if err != nil || r.ProducerResult != store.ProducerResultDuplicate || r.LastSeq != 1 {
		t.Fatalf("duplicate: %+v err=%v", r, err)
	}
	if tailAfter, _ := s.GetCurrentOffset(path); !tailAfter.Equal(tailBefore) {
		t.Error("duplicate must not write")
	}
	// Gap.
	r, err = s.Append(path, []byte("c"), popts(5, 5))
	if !errors.Is(err, store.ErrProducerSeqGap) || r.ExpectedSeq != 2 || r.ReceivedSeq != 5 {
		t.Fatalf("gap: %+v err=%v", r, err)
	}
	// Stale epoch.
	r, err = s.Append(path, []byte("c"), popts(4, 0))
	if !errors.Is(err, store.ErrStaleEpoch) || r.CurrentEpoch != 5 {
		t.Fatalf("stale epoch: %+v err=%v", r, err)
	}
	// New epoch must start at 0.
	if _, err := s.Append(path, []byte("c"), popts(6, 2)); !errors.Is(err, store.ErrInvalidEpochSeq) {
		t.Fatalf("epoch bump seq!=0: %v", err)
	}
	// Epoch bump at seq 0.
	r = mustAppend(t, s, path, []byte("c"), popts(6, 0))
	if r.ProducerResult != store.ProducerResultAccepted || r.LastSeq != 0 {
		t.Fatalf("epoch bump: %+v", r)
	}

	// Producer state survives in metadata.
	meta, err := s.Get(path)
	if err != nil {
		t.Fatal(err)
	}
	ps2 := meta.Producers["p"]
	if ps2 == nil || ps2.Epoch != 6 || ps2.LastSeq != 0 {
		t.Errorf("producer state: %+v", ps2)
	}
}
