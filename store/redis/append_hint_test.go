package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// AppendOptions.TailHint lets Append frame its first attempt against the tail
// the caller already read instead of reading it again (one Redis round trip
// fewer per append). The hint is advisory: append.lua's tail check (step 11,
// RETRY) still decides atomically, so a hinted append must be indistinguishable
// from an unhinted one in result, error and resulting stream state, whatever
// happened to the stream between the read and the write (INV-LIN-02).

// tripRecorder is a go-redis Hook recording one entry per Redis round trip:
// each single command by name, each pipeline Exec as "pipe(name+name)". It
// also counts append.lua RETRY replies and can run a callback once, just
// before the first script call it sees, to move the tail under an append.
type tripRecorder struct {
	mu           sync.Mutex
	trips        []string
	retries      int
	beforeScript func()
}

func (r *tripRecorder) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (r *tripRecorder) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		r.mu.Lock()
		r.trips = append(r.trips, cmd.Name())
		before := r.beforeScript
		if cmd.Name() == "evalsha" || cmd.Name() == "eval" {
			r.beforeScript = nil
		} else {
			before = nil
		}
		r.mu.Unlock()
		if before != nil {
			before()
		}
		err := next(ctx, cmd)
		if c, ok := cmd.(*goredis.Cmd); ok && err == nil {
			if reply, ok := c.Val().([]any); ok && len(reply) > 0 && reply[0] == stRetry {
				r.mu.Lock()
				r.retries++
				r.mu.Unlock()
			}
		}
		return err
	}
}

func (r *tripRecorder) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		names := make([]string, len(cmds))
		for i, cmd := range cmds {
			names[i] = cmd.Name()
		}
		r.mu.Lock()
		r.trips = append(r.trips, "pipe("+strings.Join(names, "+")+")")
		r.mu.Unlock()
		return next(ctx, cmds)
	}
}

// take returns the trips recorded since the last take and clears them.
func (r *tripRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	trips := r.trips
	r.trips = nil
	return trips
}

func (r *tripRecorder) retryCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retries
}

// recordedStore is a Store on its own client with a tripRecorder attached and
// the script cache warmed, so EVALSHA never falls back to EVAL mid-count.
func recordedStore(t *testing.T) (*Store, *tripRecorder) {
	t.Helper()
	client := goredis.NewClient(testClient.Options())
	t.Cleanup(func() { _ = client.Close() })
	rec := &tripRecorder{}
	client.AddHook(rec)
	s := New(client, Options{})
	warm := testPath("warm")
	mustCreate(t, s, warm, store.CreateOptions{ContentType: "text/plain"})
	mustAppend(t, s, warm, []byte("w"), store.AppendOptions{ContentType: "text/plain"})
	rec.take()
	return s, rec
}

func offsetPtr(o store.Offset) *store.Offset { return &o }

// TestAppendTailHintRoundTrips pins the round trips Append spends: a fresh
// hint with a content type is the script alone; no hint, or a hint with no
// content type (nothing then pins the framing mode), reads tail and content
// type first; a stale hint costs one RETRY and then the unhinted path.
func TestAppendTailHintRoundTrips(t *testing.T) {
	newTestStore(t)
	s, rec := recordedStore(t)
	plain := store.AppendOptions{ContentType: "text/plain"}

	newStream := func(name string) (string, *store.Offset) {
		path := testPath(name)
		mustCreate(t, s, path, store.CreateOptions{ContentType: "text/plain"})
		mustAppend(t, s, path, []byte("seed"), plain)
		meta, err := s.Get(path)
		if err != nil {
			t.Fatal(err)
		}
		rec.take()
		return path, &meta.CurrentOffset
	}
	assertTrips := func(label string, want ...string) {
		t.Helper()
		if got := rec.take(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: round trips %v, want %v", label, got, want)
		}
	}

	path, hint := newStream("fresh")
	fresh := mustAppend(t, s, path, []byte("x"), store.AppendOptions{ContentType: "text/plain", TailHint: hint})
	assertTrips("fresh hint", "evalsha")

	path, _ = newStream("nohint")
	unhinted := mustAppend(t, s, path, []byte("x"), plain)
	assertTrips("no hint", "hmget", "evalsha")

	path, hint = newStream("noct")
	noCT := mustAppend(t, s, path, []byte("x"), store.AppendOptions{TailHint: hint})
	assertTrips("hint without content type", "hmget", "evalsha")

	path, hint = newStream("stale")
	mustAppend(t, s, path, []byte("moved"), plain)
	rec.take()
	stale := mustAppend(t, s, path, []byte("x"), store.AppendOptions{ContentType: "text/plain", TailHint: hint})
	assertTrips("stale hint", "evalsha", "hmget", "evalsha")

	// Each variant appended one byte after a four-byte seed; the stale one
	// after a further five.
	for label, res := range map[string]store.AppendResult{"fresh": fresh, "unhinted": unhinted, "no content type": noCT} {
		if want := (store.Offset{ReadSeq: 0, ByteOffset: 5}); !res.Offset.Equal(want) {
			t.Errorf("%s: offset %v, want %v", label, res.Offset, want)
		}
	}
	if want := (store.Offset{ReadSeq: 0, ByteOffset: 10}); !stale.Offset.Equal(want) {
		t.Errorf("stale: offset %v, want %v", stale.Offset, want)
	}
	if rec.retryCount() != 1 {
		t.Errorf("RETRY replies = %d, want 1 (the stale hint only)", rec.retryCount())
	}
}

// streamState is everything observable about a stream after an append: the
// Get metadata that does not name the stream, the messages read back and the
// outcomes of both calls, plus whether any key exists at all (a vanished
// stream must stay vanished).
type streamState struct {
	GetErr, ReadErr string
	Tail            store.Offset
	Closed          bool
	ContentType     string
	Producers       map[string]*store.ProducerState
	ClosedBy        *store.ClosedByProducer
	Messages        []store.Message
	KeyCount        int64
}

func observe(t *testing.T, s *Store, path string) streamState {
	t.Helper()
	var st streamState
	meta, err := s.Get(path)
	st.GetErr = errString(err)
	if err == nil {
		st.Tail, st.Closed, st.ContentType = meta.CurrentOffset, meta.Closed, meta.ContentType
		st.Producers, st.ClosedBy = meta.Producers, meta.ClosedBy
	}
	st.Messages, _, err = s.Read(path, store.ZeroOffset)
	st.ReadErr = errString(err)
	st.KeyCount = testClient.Exists(context.Background(), metaKey(path), msgKey(path), prodKey(path)).Val()
	return st
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// hintParityCase prepares one stream state, lets the append take its hint
// from Get, then disturbs the stream behind the hint's back and appends.
type hintParityCase struct {
	name    string
	create  store.CreateOptions
	setup   func(t *testing.T, s *Store, path string) // state at the time the hint is read
	disturb func(t *testing.T, s *Store, path string) // what happens after the hint is read
	data    []byte
	opts    store.AppendOptions
	// hint overrides the Get-taken hint when set (a hint from elsewhere).
	hint *store.Offset
}

func producerOpts(ct, id string, epoch, seq int64) store.AppendOptions {
	return store.AppendOptions{ContentType: ct, ProducerId: id, ProducerEpoch: &epoch, ProducerSeq: &seq}
}

// TestAppendTailHintParity runs every scenario twice on identically prepared
// streams, once with the hint handleAppend would pass and once without, and
// requires identical (AppendResult, error) and identical stream state after.
func TestAppendTailHintParity(t *testing.T) {
	base := newTestStore(t)
	clock := store.NewFakeClock(time.Unix(1_765_000_000, 0))
	s := New(base.client, Options{Clock: clock})
	text := store.CreateOptions{ContentType: "text/plain"}
	jsonCT := store.CreateOptions{ContentType: "application/json"}
	plain := store.AppendOptions{ContentType: "text/plain"}
	asJSON := store.AppendOptions{ContentType: "application/json"}
	appendOne := func(t *testing.T, s *Store, path string) { mustAppend(t, s, path, []byte("moved"), plain) }
	appendJSON := func(t *testing.T, s *Store, path string) { mustAppend(t, s, path, []byte(`{"m":1}`), asJSON) }
	recreateAs := func(ct string) func(t *testing.T, s *Store, path string) {
		return func(t *testing.T, s *Store, path string) {
			if err := s.Delete(path); err != nil {
				t.Fatal(err)
			}
			mustCreate(t, s, path, store.CreateOptions{ContentType: ct})
		}
	}
	ttl := int64(10)
	f8 := claimFence(8, "w_8", "worker-a")
	fencedCreate := store.CreateOptions{ContentType: "application/json", WriteFence: true}
	grant := func(t *testing.T, s *Store, path string) { mustGrant(t, s, path, f8) }

	cases := []hintParityCase{
		{name: "tail moved", create: text, disturb: appendOne, data: []byte("x"), opts: plain},
		{name: "tail moved twice", create: text, setup: appendOne, disturb: func(t *testing.T, s *Store, path string) {
			appendOne(t, s, path)
			appendOne(t, s, path)
		}, data: []byte("x"), opts: plain},
		{name: "closed", create: text, disturb: func(t *testing.T, s *Store, path string) {
			if _, err := s.CloseStream(path); err != nil {
				t.Fatal(err)
			}
		}, data: []byte("x"), opts: plain},
		{
			name: "closed by producer, duplicate of the closing tuple", create: text,
			setup: func(t *testing.T, s *Store, path string) {
				o := producerOpts("text/plain", "p", 0, 0)
				o.Close = true
				mustAppend(t, s, path, []byte("last"), o)
			}, data: []byte("last"), opts: producerOpts("text/plain", "p", 0, 0),
		},
		{name: "hard-deleted", create: text, setup: appendOne, disturb: func(t *testing.T, s *Store, path string) {
			if err := s.Delete(path); err != nil {
				t.Fatal(err)
			}
		}, data: []byte("x"), opts: plain},
		{name: "soft-deleted", create: text, setup: func(t *testing.T, s *Store, path string) {
			appendOne(t, s, path)
			mustCreate(t, s, path+"/fork", store.CreateOptions{ForkedFrom: path})
		}, disturb: func(t *testing.T, s *Store, path string) {
			if err := s.Delete(path); err != nil {
				t.Fatal(err)
			}
		}, data: []byte("x"), opts: plain},
		{
			name: "expired", create: store.CreateOptions{ContentType: "text/plain", TTLSeconds: &ttl},
			setup:   appendOne,
			disturb: func(*testing.T, *Store, string) { clock.Advance(time.Duration(ttl+1) * time.Second) },
			data:    []byte("x"), opts: plain,
		},
		{
			name: "re-created with another content type, old request type", create: text,
			disturb: recreateAs("application/json"), data: []byte("x"), opts: plain,
		},
		{
			name: "re-created with another content type, new request type", create: text,
			disturb: recreateAs("application/json"), data: []byte(`[{"a":1},{"b":2}]`), opts: asJSON,
		},
		{
			name: "re-created with the same content type", create: text, setup: appendOne,
			disturb: recreateAs("text/plain"), data: []byte("x"), opts: plain,
		},
		{name: "json fresh", create: jsonCT, setup: appendJSON, data: []byte(`[{"a":1},{"b":2}]`), opts: asJSON},
		{name: "json stale", create: jsonCT, disturb: appendJSON, data: []byte(`[{"a":1},{"b":2}]`), opts: asJSON},
		{name: "json invalid body fresh", create: jsonCT, data: []byte(`{bad`), opts: asJSON},
		{name: "json invalid body stale", create: jsonCT, disturb: appendJSON, data: []byte(`{bad`), opts: asJSON},
		{name: "json invalid body on a closed stream", create: jsonCT, disturb: func(t *testing.T, s *Store, path string) {
			if _, err := s.CloseStream(path); err != nil {
				t.Fatal(err)
			}
		}, data: []byte(`{bad`), opts: asJSON},
		{name: "json empty array", create: jsonCT, data: []byte(`[]`), opts: asJSON},
		{name: "json empty array stale", create: jsonCT, disturb: appendJSON, data: []byte(`[]`), opts: asJSON},
		{name: "producer duplicate", create: text, setup: func(t *testing.T, s *Store, path string) {
			mustAppend(t, s, path, []byte("x"), producerOpts("text/plain", "p", 0, 0))
		}, data: []byte("x"), opts: producerOpts("text/plain", "p", 0, 0)},
		{name: "producer duplicate with an invalid json body", create: jsonCT, setup: func(t *testing.T, s *Store, path string) {
			mustAppend(t, s, path, []byte(`{"a":1}`), producerOpts("application/json", "p", 0, 0))
		}, data: []byte(`{bad`), opts: producerOpts("application/json", "p", 0, 0)},
		{name: "producer stale epoch", create: text, setup: func(t *testing.T, s *Store, path string) {
			mustAppend(t, s, path, []byte("x"), producerOpts("text/plain", "p", 1, 0))
		}, data: []byte("x"), opts: producerOpts("text/plain", "p", 0, 0)},
		{name: "producer seq gap", create: text, setup: func(t *testing.T, s *Store, path string) {
			mustAppend(t, s, path, []byte("x"), producerOpts("text/plain", "p", 0, 0))
		}, data: []byte("x"), opts: producerOpts("text/plain", "p", 0, 5)},
		{name: "producer new epoch not at zero", create: text, setup: func(t *testing.T, s *Store, path string) {
			mustAppend(t, s, path, []byte("x"), producerOpts("text/plain", "p", 0, 0))
		}, data: []byte("x"), opts: producerOpts("text/plain", "p", 1, 3)},
		{
			name: "producer accepted after the tail moved", create: text, disturb: appendOne,
			data: []byte("x"), opts: producerOpts("text/plain", "p", 0, 0),
		},
		{name: "stream-seq conflict", create: text, setup: func(t *testing.T, s *Store, path string) {
			mustAppend(t, s, path, []byte("x"), store.AppendOptions{ContentType: "text/plain", Seq: "0005"})
		}, data: []byte("x"), opts: store.AppendOptions{ContentType: "text/plain", Seq: "0003"}},
		{name: "stream-seq conflict raced in behind the hint", create: text, disturb: func(t *testing.T, s *Store, path string) {
			mustAppend(t, s, path, []byte("x"), store.AppendOptions{ContentType: "text/plain", Seq: "0005"})
		}, data: []byte("x"), opts: store.AppendOptions{ContentType: "text/plain", Seq: "0003"}},
		{
			name: "fenced accepted", create: fencedCreate, setup: grant,
			data: []byte(`{"v":1}`), opts: fencedOpts(f8, 8, 0),
		},
		{
			name: "fenced accepted after the tail moved", create: fencedCreate, setup: grant,
			disturb: func(t *testing.T, s *Store, path string) {
				if _, err := fencedAppend(s, path, f8, 0); err != nil {
					t.Fatal(err)
				}
			}, data: []byte(`{"v":1}`), opts: fencedOpts(f8, 8, 1),
		},
		{name: "fenced sealed", create: fencedCreate, setup: grant, disturb: func(t *testing.T, s *Store, path string) {
			if _, err := s.SealAppendFence(path, f8); err != nil {
				t.Fatal(err)
			}
		}, data: []byte(`{"v":1}`), opts: fencedOpts(f8, 8, 0)},
		{name: "fenced revoked", create: fencedCreate, setup: grant, disturb: func(t *testing.T, s *Store, path string) {
			if err := s.RevokeAppendFence(path, f8); err != nil {
				t.Fatal(err)
			}
		}, data: []byte(`{"v":1}`), opts: fencedOpts(f8, 8, 0)},
		{
			name: "fenced epoch mismatch", create: fencedCreate, setup: grant,
			data: []byte(`{"v":1}`), opts: fencedOpts(f8, 9, 0),
		},
		{
			name: "open write on a fenced stream", create: fencedCreate, setup: grant,
			data: []byte(`{"v":1}`), opts: asJSON,
		},
		{
			name: "hint from another stream", create: text, setup: appendOne,
			hint: offsetPtr(store.Offset{ReadSeq: 3, ByteOffset: 4096}), data: []byte("x"), opts: plain,
		},
		{name: "hint from another stream on a closed stream", create: text, disturb: func(t *testing.T, s *Store, path string) {
			if _, err := s.CloseStream(path); err != nil {
				t.Fatal(err)
			}
		}, hint: offsetPtr(store.Offset{ReadSeq: 3, ByteOffset: 4096}), data: []byte("x"), opts: plain},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prepare := func(name string) string {
				path := testPath("parity-" + name)
				mustCreate(t, s, path, tc.create)
				if tc.setup != nil {
					tc.setup(t, s, path)
				}
				return path
			}
			hinted, unhinted := prepare("hinted"), prepare("unhinted")
			hint := tc.hint
			if hint == nil {
				meta, err := s.Get(hinted)
				if err != nil {
					t.Fatalf("read the hint: %v", err)
				}
				hint = &meta.CurrentOffset
			}
			if tc.disturb != nil {
				tc.disturb(t, s, hinted)
				tc.disturb(t, s, unhinted)
			}

			hintedOpts := tc.opts
			hintedOpts.TailHint = hint
			hRes, hErr := s.Append(hinted, tc.data, hintedOpts)
			uRes, uErr := s.Append(unhinted, tc.data, tc.opts)

			if errString(hErr) != errString(uErr) {
				t.Errorf("error: hinted %v, unhinted %v", hErr, uErr)
			}
			if !reflect.DeepEqual(hRes, uRes) {
				t.Errorf("result:\n hinted   %+v\n unhinted %+v", hRes, uRes)
			}
			if hs, us := observe(t, s, hinted), observe(t, s, unhinted); !reflect.DeepEqual(hs, us) {
				t.Errorf("stream state:\n hinted   %+v\n unhinted %+v", hs, us)
			}
		})
	}
}

func fencedOpts(fence auth.AppendFence, epoch, seq int64) store.AppendOptions {
	o := producerOpts("application/json", "p", epoch, seq)
	o.Fence = &fence
	return o
}

// acceptedAppend is one append the tiling property saw succeed.
type acceptedAppend struct {
	end  store.Offset
	data []byte
}

// TestAppendTailHintConcurrentTiling races hinted and unhinted appenders over
// shared streams (plain, JSON, per-goroutine producers, Stream-Seq, one close)
// and checks that the accepted appends tile each stream exactly, that every
// producer's accepted sequence numbers are contiguous, and that every
// rejection is one the sequential model allows. It runs on the test Redis
// and, when REDIS_CLUSTER_ADDRS is set, on that cluster as well.
func TestAppendTailHintConcurrentTiling(t *testing.T) {
	newTestStore(t)
	t.Run("standalone", func(t *testing.T) {
		runConcurrentTiling(t, func() goredis.UniversalClient { return goredis.NewClient(testClient.Options()) })
	})
	t.Run("cluster", func(t *testing.T) {
		raw := os.Getenv("REDIS_CLUSTER_ADDRS")
		if raw == "" {
			t.Skip("REDIS_CLUSTER_ADDRS is required for Redis Cluster integration")
		}
		addrs := strings.Split(raw, ",")
		runConcurrentTiling(t, func() goredis.UniversalClient {
			return goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs})
		})
	})
}

func runConcurrentTiling(t *testing.T, newClient func() goredis.UniversalClient) {
	const (
		writers        = 8
		appendsPerEach = 40
	)
	// Two stores: hinted appenders go through one recorder, unhinted through
	// the other, so RETRY counts can be reported per half.
	open := func() (*Store, *tripRecorder) {
		client := newClient()
		if err := client.Ping(context.Background()).Err(); err != nil {
			t.Skipf("redis unreachable: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		rec := &tripRecorder{}
		client.AddHook(rec)
		return New(client, Options{}), rec
	}
	hintedStore, hintedRec := open()
	unhintedStore, unhintedRec := open()

	stamp := time.Now().UnixNano()
	path := func(name string) string { return fmt.Sprintf("/tiling/%d/%s", stamp, name) }
	plainPath, jsonPath, prodPath, seqPath, closePath := path("plain"), path("json"), path("prod"), path("seq"), path("close")
	for p, ct := range map[string]string{
		plainPath: "text/plain", jsonPath: "application/json", prodPath: "text/plain",
		seqPath: "text/plain", closePath: "text/plain",
	} {
		mustCreate(t, hintedStore, p, store.CreateOptions{ContentType: ct})
	}
	contentType := map[string]string{
		plainPath: "text/plain", jsonPath: "application/json", prodPath: "text/plain",
		seqPath: "text/plain", closePath: "text/plain",
	}

	var (
		mu        sync.Mutex
		accepted  = map[string][]acceptedAppend{}
		prodSeqs  = map[string][]int64{} // producer id -> accepted seqs in acceptance order
		rejected  = map[string]int{}
		closeDone = make(chan struct{})
	)
	record := func(p string, res store.AppendResult, data []byte, producer string, seq int64) {
		mu.Lock()
		defer mu.Unlock()
		accepted[p] = append(accepted[p], acceptedAppend{end: res.Offset, data: data})
		if producer != "" {
			prodSeqs[producer] = append(prodSeqs[producer], seq)
		}
	}
	reject := func(p string, err error) {
		mu.Lock()
		defer mu.Unlock()
		rejected[p+": "+err.Error()]++
	}

	var wg sync.WaitGroup
	for w := range writers {
		s := unhintedStore
		hinted := w%2 == 0
		if hinted {
			s = hintedStore
		}
		producer := fmt.Sprintf("p%d", w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			closeSent := false
			for i := range appendsPerEach {
				targets := []string{plainPath, jsonPath, prodPath, seqPath, closePath}
				p := targets[(w+i)%len(targets)]
				data := fmt.Appendf(nil, "w%d-%d", w, i)
				opts := store.AppendOptions{ContentType: contentType[p]}
				var seq int64
				switch p {
				case jsonPath:
					data = fmt.Appendf(nil, `{"w":%d,"i":%d}`, w, i)
				case prodPath:
					seq = int64(len(prodSeqsOf(&mu, prodSeqs, producer)))
					epoch := int64(0)
					opts.ProducerId, opts.ProducerEpoch, opts.ProducerSeq = producer, &epoch, &seq
				case seqPath:
					opts.Seq = fmt.Sprintf("%06d", time.Now().UnixNano()%1_000_000)
				case closePath:
					if w == 1 && i >= appendsPerEach/2 && !closeSent {
						opts.Close, closeSent = true, true
					}
				}
				if hinted {
					meta, err := s.Get(p)
					if err != nil {
						t.Errorf("get %s: %v", p, err)
						return
					}
					opts.TailHint = &meta.CurrentOffset
				}
				res, err := s.Append(p, data, opts)
				switch {
				case err == nil:
					if res.ProducerResult != store.ProducerResultDuplicate {
						record(p, res, data, opts.ProducerId, seq)
					}
					if opts.Close {
						close(closeDone)
					}
				case errors.Is(err, store.ErrStreamClosed) && p == closePath,
					errors.Is(err, store.ErrSequenceConflict) && p == seqPath:
					reject(p, err)
				default:
					t.Errorf("append %s (hinted=%t): unexpected error %v", p, hinted, err)
				}
			}
		}()
	}
	wg.Wait()
	select {
	case <-closeDone:
	default:
		t.Error("the closing append never succeeded")
	}

	for _, p := range []string{plainPath, jsonPath, prodPath, seqPath, closePath} {
		msgs, _, err := hintedStore.Read(p, store.ZeroOffset)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		want := accepted[p]
		sortByEnd(want)
		if len(msgs) != len(want) {
			t.Errorf("%s: %d messages read, %d appends accepted", p, len(msgs), len(want))
			continue
		}
		prev := store.ZeroOffset
		for i, m := range msgs {
			if !m.Offset.Equal(want[i].end) || !bytes.Equal(m.Data, want[i].data) {
				t.Errorf("%s[%d]: read (%v, %q), accepted (%v, %q)", p, i, m.Offset, m.Data, want[i].end, want[i].data)
			}
			if want := prev.Add(uint64(len(m.Data))); !m.Offset.Equal(want) {
				t.Errorf("%s[%d]: offset %v does not continue %v by %d bytes", p, i, m.Offset, prev, len(m.Data))
			}
			prev = m.Offset
		}
		meta, err := hintedStore.Get(p)
		if err != nil {
			t.Fatal(err)
		}
		if !meta.CurrentOffset.Equal(prev) {
			t.Errorf("%s: tail %v, last message ends at %v", p, meta.CurrentOffset, prev)
		}
		if meta.Closed != (p == closePath) {
			t.Errorf("%s: closed=%t", p, meta.Closed)
		}
	}
	for producer, seqs := range prodSeqs {
		for i, seq := range seqs {
			if seq != int64(i) {
				t.Errorf("producer %s: accepted seqs %v are not contiguous", producer, seqs)
				break
			}
		}
	}
	t.Logf("rejections: %v", rejected)
	t.Logf("RETRY replies: hinted half %d, unhinted half %d", hintedRec.retryCount(), unhintedRec.retryCount())
}

func prodSeqsOf(mu *sync.Mutex, m map[string][]int64, producer string) []int64 {
	mu.Lock()
	defer mu.Unlock()
	return m[producer]
}

func sortByEnd(a []acceptedAppend) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && less(a[j].end, a[j-1].end); j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

func less(a, b store.Offset) bool {
	if a.ReadSeq != b.ReadSeq {
		return a.ReadSeq < b.ReadSeq
	}
	return a.ByteOffset < b.ByteOffset
}
