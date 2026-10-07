package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

type snapshotBackend interface {
	store.Store
	store.ProjectionSnapshotStore
}

func snapshotBackends(t *testing.T, clock store.Clock) map[string]snapshotBackend {
	t.Helper()
	base := newTestStore(t)
	memory := store.NewMemoryStore(store.WithClock(clock))
	t.Cleanup(func() { _ = memory.Close() })
	return map[string]snapshotBackend{
		"go":  memory,
		"lua": New(base.client, Options{Clock: clock}),
	}
}

// Both implementations run the same sequence and must satisfy the independent
// expected error, creation flag, image, and non-mutation assertions. Unequal
// frame sizes distinguish exact boundaries from byte positions and later tails.
func TestSnapshotDifferentialPublication(t *testing.T) {
	for name, s := range snapshotBackends(t, store.RealClock()) {
		t.Run(name, func(t *testing.T) {
			path := testPath("snapshot-differential")
			meta, _, err := s.Create(path, store.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			inc := meta.Incarnation
			for _, body := range [][]byte{{255, 0, 1}, []byte("abcdef")} {
				if _, err := s.Append(path, body, store.AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := s.Get(path)
			if err != nil {
				t.Fatal(err)
			}
			beforeJSON, _ := json.Marshal(before)
			base := store.ProjectionSnapshot{Projection: "p-v1", Incarnation: inc, Offset: store.Offset{ByteOffset: 3}, ContentType: "application/octet-stream", Body: []byte{0, 255, '\n'}}
			first, created, err := s.PutSnapshot(t.Context(), path, base, store.SnapshotCondition{IfNoneMatch: true})
			if err != nil || !created {
				t.Fatalf("first publication = %v %v", created, err)
			}
			for _, tc := range []struct {
				name   string
				offset uint64
				body   []byte
				cond   store.SnapshotCondition
				want   error
			}{
				{"retry same bytes", 3, base.Body, store.SnapshotCondition{IfMatch: first.ETag}, nil},
				{"retry without current condition", 3, base.Body, store.SnapshotCondition{IfNoneMatch: true}, store.ErrSnapshotPrecondition},
				{"same cut different state", 3, []byte("wrong"), store.SnapshotCondition{IfMatch: first.ETag}, store.ErrSnapshotConflict},
				{"valid boundary regression", 0, base.Body, store.SnapshotCondition{IfMatch: first.ETag}, store.ErrSnapshotConflict},
				{"interior bytes", 4, base.Body, store.SnapshotCondition{IfMatch: first.ETag}, store.ErrSnapshotConflict},
				{"future", 10, base.Body, store.SnapshotCondition{IfMatch: first.ETag}, store.ErrSnapshotConflict},
				{"stale ETag", 9, []byte("new"), store.SnapshotCondition{IfMatch: `"stale"`}, store.ErrSnapshotPrecondition},
				{"absent condition", 9, []byte("new"), store.SnapshotCondition{}, store.ErrInvalidSnapshot},
			} {
				t.Run(tc.name, func(t *testing.T) {
					v := base
					v.Offset.ByteOffset, v.Body = tc.offset, tc.body
					_, made, err := s.PutSnapshot(t.Context(), path, v, tc.cond)
					if !errors.Is(err, tc.want) || made {
						t.Fatalf("publication = created:%v err:%v; want %v", made, err, tc.want)
					}
					got, err := s.GetSnapshot(t.Context(), path, base.Projection)
					if err != nil || got.ETag != first.ETag || !bytes.Equal(got.Body, base.Body) || got.Offset != base.Offset {
						t.Fatalf("rejection/retry mutated image: %+v %v", got, err)
					}
				})
			}
			otherVersion := base
			otherVersion.Projection = "p-v2"
			other, made, err := s.PutSnapshot(t.Context(), path, otherVersion, store.SnapshotCondition{IfNoneMatch: true})
			if err != nil || !made || other.ETag == first.ETag {
				t.Fatalf("version isolation failed: %+v %v %v", other, made, err)
			}
			base.Offset.ByteOffset, base.Body = 9, []byte("later")
			later, made, err := s.PutSnapshot(t.Context(), path, base, store.SnapshotCondition{IfMatch: first.ETag})
			if err != nil || made || later.ETag == first.ETag {
				t.Fatalf("replacement = %+v %v %v", later, made, err)
			}
			base.Body[0], later.Body[0] = 'X', 'Y'
			got, err := s.GetSnapshot(t.Context(), path, base.Projection)
			if err != nil || string(got.Body) != "later" || got.Offset.ByteOffset != 9 {
				t.Fatalf("returned/input byte slices alias stored image: %+v %v", got, err)
			}
			after, err := s.Get(path)
			if err != nil {
				t.Fatal(err)
			}
			afterJSON, _ := json.Marshal(after)
			if !bytes.Equal(beforeJSON, afterJSON) {
				t.Fatalf("snapshot publication mutated source metadata/producers: %s -> %s", beforeJSON, afterJSON)
			}
		})
	}
}

func TestSnapshotConcurrentPublishers(t *testing.T) {
	for name, s := range snapshotBackends(t, store.RealClock()) {
		t.Run(name, func(t *testing.T) {
			path := testPath("snapshot-cas")
			meta, _, err := s.Create(path, store.CreateOptions{InitialData: []byte("abc")})
			if err != nil {
				t.Fatal(err)
			}
			image := store.ProjectionSnapshot{Projection: "p", Incarnation: meta.Incarnation, Offset: meta.CurrentOffset, ContentType: "text/plain", Body: []byte("first")}
			first, _, err := s.PutSnapshot(t.Context(), path, image, store.SnapshotCondition{IfNoneMatch: true})
			if err != nil {
				t.Fatal(err)
			}
			appendResult, err := s.Append(path, []byte("defghi"), store.AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			image.Offset = appendResult.Offset
			subscriber := s.(store.NotificationSubscriber)
			sub, err := subscriber.SubscribeNotifications(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			const writers = 12
			start := make(chan struct{})
			results := make(chan error, writers)
			winners := make(chan store.ProjectionSnapshot, writers)
			var wg sync.WaitGroup
			for i := range writers {
				wg.Go(func() {
					<-start
					v := image
					v.Body = []byte(fmt.Sprintf("writer-%d", i))
					got, _, err := s.PutSnapshot(t.Context(), path, v, store.SnapshotCondition{IfMatch: first.ETag})
					if err == nil {
						winners <- got
					}
					results <- err
				})
			}
			close(start)
			wg.Wait()
			close(results)
			successes := 0
			for err := range results {
				if err == nil {
					successes++
				} else if !errors.Is(err, store.ErrSnapshotPrecondition) {
					t.Fatalf("losing publisher error = %v", err)
				}
			}
			if successes != 1 {
				t.Fatalf("CAS successes = %d, want exactly one", successes)
			}
			winner := <-winners
			got, err := s.GetSnapshot(t.Context(), path, "p")
			if err != nil || !bytes.Equal(got.Body, winner.Body) || got.ETag != winner.ETag || got.Offset != appendResult.Offset {
				t.Fatalf("torn or lost publication: %+v %v", got, err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			if _, err := sub.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("snapshot woke source subscribers: %v", err)
			}
		})
	}
}

func TestSnapshotDifferentialLifecycle(t *testing.T) {
	clock := store.NewFakeClock(time.Now())
	for name, s := range snapshotBackends(t, clock) {
		for _, mode := range []string{"delete", "soft-delete", "expiry", "expiry-with-fork"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				path := testPath("snapshot-lifecycle")
				ttl := int64(3)
				meta, _, err := s.Create(path, store.CreateOptions{TTLSeconds: &ttl, InitialData: []byte("abc")})
				if err != nil {
					t.Fatal(err)
				}
				image := store.ProjectionSnapshot{Projection: "p", Incarnation: meta.Incarnation, Offset: meta.CurrentOffset, ContentType: "text/plain", Body: []byte("image")}
				if _, _, err := s.PutSnapshot(t.Context(), path, image, store.SnapshotCondition{IfNoneMatch: true}); err != nil {
					t.Fatal(err)
				}
				if name == "lua" {
					pttl := testClient.PTTL(t.Context(), snapshotKey(path)).Val()
					if pttl <= 0 || pttl > 63*time.Second {
						t.Fatalf("image backstop = %v", pttl)
					}
				}
				if mode == "soft-delete" || mode == "expiry-with-fork" {
					if _, _, err := s.Create(path+"/fork", store.CreateOptions{ForkedFrom: path}); err != nil {
						t.Fatal(err)
					}
					if name == "lua" && testClient.PTTL(t.Context(), snapshotKey(path)).Val() >= 0 {
						t.Fatal("taking a fork reference did not persist the snapshot backstop")
					}
				}
				want := store.ErrStreamNotFound
				if mode == "delete" || mode == "soft-delete" {
					if err := s.Delete(path); err != nil {
						t.Fatal(err)
					}
					if mode == "soft-delete" {
						want = store.ErrStreamSoftDeleted
					}
				} else {
					clock.Advance(2 * time.Second)
					// Neither load nor re-publication counts as stream activity.
					got, err := s.GetSnapshot(t.Context(), path, "p")
					if err != nil {
						t.Fatal(err)
					}
					if _, _, err := s.PutSnapshot(t.Context(), path, got, store.SnapshotCondition{IfMatch: got.ETag}); err != nil {
						t.Fatal(err)
					}
					clock.Advance(2 * time.Second)
				}
				if _, err := s.GetSnapshot(t.Context(), path, "p"); !errors.Is(err, want) {
					t.Fatalf("lifecycle GET = %v, want %v", err, want)
				}
				if name == "lua" && testClient.Exists(t.Context(), snapshotKey(path)).Val() != 0 {
					t.Fatal("deleted/expired snapshot bytes survived lifecycle cleanup")
				}
				if mode == "delete" || mode == "expiry" {
					if _, _, err := s.Create(path, store.CreateOptions{InitialData: []byte("different")}); err != nil {
						t.Fatal(err)
					}
					if _, err := s.GetSnapshot(t.Context(), path, "p"); !errors.Is(err, store.ErrSnapshotNotFound) {
						t.Fatalf("recreated source inherited image: %v", err)
					}
					if _, _, err := s.PutSnapshot(t.Context(), path, image, store.SnapshotCondition{IfNoneMatch: true}); !errors.Is(err, store.ErrReadSnapshotChanged) {
						t.Fatalf("stale publisher crossed source lifetime: %v", err)
					}
				}
			})
		}
	}
}

func TestSnapshotRedisDetectsCorruptImage(t *testing.T) {
	s := newTestStore(t)
	path := testPath("corrupt-snapshot")
	meta := mustCreate(t, s, path, store.CreateOptions{InitialData: []byte("abc")})
	image, _, err := s.PutSnapshot(t.Context(), path, store.ProjectionSnapshot{
		Projection: "p", Incarnation: meta.Incarnation,
		Offset: meta.CurrentOffset, ContentType: "text/plain", Body: []byte("original"),
	}, store.SnapshotCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	for i, mutate := range []func(*store.ProjectionSnapshot){
		func(v *store.ProjectionSnapshot) { v.Body = []byte("corrupt") },
		func(v *store.ProjectionSnapshot) { v.Offset.ByteOffset-- },
		func(v *store.ProjectionSnapshot) { v.Incarnation = "another-source" },
	} {
		bad := image
		mutate(&bad)
		if err := testClient.HSet(t.Context(), snapshotKey(path), "d:p", snapshotEnvelope(image), "b:p", image.Body).Err(); err != nil {
			t.Fatal(err)
		}
		values := []any{"d:p", snapshotEnvelope(bad)}
		if i == 0 {
			values = []any{"b:p", bad.Body}
		}
		if err := testClient.HSet(t.Context(), snapshotKey(path), values...).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetSnapshot(t.Context(), path, "p"); err == nil {
			t.Fatal("corrupt image served with a newly computed digest")
		}
	}
}

func TestSnapshotDifferentialQuota(t *testing.T) {
	for name, s := range snapshotBackends(t, store.RealClock()) {
		t.Run(name, func(t *testing.T) {
			path := testPath("snapshot-quota")
			meta, _, err := s.Create(path, store.CreateOptions{InitialData: []byte("a")})
			if err != nil {
				t.Fatal(err)
			}
			image := store.ProjectionSnapshot{Incarnation: meta.Incarnation, Offset: meta.CurrentOffset, ContentType: "application/octet-stream", Body: bytes.Repeat([]byte{0, 255}, store.MaxSnapshotBytes/2)}
			publish := func(v store.ProjectionSnapshot, cond store.SnapshotCondition, want error) store.ProjectionSnapshot {
				t.Helper()
				got, _, err := s.PutSnapshot(t.Context(), path, v, cond)
				if !errors.Is(err, want) {
					t.Fatalf("projection %q publication: %v, want %v", v.Projection, err, want)
				}
				return got
			}
			saved := make([]store.ProjectionSnapshot, 0, 4)
			// These names must not collide with internal fields or each other.
			for _, projection := range []string{"p", "d:p", "b:p", "__bytes"} {
				if _, err := s.GetSnapshot(t.Context(), path, projection); !errors.Is(err, store.ErrSnapshotNotFound) {
					t.Fatalf("missing %q: %v", projection, err)
				}
				image.Projection = projection
				saved = append(saved, publish(image, store.SnapshotCondition{IfNoneMatch: true}, nil))
			}
			// Exactly 4 MiB is accepted, including same-image retries at capacity.
			publish(saved[0], store.SnapshotCondition{IfMatch: saved[0].ETag}, nil)
			image.Projection, image.Body = "overflow", []byte("x")
			publish(image, store.SnapshotCondition{IfNoneMatch: true}, store.ErrSnapshotQuota)
			if _, err := s.GetSnapshot(t.Context(), path, "overflow"); !errors.Is(err, store.ErrSnapshotNotFound) {
				t.Fatalf("quota rejection created image: %v", err)
			}
			// At a later cut shrink a body by one byte; a failed CAS must not free it.
			next, err := s.Append(path, []byte("bc"), store.AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			shrunk := saved[0]
			shrunk.Offset, shrunk.Body = next.Offset, shrunk.Body[:len(shrunk.Body)-1]
			publish(shrunk, store.SnapshotCondition{IfMatch: `"stale"`}, store.ErrSnapshotPrecondition)
			publish(image, store.SnapshotCondition{IfNoneMatch: true}, store.ErrSnapshotQuota)
			shrunk = publish(shrunk, store.SnapshotCondition{IfMatch: saved[0].ETag}, nil)
			overflow := publish(image, store.SnapshotCondition{IfNoneMatch: true}, nil)
			if err := s.DeleteSnapshot(t.Context(), path, saved[0].Projection, meta.Incarnation, saved[0].ETag); !errors.Is(err, store.ErrSnapshotPrecondition) {
				t.Fatalf("retired a replacement with stale ETag: %v", err)
			}
			if err := s.DeleteSnapshot(t.Context(), path, overflow.Projection, "old-incarnation", overflow.ETag); !errors.Is(err, store.ErrReadSnapshotChanged) {
				t.Fatalf("wrong retirement incarnation: %v", err)
			}
			if err := s.DeleteSnapshot(t.Context(), path, overflow.Projection, meta.Incarnation, overflow.ETag); err != nil {
				t.Fatal(err)
			}
			next, err = s.Append(path, []byte("d"), store.AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			grown := saved[0]
			grown.Offset = next.Offset
			publish(grown, store.SnapshotCondition{IfMatch: shrunk.ETag}, nil)
			// Empty bodies consume version quota even at the byte ceiling.
			for i := 4; i < store.MaxSnapshotVersions; i++ {
				image.Projection, image.Body = fmt.Sprintf("empty-%d", i), nil
				publish(image, store.SnapshotCondition{IfNoneMatch: true}, nil)
			}
			image.Projection = "ninth"
			publish(image, store.SnapshotCondition{IfNoneMatch: true}, store.ErrSnapshotQuota)
			if err := s.Delete(path); err != nil {
				t.Fatal(err)
			}
			meta, _, err = s.Create(path, store.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			publish(image, store.SnapshotCondition{IfNoneMatch: true}, store.ErrReadSnapshotChanged)
			image.Incarnation, image.Offset = meta.Incarnation, meta.CurrentOffset
			publish(image, store.SnapshotCondition{IfNoneMatch: true}, nil)
		})
	}
}

func TestSnapshotConcurrentQuotaAndRetirement(t *testing.T) {
	for name, s := range snapshotBackends(t, store.RealClock()) {
		for _, quota := range []string{"count", "bytes"} {
			t.Run(name+"/"+quota, func(t *testing.T) {
				path := testPath("snapshot-quota-race")
				meta, _, err := s.Create(path, store.CreateOptions{InitialData: []byte("a")})
				if err != nil {
					t.Fatal(err)
				}
				n, size := store.MaxSnapshotVersions-1, 1
				if quota == "bytes" {
					n, size = 3, store.MaxSnapshotBytes
				}
				image := store.ProjectionSnapshot{Incarnation: meta.Incarnation, Offset: meta.CurrentOffset, ContentType: "text/plain", Body: bytes.Repeat([]byte("x"), size)}
				for i := range n {
					image.Projection = fmt.Sprintf("seed-%d", i)
					if _, _, err := s.PutSnapshot(t.Context(), path, image, store.SnapshotCondition{IfNoneMatch: true}); err != nil {
						t.Fatal(err)
					}
				}
				var wg sync.WaitGroup
				start := make(chan struct{})
				results := make(chan error, 12)
				winners := make(chan store.ProjectionSnapshot, 12)
				for i := range 12 {
					wg.Go(func() {
						<-start
						v := image
						v.Projection = fmt.Sprintf("racer-%d", i)
						got, _, err := s.PutSnapshot(t.Context(), path, v, store.SnapshotCondition{IfNoneMatch: true})
						if err == nil {
							winners <- got
						}
						results <- err
					})
				}
				close(start)
				wg.Wait()
				close(results)
				for err := range results {
					if err != nil && !errors.Is(err, store.ErrSnapshotQuota) {
						t.Fatal(err)
					}
				}
				if len(winners) != 1 {
					t.Fatalf("quota race had %d successes, want 1", len(winners))
				}
				winner := <-winners
				next, err := s.Append(path, []byte("bc"), store.AppendOptions{})
				if err != nil {
					t.Fatal(err)
				}
				var putErr, deleteErr error
				wg.Go(func() {
					v := winner
					v.Offset = next.Offset
					_, _, putErr = s.PutSnapshot(t.Context(), path, v, store.SnapshotCondition{IfMatch: winner.ETag})
				})
				wg.Go(func() {
					deleteErr = s.DeleteSnapshot(t.Context(), path, winner.Projection, meta.Incarnation, winner.ETag)
				})
				wg.Wait()
				if (putErr != nil || !errors.Is(deleteErr, store.ErrSnapshotPrecondition)) &&
					(deleteErr != nil || !errors.Is(putErr, store.ErrSnapshotPrecondition)) {
					t.Fatalf("retire/replace race: put=%v delete=%v", putErr, deleteErr)
				}
				got, err := s.GetSnapshot(t.Context(), path, winner.Projection)
				if putErr == nil && (err != nil || got.Offset != next.Offset || !bytes.Equal(got.Body, winner.Body)) {
					t.Fatalf("winning replacement not intact: %+v %v", got, err)
				}
				if deleteErr == nil && !errors.Is(err, store.ErrSnapshotNotFound) {
					t.Fatalf("winning retirement not applied: %v", err)
				}
			})
		}
	}
}
