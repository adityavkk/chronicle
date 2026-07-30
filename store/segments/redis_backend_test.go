package segments

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestRedisCandidateKeysShareStreamHashTag(t *testing.T) {
	path := "/hostile/{path}%}"
	keys := RedisKeyLayout(path)
	var tag string
	for _, key := range keys {
		start := strings.IndexByte(key, '{')
		end := strings.IndexByte(key[start+1:], '}')
		if start < 0 || end < 0 {
			t.Fatalf("key lacks hash tag: %q", key)
		}
		got := key[start+1 : start+1+end]
		if tag == "" {
			tag = got
		} else if got != tag {
			t.Fatalf("cross-slot key %q: tag=%q want=%q", key, got, tag)
		}
	}
	if strings.Contains(tag, "{") || strings.Contains(tag, "}") {
		t.Fatalf("path escaped out of hash tag: %q", tag)
	}
}

func TestRedisBackendPublishReadAndCAS(t *testing.T) {
	if testing.Short() {
		t.Skip("Redis integration test")
	}
	rawURL := os.Getenv("REDIS_URL")
	if rawURL == "" {
		rawURL = "redis://localhost:6379/15"
	}
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opts)
	defer client.Close() //nolint:errcheck // test cleanup
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis not reachable at %s: %v", rawURL, err)
	}
	path := fmt.Sprintf("/segments-redis-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, scanErr := client.Scan(ctx, cursor, segmentPrefix(path)+"*", 100).Result()
			if scanErr != nil {
				return
			}
			if len(keys) > 0 {
				_ = client.Del(ctx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	})

	backend := NewRedisBackend(client, nil)
	encoded, err := Encode(store.ZeroOffset, []store.Message{
		{Data: []byte("one"), Offset: store.Offset{ByteOffset: 3}},
		{Data: []byte("two"), Offset: store.Offset{ByteOffset: 6}},
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := backend.Put(ctx, path, 1, encoded)
	if err != nil {
		t.Fatal(err)
	}
	manifest := &Manifest{
		Version:          ManifestVersion,
		Mode:             ModeRedisChunks,
		Path:             path,
		Incarnation:      "test-incarnation",
		ContentType:      "application/octet-stream",
		Generation:       1,
		State:            StateServing,
		SealedThrough:    encoded.EndInclusive.String(),
		Segments:         []SegmentRef{ref},
		PublishedAtUnixN: time.Now().UnixNano(),
	}
	token, err := backend.Publish(ctx, path, "", manifest)
	if err != nil {
		t.Fatal(err)
	}
	loaded, loadedToken, err := backend.Load(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if loadedToken != token || loaded.Generation != 1 || loaded.SealedThrough != manifest.SealedThrough {
		t.Fatalf("Load = (%+v,%q), want token %q", loaded, loadedToken, token)
	}
	data, index, err := backend.Read(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := DecodeAfter(ref, data, index, store.ZeroOffset)
	if err != nil || len(messages) != 2 || string(messages[1].Data) != "two" {
		t.Fatalf("DecodeAfter = %#v err=%v", messages, err)
	}
	blockLength := min(ref.BlockBytes, ref.DataBytes)
	block, err := backend.ReadDataRange(ctx, ref, 0, blockLength)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(block)) != blockLength || byteChecksum(block) != ref.BlockChecksums[0] {
		t.Fatalf("authenticated Redis range = %d bytes", len(block))
	}
	if _, err := backend.Publish(ctx, path, "stale-token", manifest); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale CAS error = %v", err)
	}
}

func TestRedisReadRejectsDeleteRecreateBetweenManifestAndTail(t *testing.T) {
	client, ctx := redisSegmentTestClient(t)
	path := fmt.Sprintf("/segments-read-incarnation-%d", time.Now().UnixNano())
	cleanupRedisSegmentPath(t, client, ctx, path)

	primary := store.NewMemoryStore(store.WithClock(store.NewFakeClock(time.Unix(42, 0))))
	interleaved := &readInterleavingStore{Store: primary}
	seg, err := New(interleaved, Options{
		Backend:      NewRedisBackend(client, nil),
		TargetBytes:  64,
		IndexStride:  2,
		InitialState: StateServing,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	mustSegmentAppend(t, seg, path, []byte("old-prefix"), store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	interleaved.arm(func() {
		if err := primary.Delete(path); err != nil {
			t.Fatalf("delete old incarnation: %v", err)
		}
		if _, created, err := primary.Create(path, store.CreateOptions{ContentType: "application/octet-stream"}); err != nil || !created {
			t.Fatalf("create new incarnation: created=%v err=%v", created, err)
		}
		if _, err := primary.Append(path, []byte("new-incarnation-payload"), store.AppendOptions{}); err != nil {
			t.Fatalf("append new incarnation: %v", err)
		}
	})

	got, upToDate, err := seg.Read(path, store.ZeroOffset)
	if err != nil || !upToDate {
		t.Fatalf("Read: upToDate=%v err=%v", upToDate, err)
	}
	assertMessages(t, got, []byte("new-incarnation-payload"))
}

func TestRedisGCKeepsUnpublishedObjectsRacingWithPublish(t *testing.T) {
	client, ctx := redisSegmentTestClient(t)
	path := fmt.Sprintf("/segments-publisher-gc-%d", time.Now().UnixNano())
	cleanupRedisSegmentPath(t, client, ctx, path)

	primary := store.NewMemoryStore()
	writerBackend := NewRedisBackend(client, nil)
	blocking := &blockingPublishBackend{
		Backend:    writerBackend,
		generation: 2,
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	writer, err := New(primary, Options{
		Backend:      blocking,
		TargetBytes:  64,
		IndexStride:  2,
		InitialState: StateServing,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustSegmentCreate(t, writer, path, "application/octet-stream")
	mustSegmentAppend(t, writer, path, []byte("one"), store.AppendOptions{})
	if _, err := writer.Seal(path); err != nil {
		t.Fatal(err)
	}
	mustSegmentAppend(t, writer, path, []byte("two"), store.AppendOptions{})

	sealResult := make(chan struct {
		manifest *Manifest
		err      error
	}, 1)
	go func() {
		manifest, sealErr := writer.Seal(path)
		sealResult <- struct {
			manifest *Manifest
			err      error
		}{manifest: manifest, err: sealErr}
	}()
	<-blocking.entered

	collector, err := New(primary, Options{
		Backend:      NewRedisBackend(client, nil),
		InitialState: StateServing,
	}, nil)
	if err != nil {
		close(blocking.release)
		t.Fatal(err)
	}
	gcResult, gcErr := collector.GC(path, GCRetention{
		KeepGenerations: 1,
		Now:             time.Now().Add(time.Hour),
	})
	close(blocking.release)
	sealed := <-sealResult
	if gcErr != nil {
		t.Fatal(gcErr)
	}
	if sealed.err != nil {
		t.Fatal(sealed.err)
	}
	if gcResult.SegmentsDeleted != 0 || gcResult.SegmentsDeferred < 2 {
		t.Fatalf("unsafe Redis GC result during staged publish: %+v", gcResult)
	}
	for _, ref := range sealed.manifest.Segments {
		if _, _, err := writerBackend.Read(ctx, ref); err != nil {
			t.Fatalf("published generation %d references deleted Redis segment %q: %v", sealed.manifest.Generation, ref.ID, err)
		}
	}
}

func TestRedisShadowRepairRebuildsCorruptGeneration(t *testing.T) {
	client, ctx := redisSegmentTestClient(t)
	path := fmt.Sprintf("/segments-repair-%d", time.Now().UnixNano())
	cleanupRedisSegmentPath(t, client, ctx, path)

	primary := store.NewMemoryStore()
	backend := NewRedisBackend(client, nil)
	seg, err := New(primary, Options{
		Backend:      backend,
		TargetBytes:  64,
		IndexStride:  2,
		InitialState: StateShadow,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	mustSegmentAppend(t, seg, path, []byte("verified"), store.AppendOptions{})
	manifest, err := seg.Seal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, manifest.Segments[0].DataKey, "corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.Transition(path, StateServing); err == nil {
		t.Fatal("Redis shadow promotion accepted corrupt immutable bytes")
	}
	repaired, err := seg.Repair(path)
	if err != nil {
		t.Fatal(err)
	}
	if repaired.Generation != manifest.Generation+1 ||
		repaired.Segments[0].DataKey == manifest.Segments[0].DataKey {
		t.Fatalf("repair did not publish generation-qualified replacement: old=%+v new=%+v", manifest, repaired)
	}
	if _, err := seg.Transition(path, StateServing); err != nil {
		t.Fatal(err)
	}
	got, _, err := seg.Read(path, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, got, []byte("verified"))
}

func redisSegmentTestClient(t *testing.T) (*redis.Client, context.Context) {
	t.Helper()
	if testing.Short() {
		t.Skip("Redis integration test")
	}
	rawURL := os.Getenv("REDIS_URL")
	if rawURL == "" {
		rawURL = "redis://localhost:6379/15"
	}
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis not reachable at %s: %v", rawURL, err)
	}
	return client, ctx
}

func cleanupRedisSegmentPath(t *testing.T, client *redis.Client, ctx context.Context, path string) {
	t.Helper()
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, segmentPrefix(path)+"*", 100).Result()
			if err != nil {
				return
			}
			if len(keys) > 0 {
				_ = client.Del(ctx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	})
}
