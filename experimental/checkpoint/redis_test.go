package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
)

func TestRedisCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("requires local Redis")
	}
	rawURL := os.Getenv("CHECKPOINT_REDIS_URL")
	if rawURL == "" {
		rawURL = "redis://localhost:6379/11"
	}
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatal("invalid CHECKPOINT_REDIS_URL")
	}
	client := redis.NewClient(opts)
	defer client.Close()
	if err := client.Ping(t.Context()).Err(); err != nil {
		t.Fatal(err)
	}
	// Never flush a database. All writes are under a unique test path.
	path := fmt.Sprintf("/checkpoint-test-%d", time.Now().UnixNano())
	s := redisstore.New(client, redisstore.Options{})
	defer s.Close()
	createJSON(t, s, path)
	defer s.Delete(path)
	cache := RedisCache{Client: client}
	images := make([]Image, 0, 3)
	for _, batch := range []string{"[2,7]", "[-4,9]", "[0,3]"} {
		appendJSON(t, s, path, batch)
		_, image, _, err := Capture(t.Context(), s, path, projection(), nil)
		if err != nil {
			t.Fatal(err)
		}
		images = append(images, image)
	}
	key := imageKey(path, images[0].Incarnation, projection().Version)
	defer client.Del(t.Context(), key)
	if got, err := cache.Load(t.Context(), path, images[0].Incarnation, projection().Version); err != nil || got != nil {
		t.Fatalf("expected initial miss, got %+v, %v", got, err)
	}
	if err := cache.Save(t.Context(), images[0]); err != nil {
		t.Fatal(err)
	}
	// A new client simulates losing every process-local cache after publication.
	other := redis.NewClient(opts)
	defer other.Close()
	restarted := RedisCache{Client: other}
	loaded, err := restarted.Load(t.Context(), path, images[0].Incarnation, projection().Version)
	if err != nil || loaded == nil {
		t.Fatalf("load: %+v, %v", loaded, err)
	}
	got, _, stats, err := Capture(t.Context(), s, path, projection(), loaded)
	// [2,7,-4,9,0,3] -> 2,13,35,114,342,1029.
	if err != nil || got != (aggregate{1029, 6}) || !stats.Restored || stats.Frames != 4 {
		t.Fatalf("got %+v, %+v, %v", got, stats, err)
	}
	var wg sync.WaitGroup
	for range 8 {
		for _, image := range images {
			wg.Go(func() {
				if err := restarted.Save(t.Context(), image); err != nil && !errors.Is(err, ErrConflict) {
					t.Error(err)
				}
			})
		}
	}
	wg.Wait()
	loaded, err = cache.Load(t.Context(), path, images[0].Incarnation, projection().Version)
	if err != nil || loaded == nil || loaded.SHA256 != images[2].SHA256 {
		t.Fatalf("concurrent writers regressed latest image: %+v, %v", loaded, err)
	}
	if err := cache.Save(t.Context(), images[0]); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale publisher won: %v", err)
	}
	if err := cache.Save(t.Context(), images[2]); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	conflicting := images[2]
	conflicting.State = json.RawMessage(`{"Value":0,"Count":6}`)
	conflicting.SHA256 = conflicting.digest()
	if err := cache.Save(t.Context(), conflicting); !errors.Is(err, ErrConflict) {
		t.Fatalf("equal-offset state conflict accepted: %v", err)
	}
	if ttl := client.TTL(t.Context(), key).Val(); ttl <= 0 || ttl > 24*time.Hour {
		t.Fatalf("checkpoint has no bounded GC horizon: %v", ttl)
	}
	// A fork must fold its own prefix, never reuse the parent's newer image.
	fork := path + "/fork"
	boundary, _ := store.ParseOffset(images[0].Offset)
	if _, _, err := s.Create(fork, store.CreateOptions{ContentType: "application/json", ForkedFrom: path, ForkOffset: &boundary}); err != nil {
		t.Fatal(err)
	}
	defer s.Delete(fork)
	appendJSON(t, s, fork, "11")
	got, _, stats, err = Capture(t.Context(), s, fork, projection(), loaded)
	if err != nil || got != (aggregate{50, 3}) || stats.Restored {
		t.Fatalf("fork used parent future: %+v, %+v, %v", got, stats, err)
	}
	// Truncated/corrupt cache bytes cannot be interpreted as an empty image.
	if err := client.Set(t.Context(), key, `{"format":1`, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Load(t.Context(), path, images[0].Incarnation, projection().Version); !errors.Is(err, ErrInvalidImage) {
		t.Fatalf("corrupt cache accepted: %v", err)
	}
}
