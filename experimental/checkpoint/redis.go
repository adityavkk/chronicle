package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// ErrConflict means another image at this or a later boundary already exists,
// or publication exhausted its bounded contention retries.
var ErrConflict = errors.New("checkpoint publication conflict")

// RedisCache stores one complete image per (path, incarnation, projection).
// It is a trusted, disposable acceleration cache, not a public upload API.
// Entries expire after 24 hours without publication. Source deletion makes an
// old incarnation unreachable to Capture; TTL eventually reclaims its bytes.
// Deployments requiring immediate erasure must add source-lifecycle cleanup.
type RedisCache struct {
	Client redis.UniversalClient
}

func imageKey(path, incarnation, projection string) string {
	encoded, _ := json.Marshal([3]string{path, incarnation, projection})
	sum := sha256.Sum256(encoded)
	return "chronicle:checkpoint:prototype:" + hex.EncodeToString(sum[:])
}

// Load looks up an image in constant key-count work. nil means a cache miss.
// Callers may fall back on ErrInvalidImage, but should surface availability and
// access errors explicitly. Capture independently checks identity and integrity.
func (c RedisCache) Load(ctx context.Context, path, incarnation, projection string) (*Image, error) {
	raw, err := c.Client.Get(ctx, imageKey(path, incarnation, projection)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	image, err := decodeImage(raw)
	if err != nil {
		return nil, err
	}
	if image.Path != path || image.Incarnation != incarnation || image.Projection != projection {
		return nil, ErrInvalidImage
	}
	return &image, nil
}

// Bytewise CAS keeps the version/offset decision made in Go atomic. It is not
// a second implementation of offset ordering or any validation predicate.
// Payload and pointer are one Redis string, so publication cannot tear them.
var publish = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if (current or '') ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', 86400)
return 1
`)

// Save atomically publishes a complete image without moving an existing
// checkpoint backwards. An identical retry succeeds without rewriting. Equal
// offsets with different state are rejected, not last-writer-wins. The stream
// may advance while publishing; its tail must never replace image.Offset.
func (c RedisCache) Save(ctx context.Context, image Image) error {
	off, err := image.validate()
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(image)
	if err != nil {
		return err
	}
	if len(encoded) > MaxImageBytes {
		return fmt.Errorf("checkpoint exceeds %d bytes", MaxImageBytes)
	}
	key := imageKey(image.Path, image.Incarnation, image.Projection)
	for range 16 {
		old, err := c.Client.Get(ctx, key).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		if old != "" {
			previous, err := decodeImage(old)
			if err != nil {
				return err
			}
			previousOffset, _ := store.ParseOffset(previous.Offset)
			switch store.Compare(previousOffset, off) {
			case 0:
				if previous.SHA256 == image.SHA256 {
					return nil
				}
				return ErrConflict
			case 1:
				return ErrConflict
			}
		}
		ok, err := publish.Run(ctx, c.Client, []string{key}, old, string(encoded)).Int()
		if err != nil {
			return err
		}
		if ok == 1 {
			return nil
		}
	}
	return ErrConflict
}

func decodeImage(raw string) (Image, error) {
	var image Image
	if len(raw) > MaxImageBytes || json.Unmarshal([]byte(raw), &image) != nil {
		return Image{}, ErrInvalidImage
	}
	if _, err := image.validate(); err != nil {
		return Image{}, err
	}
	return image, nil
}
