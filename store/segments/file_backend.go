package segments

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type currentPointer struct {
	Token      string `json:"token"`
	Generation uint64 `json:"generation"`
	Tombstone  bool   `json:"tombstone,omitempty"`
}

// FileBackend implements both the local-file candidate and a deterministic
// filesystem object-store emulator with a bounded local cache. The object mode
// deliberately separates origin and cache directories so restart, stale-cache,
// and interrupted-upload tests exercise the same visibility rules as a remote
// object store without paid infrastructure.
type FileBackend struct {
	mode        Mode
	root        string
	cacheRoot   string
	cacheBytes  int64
	cacheAccess map[string]time.Time
	faults      FaultInjector

	mu    sync.Mutex
	stats FileBackendStats
}

// FileBackendStats exposes backend evidence to tests and benchmarks.
type FileBackendStats struct {
	OriginReads    uint64 `json:"origin_reads"`
	OriginBytes    uint64 `json:"origin_bytes"`
	CacheHits      uint64 `json:"cache_hits"`
	CacheMisses    uint64 `json:"cache_misses"`
	CacheEvictions uint64 `json:"cache_evictions"`
	BytesRead      uint64 `json:"bytes_read"`
	BytesWritten   uint64 `json:"bytes_written"`
}

// NewFileBackend constructs local-files or object-cache storage.
func NewFileBackend(mode Mode, root string, cacheBytes int64, faults FaultInjector) (*FileBackend, error) {
	if mode != ModeLocalFiles && mode != ModeObjectCache {
		return nil, fmt.Errorf("file backend does not implement mode %q", mode)
	}
	if root == "" {
		return nil, errors.New("segment directory is required")
	}
	if cacheBytes <= 0 {
		cacheBytes = 256 << 20
	}
	b := &FileBackend{
		mode:        mode,
		root:        root,
		cacheBytes:  cacheBytes,
		cacheAccess: map[string]time.Time{},
		faults:      faults,
	}
	if mode == ModeObjectCache {
		b.root = filepath.Join(root, "object-origin")
		b.cacheRoot = filepath.Join(root, "object-cache")
	}
	if err := os.MkdirAll(b.root, 0o750); err != nil {
		return nil, err
	}
	if b.cacheRoot != "" {
		if err := os.MkdirAll(b.cacheRoot, 0o750); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// Mode returns the candidate storage mode implemented by this backend.
func (b *FileBackend) Mode() Mode { return b.mode }

func streamID(path string) string {
	sum := sha256.Sum256([]byte(path))
	return hex.EncodeToString(sum[:])
}

func (b *FileBackend) streamDir(path string) string {
	return filepath.Join(b.root, "streams", streamID(path))
}

func (b *FileBackend) cacheDir(path string) string {
	return filepath.Join(b.cacheRoot, streamID(path))
}

// Load returns the manifest referenced by the current durable pointer.
func (b *FileBackend) Load(_ context.Context, path string) (*Manifest, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ptr, err := b.readPointer(path)
	if err != nil {
		return nil, "", err
	}
	raw, err := os.ReadFile(filepath.Join(b.streamDir(path), "manifests", ptr.Token+".json"))
	if err != nil {
		return nil, "", fmt.Errorf("%w: current manifest: %w", ErrCorrupt, err)
	}
	if manifestDigest(raw) != ptr.Token {
		return nil, "", fmt.Errorf("%w: manifest digest", ErrChecksum)
	}
	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, "", fmt.Errorf("%w: manifest json: %w", ErrCorrupt, err)
	}
	if manifest.Generation != ptr.Generation {
		return nil, "", fmt.Errorf("%w: manifest generation", ErrCorrupt)
	}
	return &manifest, ptr.Token, nil
}

func (b *FileBackend) readPointer(path string) (currentPointer, error) {
	raw, err := os.ReadFile(filepath.Join(b.streamDir(path), "CURRENT"))
	if errors.Is(err, fs.ErrNotExist) {
		return currentPointer{}, ErrNoManifest
	}
	if err != nil {
		return currentPointer{}, err
	}
	var ptr currentPointer
	if err := json.Unmarshal(raw, &ptr); err != nil {
		return currentPointer{}, fmt.Errorf("%w: current pointer: %w", ErrCorrupt, err)
	}
	if ptr.Tombstone || ptr.Token == "" {
		return currentPointer{}, ErrNoManifest
	}
	return ptr, nil
}

// Put durably stores one checksum-addressed segment and sparse index.
func (b *FileBackend) Put(ctx context.Context, path string, generation uint64, encoded EncodedSegment) (SegmentRef, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return SegmentRef{}, err
	}
	if err := hit(b.faults, FaultChecksum); err != nil {
		return SegmentRef{}, err
	}
	if segmentChecksum(encoded.Data, encoded.Index) != encoded.Checksum {
		return SegmentRef{}, ErrChecksum
	}
	if b.mode == ModeObjectCache {
		if err := hit(b.faults, FaultUpload); err != nil {
			return SegmentRef{}, err
		}
	}
	dir := filepath.Join(b.streamDir(path), "segments")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return SegmentRef{}, err
	}
	stem := fmt.Sprintf("%020d-%s", generation, encoded.Checksum)
	dataKey := filepath.Join("segments", stem+".data")
	indexKey := filepath.Join("segments", stem+".index")
	if err := b.writeImmutable(filepath.Join(b.streamDir(path), dataKey), encoded.Data); err != nil {
		return SegmentRef{}, err
	}
	if err := b.writeImmutable(filepath.Join(b.streamDir(path), indexKey), encoded.Index); err != nil {
		return SegmentRef{}, err
	}
	b.stats.BytesWritten += uint64(len(encoded.Data) + len(encoded.Index))
	ref := SegmentRef{
		ID:             encoded.Checksum,
		DataKey:        dataKey,
		IndexKey:       indexKey,
		StartExclusive: encoded.StartExclusive.String(),
		EndInclusive:   encoded.EndInclusive.String(),
		Records:        encoded.Records,
		DataBytes:      int64(len(encoded.Data)),
		IndexBytes:     int64(len(encoded.Index)),
		IndexStride:    encoded.IndexStride,
		BlockBytes:     encoded.BlockBytes,
		BlockChecksums: append([]string(nil), encoded.BlockChecksums...),
		IndexChecksum:  encoded.IndexChecksum,
		IndexEntries:   append([]IndexEntry(nil), encoded.IndexEntries...),
		Checksum:       encoded.Checksum,
	}
	qualifyRef(path, &ref)
	return ref, nil
}

func (b *FileBackend) writeImmutable(name string, data []byte) error {
	if existing, err := os.ReadFile(name); err == nil {
		if string(existing) != string(data) {
			return fmt.Errorf("%w: immutable object collision at %s", ErrChecksum, name)
		}
		// A prior attempt may have completed rename but failed the directory
		// fsync. Re-establish the durability boundary before a retry can make
		// this object reachable from a manifest.
		return syncDir(filepath.Dir(name))
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := hit(b.faults, FaultCreate); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(name), 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(name), ".stage-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if err := hit(b.faults, FaultWrite); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := hit(b.faults, FaultSync); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := hit(b.faults, FaultRename); err != nil {
		return err
	}
	if err := os.Rename(tmp, name); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(name)); err != nil {
		return err
	}
	ok = true
	return nil
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close() //nolint:errcheck // the durability result is from Sync
	return d.Sync()
}

func manifestBytes(manifest *Manifest) ([]byte, string, error) {
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, "", err
	}
	return raw, manifestDigest(raw), nil
}

func manifestDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Publish durably advances CURRENT when expectedToken still matches.
func (b *FileBackend) Publish(_ context.Context, path, expectedToken string, manifest *Manifest) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	unlock, err := b.lockStream(path)
	if err != nil {
		return "", err
	}
	defer unlock()
	current := ""
	ptr, err := b.readPointer(path)
	if err == nil {
		current = ptr.Token
	} else if !errors.Is(err, ErrNoManifest) {
		return "", err
	}
	if current != expectedToken {
		return "", ErrConflict
	}
	if err := hit(b.faults, FaultManifest); err != nil {
		return "", err
	}
	raw, token, err := manifestBytes(manifest)
	if err != nil {
		return "", err
	}
	manifestPath := filepath.Join(b.streamDir(path), "manifests", token+".json")
	if err := b.writeImmutable(manifestPath, raw); err != nil {
		return "", err
	}
	pointerRaw, err := json.Marshal(currentPointer{Token: token, Generation: manifest.Generation})
	if err != nil {
		return "", err
	}
	if err := b.writeReplace(filepath.Join(b.streamDir(path), "CURRENT"), pointerRaw); err != nil {
		return "", err
	}
	return token, nil
}

func (b *FileBackend) lockStream(path string) (func(), error) {
	dir := b.streamDir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
		_ = f.Close()
	}, nil
}

func (b *FileBackend) writeReplace(name string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(name), 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(name), ".pointer-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, name); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(name)); err != nil {
		return err
	}
	ok = true
	return nil
}

// Read returns and verifies the immutable data and index for ref.
func (b *FileBackend) Read(ctx context.Context, ref SegmentRef) ([]byte, []byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := hit(b.faults, FaultChecksum); err != nil {
		return nil, nil, err
	}
	if b.mode == ModeObjectCache {
		return b.readCached(ref)
	}
	pathID := pathIDFromKey(ref.DataKey)
	if pathID == "" {
		// Refs are relative to their stream directory and intentionally do not
		// embed the path. Manager calls Read through refs loaded per path, so
		// FileBackend resolves the owning directory recorded in DataKey below.
		return nil, nil, fmt.Errorf("%w: local ref lacks stream id", ErrCorrupt)
	}
	return b.readPair(filepath.Join(b.root, pathID), ref)
}

// ReadDataRange returns one independently authenticated block. Object-cache
// mode caches blocks rather than materializing the complete origin object.
func (b *FileBackend) ReadDataRange(
	ctx context.Context,
	ref SegmentRef,
	start, length int64,
) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := hit(b.faults, FaultChecksum); err != nil {
		return nil, err
	}
	block, err := validateDataRange(ref, start, length)
	if err != nil {
		return nil, err
	}
	if b.mode == ModeObjectCache {
		return b.readCachedBlock(ctx, ref, block, start, length)
	}
	pathID := pathIDFromKey(ref.DataKey)
	if pathID == "" {
		return nil, fmt.Errorf("%w: local ref lacks stream id", ErrCorrupt)
	}
	data, err := readFileRange(ctx, filepath.Join(b.root, pathID, trimStreamPrefix(ref.DataKey)), start, length)
	if err != nil {
		return nil, err
	}
	if byteChecksum(data) != ref.BlockChecksums[block] {
		return nil, ErrChecksum
	}
	b.stats.BytesRead += uint64(len(data))
	return data, nil
}

func readFileRange(ctx context.Context, name string, start, length int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("%w: open immutable range: %v", ErrCorrupt, err)
	}
	defer f.Close() //nolint:errcheck // the read result is authoritative
	data := make([]byte, length)
	n, err := f.ReadAt(data, start)
	if err != nil && !(errors.Is(err, io.EOF) && int64(n) == length) {
		return nil, fmt.Errorf("%w: read immutable range: %v", ErrCorrupt, err)
	}
	if int64(n) != length {
		return nil, fmt.Errorf("%w: short immutable range", ErrCorrupt)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

// qualifyRef embeds the hashed stream directory into opaque backend keys. It
// is applied at publication time so Read never needs the plaintext stream path.
func qualifyRef(path string, ref *SegmentRef) {
	prefix := filepath.Join("streams", streamID(path))
	ref.DataKey = filepath.Join(prefix, ref.DataKey)
	ref.IndexKey = filepath.Join(prefix, ref.IndexKey)
}

func pathIDFromKey(key string) string {
	clean := filepath.Clean(key)
	parts := strings.Split(clean, string(filepath.Separator))
	if len(parts) < 4 || parts[0] != "streams" {
		return ""
	}
	return filepath.Join(parts[0], parts[1])
}

func (b *FileBackend) readPair(root string, ref SegmentRef) ([]byte, []byte, error) {
	data, err := os.ReadFile(filepath.Join(root, trimStreamPrefix(ref.DataKey)))
	if err != nil {
		return nil, nil, err
	}
	index, err := os.ReadFile(filepath.Join(root, trimStreamPrefix(ref.IndexKey)))
	if err != nil {
		return nil, nil, err
	}
	if segmentChecksum(data, index) != ref.Checksum {
		return nil, nil, ErrChecksum
	}
	b.stats.BytesRead += uint64(len(data) + len(index))
	return data, index, nil
}

func trimStreamPrefix(key string) string {
	parts := strings.Split(filepath.Clean(key), string(filepath.Separator))
	if len(parts) >= 3 && parts[0] == "streams" {
		return filepath.Join(parts[2:]...)
	}
	return filepath.Clean(key)
}

func (b *FileBackend) readCached(ref SegmentRef) ([]byte, []byte, error) {
	pathID := pathIDFromKey(ref.DataKey)
	if pathID == "" {
		return nil, nil, fmt.Errorf("%w: object ref lacks stream id", ErrCorrupt)
	}
	cacheDir := filepath.Join(b.cacheRoot, strings.TrimPrefix(pathID, "streams"+string(filepath.Separator)))
	cacheData := filepath.Join(cacheDir, ref.Checksum+".data")
	cacheIndex := filepath.Join(cacheDir, ref.Checksum+".index")
	data, dataErr := os.ReadFile(cacheData)
	index, indexErr := os.ReadFile(cacheIndex)
	if dataErr == nil && indexErr == nil && segmentChecksum(data, index) == ref.Checksum {
		now := time.Now()
		b.cacheAccess[cacheData] = now
		b.cacheAccess[cacheIndex] = now
		b.stats.CacheHits++
		b.stats.BytesRead += uint64(len(data) + len(index))
		return data, index, nil
	}
	_ = os.Remove(cacheData)
	_ = os.Remove(cacheIndex)
	delete(b.cacheAccess, cacheData)
	delete(b.cacheAccess, cacheIndex)
	b.stats.CacheMisses++

	originRoot := filepath.Join(b.root, pathID)
	data, index, err := b.readPair(originRoot, ref)
	if err != nil {
		return nil, nil, err
	}
	b.stats.OriginReads++
	b.stats.OriginBytes += uint64(len(data) + len(index))
	if err := hit(b.faults, FaultCache); err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return nil, nil, err
	}
	if err := b.writeCacheFile(cacheData, data); err != nil {
		return nil, nil, err
	}
	if err := b.writeCacheFile(cacheIndex, index); err != nil {
		return nil, nil, err
	}
	now := time.Now()
	b.cacheAccess[cacheData] = now
	b.cacheAccess[cacheIndex] = now
	b.evictCache()
	return data, index, nil
}

func (b *FileBackend) readCachedBlock(
	ctx context.Context,
	ref SegmentRef,
	block int,
	start, length int64,
) ([]byte, error) {
	pathID := pathIDFromKey(ref.DataKey)
	if pathID == "" {
		return nil, fmt.Errorf("%w: object ref lacks stream id", ErrCorrupt)
	}
	cacheDir := filepath.Join(b.cacheRoot, strings.TrimPrefix(pathID, "streams"+string(filepath.Separator)))
	cacheName := filepath.Join(cacheDir, fmt.Sprintf("%s.block-%06d", ref.Checksum, block))
	if data, err := os.ReadFile(cacheName); err == nil &&
		int64(len(data)) == length && byteChecksum(data) == ref.BlockChecksums[block] {
		b.cacheAccess[cacheName] = time.Now()
		b.stats.CacheHits++
		b.stats.BytesRead += uint64(len(data))
		return data, nil
	}
	_ = os.Remove(cacheName)
	delete(b.cacheAccess, cacheName)
	b.stats.CacheMisses++

	originName := filepath.Join(b.root, pathID, trimStreamPrefix(ref.DataKey))
	data, err := readFileRange(ctx, originName, start, length)
	if err != nil {
		return nil, err
	}
	if byteChecksum(data) != ref.BlockChecksums[block] {
		return nil, ErrChecksum
	}
	b.stats.OriginReads++
	b.stats.OriginBytes += uint64(len(data))
	b.stats.BytesRead += uint64(len(data))
	if err := hit(b.faults, FaultCache); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return nil, err
	}
	if err := b.writeCacheFile(cacheName, data); err != nil {
		return nil, err
	}
	b.cacheAccess[cacheName] = time.Now()
	b.evictCache()
	return data, nil
}

func (b *FileBackend) writeCacheFile(name string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(name), ".cache-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) //nolint:errcheck // best-effort abandoned cache cleanup
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, name)
}

func (b *FileBackend) evictCache() {
	type item struct {
		name string
		size int64
		mod  time.Time
	}
	var items []item
	var total int64
	_ = filepath.WalkDir(b.cacheRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // cache eviction is deliberately best effort
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr == nil {
			mod := info.ModTime()
			if access, ok := b.cacheAccess[path]; ok {
				mod = access
			}
			items = append(items, item{name: path, size: info.Size(), mod: mod})
			total += info.Size()
		}
		return nil
	})
	sort.Slice(items, func(i, j int) bool { return items[i].mod.Before(items[j].mod) })
	for _, it := range items {
		if total <= b.cacheBytes {
			break
		}
		if os.Remove(it.name) == nil {
			total -= it.size
			delete(b.cacheAccess, it.name)
			b.stats.CacheEvictions++
		}
	}
}

// Tombstone makes the current generation unreachable for a deleted stream.
func (b *FileBackend) Tombstone(_ context.Context, path string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	unlock, err := b.lockStream(path)
	if err != nil {
		return err
	}
	defer unlock()
	raw, err := json.Marshal(currentPointer{Tombstone: true})
	if err != nil {
		return err
	}
	return b.writeReplace(filepath.Join(b.streamDir(path), "CURRENT"), raw)
}

// GC classifies reclaimable generations and objects but deliberately deletes
// neither. Put and Publish are separate durability steps, so a collector
// cannot distinguish an orphan from a publisher paused between them without a
// shared staging barrier. Retention is correctness-first until that protocol
// exists.
func (b *FileBackend) GC(_ context.Context, path string, retention GCRetention) (GCResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	unlock, err := b.lockStream(path)
	if err != nil {
		return GCResult{}, err
	}
	defer unlock()
	if err := hit(b.faults, FaultGC); err != nil {
		return GCResult{}, err
	}
	if retention.KeepGenerations < 1 {
		retention.KeepGenerations = 2
	}
	if retention.Now.IsZero() {
		retention.Now = time.Now()
	}
	dir := filepath.Join(b.streamDir(path), "manifests")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return GCResult{}, nil
	}
	if err != nil {
		return GCResult{}, err
	}
	type generation struct {
		name string
		m    Manifest
		mod  time.Time
	}
	var generations []generation
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, rerr := os.ReadFile(filepath.Join(dir, entry.Name()))
		if rerr != nil {
			continue
		}
		var m Manifest
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		info, _ := entry.Info()
		generations = append(generations, generation{name: entry.Name(), m: m, mod: info.ModTime()})
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i].m.Generation > generations[j].m.Generation })
	current := ""
	if ptr, perr := b.readPointer(path); perr == nil {
		current = ptr.Token + ".json"
	}
	referencedObjects := map[string]bool{}
	protected := make(map[string]bool, len(retention.ProtectedTokens))
	for _, token := range retention.ProtectedTokens {
		protected[token+".json"] = true
	}
	var result GCResult
	for i, gen := range generations {
		young := retention.MinAge > 0 && retention.Now.Sub(gen.mod) < retention.MinAge
		keep := i < retention.KeepGenerations || gen.name == current || protected[gen.name] || young
		if keep {
			result.ManifestsKept++
		} else {
			result.ManifestsDeferred++
		}
		for _, ref := range gen.m.Segments {
			referencedObjects[filepath.Base(ref.DataKey)] = true
			referencedObjects[filepath.Base(ref.IndexKey)] = true
		}
	}
	segmentDir := filepath.Join(b.streamDir(path), "segments")
	objects, _ := os.ReadDir(segmentDir)
	for _, object := range objects {
		if object.IsDir() {
			continue
		}
		if referencedObjects[object.Name()] {
			result.SegmentsKept++
			continue
		}
		result.SegmentsDeferred++
	}
	return result, nil
}

// Close releases backend resources.
func (b *FileBackend) Close() error { return nil }

// Stats returns a snapshot for benchmark artifacts.
func (b *FileBackend) Stats() FileBackendStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}

// BackendStats returns generic operational counters for metrics.
func (b *FileBackend) BackendStats() BackendStats {
	stats := b.Stats()
	return BackendStats{
		OriginReads:    stats.OriginReads,
		OriginBytes:    stats.OriginBytes,
		CacheHits:      stats.CacheHits,
		CacheMisses:    stats.CacheMisses,
		CacheEvictions: stats.CacheEvictions,
		CacheBytes:     directoryBytes(b.cacheRoot),
		BytesRead:      stats.BytesRead,
		BytesWritten:   stats.BytesWritten,
	}
}

func directoryBytes(root string) uint64 {
	if root == "" {
		return 0
	}
	var total uint64
	_ = filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // occupancy reporting is deliberately best effort
		}
		if entry.IsDir() {
			return nil
		}
		if info, infoErr := entry.Info(); infoErr == nil {
			total += uint64(info.Size())
		}
		return nil
	})
	return total
}

// DebugPaths exposes deterministic paths only to same-package tests and local
// repair tooling; no protocol path is ever used as a filesystem component.
func (b *FileBackend) DebugPaths(path string, ref SegmentRef) (string, string) {
	root := filepath.Join(b.root, pathIDFromKey(ref.DataKey))
	return filepath.Join(root, trimStreamPrefix(ref.DataKey)), filepath.Join(root, trimStreamPrefix(ref.IndexKey))
}
