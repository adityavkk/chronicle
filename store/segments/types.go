// Package segments implements the feature-gated immutable-segment read plane.
//
// Redis remains the linearizable source of acknowledged stream bytes. Segment
// manifests describe a checksum-protected prefix which readers may serve from
// immutable Redis strings, local files, or an object-store emulator before
// reading the bounded hot tail from the primary store.
package segments

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// Mode selects the immutable-byte substrate. Off is represented by not
// constructing a segments.Store at all; it remains chronicle's default.
type Mode string

const (
	// ModeOff leaves the authoritative Redis frame layout unchanged.
	ModeOff Mode = "off"
	// ModeRedisChunks stores immutable candidates in Redis strings.
	ModeRedisChunks Mode = "redis-chunks"
	// ModeLocalFiles stores immutable candidates on the local filesystem.
	ModeLocalFiles Mode = "local-files"
	// ModeObjectCache stores immutable candidates in an object-store emulator
	// fronted by a bounded local cache.
	ModeObjectCache Mode = "object-cache"
)

// ParseMode validates the feature gate.
func ParseMode(v string) (Mode, error) {
	mode := Mode(v)
	switch mode {
	case ModeOff, ModeRedisChunks, ModeLocalFiles, ModeObjectCache:
		return mode, nil
	default:
		return "", fmt.Errorf("unknown segment mode %q (want %q, %q, %q, or %q)",
			v, ModeOff, ModeRedisChunks, ModeLocalFiles, ModeObjectCache)
	}
}

// MigrationState is the reversible read-plane migration state.
//
// Shadow writes and verifies segments but never serves them. Serving permits
// checksum-verified segment reads with Redis fallback. Cutover is the explicit
// rollback boundary; this prototype can record and test it, but never deletes
// the Redis authoritative copy.
type MigrationState string

const (
	// StateShadow writes candidates but serves all reads from the primary.
	StateShadow MigrationState = "shadow"
	// StateServing permits verified candidate reads with primary fallback.
	StateServing MigrationState = "serving"
	// StateCutover records the explicit one-way rollback boundary.
	StateCutover MigrationState = "cutover"
)

const (
	// ManifestVersion 2 adds independently authenticated data blocks and
	// manifest-bound sparse entries for bounded range reads.
	ManifestVersion = 2
	// SegmentBlockBytes is the maximum authenticated backend range fetched in
	// one operation.
	SegmentBlockBytes = 64 << 10
	// SegmentMaxFrames bounds headers and sparse-index growth for tiny frames.
	SegmentMaxFrames = store.DefaultReadPageFrames
)

// Manifest is the only visibility boundary for immutable segments. A segment
// object is not readable until a complete manifest generation referencing it
// has been durably published through the backend's compare-and-swap pointer.
type Manifest struct {
	Version          int            `json:"version"`
	Mode             Mode           `json:"mode"`
	Path             string         `json:"path"`
	Incarnation      string         `json:"incarnation"`
	ContentType      string         `json:"content_type"`
	Generation       uint64         `json:"generation"`
	State            MigrationState `json:"state"`
	SealedThrough    string         `json:"sealed_through"`
	Segments         []SegmentRef   `json:"segments"`
	Fork             *ForkReference `json:"fork,omitempty"`
	CreatedAtUnixNS  int64          `json:"created_at_unix_ns"`
	PublishedAtUnixN int64          `json:"published_at_unix_ns"`
}

// ForkReference records the logical source range materialized by this
// manifest. Segment bytes are self-contained so a fork survives source
// deletion; the reference is retained for audit, repair, and conservative GC.
type ForkReference struct {
	SourcePath      string `json:"source_path"`
	Through         string `json:"through"`
	RequestedOffset string `json:"requested_offset,omitempty"`
	SubOffset       uint64 `json:"sub_offset,omitempty"`
}

// SegmentRef describes one immutable, ordered record run. StartExclusive and
// EndInclusive use the Durable Streams logical offset strings. DataKey and
// IndexKey are backend-owned opaque names.
type SegmentRef struct {
	ID             string       `json:"id"`
	DataKey        string       `json:"data_key"`
	IndexKey       string       `json:"index_key"`
	StartExclusive string       `json:"start_exclusive"`
	EndInclusive   string       `json:"end_inclusive"`
	Records        uint64       `json:"records"`
	DataBytes      int64        `json:"data_bytes"`
	IndexBytes     int64        `json:"index_bytes"`
	IndexStride    uint32       `json:"index_stride"`
	BlockBytes     int64        `json:"block_bytes"`
	BlockChecksums []string     `json:"block_checksums"`
	IndexChecksum  string       `json:"index_checksum"`
	IndexEntries   []IndexEntry `json:"index_entries"`
	Checksum       string       `json:"checksum"`
}

// IndexEntry is one manifest-authenticated seek boundary. The separate index
// object is retained for full-generation verification and repair audits.
type IndexEntry struct {
	Offset       string `json:"offset"`
	Ordinal      uint64 `json:"ordinal"`
	DataPosition uint64 `json:"data_position"`
}

// GCRetention defines the minimum rollback/history protection for cleanup.
type GCRetention struct {
	KeepGenerations int
	MinAge          time.Duration
	Now             time.Time
	// ProtectedTokens are manifest digests held by active snapshot readers.
	// Store.GC fills this from process-local SnapshotPins.
	ProtectedTokens []string
}

// SnapshotPin keeps one immutable manifest generation reachable while a
// bounded reader uses it. Release is idempotent.
type SnapshotPin struct {
	Token    string
	Manifest *Manifest

	once    sync.Once
	release func()
}

// Release removes the active-snapshot GC protection.
func (p *SnapshotPin) Release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		if p.release != nil {
			p.release()
		}
	})
}

var (
	// ErrNoManifest means no visible manifest generation exists for a stream.
	ErrNoManifest = errors.New("segment manifest not found")
	// ErrConflict means a manifest compare-and-swap precondition failed.
	ErrConflict = errors.New("segment manifest generation conflict")
	// ErrChecksum means immutable bytes did not match their recorded digest.
	ErrChecksum = errors.New("segment checksum mismatch")
	// ErrCorrupt means immutable metadata or bytes violate the storage format.
	ErrCorrupt = errors.New("corrupt immutable segment")
	// ErrCutover means a transition attempted to cross the rollback boundary.
	ErrCutover = errors.New("segment migration is past the rollback boundary")
)

// Backend is the durability boundary for immutable bytes and manifest
// generations. Put must make both data and index durable before returning.
// Publish must use expectedToken as a CAS precondition and make the manifest
// durable before changing the current pointer.
type Backend interface {
	Mode() Mode
	Load(ctx context.Context, path string) (*Manifest, string, error)
	Put(ctx context.Context, path string, generation uint64, encoded EncodedSegment) (SegmentRef, error)
	Publish(ctx context.Context, path, expectedToken string, manifest *Manifest) (string, error)
	Read(ctx context.Context, ref SegmentRef) (data, index []byte, err error)
	// ReadDataRange returns exactly length bytes beginning at start. Callers use
	// block-aligned ranges; implementations authenticate the corresponding
	// checksum from ref before returning any byte.
	ReadDataRange(ctx context.Context, ref SegmentRef, start, length int64) ([]byte, error)
	Tombstone(ctx context.Context, path string) error
	GC(ctx context.Context, path string, retention GCRetention) (GCResult, error)
	Close() error
}

// GCResult is machine-readable evidence from the audit-only collector.
// Deferred items are policy-eligible but intentionally retained until a
// publication staging barrier exists. Deleted counts remain zero.
type GCResult struct {
	ManifestsKept     int `json:"manifests_kept"`
	ManifestsDeferred int `json:"manifests_deferred"`
	ManifestsDeleted  int `json:"manifests_deleted"`
	SegmentsKept      int `json:"segments_kept"`
	SegmentsDeferred  int `json:"segments_deferred"`
	SegmentsDeleted   int `json:"segments_deleted"`
}

// BackendStats is an operational snapshot. Counters are monotonic; CacheBytes
// is the current bounded-cache occupancy. OriginReads counts
// immutable data+index pairs; object-cache therefore performs two emulated
// object GETs per origin read. RedisReads/RedisWrites count logical segment
// operations rather than the primary store's Redis commands.
type BackendStats struct {
	OriginReads    uint64 `json:"origin_reads"`
	OriginBytes    uint64 `json:"origin_bytes"`
	CacheHits      uint64 `json:"cache_hits"`
	CacheMisses    uint64 `json:"cache_misses"`
	CacheEvictions uint64 `json:"cache_evictions"`
	CacheBytes     uint64 `json:"cache_bytes"`
	BytesRead      uint64 `json:"bytes_read"`
	BytesWritten   uint64 `json:"bytes_written"`
	RedisReads     uint64 `json:"redis_reads"`
	RedisWrites    uint64 `json:"redis_writes"`
}

type backendStatsProvider interface {
	BackendStats() BackendStats
}

// Options configures the segment wrapper.
type Options struct {
	Backend      Backend
	TargetBytes  int
	IndexStride  int
	AutoSealRead bool
	InitialState MigrationState
	Faults       FaultInjector
}

func (o *Options) defaults() {
	if o.TargetBytes <= 0 {
		o.TargetBytes = 256 << 10
	}
	if o.IndexStride <= 0 {
		o.IndexStride = 128
	}
	if o.InitialState == "" {
		o.InitialState = StateShadow
	}
}

// validateManifest rejects a pointer which cannot describe this stream.
func validateManifest(m *Manifest, mode Mode, path, incarnation string) error {
	if m == nil || m.Version != ManifestVersion {
		return fmt.Errorf("%w: unsupported manifest version", ErrCorrupt)
	}
	if m.Mode != mode || m.Path != path || m.Incarnation != incarnation {
		return fmt.Errorf("%w: manifest identity mismatch", ErrCorrupt)
	}
	switch m.State {
	case StateShadow, StateServing, StateCutover:
	default:
		return fmt.Errorf("%w: unknown migration state %q", ErrCorrupt, m.State)
	}
	if _, err := store.ParseOffset(m.SealedThrough); err != nil {
		return fmt.Errorf("%w: sealed offset: %w", ErrCorrupt, err)
	}
	last := store.ZeroOffset
	for i, ref := range m.Segments {
		start, err := store.ParseOffset(ref.StartExclusive)
		if err != nil {
			return fmt.Errorf("%w: segment %d start: %w", ErrCorrupt, i, err)
		}
		end, err := store.ParseOffset(ref.EndInclusive)
		if err != nil {
			return fmt.Errorf("%w: segment %d end: %w", ErrCorrupt, i, err)
		}
		if !start.Equal(last) || !start.LessThan(end) || ref.Records == 0 || ref.Checksum == "" {
			return fmt.Errorf("%w: segment %d is not a contiguous non-empty range", ErrCorrupt, i)
		}
		if err := validateSegmentRef(ref); err != nil {
			return fmt.Errorf("segment %d: %w", i, err)
		}
		last = end
	}
	sealed, _ := store.ParseOffset(m.SealedThrough)
	if len(m.Segments) == 0 {
		if !sealed.IsZero() {
			return fmt.Errorf("%w: empty manifest has non-zero boundary", ErrCorrupt)
		}
	} else if !last.Equal(sealed) {
		return fmt.Errorf("%w: segment ranges do not reach sealed boundary", ErrCorrupt)
	}
	return nil
}

func validateSegmentRef(ref SegmentRef) error {
	if ref.DataKey == "" || ref.IndexKey == "" || ref.Records == 0 ||
		ref.Records > SegmentMaxFrames || ref.DataBytes < recordHeaderBytes ||
		ref.IndexBytes <= 0 || ref.IndexStride == 0 {
		return fmt.Errorf("%w: invalid segment sizes or identity", ErrCorrupt)
	}
	if ref.BlockBytes != SegmentBlockBytes {
		return fmt.Errorf("%w: unsupported authenticated block size %d", ErrCorrupt, ref.BlockBytes)
	}
	wantBlocks := (ref.DataBytes + ref.BlockBytes - 1) / ref.BlockBytes
	if int64(len(ref.BlockChecksums)) != wantBlocks {
		return fmt.Errorf("%w: authenticated block cardinality mismatch", ErrCorrupt)
	}
	wantEntries := (ref.Records + uint64(ref.IndexStride) - 1) / uint64(ref.IndexStride)
	if uint64(len(ref.IndexEntries)) != wantEntries ||
		ref.IndexBytes != int64(len(ref.IndexEntries))*indexEntryBytes {
		return fmt.Errorf("%w: sparse index cardinality mismatch", ErrCorrupt)
	}
	if !validSHA256(ref.Checksum) || !validSHA256(ref.IndexChecksum) {
		return fmt.Errorf("%w: invalid object checksum", ErrCorrupt)
	}
	for _, checksum := range ref.BlockChecksums {
		if !validSHA256(checksum) {
			return fmt.Errorf("%w: invalid block checksum", ErrCorrupt)
		}
	}
	start, err := store.ParseOffset(ref.StartExclusive)
	if err != nil {
		return fmt.Errorf("%w: invalid segment start", ErrCorrupt)
	}
	end, err := store.ParseOffset(ref.EndInclusive)
	if err != nil || !start.LessThan(end) {
		return fmt.Errorf("%w: invalid segment end", ErrCorrupt)
	}
	var previous store.Offset
	var previousPosition uint64
	for i, entry := range ref.IndexEntries {
		offset, parseErr := store.ParseOffset(entry.Offset)
		if parseErr != nil || !start.LessThan(offset) || end.LessThan(offset) {
			return fmt.Errorf("%w: invalid sparse offset %d", ErrCorrupt, i)
		}
		wantOrdinal := uint64(i) * uint64(ref.IndexStride)
		if entry.Ordinal != wantOrdinal || entry.Ordinal >= ref.Records ||
			entry.DataPosition >= uint64(ref.DataBytes) {
			return fmt.Errorf("%w: invalid sparse boundary %d", ErrCorrupt, i)
		}
		if i == 0 {
			if entry.Ordinal != 0 || entry.DataPosition != 0 {
				return fmt.Errorf("%w: sparse index does not start at the segment", ErrCorrupt)
			}
		} else if !previous.LessThan(offset) || entry.DataPosition <= previousPosition {
			return fmt.Errorf("%w: unordered sparse boundary %d", ErrCorrupt, i)
		}
		previous = offset
		previousPosition = entry.DataPosition
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validateDataRange(ref SegmentRef, start, length int64) (int, error) {
	if err := validateSegmentRef(ref); err != nil {
		return 0, err
	}
	if start < 0 || length <= 0 || start >= ref.DataBytes ||
		start%ref.BlockBytes != 0 ||
		length != min(ref.BlockBytes, ref.DataBytes-start) {
		return 0, fmt.Errorf("%w: data range must name one complete authenticated block", ErrCorrupt)
	}
	return int(start / ref.BlockBytes), nil
}
