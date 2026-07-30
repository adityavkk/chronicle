package segments

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// Store overlays a checksum-verified immutable read plane on a complete
// primary store. Every mutation is linearized by primary first. Segment work
// never changes a successful mutation into a failure, and every segment read
// can fall back to primary without losing an acknowledged byte.
type Store struct {
	primary store.Store
	reader  store.PageReader
	backend Backend
	opts    Options
	log     *slog.Logger

	locksMu  sync.Mutex
	locks    map[string]*sync.Mutex
	knownMu  sync.RWMutex
	known    map[string]knownSnapshot
	pinsMu   sync.Mutex
	pins     map[string]map[string]int
	leasesMu sync.Mutex
	leases   map[string]*readLease
	stats    storeStats
}

type knownSnapshot struct {
	incarnation string
	tail        store.Offset
}

type readLease struct {
	path            string
	snapshot        store.ReadSnapshot
	primarySnapshot store.ReadSnapshot
	manifest        *Manifest
	manifestToken   string
	releasePin      func()
	primaryOnly     atomic.Bool
	lastUsed        time.Time
}

type storeStats struct {
	seals                     atomic.Uint64
	sealDurationNanoseconds   atomic.Uint64
	sealDurationMaxNanosecond atomic.Uint64
	segmentReads              atomic.Uint64
	primaryFallbacks          atomic.Uint64
	checksumFailures          atomic.Uint64
	bytesServed               atomic.Uint64
}

// Stats is a benchmark- and repair-friendly snapshot.
type Stats struct {
	Seals                      uint64       `json:"seals"`
	SealDurationNanoseconds    uint64       `json:"seal_duration_nanoseconds"`
	SealDurationMaxNanoseconds uint64       `json:"seal_duration_max_nanoseconds"`
	SegmentReads               uint64       `json:"segment_reads"`
	PrimaryFallbacks           uint64       `json:"primary_fallbacks"`
	ChecksumFailures           uint64       `json:"checksum_failures"`
	BytesServed                uint64       `json:"bytes_served"`
	Backend                    BackendStats `json:"backend"`
}

var (
	_ store.Store                          = (*Store)(nil)
	_ store.PageReader                     = (*Store)(nil)
	_ store.PageSnapshotReleaser           = (*Store)(nil)
	_ store.NotificationSubscriberProvider = (*Store)(nil)
)

// New wraps primary with a feature-gated immutable read plane.
func New(primary store.Store, opts Options, logger *slog.Logger) (*Store, error) {
	if primary == nil || opts.Backend == nil {
		return nil, errors.New("segment store requires primary and backend")
	}
	reader, ok := primary.(store.PageReader)
	if !ok {
		return nil, errors.New("segment store primary must provide bounded page snapshots")
	}
	opts.defaults()
	if opts.InitialState != StateShadow && opts.InitialState != StateServing {
		return nil, fmt.Errorf("initial segment state must be %q or %q", StateShadow, StateServing)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{
		primary: primary,
		reader:  reader,
		backend: opts.Backend,
		opts:    opts,
		log:     logger,
		locks:   map[string]*sync.Mutex{},
		known:   map[string]knownSnapshot{},
		pins:    map[string]map[string]int{},
		leases:  map[string]*readLease{},
	}, nil
}

func (s *Store) pathLock(path string) *sync.Mutex {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	if lock := s.locks[path]; lock != nil {
		return lock
	}
	lock := &sync.Mutex{}
	s.locks[path] = lock
	return lock
}

func incarnation(path string, meta *store.StreamMetadata) string {
	if meta.Incarnation != "" {
		return meta.Incarnation
	}
	h := sha256.New()
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(meta.CreatedAt.UnixNano(), 10)))
	h.Write([]byte{0})
	h.Write([]byte(meta.ContentType))
	h.Write([]byte{0})
	h.Write([]byte(meta.ForkedFrom))
	h.Write([]byte{0})
	h.Write([]byte(meta.ForkOffset.String()))
	return hex.EncodeToString(h.Sum(nil))
}

// Seal reconciles the immutable prefix with one atomic primary Read snapshot.
// A crash before Publish leaves only unreachable immutable objects. A crash
// after Publish is safe because Put returned only after the referenced objects
// were durable.
func (s *Store) Seal(path string) (*Manifest, error) {
	lock := s.pathLock(path)
	lock.Lock()
	defer lock.Unlock()
	manifest, err := s.sealLocked(path)
	if err == nil {
		if sealed, parseErr := store.ParseOffset(manifest.SealedThrough); parseErr == nil {
			s.setKnown(path, manifest.Incarnation, sealed)
		}
	}
	return manifest, err
}

func (s *Store) setKnown(path, identity string, offset store.Offset) {
	s.knownMu.Lock()
	s.known[path] = knownSnapshot{incarnation: identity, tail: offset}
	s.knownMu.Unlock()
}

func (s *Store) clearKnown(path string) {
	s.knownMu.Lock()
	delete(s.known, path)
	s.knownMu.Unlock()
}

func (s *Store) sealForRead(path, identity string, tail store.Offset) error {
	s.knownMu.RLock()
	known, ok := s.known[path]
	s.knownMu.RUnlock()
	if ok && known.incarnation == identity && known.tail.Equal(tail) {
		return nil
	}
	lock := s.pathLock(path)
	lock.Lock()
	defer lock.Unlock()
	// A peer reader may have sealed while this reader waited for the path lock.
	s.knownMu.RLock()
	known, ok = s.known[path]
	s.knownMu.RUnlock()
	if ok && known.incarnation == identity && known.tail.Equal(tail) {
		return nil
	}
	manifest, err := s.sealLocked(path)
	if err != nil {
		return err
	}
	sealed, err := store.ParseOffset(manifest.SealedThrough)
	if err != nil {
		return err
	}
	s.setKnown(path, manifest.Incarnation, sealed)
	return nil
}

func (s *Store) sealLocked(path string) (*Manifest, error) {
	ctx := context.Background()
	started := time.Now()
	for attempt := 0; attempt < 8; attempt++ {
		replacePriorIncarnation := false
		meta, err := s.primary.Get(path)
		if err != nil {
			return nil, err
		}
		identity := incarnation(path, meta)
		manifest, token, err := s.backend.Load(ctx, path)
		switch {
		case errors.Is(err, ErrNoManifest):
			manifest = &Manifest{
				Version:         ManifestVersion,
				Mode:            s.backend.Mode(),
				Path:            path,
				Incarnation:     identity,
				ContentType:     meta.ContentType,
				State:           s.opts.InitialState,
				SealedThrough:   store.ZeroOffset.String(),
				CreatedAtUnixNS: meta.CreatedAt.UnixNano(),
			}
			token = ""
		case err != nil:
			return nil, err
		default:
			// A mixed-version node can delete and recreate the authoritative
			// path without tombstoning candidate metadata. Treat a manifest
			// from that prior incarnation as the CAS predecessor for a fresh
			// generation, never as bytes belonging to the new stream.
			if manifest.Incarnation != identity {
				replacePriorIncarnation = true
				manifest = &Manifest{
					Version:         ManifestVersion,
					Mode:            s.backend.Mode(),
					Path:            path,
					Incarnation:     identity,
					ContentType:     meta.ContentType,
					State:           s.opts.InitialState,
					SealedThrough:   store.ZeroOffset.String(),
					CreatedAtUnixNS: meta.CreatedAt.UnixNano(),
				}
			} else if err := validateManifest(manifest, s.backend.Mode(), path, identity); err != nil {
				return nil, err
			}
		}

		sealed, err := store.ParseOffset(manifest.SealedThrough)
		if err != nil {
			return nil, err
		}
		pageOpts := store.ReadPageOptions{
			TargetBytes: s.opts.TargetBytes,
			MaxFrames:   store.DefaultReadPageFrames,
			NoTouch:     true,
		}
		page, err := s.reader.ReadPage(ctx, path, sealed, pageOpts)
		if err != nil {
			return nil, err
		}
		primarySnapshot := page.Snapshot
		releasePrimary := func() {
			s.releasePrimarySnapshot(path, primarySnapshot)
		}
		if primarySnapshot.Incarnation != identity ||
			!store.ContentTypeMatches(primarySnapshot.ContentType, meta.ContentType) {
			releasePrimary()
			continue
		}
		// A current manifest with no new bytes needs no generation churn.
		if token != "" && !replacePriorIncarnation && len(page.Messages) == 0 && page.UpToDate {
			if err := s.verifyManifest(manifest); err != nil {
				releasePrimary()
				return nil, err
			}
			releasePrimary()
			return manifest, nil
		}

		next := *manifest
		next.Generation = manifest.Generation + 1
		next.Segments = append([]SegmentRef(nil), manifest.Segments...)
		next.PublishedAtUnixN = time.Now().UnixNano()
		if next.Fork == nil && meta.ForkedFrom != "" {
			next.Fork = forkReference(meta)
		}

		start := sealed
		for {
			if len(page.Messages) == 0 && !page.UpToDate {
				releasePrimary()
				return nil, store.ErrReadDataMissing
			}
			if len(page.Messages) > 0 {
				encoded, encodeErr := Encode(start, page.Messages, s.opts.IndexStride)
				if encodeErr != nil {
					releasePrimary()
					return nil, encodeErr
				}
				ref, putErr := s.backend.Put(ctx, path, next.Generation, encoded)
				if putErr != nil {
					releasePrimary()
					return nil, putErr
				}
				next.Segments = append(next.Segments, ref)
				start = encoded.EndInclusive
			}
			if page.UpToDate {
				break
			}
			pageOpts.Snapshot = &primarySnapshot
			page, err = s.reader.ReadPage(ctx, path, start, pageOpts)
			if err != nil {
				releasePrimary()
				return nil, err
			}
		}
		releasePrimary()
		next.SealedThrough = start.String()
		if err := validateManifest(&next, s.backend.Mode(), path, identity); err != nil {
			return nil, err
		}
		freshMeta, err := s.primary.Get(path)
		if err != nil || incarnation(path, freshMeta) != identity {
			continue
		}
		if _, err := s.backend.Publish(ctx, path, token, &next); errors.Is(err, ErrConflict) {
			continue
		} else if err != nil {
			return nil, err
		}
		s.stats.seals.Add(1)
		s.observeSealDuration(time.Since(started))
		return &next, nil
	}
	return nil, fmt.Errorf("%w: seal %s exceeded CAS retries", ErrConflict, path)
}

func (s *Store) observeSealDuration(duration time.Duration) {
	nanoseconds := uint64(duration)
	s.stats.sealDurationNanoseconds.Add(nanoseconds)
	for {
		previous := s.stats.sealDurationMaxNanosecond.Load()
		if nanoseconds <= previous ||
			s.stats.sealDurationMaxNanosecond.CompareAndSwap(previous, nanoseconds) {
			return
		}
	}
}

func forkReference(meta *store.StreamMetadata) *ForkReference {
	ref := &ForkReference{
		SourcePath: meta.ForkedFrom,
		Through:    meta.ForkOffset.String(),
		SubOffset:  meta.ForkSubOffset,
	}
	if meta.ForkOffsetRequested != nil {
		ref.RequestedOffset = meta.ForkOffsetRequested.String()
	}
	return ref
}

// Create delegates stream creation to the authoritative primary.
func (s *Store) Create(path string, opts store.CreateOptions) (*store.StreamMetadata, bool, error) {
	meta, created, err := s.primary.Create(path, opts)
	if err == nil && created {
		s.clearKnown(path)
		// Invalidate a prior incarnation's current pointer. The authoritative
		// Create result is not made ambiguous if best-effort cleanup fails:
		// identity validation still prevents stale reads.
		if terr := s.backend.Tombstone(context.Background(), path); terr != nil {
			s.log.Warn("segment incarnation tombstone failed", "path", path, "error", terr)
		}
		if len(opts.InitialData) > 0 || opts.Closed {
			s.sealBestEffort(path)
		}
	}
	return meta, created, err
}

// Get returns authoritative stream metadata.
func (s *Store) Get(path string) (*store.StreamMetadata, error) { return s.primary.Get(path) }

// Has reports whether the authoritative primary contains path.
func (s *Store) Has(path string) bool { return s.primary.Has(path) }

// Delete removes the primary stream before tombstoning its candidate pointer.
func (s *Store) Delete(path string) error {
	err := s.primary.Delete(path)
	if err == nil {
		s.clearKnown(path)
		if terr := s.backend.Tombstone(context.Background(), path); terr != nil {
			s.log.Warn("segment delete tombstone failed", "path", path, "error", terr)
		}
	}
	return err
}

// Append linearizes bytes through the authoritative primary.
func (s *Store) Append(path string, data []byte, opts store.AppendOptions) (store.AppendResult, error) {
	result, err := s.primary.Append(path, data, opts)
	if err == nil && opts.Close {
		s.sealBestEffort(path)
	}
	return result, err
}

// CloseStream closes the primary stream before best-effort sealing.
func (s *Store) CloseStream(path string) (*store.CloseResult, error) {
	result, err := s.primary.CloseStream(path)
	if err == nil {
		s.sealBestEffort(path)
	}
	return result, err
}

// CloseStreamWithProducer applies producer fencing through the primary.
func (s *Store) CloseStreamWithProducer(path string, opts store.CloseProducerOptions) (*store.CloseProducerResult, error) {
	result, err := s.primary.CloseStreamWithProducer(path, opts)
	if err == nil {
		s.sealBestEffort(path)
	}
	return result, err
}

func (s *Store) sealBestEffort(path string) {
	if _, err := s.Seal(path); err != nil {
		s.log.Warn("immutable segment seal deferred to recovery", "path", path, "error", err)
	}
}

// ReadPage serves at most one immutable segment range or one primary hot-tail
// page. The first call captures both one primary snapshot and one immutable
// manifest generation; continuations resolve only the opaque per-reader lease.
func (s *Store) ReadPage(
	ctx context.Context,
	path string,
	offset store.Offset,
	opts store.ReadPageOptions,
) (store.ReadPage, error) {
	opts = opts.Normalize()
	if err := ctx.Err(); err != nil {
		return store.ReadPage{}, err
	}

	if opts.Snapshot != nil {
		if opts.Snapshot.StoreToken == "" {
			return s.readPrimaryPage(ctx, path, offset, opts, *opts.Snapshot)
		}
		lease, err := s.lookupLease(path, *opts.Snapshot)
		if err != nil {
			return store.ReadPage{}, err
		}
		page, err := s.readLeasedPage(ctx, lease, offset, opts, false, store.ReadPageStats{})
		if err != nil || page.UpToDate {
			s.ReleaseReadSnapshot(path, *opts.Snapshot)
		}
		return page, err
	}

	meta, err := s.primary.Get(path)
	if err != nil {
		return store.ReadPage{}, err
	}
	identity := incarnation(path, meta)
	if s.opts.AutoSealRead {
		if err := s.sealForRead(path, identity, meta.CurrentOffset); err != nil {
			s.stats.primaryFallbacks.Add(1)
		}
	}

	primaryPage, err := s.reader.ReadPage(ctx, path, offset, opts)
	if err != nil {
		return store.ReadPage{}, err
	}
	primarySnapshot := primaryPage.Snapshot
	manifest, manifestToken, err := s.backend.Load(ctx, path)
	if err != nil {
		if !errors.Is(err, ErrNoManifest) {
			s.stats.primaryFallbacks.Add(1)
		}
		return primaryPage, nil
	}
	sealed, parseErr := store.ParseOffset(manifest.SealedThrough)
	if parseErr != nil ||
		validateManifest(manifest, s.backend.Mode(), path, primarySnapshot.Incarnation) != nil ||
		!store.ContentTypeMatches(manifest.ContentType, primarySnapshot.ContentType) ||
		manifest.State == StateShadow ||
		primarySnapshot.Tail.LessThan(sealed) ||
		!offset.LessThan(sealed) {
		s.stats.primaryFallbacks.Add(1)
		return primaryPage, nil
	}

	leaseID, err := store.NewIncarnationID()
	if err != nil {
		s.releasePrimarySnapshot(path, primarySnapshot)
		return store.ReadPage{}, err
	}
	snapshot := primarySnapshot
	snapshot.StoreToken = leaseID
	lease := &readLease{
		path:            path,
		snapshot:        snapshot,
		primarySnapshot: primarySnapshot,
		manifest:        cloneManifest(manifest),
		manifestToken:   manifestToken,
		releasePin:      s.pinToken(path, manifestToken),
		lastUsed:        time.Now(),
	}
	s.leasesMu.Lock()
	s.leases[leaseID] = lease
	s.leasesMu.Unlock()

	captureStats := primaryPage.Stats
	captureStats.DiscardedBytes += captureStats.ReturnedBytes
	captureStats.ReturnedBytes = 0
	page, err := s.readLeasedPage(ctx, lease, offset, opts, true, captureStats)
	if err != nil {
		s.ReleaseReadSnapshot(path, snapshot)
		return store.ReadPage{}, err
	}
	if page.UpToDate {
		s.ReleaseReadSnapshot(path, snapshot)
	}
	return page, nil
}

func (s *Store) readLeasedPage(
	ctx context.Context,
	lease *readLease,
	offset store.Offset,
	opts store.ReadPageOptions,
	rootAlreadyValidated bool,
	initialStats store.ReadPageStats,
) (store.ReadPage, error) {
	sealed, err := store.ParseOffset(lease.manifest.SealedThrough)
	if err != nil {
		return store.ReadPage{}, err
	}

	if lease.primaryOnly.Load() || !offset.LessThan(sealed) {
		return s.readPrimaryPage(ctx, lease.path, offset, opts, lease.snapshot)
	}
	if !rootAlreadyValidated {
		validationOpts := store.ReadPageOptions{
			TargetBytes: 1,
			MaxFrames:   1,
			Snapshot:    &lease.primarySnapshot,
			NoTouch:     true,
		}
		validation, validationErr := s.reader.ReadPage(
			ctx,
			lease.path,
			lease.primarySnapshot.Tail,
			validationOpts,
		)
		if validationErr != nil {
			if errors.Is(validationErr, store.ErrStreamNotFound) {
				return store.ReadPage{}, store.ErrReadSnapshotChanged
			}
			return store.ReadPage{}, validationErr
		}
		initialStats = addReadStats(initialStats, validation.Stats)
		if len(validation.Messages) != 0 || !validation.UpToDate {
			return store.ReadPage{}, store.ErrReadSnapshotChanged
		}
	}

	var ref *SegmentRef
	for i := range lease.manifest.Segments {
		end, parseErr := store.ParseOffset(lease.manifest.Segments[i].EndInclusive)
		if parseErr != nil {
			return store.ReadPage{}, parseErr
		}
		if offset.LessThan(end) {
			ref = &lease.manifest.Segments[i]
			break
		}
	}
	if ref == nil {
		return store.ReadPage{}, store.ErrReadDataMissing
	}

	messages, fetched, discarded, err := DecodePageAfter(
		ctx,
		s.backend,
		*ref,
		offset,
		opts.TargetBytes,
		opts.MaxFrames,
	)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return store.ReadPage{}, err
		}
		attemptStats := addReadStats(initialStats, store.ReadPageStats{
			RequestedBytes: opts.TargetBytes,
			FetchedBytes:   fetched,
			DiscardedBytes: discarded,
		})
		return s.fallbackPrimaryPage(ctx, lease, offset, opts, err, attemptStats)
	}
	if len(messages) == 0 {
		return store.ReadPage{}, store.ErrReadDataMissing
	}

	next := messages[len(messages)-1].Offset
	returned := 0
	for _, message := range messages {
		returned += len(message.Data)
	}
	stats := addReadStats(initialStats, store.ReadPageStats{
		RequestedBytes: opts.TargetBytes,
		FetchedBytes:   fetched,
		ReturnedBytes:  returned,
		DiscardedBytes: discarded,
	})
	s.stats.segmentReads.Add(1)
	s.stats.bytesServed.Add(uint64(returned))
	return store.ReadPage{
		Messages:   messages,
		NextOffset: next,
		Snapshot:   lease.snapshot,
		UpToDate:   next.Equal(lease.snapshot.Tail),
		Stats:      stats,
	}, nil
}

func (s *Store) fallbackPrimaryPage(
	ctx context.Context,
	lease *readLease,
	offset store.Offset,
	opts store.ReadPageOptions,
	cause error,
	attemptStats store.ReadPageStats,
) (store.ReadPage, error) {
	if errors.Is(cause, ErrChecksum) || errors.Is(cause, ErrCorrupt) {
		s.stats.checksumFailures.Add(1)
	}
	s.stats.primaryFallbacks.Add(1)
	lease.primaryOnly.Store(true)
	page, err := s.readPrimaryPage(ctx, lease.path, offset, opts, lease.snapshot)
	if err != nil {
		return store.ReadPage{}, err
	}
	page.Stats = addReadStats(attemptStats, page.Stats)
	return page, nil
}

func (s *Store) readPrimaryPage(
	ctx context.Context,
	path string,
	offset store.Offset,
	opts store.ReadPageOptions,
	snapshot store.ReadSnapshot,
) (store.ReadPage, error) {
	primarySnapshot := snapshot
	if snapshot.StoreToken != "" {
		lease, err := s.lookupLease(path, snapshot)
		if err != nil {
			return store.ReadPage{}, err
		}
		primarySnapshot = lease.primarySnapshot
	}
	opts.Snapshot = &primarySnapshot
	page, err := s.reader.ReadPage(ctx, path, offset, opts)
	if err != nil {
		if errors.Is(err, store.ErrStreamNotFound) {
			return store.ReadPage{}, store.ErrReadSnapshotChanged
		}
		return store.ReadPage{}, err
	}
	page.Snapshot = snapshot
	return page, nil
}

func addReadStats(left, right store.ReadPageStats) store.ReadPageStats {
	if left.RequestedBytes == 0 {
		left.RequestedBytes = right.RequestedBytes
	}
	left.FetchedBytes += right.FetchedBytes
	left.ReturnedBytes += right.ReturnedBytes
	left.DiscardedBytes += right.DiscardedBytes
	left.RedisScriptTime += right.RedisScriptTime
	left.RedisScriptInvokes += right.RedisScriptInvokes
	return left
}

// Read implements Store compatibility by draining the bounded PageReader.
func (s *Store) Read(path string, offset store.Offset) ([]store.Message, bool, error) {
	var (
		messages []store.Message
		snapshot *store.ReadSnapshot
		next     = offset
	)
	defer func() {
		if snapshot != nil {
			s.ReleaseReadSnapshot(path, *snapshot)
		}
	}()
	for {
		page, err := s.ReadPage(
			context.Background(),
			path,
			next,
			store.ReadPageOptions{Snapshot: snapshot},
		)
		if err != nil {
			return nil, false, err
		}
		if snapshot == nil {
			captured := page.Snapshot
			snapshot = &captured
		}
		messages = append(messages, page.Messages...)
		if page.UpToDate {
			upToDate := len(messages) > 0 || offset.Equal(snapshot.Tail) ||
				snapshot.Tail.Equal(store.ZeroOffset)
			return messages, upToDate, nil
		}
		if page.NextOffset.Equal(next) {
			return nil, false, store.ErrReadDataMissing
		}
		next = page.NextOffset
	}
}

func (s *Store) lookupLease(path string, snapshot store.ReadSnapshot) (*readLease, error) {
	s.leasesMu.Lock()
	defer s.leasesMu.Unlock()
	lease := s.leases[snapshot.StoreToken]
	if lease == nil || lease.path != path ||
		!lease.snapshot.Tail.Equal(snapshot.Tail) ||
		lease.snapshot.Incarnation != snapshot.Incarnation ||
		lease.snapshot.Closed != snapshot.Closed ||
		!store.ContentTypeMatches(lease.snapshot.ContentType, snapshot.ContentType) {
		return nil, store.ErrReadSnapshotChanged
	}
	lease.lastUsed = time.Now()
	return lease, nil
}

// ReleaseReadSnapshot releases one per-reader manifest pin and any nested
// primary-store snapshot. It is idempotent for the opaque StoreToken.
func (s *Store) ReleaseReadSnapshot(path string, snapshot store.ReadSnapshot) {
	if snapshot.StoreToken == "" {
		s.releasePrimarySnapshot(path, snapshot)
		return
	}
	s.leasesMu.Lock()
	lease := s.leases[snapshot.StoreToken]
	if lease != nil && lease.path == path {
		delete(s.leases, snapshot.StoreToken)
	} else {
		lease = nil
	}
	s.leasesMu.Unlock()
	if lease == nil {
		return
	}
	if lease.releasePin != nil {
		lease.releasePin()
	}
	s.releasePrimarySnapshot(path, lease.primarySnapshot)
}

func (s *Store) releasePrimarySnapshot(path string, snapshot store.ReadSnapshot) {
	if releaser, ok := s.primary.(store.PageSnapshotReleaser); ok {
		releaser.ReleaseReadSnapshot(path, snapshot)
	}
}

func cloneManifest(manifest *Manifest) *Manifest {
	if manifest == nil {
		return nil
	}
	copy := *manifest
	copy.Segments = append([]SegmentRef(nil), manifest.Segments...)
	for i := range copy.Segments {
		copy.Segments[i].BlockChecksums = append(
			[]string(nil),
			manifest.Segments[i].BlockChecksums...,
		)
		copy.Segments[i].IndexEntries = append(
			[]IndexEntry(nil),
			manifest.Segments[i].IndexEntries...,
		)
	}
	if manifest.Fork != nil {
		fork := *manifest.Fork
		copy.Fork = &fork
	}
	return &copy
}

// verifyManifest reads and decodes every referenced immutable object. A shadow
// generation cannot be promoted merely because its pointer and range metadata
// are valid.
func (s *Store) verifyManifest(manifest *Manifest) error {
	for _, ref := range manifest.Segments {
		start, err := store.ParseOffset(ref.StartExclusive)
		if err != nil {
			return fmt.Errorf("%w: verify segment start: %w", ErrCorrupt, err)
		}
		data, index, err := s.backend.Read(context.Background(), ref)
		if err != nil {
			return err
		}
		if _, err := DecodeAfter(ref, data, index, start); err != nil {
			return err
		}
	}
	return nil
}

// WaitForMessages delegates blocking reads to the authoritative primary.
func (s *Store) WaitForMessages(ctx context.Context, path string, offset store.Offset, timeout time.Duration) ([]store.Message, bool, bool, error) {
	return s.primary.WaitForMessages(ctx, path, offset, timeout)
}

// NotificationSubscriber exposes the authoritative primary's optional
// persistent wake feed without making the segment wrapper itself a
// notification authority.
func (s *Store) NotificationSubscriber() (store.NotificationSubscriber, bool) {
	if provider, ok := s.primary.(store.NotificationSubscriberProvider); ok {
		return provider.NotificationSubscriber()
	}
	subscriber, ok := s.primary.(store.NotificationSubscriber)
	return subscriber, ok
}

// GetCurrentOffset returns the authoritative primary tail.
func (s *Store) GetCurrentOffset(path string) (store.Offset, error) {
	return s.primary.GetCurrentOffset(path)
}

// Repair rebuilds a corrupt or incomplete candidate generation from the
// authoritative primary without editing any immutable object in place. The
// replacement uses generation-qualified object keys and becomes visible only
// through the normal manifest CAS.
func (s *Store) Repair(path string) (*Manifest, error) {
	lock := s.pathLock(path)
	lock.Lock()
	defer lock.Unlock()

	ctx := context.Background()
	for attempt := 0; attempt < 8; attempt++ {
		meta, err := s.primary.Get(path)
		if err != nil {
			return nil, err
		}
		identity := incarnation(path, meta)
		manifest, token, err := s.backend.Load(ctx, path)
		if err != nil {
			return nil, err
		}
		if err := validateManifest(manifest, s.backend.Mode(), path, identity); err != nil {
			return nil, err
		}
		if err := s.verifyManifest(manifest); err == nil {
			return manifest, nil
		}

		pageOpts := store.ReadPageOptions{
			TargetBytes: s.opts.TargetBytes,
			MaxFrames:   store.DefaultReadPageFrames,
			NoTouch:     true,
		}
		page, err := s.reader.ReadPage(ctx, path, store.ZeroOffset, pageOpts)
		if err != nil {
			return nil, err
		}
		primarySnapshot := page.Snapshot
		releasePrimary := func() {
			s.releasePrimarySnapshot(path, primarySnapshot)
		}
		if primarySnapshot.Incarnation != identity ||
			!store.ContentTypeMatches(primarySnapshot.ContentType, meta.ContentType) {
			releasePrimary()
			continue
		}
		next := *manifest
		next.Generation++
		next.Segments = nil
		next.SealedThrough = store.ZeroOffset.String()
		next.PublishedAtUnixN = time.Now().UnixNano()
		start := store.ZeroOffset
		for {
			if len(page.Messages) == 0 && !page.UpToDate {
				releasePrimary()
				return nil, store.ErrReadDataMissing
			}
			if len(page.Messages) > 0 {
				encoded, encodeErr := Encode(start, page.Messages, s.opts.IndexStride)
				if encodeErr != nil {
					releasePrimary()
					return nil, encodeErr
				}
				ref, putErr := s.backend.Put(ctx, path, next.Generation, encoded)
				if putErr != nil {
					releasePrimary()
					return nil, putErr
				}
				next.Segments = append(next.Segments, ref)
				start = encoded.EndInclusive
			}
			if page.UpToDate {
				break
			}
			pageOpts.Snapshot = &primarySnapshot
			page, err = s.reader.ReadPage(ctx, path, start, pageOpts)
			if err != nil {
				releasePrimary()
				return nil, err
			}
		}
		releasePrimary()
		next.SealedThrough = start.String()
		if err := validateManifest(&next, s.backend.Mode(), path, identity); err != nil {
			return nil, err
		}
		if err := s.verifyManifest(&next); err != nil {
			return nil, err
		}
		freshMeta, err := s.primary.Get(path)
		if err != nil || incarnation(path, freshMeta) != identity {
			continue
		}
		if _, err := s.backend.Publish(ctx, path, token, &next); errors.Is(err, ErrConflict) {
			continue
		} else if err != nil {
			return nil, err
		}
		s.setKnown(path, identity, start)
		s.stats.seals.Add(1)
		return &next, nil
	}
	return nil, fmt.Errorf("%w: repair %s exceeded CAS retries", ErrConflict, path)
}

// Transition changes only the read-plane pointer. Shadow↔Serving is reversible;
// Cutover is the explicit one-way rollback boundary.
func (s *Store) Transition(path string, target MigrationState) (*Manifest, error) {
	lock := s.pathLock(path)
	lock.Lock()
	defer lock.Unlock()
	if err := hit(s.opts.Faults, FaultMigration); err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 8; attempt++ {
		meta, err := s.primary.Get(path)
		if err != nil {
			return nil, err
		}
		manifest, token, err := s.backend.Load(context.Background(), path)
		if err != nil {
			return nil, err
		}
		if err := validateManifest(manifest, s.backend.Mode(), path, incarnation(path, meta)); err != nil {
			return nil, err
		}
		if err := s.verifyManifest(manifest); err != nil {
			return nil, fmt.Errorf("verify generation %d before transition: %w", manifest.Generation, err)
		}
		switch {
		case manifest.State == StateCutover && target != StateCutover:
			return nil, ErrCutover
		case target == StateCutover:
			if err := hit(s.opts.Faults, FaultCutover); err != nil {
				return nil, err
			}
		case target == StateShadow && manifest.State == StateServing:
			if err := hit(s.opts.Faults, FaultRollback); err != nil {
				return nil, err
			}
		case target != StateShadow && target != StateServing:
			return nil, fmt.Errorf("invalid migration state %q", target)
		}
		next := *manifest
		next.Generation++
		next.State = target
		next.PublishedAtUnixN = time.Now().UnixNano()
		if _, err := s.backend.Publish(context.Background(), path, token, &next); errors.Is(err, ErrConflict) {
			continue
		} else if err != nil {
			return nil, err
		}
		return &next, nil
	}
	return nil, ErrConflict
}

// PinSnapshot protects the current manifest generation from garbage
// collection for control-plane verification and migration operations.
func (s *Store) PinSnapshot(path string) (*SnapshotPin, error) {
	lock := s.pathLock(path)
	lock.Lock()
	defer lock.Unlock()

	meta, err := s.primary.Get(path)
	if err != nil {
		return nil, err
	}
	manifest, token, err := s.backend.Load(context.Background(), path)
	if err != nil {
		return nil, err
	}
	if err := validateManifest(manifest, s.backend.Mode(), path, incarnation(path, meta)); err != nil {
		return nil, err
	}
	page, err := s.reader.ReadPage(
		context.Background(),
		path,
		meta.CurrentOffset,
		store.ReadPageOptions{NoTouch: true},
	)
	if err != nil {
		return nil, err
	}
	s.releasePrimarySnapshot(path, page.Snapshot)
	if page.Snapshot.Incarnation != incarnation(path, meta) {
		return nil, fmt.Errorf("%w: stream incarnation changed while pinning manifest", ErrCorrupt)
	}

	release := s.pinToken(path, token)
	return &SnapshotPin{
		Token:    token,
		Manifest: cloneManifest(manifest),
		release:  release,
	}, nil
}

func (s *Store) pinToken(path, token string) func() {
	s.pinsMu.Lock()
	if s.pins[path] == nil {
		s.pins[path] = map[string]int{}
	}
	s.pins[path][token]++
	s.pinsMu.Unlock()
	return func() {
		s.pinsMu.Lock()
		defer s.pinsMu.Unlock()
		s.pins[path][token]--
		if s.pins[path][token] == 0 {
			delete(s.pins[path], token)
		}
		if len(s.pins[path]) == 0 {
			delete(s.pins, path)
		}
	}
}

// GC protects active snapshot pins and delegates conservative collection.
func (s *Store) GC(path string, retention GCRetention) (GCResult, error) {
	lock := s.pathLock(path)
	lock.Lock()
	defer lock.Unlock()
	s.pinsMu.Lock()
	for token := range s.pins[path] {
		retention.ProtectedTokens = append(retention.ProtectedTokens, token)
	}
	s.pinsMu.Unlock()
	return s.backend.GC(context.Background(), path, retention)
}

// Stats returns a snapshot of read-plane and backend counters.
func (s *Store) Stats() Stats {
	stats := Stats{
		Seals:                      s.stats.seals.Load(),
		SealDurationNanoseconds:    s.stats.sealDurationNanoseconds.Load(),
		SealDurationMaxNanoseconds: s.stats.sealDurationMaxNanosecond.Load(),
		SegmentReads:               s.stats.segmentReads.Load(),
		PrimaryFallbacks:           s.stats.primaryFallbacks.Load(),
		ChecksumFailures:           s.stats.checksumFailures.Load(),
		BytesServed:                s.stats.bytesServed.Load(),
	}
	if provider, ok := s.backend.(backendStatsProvider); ok {
		stats.Backend = provider.BackendStats()
	}
	return stats
}

// Close releases both the candidate backend and authoritative primary.
func (s *Store) Close() error {
	s.leasesMu.Lock()
	leases := make([]*readLease, 0, len(s.leases))
	for token, lease := range s.leases {
		leases = append(leases, lease)
		delete(s.leases, token)
	}
	s.leasesMu.Unlock()
	for _, lease := range leases {
		if lease.releasePin != nil {
			lease.releasePin()
		}
		s.releasePrimarySnapshot(lease.path, lease.primarySnapshot)
	}
	backendErr := s.backend.Close()
	primaryErr := s.primary.Close()
	return errors.Join(backendErr, primaryErr)
}
