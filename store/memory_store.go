// MemoryStore ported from the Durable Streams reference Caddy plugin
// (packages/caddy-plugin/store/memory_store.go @ 82f9963). Deviations from
// upstream: producer validation delegates to the pure ValidateProducer
// (producer.go), and the JSON-mode helpers live in json.go / store.go
// (ProcessJSONAppend, IsJSONContentType, FormatJSONResponse) instead of
// being redeclared here.
package store

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// MemoryStore is an in-memory implementation of Store for testing
type MemoryStore struct {
	mu       sync.RWMutex
	streams  map[string]*memoryStream
	longPoll *longPollManager

	// clock is the time source for sliding-TTL touches, lazy-expiry
	// decisions, and producer-state stamping. Defaults to the real wall
	// clock; the equivalence harness (issue #26) injects a FakeClock so
	// TTL/expiry is reproducible at a frozen instant.
	clock Clock

	// Per-producer locks for serializing validation+append
	// Key: "{streamPath}:{producerId}"
	producerLocks   map[string]*sync.Mutex
	producerLocksMu sync.Mutex

	// markers holds the stream-slot claim markers by path, then by
	// FenceAuthority (#183). Like the Redis marker keys they live beside the
	// stream, not inside it: a delete or expiry reaps the stream's seals and
	// bindings but leaves its markers, whose stream incarnation then matches
	// no recreated stream.
	markers map[string]map[string]memoryMarker
}

var (
	_ PageWaiter             = (*MemoryStore)(nil)
	_ NotificationSubscriber = (*MemoryStore)(nil)
	_ FencedCloser           = (*MemoryStore)(nil)
	_ WriteFenceStore        = (*MemoryStore)(nil)
)

// MemoryStoreOption configures a MemoryStore at construction.
type MemoryStoreOption func(*MemoryStore)

// WithClock injects a Clock into the MemoryStore so expiry/TTL decisions are
// driven by a controllable time source (default: the real wall clock). A nil
// clock is ignored.
func WithClock(c Clock) MemoryStoreOption {
	return func(s *MemoryStore) {
		if c != nil {
			s.clock = c
		}
	}
}

type memoryStream struct {
	metadata StreamMetadata
	messages []Message
	data     []byte // Raw accumulated data for non-JSON streams

	// Write-fence state of a fenced stream (#183): the counterpart of the
	// wfseal:<auth>, wfbind:<producer_id> and wfLastOff meta fields on Redis.
	seals         map[string]WriteFenceSeal // per FenceAuthority
	bound         map[string]int64          // producer id -> generation of its last accepted fenced write
	lastFencedOff *Offset                   // tail after the last accepted fenced-class write; nil = none
}

type longPollManager struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

type memoryNotificationSubscription struct {
	manager *longPollManager
	path    string
	wake    chan struct{}
	done    chan struct{}
	once    sync.Once
}

// NewMemoryStore creates a new in-memory store. By default it uses the real
// wall clock; pass WithClock to inject a controllable time source.
func NewMemoryStore(opts ...MemoryStoreOption) *MemoryStore {
	s := &MemoryStore{
		streams: make(map[string]*memoryStream),
		longPoll: &longPollManager{
			waiters: make(map[string][]chan struct{}),
		},
		clock:         RealClock(),
		producerLocks: make(map[string]*sync.Mutex),
		markers:       make(map[string]map[string]memoryMarker),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// now returns the store's current time via the injected clock.
func (s *MemoryStore) now() time.Time { return s.clock.Now() }

// isExpired evaluates a stream's expiry against the injected clock, so the
// MemoryStore's lazy-expiry agrees with the Redis backend at a shared frozen
// instant (issue #26).
func (s *MemoryStore) isExpired(m *StreamMetadata) bool {
	return m.IsExpiredAt(s.clock.Now())
}

// getProducerLock returns a per-producer mutex for serializing validation+append.
// This prevents race conditions when HTTP requests arrive out-of-order.
func (s *MemoryStore) getProducerLock(streamPath, producerId string) *sync.Mutex {
	key := streamPath + ":" + producerId
	s.producerLocksMu.Lock()
	defer s.producerLocksMu.Unlock()

	if mu, ok := s.producerLocks[key]; ok {
		return mu
	}
	mu := &sync.Mutex{}
	s.producerLocks[key] = mu
	return mu
}

// validateProducer validates producer headers via the pure ValidateProducer
// state machine. It returns (result, updatedState, error) where updatedState
// is nil if no update is needed.
func (s *MemoryStore) validateProducer(meta *StreamMetadata, opts AppendOptions) (AppendResult, *ProducerState, error) {
	// Get current producer state (may not exist)
	var state *ProducerState
	if meta.Producers != nil {
		state = meta.Producers[opts.ProducerId]
	}
	return ValidateProducer(state, *opts.ProducerEpoch, *opts.ProducerSeq, s.now().Unix())
}

// Create implements Store.
func (s *MemoryStore) Create(path string, opts CreateOptions) (*StreamMetadata, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if stream already exists
	if existing, ok := s.streams[path]; ok {
		if s.isExpired(&existing.metadata) {
			// Expired: delete and proceed with creation
			delete(s.streams, path)
		} else if existing.metadata.SoftDeleted {
			// Soft-deleted streams block new creation
			return nil, false, ErrStreamExists
		} else if existing.metadata.ConfigMatches(opts) {
			// Idempotent success - return false to indicate not newly created
			return &existing.metadata, false, nil
		} else {
			return nil, false, ErrConfigMismatch
		}
	}
	incarnation, err := NewIncarnationID()
	if err != nil {
		return nil, false, err
	}

	// Fork creation: validate source stream and resolve fork parameters
	var forkOffset Offset
	var sourceContentType string
	var sourceMeta *StreamMetadata
	var sourceStream *memoryStream
	var binarySubOffsetPrefix []byte
	isFork := opts.ForkedFrom != ""

	if isFork {
		ss, ok := s.streams[opts.ForkedFrom]
		if !ok {
			return nil, false, ErrStreamNotFound
		}
		if ss.metadata.SoftDeleted {
			return nil, false, ErrStreamSoftDeleted
		}
		if s.isExpired(&ss.metadata) {
			return nil, false, ErrStreamNotFound
		}

		sourceStream = ss
		sourceMeta = &ss.metadata
		sourceContentType = sourceMeta.ContentType

		// Reject a content-type mismatch up front, before taking a reference on
		// the source. Doing this after the refcount increment would leak a
		// reference on the failed fork and pin the source in a soft-deleted
		// state forever.
		if opts.ContentType != "" && !strings.EqualFold(opts.ContentType, sourceContentType) {
			return nil, false, ErrContentTypeMismatch
		}

		// Resolve fork offset: use opts.ForkOffset if set, else source's CurrentOffset
		if opts.ForkOffset != nil {
			forkOffset = *opts.ForkOffset
		} else {
			forkOffset = sourceMeta.CurrentOffset
		}

		// Validate: ZeroOffset <= forkOffset <= source.CurrentOffset
		if forkOffset.LessThan(ZeroOffset) || sourceMeta.CurrentOffset.LessThan(forkOffset) {
			return nil, false, ErrInvalidForkOffset
		}

		// Resolve sub-offset against the source stream
		if opts.ForkSubOffset != nil && *opts.ForkSubOffset > 0 {
			resolvedOffset, prefixBytes, err := s.resolveForkSubOffset(sourceStream, forkOffset, *opts.ForkSubOffset)
			if err != nil {
				return nil, false, err
			}
			if IsJSONContentType(sourceMeta.ContentType) {
				forkOffset = resolvedOffset
			} else {
				binarySubOffsetPrefix = prefixBytes
			}
		}

		// Increment source refcount
		sourceStream.metadata.RefCount++
	}

	// Determine content type: use opts.ContentType, or inherit from source if
	// fork. A fork content-type mismatch is already rejected above, before the
	// source refcount is taken.
	contentType := opts.ContentType
	if contentType == "" {
		if isFork {
			contentType = sourceContentType
		} else {
			contentType = "application/octet-stream"
		}
	}

	// Build metadata
	now := s.now()
	meta := StreamMetadata{
		Path:           path,
		Incarnation:    incarnation,
		ContentType:    contentType,
		CreatedAt:      now,
		LastAccessedAt: now,
		Closed:         opts.Closed,     // Support creating stream in closed state
		WriteFence:     opts.WriteFence, // Never inherited: a fork declares its own (#183)
	}

	if isFork {
		forkTTL, forkExpiresAt := s.resolveForkExpiry(opts, *sourceMeta)
		meta.CurrentOffset = forkOffset
		meta.ForkOffset = forkOffset
		meta.ForkedFrom = opts.ForkedFrom
		meta.TTLSeconds = forkTTL
		meta.ExpiresAt = forkExpiresAt
		// Persist the user-supplied ForkOffset (may be nil if omitted) and
		// the user-supplied ForkSubOffset for idempotent re-creation matching.
		// These differ from meta.ForkOffset for JSON forks created with
		// sub-offset > 0 (where meta.ForkOffset is advanced internally).
		if opts.ForkOffset != nil {
			requested := *opts.ForkOffset
			meta.ForkOffsetRequested = &requested
		}
		if opts.ForkSubOffset != nil {
			meta.ForkSubOffset = *opts.ForkSubOffset
		}
	} else {
		meta.CurrentOffset = ZeroOffset
		meta.TTLSeconds = opts.TTLSeconds
		meta.ExpiresAt = opts.ExpiresAt
	}

	stream := &memoryStream{
		metadata: meta,
		messages: make([]Message, 0),
		data:     make([]byte, 0),
	}

	// Materialize binary sub-offset prefix as the fork's first own message.
	if isFork && len(binarySubOffsetPrefix) > 0 {
		newOffset := stream.metadata.CurrentOffset.Add(uint64(len(binarySubOffsetPrefix)))
		stream.messages = append(stream.messages, Message{
			Data:   binarySubOffsetPrefix,
			Offset: newOffset,
		})
		stream.data = append(stream.data, binarySubOffsetPrefix...)
		stream.metadata.CurrentOffset = newOffset
	}

	// Handle initial data
	if len(opts.InitialData) > 0 {
		newOffset, err := s.appendToStream(stream, opts.InitialData, true) // Allow empty arrays on create
		if err != nil {
			// Rollback source refcount on failure
			if isFork {
				if sourceStream, ok := s.streams[opts.ForkedFrom]; ok {
					sourceStream.metadata.RefCount--
				}
			}
			return nil, false, err
		}
		stream.metadata.CurrentOffset = newOffset
	}

	s.streams[path] = stream
	return &stream.metadata, true, nil // true = newly created
}

// resolveForkExpiry resolves fork TTL/expiry per the decision table.
// Forks have independent lifetimes — no capping at source expiry.
func (s *MemoryStore) resolveForkExpiry(opts CreateOptions, sourceMeta StreamMetadata) (*int64, *time.Time) {
	// Fork explicitly requests TTL — use it
	if opts.TTLSeconds != nil {
		return opts.TTLSeconds, nil
	}

	// Fork explicitly requests Expires-At — use it
	if opts.ExpiresAt != nil {
		return nil, opts.ExpiresAt
	}

	// No expiry requested — inherit from source
	if sourceMeta.TTLSeconds != nil {
		ttl := *sourceMeta.TTLSeconds
		return &ttl, nil
	}
	if sourceMeta.ExpiresAt != nil {
		t := *sourceMeta.ExpiresAt
		return nil, &t
	}

	// Source has no expiry either
	return nil, nil
}

// Get implements Store.
func (s *MemoryStore) Get(path string) (*StreamMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, ok := s.streams[path]
	if !ok {
		return nil, ErrStreamNotFound
	}

	// Check if stream is soft-deleted (external callers shouldn't see them)
	if stream.metadata.SoftDeleted {
		return nil, ErrStreamSoftDeleted
	}

	// Check if stream has expired
	if s.isExpired(&stream.metadata) {
		return nil, ErrStreamNotFound // Return not found for expired streams
	}

	meta := stream.metadata // Copy
	return &meta, nil
}

// Has implements Store.
func (s *MemoryStore) Has(path string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stream, ok := s.streams[path]
	if !ok {
		return false
	}
	// Soft-deleted streams are not visible
	if stream.metadata.SoftDeleted {
		return false
	}
	// Check if stream has expired
	return !s.isExpired(&stream.metadata)
}

// Delete implements Store.
func (s *MemoryStore) Delete(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[path]
	if !ok {
		return ErrStreamNotFound
	}

	// Already soft-deleted: the stream is gone for direct operations (a
	// soft-deleted stream returns 410 Gone for GET/HEAD/POST/DELETE).
	if stream.metadata.SoftDeleted {
		return ErrStreamSoftDeleted
	}

	// If there are forks referencing this stream, soft-delete instead
	if stream.metadata.RefCount > 0 {
		stream.metadata.SoftDeleted = true
		return nil
	}

	// RefCount == 0: full delete with cascading GC
	return s.deleteWithCascade(path)
}

// deleteWithCascade fully deletes a stream and cascades to soft-deleted parents
// whose refcount drops to zero. Caller must hold s.mu.
func (s *MemoryStore) deleteWithCascade(path string) error {
	stream, ok := s.streams[path]
	if !ok {
		return nil
	}

	forkedFrom := stream.metadata.ForkedFrom

	// Delete this stream's data
	delete(s.streams, path)

	// Cancel long-poll waiters for this stream
	s.longPoll.notify(path)

	// If this stream is a fork, decrement the source's refcount
	if forkedFrom != "" {
		parent, ok := s.streams[forkedFrom]
		if ok {
			parent.metadata.RefCount--

			if parent.metadata.RefCount < 0 {
				// Bug: refcount should never go negative
				parent.metadata.RefCount = 0
				return ErrRefCountUnderflow
			}

			// If parent refcount hit 0 and parent is soft-deleted, cascade
			if parent.metadata.RefCount == 0 && parent.metadata.SoftDeleted {
				return s.deleteWithCascade(forkedFrom)
			}
		}
	}

	return nil
}

// CloseStream closes a stream without appending data
func (s *MemoryStore) CloseStream(path string) (*CloseResult, error) {
	return s.closeStream(path, nil)
}

// CloseStreamFenced closes a stream only while the supplied claim still owns
// its live stream-slot fence (FencedCloser), through the same rung as Append.
func (s *MemoryStore) CloseStreamFenced(path string, fence auth.AppendFence) (*CloseResult, error) {
	if !fence.Complete() {
		return nil, ErrAppendFenced
	}
	return s.closeStream(path, &fence)
}

func (s *MemoryStore) closeStream(path string, fence *auth.AppendFence) (*CloseResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[path]
	if !ok {
		return nil, ErrStreamNotFound
	}

	// Check if stream has expired
	if s.isExpired(&stream.metadata) {
		return nil, ErrStreamNotFound
	}

	// Write fence (#183): the same rung as Append, one decision with the
	// close; a holder's close is not a seal. The final offset is not
	// disclosed on refusal.
	if out := s.fenceRung(path, stream, fence, "", nil); out.Reason != FenceNone {
		return &CloseResult{FenceReason: out.Reason, FenceGeneration: out.Generation, FenceHolder: out.Holder}, ErrAppendFenced
	}

	alreadyClosed := stream.metadata.Closed
	stream.metadata.Closed = true

	// Notify pending long-polls that stream is closed
	s.longPoll.notifyClosed(path)

	return &CloseResult{
		FinalOffset:   stream.metadata.CurrentOffset,
		AlreadyClosed: alreadyClosed,
	}, nil
}

// CloseStreamWithProducer closes a stream without appending data, using producer headers.
func (s *MemoryStore) CloseStreamWithProducer(path string, opts CloseProducerOptions) (*CloseProducerResult, error) {
	if opts.Fence != nil && !opts.Fence.Complete() {
		return nil, ErrAppendFenced
	}
	// Acquire per-producer lock for serialization
	producerLock := s.getProducerLock(path, opts.ProducerId)
	producerLock.Lock()
	defer producerLock.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[path]
	if !ok {
		return nil, ErrStreamNotFound
	}

	// Check if stream has expired
	if s.isExpired(&stream.metadata) {
		return nil, ErrStreamNotFound
	}

	// Write fence (#183): seal, claim marker, epoch binding, bound producer,
	// decided before the closed check exactly as in close.lua.
	if out := s.fenceRung(path, stream, opts.Fence, opts.ProducerId, &opts.ProducerEpoch); out.Reason != FenceNone {
		return &CloseProducerResult{FenceReason: out.Reason, FenceGeneration: out.Generation, FenceHolder: out.Holder}, ErrAppendFenced
	}

	// If already closed, check if this is a duplicate of the closing request
	if stream.metadata.Closed {
		if stream.metadata.ClosedBy != nil &&
			stream.metadata.ClosedBy.ProducerId == opts.ProducerId &&
			stream.metadata.ClosedBy.Epoch == opts.ProducerEpoch &&
			stream.metadata.ClosedBy.Seq == opts.ProducerSeq {
			return &CloseProducerResult{
				FinalOffset:    stream.metadata.CurrentOffset,
				ProducerResult: ProducerResultDuplicate,
				LastSeq:        opts.ProducerSeq,
				StreamClosed:   true,
				AlreadyClosed:  true,
			}, nil
		}

		return &CloseProducerResult{
			FinalOffset:   stream.metadata.CurrentOffset,
			StreamClosed:  true,
			AlreadyClosed: true,
		}, ErrStreamClosed
	}

	// Validate producer state
	appendOpts := AppendOptions{
		ProducerId:    opts.ProducerId,
		ProducerEpoch: &opts.ProducerEpoch,
		ProducerSeq:   &opts.ProducerSeq,
	}
	result, newState, err := s.validateProducer(&stream.metadata, appendOpts)
	if err != nil {
		return &CloseProducerResult{
			FinalOffset:    stream.metadata.CurrentOffset,
			ProducerResult: result.ProducerResult,
			CurrentEpoch:   result.CurrentEpoch,
			ExpectedSeq:    result.ExpectedSeq,
			ReceivedSeq:    result.ReceivedSeq,
			LastSeq:        result.LastSeq,
			StreamClosed:   stream.metadata.Closed,
		}, err
	}

	if result.ProducerResult == ProducerResultDuplicate {
		return &CloseProducerResult{
			FinalOffset:    stream.metadata.CurrentOffset,
			ProducerResult: ProducerResultDuplicate,
			LastSeq:        result.LastSeq,
			StreamClosed:   stream.metadata.Closed,
			AlreadyClosed:  stream.metadata.Closed,
		}, nil
	}

	// Accept: commit producer state and close stream
	if stream.metadata.Producers == nil {
		stream.metadata.Producers = make(map[string]*ProducerState)
	}
	stream.metadata.Producers[opts.ProducerId] = newState
	stream.metadata.Closed = true
	stream.metadata.ClosedBy = &ClosedByProducer{
		ProducerId: opts.ProducerId,
		Epoch:      opts.ProducerEpoch,
		Seq:        opts.ProducerSeq,
	}
	stream.bind(opts.Fence, opts.ProducerId, stream.metadata.CurrentOffset)

	// Notify pending long-polls that stream is closed
	s.longPoll.notifyClosed(path)

	return &CloseProducerResult{
		FinalOffset:    stream.metadata.CurrentOffset,
		ProducerResult: result.ProducerResult,
		LastSeq:        result.LastSeq,
		StreamClosed:   true,
		AlreadyClosed:  false,
	}, nil
}

// Append implements Store.
func (s *MemoryStore) Append(path string, data []byte, opts AppendOptions) (AppendResult, error) {
	if opts.Fence != nil && !opts.Fence.Complete() {
		return AppendResult{}, ErrAppendFenced
	}
	// Validate producer headers - must be all or none
	if opts.HasProducerHeaders() && !opts.HasAllProducerHeaders() {
		return AppendResult{}, ErrPartialProducer
	}

	// If producer headers provided, acquire per-producer lock for serialization
	if opts.HasAllProducerHeaders() {
		producerLock := s.getProducerLock(path, opts.ProducerId)
		producerLock.Lock()
		defer producerLock.Unlock()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[path]
	if !ok {
		return AppendResult{}, ErrStreamNotFound
	}

	// Check if stream is soft-deleted
	if stream.metadata.SoftDeleted {
		return AppendResult{}, ErrStreamSoftDeleted
	}

	// Check if stream has expired
	if s.isExpired(&stream.metadata) {
		return AppendResult{}, ErrStreamNotFound
	}

	// Write fence (#183): seal, claim marker, epoch binding, bound producer,
	// decided before the sliding-TTL touch so a refusal leaves the window as
	// it was (append.lua step 4). The tail is not disclosed on refusal: a
	// deposed writer stands down.
	if out := s.fenceRung(path, stream, opts.Fence, opts.ProducerId, opts.ProducerEpoch); out.Reason != FenceNone {
		return AppendResult{FenceReason: out.Reason, FenceGeneration: out.Generation, FenceHolder: out.Holder}, ErrAppendFenced
	}

	// Refresh TTL sliding window
	stream.metadata.LastAccessedAt = s.now()

	// Check if stream is closed
	if stream.metadata.Closed {
		// Check if this is a duplicate of the closing request (idempotent producer)
		if opts.HasAllProducerHeaders() && stream.metadata.ClosedBy != nil &&
			stream.metadata.ClosedBy.ProducerId == opts.ProducerId &&
			stream.metadata.ClosedBy.Epoch == *opts.ProducerEpoch &&
			stream.metadata.ClosedBy.Seq == *opts.ProducerSeq {
			// Idempotent success - duplicate of closing request
			return AppendResult{
				Offset:         stream.metadata.CurrentOffset,
				ProducerResult: ProducerResultDuplicate,
				LastSeq:        *opts.ProducerSeq,
				StreamClosed:   true,
			}, nil
		}
		// Stream is closed - reject append
		return AppendResult{
			Offset:       stream.metadata.CurrentOffset,
			StreamClosed: true,
		}, ErrStreamClosed
	}

	// Validate content type if provided
	if opts.ContentType != "" && !ContentTypeMatches(stream.metadata.ContentType, opts.ContentType) {
		return AppendResult{}, ErrContentTypeMismatch
	}

	// Validate producer FIRST (if headers provided)
	// This must happen before Stream-Seq validation so that retries
	// are deduplicated at the transport layer even if Stream-Seq would conflict.
	var producerState *ProducerState
	producerResult := ProducerResultNone
	var producerLastSeq int64
	if opts.HasAllProducerHeaders() {
		result, newState, err := s.validateProducer(&stream.metadata, opts)
		if err != nil {
			result.Offset = stream.metadata.CurrentOffset
			return result, err
		}
		if result.ProducerResult == ProducerResultDuplicate {
			// Duplicate - return current offset, no append needed
			return AppendResult{
				Offset:         stream.metadata.CurrentOffset,
				ProducerResult: ProducerResultDuplicate,
				LastSeq:        result.LastSeq,
			}, nil
		}
		producerState = newState
		producerResult = result.ProducerResult
		producerLastSeq = result.LastSeq
	}

	// Validate sequence number if provided (Stream-Seq - application layer)
	// Only checked for non-duplicate appends.
	if opts.Seq != "" {
		if stream.metadata.LastSeq != "" && opts.Seq <= stream.metadata.LastSeq {
			return AppendResult{}, ErrSequenceConflict
		}
	}

	newOffset, err := s.appendToStream(stream, data, false) // Don't allow empty arrays on append
	if err != nil {
		return AppendResult{}, err
	}

	stream.metadata.CurrentOffset = newOffset
	stream.bind(opts.Fence, opts.ProducerId, newOffset)
	if opts.Seq != "" {
		stream.metadata.LastSeq = opts.Seq
	}
	if producerState != nil {
		if stream.metadata.Producers == nil {
			stream.metadata.Producers = make(map[string]*ProducerState)
		}
		stream.metadata.Producers[opts.ProducerId] = producerState
	}

	// Handle stream closure if requested
	streamClosed := false
	if opts.Close {
		stream.metadata.Closed = true
		streamClosed = true
		// Track which producer tuple closed the stream for idempotent duplicate detection
		if opts.HasAllProducerHeaders() {
			stream.metadata.ClosedBy = &ClosedByProducer{
				ProducerId: opts.ProducerId,
				Epoch:      *opts.ProducerEpoch,
				Seq:        *opts.ProducerSeq,
			}
		}
		// Notify pending long-polls that stream is closed
		s.longPoll.notifyClosed(path)
	}

	// Notify long-poll waiters
	s.longPoll.notify(path)

	return AppendResult{
		Offset:         newOffset,
		ProducerResult: producerResult,
		LastSeq:        producerLastSeq,
		StreamClosed:   streamClosed,
	}, nil
}

// appendToStream handles the actual append logic, including JSON mode
func (s *MemoryStore) appendToStream(stream *memoryStream, data []byte, allowEmpty bool) (Offset, error) {
	isJSON := IsJSONContentType(stream.metadata.ContentType)

	if isJSON {
		// JSON mode: parse and potentially flatten arrays
		messages, err := ProcessJSONAppend(data, allowEmpty)
		if err != nil {
			return Offset{}, err
		}

		currentOffset := stream.metadata.CurrentOffset
		for _, msgData := range messages {
			currentOffset = currentOffset.Add(uint64(len(msgData)))
			stream.messages = append(stream.messages, Message{
				Data:   msgData,
				Offset: currentOffset,
			})
		}
		return currentOffset, nil
	}

	// Non-JSON mode: store raw bytes
	newOffset := stream.metadata.CurrentOffset.Add(uint64(len(data)))
	stream.messages = append(stream.messages, Message{
		Data:   data,
		Offset: newOffset,
	})
	stream.data = append(stream.data, data...)
	return newOffset, nil
}

// readOwnMessages reads messages from a single stream's own messages slice,
// returning those with offset > the given offset. It does NOT follow fork chains.
// If capAtOffset is non-nil, messages at or beyond that offset are excluded.
func readOwnMessages(stream *memoryStream, offset Offset, capAtOffset *Offset) []Message {
	var messages []Message
	for _, msg := range stream.messages {
		if msg.Offset.ByteOffset > offset.ByteOffset {
			if capAtOffset != nil && !msg.Offset.LessThanOrEqual(*capAtOffset) {
				break
			}
			messages = append(messages, msg)
		}
	}
	return messages
}

// resolveForkSubOffset walks the source stream from forkOffset and resolves a
// non-zero sub-offset. For JSON sources the sub-offset counts flattened
// messages and advances the fork offset; for binary sources it counts bytes
// into the first following message, returned as a prefix the fork must
// materialize as its first own message.
func (s *MemoryStore) resolveForkSubOffset(sourceStream *memoryStream, forkOffset Offset, subOffset uint64) (Offset, []byte, error) {
	// Read the source from forkOffset onward (across its fork chain if any)
	sourceMessages := s.readForkedStream(sourceStream, forkOffset)

	if IsJSONContentType(sourceStream.metadata.ContentType) {
		if uint64(len(sourceMessages)) < subOffset {
			return Offset{}, nil, ErrInvalidForkSubOffset
		}
		return sourceMessages[subOffset-1].Offset, nil, nil
	}

	// Binary: at least one message must follow forkOffset
	if len(sourceMessages) == 0 {
		return Offset{}, nil, ErrInvalidForkSubOffset
	}
	first := sourceMessages[0].Data
	if uint64(len(first)) < subOffset {
		return Offset{}, nil, ErrInvalidForkSubOffset
	}
	prefix := make([]byte, subOffset)
	copy(prefix, first[:subOffset])
	return forkOffset, prefix, nil
}

// readForkedStream reads messages across the fork chain. For non-forks it delegates
// to readOwnMessages. For forks, it reads inherited messages from the source chain
// (capped at ForkOffset) and then the fork's own messages, concatenating the results.
// This method does NOT check SoftDeleted — forks must read through soft-deleted sources.
func (s *MemoryStore) readForkedStream(stream *memoryStream, offset Offset) []Message {
	if stream.metadata.ForkedFrom == "" {
		// Not a fork: just read own messages, no cap
		return readOwnMessages(stream, offset, nil)
	}

	var inherited []Message

	// Only read from source if the requested offset is before the fork point
	if offset.LessThan(stream.metadata.ForkOffset) {
		sourceStream, ok := s.streams[stream.metadata.ForkedFrom]
		if ok {
			// Recursively read from source (source may itself be a fork)
			sourceMessages := s.readForkedStream(sourceStream, offset)
			// Cap at ForkOffset — source appends after fork creation are not visible
			for _, msg := range sourceMessages {
				if msg.Offset.LessThanOrEqual(stream.metadata.ForkOffset) {
					inherited = append(inherited, msg)
				}
			}
		}
	}

	// Read fork's own messages (offset >= ForkOffset)
	ownMessages := readOwnMessages(stream, offset, nil)

	if len(inherited) == 0 {
		return ownMessages
	}
	if len(ownMessages) == 0 {
		return inherited
	}
	return append(inherited, ownMessages...)
}

func memoryReadIncarnation(meta *StreamMetadata) string {
	if meta.Incarnation != "" {
		return meta.Incarnation
	}
	return strconv.FormatInt(meta.CreatedAt.UnixNano(), 10)
}

// walkForkedMessages visits messages in the same inherited-prefix then
// child-suffix order as readForkedStream. upper is inclusive. Returning false
// from visit stops the walk without materializing the remaining suffix.
func (s *MemoryStore) walkForkedMessages(stream *memoryStream, offset, upper Offset, visit func(Message) bool) bool {
	if stream.metadata.ForkedFrom != "" && offset.LessThan(stream.metadata.ForkOffset) {
		sourceUpper := upper
		if stream.metadata.ForkOffset.LessThan(sourceUpper) {
			sourceUpper = stream.metadata.ForkOffset
		}
		if source, ok := s.streams[stream.metadata.ForkedFrom]; ok {
			if !s.walkForkedMessages(source, offset, sourceUpper, visit) {
				return false
			}
		}
	}

	for _, msg := range stream.messages {
		if !offset.LessThan(msg.Offset) {
			continue
		}
		if upper.LessThan(msg.Offset) {
			break
		}
		if !visit(msg) {
			return false
		}
	}
	return true
}

// ReadPage implements PageReader.
func (s *MemoryStore) ReadPage(ctx context.Context, path string, offset Offset, opts ReadPageOptions) (ReadPage, error) {
	opts = opts.Normalize()
	if err := ctx.Err(); err != nil {
		return ReadPage{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[path]
	if !ok {
		return ReadPage{}, ErrStreamNotFound
	}

	// Check if stream has expired
	if s.isExpired(&stream.metadata) {
		if stream.metadata.RefCount > 0 {
			// Expiry with active forks: treat as soft-delete
			stream.metadata.SoftDeleted = true
		}
		return ReadPage{}, ErrStreamNotFound
	}

	// Soft-deleted streams are not visible for direct reads.
	if stream.metadata.SoftDeleted {
		return ReadPage{}, ErrStreamSoftDeleted
	}

	incarnation := memoryReadIncarnation(&stream.metadata)
	var snapshot ReadSnapshot
	if opts.Snapshot == nil {
		snapshot = ReadSnapshotFromMetadata(&stream.metadata)
		snapshot.Incarnation = incarnation
	} else {
		snapshot = *opts.Snapshot
		current := ReadSnapshotFromMetadata(&stream.metadata)
		current.Incarnation = incarnation
		if !SameReadStream(snapshot, current) {
			return ReadPage{}, ErrReadSnapshotChanged
		}
	}
	// One logical client read renews the root stream once, when it captures its
	// snapshot. Continuation pages and internal sealing must not extend the TTL.
	if !opts.NoTouch && opts.Snapshot == nil && ShouldRenewReadAccess(&stream.metadata) {
		stream.metadata.LastAccessedAt = s.now()
	}

	readOffset := offset
	if readOffset.IsNow() {
		readOffset = snapshot.Tail
	}
	page := ReadPage{
		NextOffset: readOffset,
		Snapshot:   snapshot,
		Stats: ReadPageStats{
			RequestedBytes: opts.TargetBytes,
		},
	}
	messages := make([]Message, 0, min(opts.MaxFrames, 16))
	s.walkForkedMessages(stream, readOffset, snapshot.Tail, func(msg Message) bool {
		if err := ctx.Err(); err != nil {
			return false
		}
		if len(messages) >= opts.MaxFrames {
			return false
		}

		n := len(msg.Data)
		page.Stats.FetchedBytes += n
		if len(messages) > 0 && page.Stats.ReturnedBytes+n > opts.TargetBytes {
			page.Stats.DiscardedBytes += n
			return false
		}

		messages = append(messages, msg)
		page.Stats.ReturnedBytes += n
		page.NextOffset = msg.Offset
		return page.Stats.ReturnedBytes < opts.TargetBytes
	})
	if err := ctx.Err(); err != nil {
		return ReadPage{}, err
	}
	page.Messages = messages

	if len(messages) == 0 {
		// This matches the HTTP read path's historical behavior for empty,
		// stale, and beyond-tail reads.
		page.NextOffset = snapshot.Tail
	}
	page.UpToDate = page.NextOffset.Equal(snapshot.Tail)
	return page, nil
}

// WaitForPage implements PageWaiter. Registration happens before the first
// no-touch recheck, closing the append race without renewing the logical read a
// second time.
func (s *MemoryStore) WaitForPage(
	ctx context.Context,
	path string,
	offset Offset,
	initial ReadSnapshot,
	timeout time.Duration,
	opts ReadPageOptions,
) (ReadWaitResult, error) {
	ch := make(chan struct{}, 1)
	s.longPoll.register(path, ch)
	defer s.longPoll.unregister(path, ch)

	recheck := func() (ReadPage, bool, error) {
		recheckOpts := opts
		recheckOpts.Snapshot = nil
		recheckOpts.NoTouch = true
		page, err := s.ReadPage(ctx, path, offset, recheckOpts)
		if err != nil {
			return ReadPage{}, false, err
		}
		if !SameReadStream(initial, page.Snapshot) {
			return ReadPage{}, false, ErrReadSnapshotChanged
		}
		done := len(page.Messages) > 0 ||
			(page.Snapshot.Closed && offset.Equal(page.Snapshot.Tail))
		return page, done, nil
	}

	if page, done, err := recheck(); err != nil {
		return ReadWaitResult{}, err
	} else if done {
		return ReadWaitResult{Page: page}, nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ch:
			page, done, err := recheck()
			if err != nil {
				return ReadWaitResult{}, err
			}
			if done {
				return ReadWaitResult{Page: page}, nil
			}
		case <-timer.C:
			page, done, err := recheck()
			if err != nil {
				return ReadWaitResult{}, err
			}
			return ReadWaitResult{Page: page, TimedOut: !done}, nil
		case <-ctx.Done():
			return ReadWaitResult{}, ctx.Err()
		}
	}
}

// SubscribeNotifications exposes the same register-before-read wake seam used
// by the Redis store. Events are hints, so the in-memory implementation can
// coalesce append, close, and delete into NotificationAppend.
func (s *MemoryStore) SubscribeNotifications(
	ctx context.Context,
	path string,
) (NotificationSubscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sub := &memoryNotificationSubscription{
		manager: s.longPoll,
		path:    path,
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	s.longPoll.register(path, sub.wake)
	return sub, nil
}

func (s *memoryNotificationSubscription) Wait(ctx context.Context) (NotificationEvent, error) {
	select {
	case <-s.wake:
		return NotificationAppend, nil
	case <-s.done:
		return NotificationAppend, ErrNotificationSubscriptionClosed
	case <-ctx.Done():
		return NotificationAppend, ctx.Err()
	}
}

func (s *memoryNotificationSubscription) Close() error {
	s.once.Do(func() {
		s.manager.unregister(s.path, s.wake)
		close(s.done)
	})
	return nil
}

// Read implements Store by concatenating bounded pages for one snapshot.
func (s *MemoryStore) Read(path string, offset Offset) ([]Message, bool, error) {
	var (
		messages []Message
		snapshot *ReadSnapshot
		next     = offset
	)
	for {
		page, err := s.ReadPage(context.Background(), path, next, ReadPageOptions{Snapshot: snapshot})
		if err != nil {
			if errors.Is(err, ErrStreamSoftDeleted) {
				err = ErrStreamNotFound
			}
			return nil, false, err
		}
		if snapshot == nil {
			captured := page.Snapshot
			snapshot = &captured
		}
		messages = append(messages, page.Messages...)
		if page.UpToDate {
			upToDate := len(messages) > 0 || offset.Equal(snapshot.Tail) || snapshot.Tail.Equal(ZeroOffset)
			return messages, upToDate, nil
		}
		if page.NextOffset.Equal(next) {
			return nil, false, errors.New("memory read page made no progress")
		}
		next = page.NextOffset
	}
}

// WaitForMessages implements Store.
func (s *MemoryStore) WaitForMessages(ctx context.Context, path string, offset Offset, timeout time.Duration) ([]Message, bool, bool, error) {
	// First check if there are already messages. One bounded page is enough to
	// decide whether the waiter should return.
	page, err := s.ReadPage(ctx, path, offset, ReadPageOptions{})
	if err != nil {
		return nil, false, false, err
	}
	if len(page.Messages) > 0 {
		return page.Messages, false, false, nil
	}
	if page.Snapshot.Closed && offset.Equal(page.Snapshot.Tail) {
		return nil, false, true, nil
	}

	// For forks: if offset is in the inherited range (< ForkOffset),
	// inherited data exists in the source. The Read call above should have
	// returned it already, but if the source is missing/empty, don't wait
	// — inherited data will never arrive via long-poll notifications
	// (source appends don't notify fork waiters).
	if page.Snapshot.ForkedFrom != "" && offset.LessThan(page.Snapshot.ForkOffset) {
		return nil, false, false, nil
	}

	result, err := s.WaitForPage(ctx, path, offset, page.Snapshot, timeout, ReadPageOptions{})
	if err != nil {
		return nil, false, false, err
	}
	closed := result.Page.Snapshot.Closed &&
		offset.Equal(result.Page.Snapshot.Tail) &&
		len(result.Page.Messages) == 0
	return result.Page.Messages, result.TimedOut, closed, nil
}

// GetCurrentOffset implements Store.
func (s *MemoryStore) GetCurrentOffset(path string) (Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, ok := s.streams[path]
	if !ok {
		return Offset{}, ErrStreamNotFound
	}
	return stream.metadata.CurrentOffset, nil
}

// Close implements Store.
func (s *MemoryStore) Close() error {
	return nil
}

// FormatResponse formats messages for HTTP response based on content type
func (s *MemoryStore) FormatResponse(path string, messages []Message) ([]byte, error) {
	s.mu.RLock()
	stream, ok := s.streams[path]
	s.mu.RUnlock()

	if !ok {
		return nil, ErrStreamNotFound
	}

	if IsJSONContentType(stream.metadata.ContentType) {
		return FormatJSONResponse(messages), nil
	}

	// Non-JSON: concatenate raw data
	var buf bytes.Buffer
	for _, msg := range messages {
		buf.Write(msg.Data)
	}
	return buf.Bytes(), nil
}

// ---- write fence (#183) ----

// appendFenceRetention keeps a revoked marker beyond the longest accepted
// write-token lifetime: the value the Redis backend uses for its marker key
// TTL (store/redis/append_fence.go). It only bounds stale marker cleanup. The
// live fence is the marker lease, and on a write-fenced stream the
// per-authority seal is what keeps a sealed generation from being re-granted.
const appendFenceRetention = 2 * time.Minute

// memoryMarker is one stream-slot claim marker with its reaper deadline. On
// Redis the marker is reaped by a key TTL on the server's wall clock, never by
// the scripts' now argument, so expiresAt is wall-clock here too: the injected
// Clock drives the lease (the live fence), not the retention. Tests simulate
// the reaper by dropping the entry, as the Redis tests DEL the key.
type memoryMarker struct {
	WriteFenceMarker
	expiresAt time.Time
}

// marker returns one authority's marker on path as the fence rung reads it,
// reaping it lazily once its retention has lapsed. Caller holds s.mu.
func (s *MemoryStore) marker(path, authority string) WriteFenceMarker {
	m, ok := s.markers[path][authority]
	if !ok {
		return WriteFenceMarker{}
	}
	if time.Now().After(m.expiresAt) {
		delete(s.markers[path], authority)
		return WriteFenceMarker{}
	}
	return m.WriteFenceMarker
}

// setMarker installs or overwrites one authority's marker on path, retained
// until expiresAt. Caller holds s.mu.
func (s *MemoryStore) setMarker(path, authority string, m WriteFenceMarker, expiresAt time.Time) {
	if s.markers[path] == nil {
		s.markers[path] = make(map[string]memoryMarker)
	}
	m.Present = true
	s.markers[path][authority] = memoryMarker{WriteFenceMarker: m, expiresAt: expiresAt}
}

// tombstone revokes one authority's marker on path at fence's generation,
// unless a newer generation, or another claim at this one, holds it: the
// STALE reply of revoke_append_fence.lua and seal_append_fence.lua, which
// mutates nothing. It reports whether the tombstone was written. Caller holds
// s.mu.
func (s *MemoryStore) tombstone(path, authority string, fence auth.AppendFence) bool {
	cur := s.marker(path, authority)
	if cur.Present && (fence.Generation < cur.Generation ||
		(fence.Generation == cur.Generation && (cur.WakeID != fence.WakeID || cur.Holder != fence.Holder))) {
		return false
	}
	s.setMarker(path, authority, WriteFenceMarker{
		State:             WriteFenceMarkerRevoked,
		Generation:        fence.Generation,
		WakeID:            fence.WakeID,
		Holder:            fence.Holder,
		StreamIncarnation: cur.StreamIncarnation, // the scripts' HSET leaves this field as it was
	}, time.Now().Add(appendFenceRetention))
	return true
}

// seal records authority's seal at generation on the stream's definite last
// fenced-class offset (the tail when the class never wrote) and refreshes the
// HEAD summary, as the seal writers do on Redis.
func (st *memoryStream) seal(authority string, generation int64, wakeID string) WriteFenceSeal {
	off := st.metadata.CurrentOffset
	if st.lastFencedOff != nil {
		off = *st.lastFencedOff
	}
	if st.seals == nil {
		st.seals = make(map[string]WriteFenceSeal)
	}
	sealed := WriteFenceSeal{Present: true, Generation: generation, WakeID: wakeID, Offset: off}
	st.seals[authority] = sealed
	st.metadata.SealedGeneration = generation
	st.metadata.SealedOffset = &off
	return sealed
}

// bind records an accepted fenced-class write on a write-fenced stream: the
// class's last offset (what a seal fixes — never a later open-class tail) and
// the producer id bound to the fence at this generation, as fence_bind in
// common.lua does. A no-op for the open class and on streams that never opted
// in.
func (st *memoryStream) bind(fence *auth.AppendFence, producerID string, off Offset) {
	if fence == nil || !st.metadata.WriteFence {
		return
	}
	st.lastFencedOff = &off
	if st.bound == nil {
		st.bound = make(map[string]int64)
	}
	st.bound[producerID] = fence.Generation
}

// fenceRung is the write-fence rung of the append ladder, shared by Append and
// the close paths as fence_rung in common.lua is shared by append.lua and
// close.lua: it gathers EvaluateWriteFence's inputs — the request authority's
// marker and seal for the fenced class, the producer's binding for the open
// class — and returns the decision. epoch is nil when the request carries no
// producer headers. Caller holds s.mu.
func (s *MemoryStore) fenceRung(path string, stream *memoryStream, fence *auth.AppendFence, producerID string, epoch *int64) WriteFenceOutcome {
	in := WriteFenceInput{
		StreamFenced:      stream.metadata.WriteFence,
		StreamIncarnation: stream.metadata.Incarnation,
		Fence:             fence,
		NowNs:             s.now().UnixNano(),
		HasProducer:       epoch != nil,
	}
	if fence == nil && !in.StreamFenced {
		return WriteFenceOutcome{}
	}
	if in.HasProducer {
		in.ProducerEpoch = *epoch
	}
	switch {
	case fence != nil:
		authority := FenceAuthority(*fence)
		in.Marker = s.marker(path, authority)
		in.Seal = stream.seals[authority]
	case in.StreamFenced && in.HasProducer:
		in.BoundGeneration = stream.bound[producerID]
	}
	return EvaluateWriteFence(in)
}

// GrantAppendFence installs or renews the stream-slot marker for one live
// subscription claim, mirroring grant_append_fence.lua check for check: lease,
// stream existence, the authority's seal, marker generation, same-generation
// exactness (a renewal never shortens the lease), the supersession seal on a
// write-fenced stream, install. A sealed generation is never re-granted
// (ErrAppendFenced); an absent stream reports (false, nil).
func (s *MemoryStore) GrantAppendFence(path string, fence auth.AppendFence) (bool, error) {
	nowNs := s.now().UnixNano()
	if !fence.Complete() || fence.LeaseUntilNs <= nowNs {
		return false, ErrAppendFenced
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Expiry is not consulted: the scripts grant against the meta hash as it
	// stands, and a stream that predates incarnations cannot carry a marker.
	stream, ok := s.streams[path]
	if !ok || stream.metadata.Incarnation == "" {
		return false, nil
	}
	authority := FenceAuthority(fence)
	seal := stream.seals[authority]
	if seal.Present && fence.Generation <= seal.Generation {
		return false, ErrAppendFenced
	}
	lease := fence.LeaseUntilNs
	if cur := s.marker(path, authority); cur.Present {
		switch {
		case fence.Generation < cur.Generation:
			return false, ErrAppendFenced
		case fence.Generation == cur.Generation:
			if cur.State != WriteFenceMarkerLive || cur.WakeID != fence.WakeID || cur.Holder != fence.Holder {
				return false, ErrAppendFenced
			}
			lease = max(lease, cur.LeaseUntilNs)
		case stream.metadata.WriteFence && (!seal.Present || cur.Generation > seal.Generation):
			// Supersession: fix the predecessor's definite last fenced offset.
			stream.seal(authority, cur.Generation, cur.WakeID)
		}
	}
	s.setMarker(path, authority, WriteFenceMarker{
		State:             WriteFenceMarkerLive,
		Generation:        fence.Generation,
		WakeID:            fence.WakeID,
		Holder:            fence.Holder,
		LeaseUntilNs:      lease,
		StreamIncarnation: stream.metadata.Incarnation,
	}, time.Now().Add(time.Duration(lease-nowNs)).Add(appendFenceRetention))
	return true, nil
}

// RevokeAppendFence tombstones the named claim marker without sealing,
// mirroring revoke_append_fence.lua: the rollback of a partially granted
// claim. A delayed revocation of an older generation, or of another claim at
// the same one, is a harmless no-op.
func (s *MemoryStore) RevokeAppendFence(path string, fence auth.AppendFence) error {
	if !fence.Complete() {
		return ErrAppendFenced
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tombstone(path, FenceAuthority(fence), fence)
	return nil
}

// SealAppendFence tombstones the named claim marker and, on a write-fenced
// stream, seals its generation for its authority at the definite last
// fenced-class offset, mirroring seal_append_fence.lua check for check: marker
// staleness (nothing mutated on SealStale), tombstone, stream existence,
// fenced, seal monotone per authority (a redelivered done is SealAlready).
func (s *MemoryStore) SealAppendFence(path string, fence auth.AppendFence) (SealResult, error) {
	if !fence.Complete() {
		return SealResult{}, ErrAppendFenced
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	authority := FenceAuthority(fence)
	if !s.tombstone(path, authority, fence) {
		return SealResult{Outcome: SealStale}, nil
	}
	stream, ok := s.streams[path]
	switch {
	case !ok:
		return SealResult{Outcome: SealNotFound}, nil
	case !stream.metadata.WriteFence:
		return SealResult{Outcome: SealUnfenced}, nil
	}
	if seal := stream.seals[authority]; seal.Present && fence.Generation <= seal.Generation {
		return SealResult{Outcome: SealAlready, Generation: seal.Generation, FinalOffset: seal.Offset}, nil
	}
	sealed := stream.seal(authority, fence.Generation, fence.WakeID)
	return SealResult{Outcome: SealSealed, Generation: sealed.Generation, FinalOffset: sealed.Offset}, nil
}

// Long-poll manager methods
func (m *longPollManager) register(path string, ch chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waiters[path] = append(m.waiters[path], ch)
}

func (m *longPollManager) unregister(path string, ch chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	waiters := m.waiters[path]
	for i, w := range waiters {
		if w == ch {
			m.waiters[path] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
}

func (m *longPollManager) notify(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ch := range m.waiters[path] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// notifyClosed notifies all waiters for a path that the stream has been closed
// This is the same as notify - waiters will wake up and check stream state
func (m *longPollManager) notifyClosed(path string) {
	m.notify(path)
}
