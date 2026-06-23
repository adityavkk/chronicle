package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// maxAppendRetries bounds the optimistic re-frame loop. Each retry means
// another writer advanced the tail between our snapshot and the script run;
// the script itself is atomic, so this is contention back-off, not
// correctness.
const maxAppendRetries = 64

// Options configures a Store.
type Options struct {
	// Logger receives operational warnings (e.g. eviction policy). Defaults
	// to slog.Default().
	Logger *slog.Logger

	// Clock is the time source for the now argument passed into the Lua
	// scripts (is_expired / sliding-TTL) and the Go-side expiry checks.
	// Defaults to the real wall clock; the equivalence harness (issue #26)
	// injects a shared FakeClock so the Redis backend and the MemoryStore
	// oracle make identical lazy-expiry decisions at a frozen now.
	Clock store.Clock
}

// Store implements store.Store on Redis 8 using the ZSET-lex frame
// model (docs/PLAN.md §4). All mutations run as per-stream-atomic Lua
// scripts; live tailing uses pub/sub with a re-read and a defensive poll.
//
// Durability note: Redis replication is asynchronous — an acknowledged
// append can be lost on failover. See docs/PLAN.md §4.7.
type Store struct {
	client redis.UniversalClient
	log    *slog.Logger
	clock  store.Clock
}

var _ store.Store = (*Store)(nil)

// New wraps a go-redis client as a store.Store. The store takes ownership
// of the client: Close() closes it. On connect it best-effort checks that
// maxmemory-policy is noeviction (anything else can silently truncate
// streams) and warns through the logger if not.
func New(client redis.UniversalClient, opts Options) *Store {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	clock := opts.Clock
	if clock == nil {
		clock = store.RealClock()
	}
	s := &Store{client: client, log: log, clock: clock}
	s.warnEvictionPolicy()
	return s
}

func (s *Store) warnEvictionPolicy() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := s.client.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		return // best-effort: CONFIG is often restricted on managed Redis
	}
	if v, ok := res["maxmemory-policy"]; ok && v != "noeviction" {
		s.log.Warn("chronicle requires maxmemory-policy=noeviction; other policies can evict stream data",
			"maxmemory-policy", v)
	}
}

func keysFor(path string) []string {
	return []string{metaKey(path), msgKey(path), prodKey(path), forksKey(path)}
}

// isExpired evaluates a stream's lazy expiry against the injected clock, so
// the Go-side pre-checks (Get/Has/Create) agree with the Lua is_expired
// (which receives the same now via nowNsArg) and the MemoryStore oracle at a
// shared frozen instant (issue #26).
func (s *Store) isExpired(m *store.StreamMetadata) bool {
	return m.IsExpiredAt(s.clock.Now())
}

// nowNsArg renders the now argument (UnixNano decimal) passed into every
// mutation/read script. It reads the store's injected clock so the Lua
// is_expired / sliding-TTL math is driven by the same time source as the
// Go-side checks and the MemoryStore oracle (issue #26).
func (s *Store) nowNsArg() string {
	return strconv.FormatInt(s.clock.Now().UnixNano(), 10)
}

// normalizeCT mirrors store.ContentTypeMatches normalization: empty
// defaults to application/octet-stream, parameters stripped, ASCII lowered.
func normalizeCT(ct string) string {
	if ct == "" {
		ct = "application/octet-stream"
	}
	mt := store.ExtractMediaType(ct)
	b := []byte(mt)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

// buildFrames splits data into messages per the stream's content type
// (JSON one-level flatten vs single binary message) and encodes ZSET
// members with end offsets advancing from base by payload byte count.
func buildFrames(ct string, base store.Offset, data []byte, allowEmpty bool) ([]any, store.Offset, error) {
	var msgs [][]byte
	if store.IsJSONContentType(ct) {
		var err error
		msgs, err = store.ProcessJSONAppend(data, allowEmpty)
		if err != nil {
			return nil, store.Offset{}, err
		}
	} else {
		msgs = [][]byte{data}
	}
	frames := make([]any, 0, len(msgs))
	cur := base
	for _, msg := range msgs {
		cur = cur.Add(uint64(len(msg)))
		frames = append(frames, encodeFrame(cur, msg))
	}
	return frames, cur, nil
}

// fetchMeta loads stream metadata without producers and without touching
// the sliding TTL. Returns (nil, nil) when the stream does not exist.
func (s *Store) fetchMeta(ctx context.Context, path string) (*store.StreamMetadata, error) {
	fields, err := s.client.HGetAll(ctx, metaKey(path)).Result()
	if err != nil {
		return nil, err
	}
	return metaFromFields(path, fields)
}

// Get returns metadata for a stream. It does NOT refresh the sliding TTL
// (only Read and Append count as access).
func (s *Store) Get(path string) (*store.StreamMetadata, error) {
	ctx := context.Background()
	pipe := s.client.Pipeline()
	metaCmd := pipe.HGetAll(ctx, metaKey(path))
	prodCmd := pipe.HGetAll(ctx, prodKey(path))
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	m, err := metaFromFields(path, metaCmd.Val())
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, store.ErrStreamNotFound
	}
	if m.SoftDeleted {
		return nil, store.ErrStreamSoftDeleted
	}
	if s.isExpired(m) {
		return nil, store.ErrStreamNotFound
	}
	if m.Producers, err = producersFromHash(prodCmd.Val()); err != nil {
		return nil, err
	}
	return m, nil
}

// Has reports stream existence; soft-deleted and expired streams are
// invisible. No TTL touch.
func (s *Store) Has(path string) bool {
	m, err := s.fetchMeta(context.Background(), path)
	return err == nil && m != nil && !m.SoftDeleted && !s.isExpired(m)
}

// GetCurrentOffset returns the tail offset. Mirroring MemoryStore, it does
// not check expiry or soft-deletion and does not touch the TTL.
func (s *Store) GetCurrentOffset(path string) (store.Offset, error) {
	tail, err := s.client.HGet(context.Background(), metaKey(path), fTail).Result()
	if errors.Is(err, redis.Nil) {
		return store.Offset{}, store.ErrStreamNotFound
	}
	if err != nil {
		return store.Offset{}, err
	}
	return store.ParseOffset(tail)
}

// GetCurrentOffsets returns the tail offset for each given path that exists, in
// pipelined batches. Missing streams (and unparseable tails) are omitted, so the
// caller cannot tell "missing" from "errored" — the batched form of
// GetCurrentOffset for read-many callers like the subscription recovery sweep.
// Like GetCurrentOffset it ignores expiry and soft-deletion and touches no TTLs.
func (s *Store) GetCurrentOffsets(ctx context.Context, paths []string) (map[string]store.Offset, error) {
	const chunk = 512
	out := make(map[string]store.Offset, len(paths))
	for start := 0; start < len(paths); start += chunk {
		end := start + chunk
		if end > len(paths) {
			end = len(paths)
		}
		batch := paths[start:end]
		pipe := s.client.Pipeline()
		cmds := make([]*redis.StringCmd, len(batch))
		for i, p := range batch {
			cmds[i] = pipe.HGet(ctx, metaKey(p), fTail)
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		for i, p := range batch {
			tail, err := cmds[i].Result()
			if err != nil {
				continue // redis.Nil (missing) or a read error: omit
			}
			off, err := store.ParseOffset(tail)
			if err != nil {
				continue
			}
			out[p] = off
		}
	}
	return out, nil
}

// Create creates a stream (idempotent PUT). Fork creation is orchestrated
// across two cluster slots (source and fork) and is therefore NOT atomic:
// between the source refcount increment and the fork-key creation a crash
// leaks one refcount (reaped only by delete-cascade arithmetic), and a
// concurrent identical create is resolved by rolling our reference back.
func (s *Store) Create(path string, opts store.CreateOptions) (*store.StreamMetadata, bool, error) {
	ctx := context.Background()

	// Existence pre-check (also re-checked atomically inside create.lua).
	// Doing it first mirrors upstream precedence: idempotent match returns
	// before fork validation and before InitialData parsing.
	if existing, err := s.fetchMeta(ctx, path); err != nil {
		return nil, false, err
	} else if existing != nil && !s.isExpired(existing) {
		if existing.SoftDeleted {
			return nil, false, store.ErrStreamExists
		}
		if existing.ConfigMatches(opts) {
			prod, err := s.client.HGetAll(ctx, prodKey(path)).Result()
			if err != nil {
				return nil, false, err
			}
			if existing.Producers, err = producersFromHash(prod); err != nil {
				return nil, false, err
			}
			return existing, false, nil
		}
		return nil, false, store.ErrConfigMismatch
	}

	now := s.clock.Now()
	meta := &store.StreamMetadata{
		Path:           path,
		ContentType:    opts.ContentType,
		CurrentOffset:  store.ZeroOffset,
		CreatedAt:      now,
		LastAccessedAt: now,
		Closed:         opts.Closed,
		Producers:      map[string]*store.ProducerState{},
	}

	isFork := opts.ForkedFrom != ""
	var binaryPrefix []byte
	if isFork {
		srcMeta, err := s.fetchMeta(ctx, opts.ForkedFrom)
		if err != nil {
			return nil, false, err
		}
		if srcMeta == nil {
			return nil, false, store.ErrStreamNotFound
		}
		if srcMeta.SoftDeleted {
			return nil, false, store.ErrStreamSoftDeleted
		}
		if s.isExpired(srcMeta) {
			return nil, false, store.ErrStreamNotFound
		}
		// Content-type mismatch is rejected before taking a reference on the
		// source (a leaked reference would pin the source forever).
		if opts.ContentType != "" && !strings.EqualFold(opts.ContentType, srcMeta.ContentType) {
			return nil, false, store.ErrContentTypeMismatch
		}
		forkOffset := srcMeta.CurrentOffset
		if opts.ForkOffset != nil {
			forkOffset = *opts.ForkOffset
		}
		if srcMeta.CurrentOffset.LessThan(forkOffset) {
			return nil, false, store.ErrInvalidForkOffset
		}
		if opts.ForkSubOffset != nil && *opts.ForkSubOffset > 0 {
			resolved, prefix, err := s.resolveForkSubOffset(ctx, opts.ForkedFrom, srcMeta, forkOffset, *opts.ForkSubOffset)
			if err != nil {
				return nil, false, err
			}
			if store.IsJSONContentType(srcMeta.ContentType) {
				forkOffset = resolved
			} else {
				binaryPrefix = prefix
			}
		}

		if meta.ContentType == "" {
			meta.ContentType = srcMeta.ContentType
		}
		meta.ForkedFrom = opts.ForkedFrom
		meta.ForkOffset = forkOffset
		meta.CurrentOffset = forkOffset
		meta.TTLSeconds, meta.ExpiresAt = resolveForkExpiry(opts, srcMeta)
		if opts.ForkOffset != nil {
			requested := *opts.ForkOffset
			meta.ForkOffsetRequested = &requested
		}
		if opts.ForkSubOffset != nil {
			meta.ForkSubOffset = *opts.ForkSubOffset
		}

		// Take the fork reference on the source (re-validated atomically).
		status, _, err := s.runStatusScript(ctx, incrRefScript, keysFor(opts.ForkedFrom), s.nowNsArg(), path)
		if err != nil {
			return nil, false, err
		}
		switch status {
		case stNotFound:
			return nil, false, store.ErrStreamNotFound
		case stSoftDel:
			return nil, false, store.ErrStreamSoftDeleted
		}
	} else {
		if meta.ContentType == "" {
			meta.ContentType = "application/octet-stream"
		}
		meta.TTLSeconds = opts.TTLSeconds
		meta.ExpiresAt = opts.ExpiresAt
	}

	rollbackRef := func() {
		if !isFork {
			return
		}
		if err := s.releaseRef(ctx, opts.ForkedFrom, path); err != nil {
			s.log.Warn("fork create rollback: failed to release source reference",
				"source", opts.ForkedFrom, "fork", path, "error", err)
		}
	}

	// Materialize a binary sub-offset prefix as the fork's first own frame.
	var frames []any
	if len(binaryPrefix) > 0 {
		newOffset := meta.CurrentOffset.Add(uint64(len(binaryPrefix)))
		frames = append(frames, encodeFrame(newOffset, binaryPrefix))
		meta.CurrentOffset = newOffset
	}
	if len(opts.InitialData) > 0 {
		dataFrames, newTail, err := buildFrames(meta.ContentType, meta.CurrentOffset, opts.InitialData, true)
		if err != nil {
			rollbackRef()
			return nil, false, err
		}
		frames = append(frames, dataFrames...)
		meta.CurrentOffset = newTail
	}

	args := s.createArgs(meta, opts, frames)
	raw, err := createScript.Run(ctx, s.client, keysFor(path), args...).Result()
	if err != nil {
		rollbackRef()
		return nil, false, err
	}
	status, rest, err := decodeStatusReply(raw)
	if err != nil {
		rollbackRef()
		return nil, false, err
	}
	switch status {
	case stCreated:
		return meta, true, nil
	case stExists:
		rollbackRef()
		return nil, false, store.ErrStreamExists
	case stMismatch:
		rollbackRef()
		return nil, false, store.ErrConfigMismatch
	case stMatched:
		// Lost a creation race against an identical create: release our
		// reference (the established fork holds its own) and return theirs.
		rollbackRef()
		return s.decodeMatched(path, rest)
	default:
		rollbackRef()
		return nil, false, fmt.Errorf("create.lua: unexpected status %q", status)
	}
}

// createArgs assembles the create.lua argument list: probe args for
// config matching, then the meta field pairs, then initial frames.
func (s *Store) createArgs(meta *store.StreamMetadata, opts store.CreateOptions, frames []any) []any {
	probeTTL := ""
	if opts.TTLSeconds != nil {
		probeTTL = strconv.FormatInt(*opts.TTLSeconds, 10)
	}
	probeExp := ""
	if opts.ExpiresAt != nil {
		probeExp = strconv.FormatInt(opts.ExpiresAt.UnixNano(), 10)
	}
	probeClosed := "0"
	if opts.Closed {
		probeClosed = "1"
	}
	probeForkOff := ""
	if opts.ForkOffset != nil {
		probeForkOff = opts.ForkOffset.String()
	}
	probeSubOff := "0"
	if opts.ForkSubOffset != nil {
		probeSubOff = strconv.FormatUint(*opts.ForkSubOffset, 10)
	}

	fields := metaToFields(meta)
	args := make([]any, 0, 10+2*len(fields)+len(frames))
	args = append(args, s.nowNsArg(), notifyChannel(meta.Path),
		normalizeCT(opts.ContentType), probeTTL, probeExp, probeClosed,
		opts.ForkedFrom, probeForkOff, probeSubOff,
		strconv.Itoa(len(fields)))
	for k, v := range fields {
		args = append(args, k, v)
	}
	args = append(args, frames...)
	return args
}

func (s *Store) decodeMatched(path string, rest []any) (*store.StreamMetadata, bool, error) {
	if len(rest) < 2 {
		return nil, false, fmt.Errorf("create.lua: malformed MATCHED reply")
	}
	metaFields, err := flatToMap(rest[0])
	if err != nil {
		return nil, false, err
	}
	prodFields, err := flatToMap(rest[1])
	if err != nil {
		return nil, false, err
	}
	m, err := metaFromFields(path, metaFields)
	if err != nil {
		return nil, false, err
	}
	if m == nil {
		return nil, false, fmt.Errorf("create.lua: MATCHED without metadata")
	}
	if m.Producers, err = producersFromHash(prodFields); err != nil {
		return nil, false, err
	}
	return m, false, nil
}

// resolveForkExpiry mirrors MemoryStore.resolveForkExpiry: explicit TTL
// wins, then explicit ExpiresAt, then the source's TTL, then the source's
// ExpiresAt. Forks have independent lifetimes.
func resolveForkExpiry(opts store.CreateOptions, src *store.StreamMetadata) (*int64, *time.Time) {
	if opts.TTLSeconds != nil {
		return opts.TTLSeconds, nil
	}
	if opts.ExpiresAt != nil {
		return nil, opts.ExpiresAt
	}
	if src.TTLSeconds != nil {
		ttl := *src.TTLSeconds
		return &ttl, nil
	}
	if src.ExpiresAt != nil {
		t := *src.ExpiresAt
		return nil, &t
	}
	return nil, nil
}

// resolveForkSubOffset mirrors MemoryStore.resolveForkSubOffset: JSON
// sources resolve the sub-offset to the n-th flattened message boundary
// after the fork point; binary sources return the byte prefix of the first
// message after it, which becomes the fork's first own frame.
func (s *Store) resolveForkSubOffset(ctx context.Context, srcPath string, srcMeta *store.StreamMetadata, forkOffset store.Offset, subOffset uint64) (store.Offset, []byte, error) {
	msgs, err := s.readForkChain(ctx, srcPath, srcMeta, forkOffset)
	if err != nil {
		return store.Offset{}, nil, err
	}
	if store.IsJSONContentType(srcMeta.ContentType) {
		if uint64(len(msgs)) < subOffset {
			return store.Offset{}, nil, store.ErrInvalidForkSubOffset
		}
		return msgs[subOffset-1].Offset, nil, nil
	}
	if len(msgs) == 0 {
		return store.Offset{}, nil, store.ErrInvalidForkSubOffset
	}
	first := msgs[0].Data
	if uint64(len(first)) < subOffset {
		return store.Offset{}, nil, store.ErrInvalidForkSubOffset
	}
	prefix := make([]byte, subOffset)
	copy(prefix, first[:subOffset])
	return forkOffset, prefix, nil
}

// Append adds data to a stream. All validation and the write happen in one
// atomic Lua script; frames are pre-encoded against a tail snapshot and the
// script returns RETRY when the tail moved.
func (s *Store) Append(path string, data []byte, opts store.AppendOptions) (store.AppendResult, error) {
	if opts.HasProducerHeaders() && !opts.HasAllProducerHeaders() {
		return store.AppendResult{}, store.ErrPartialProducer
	}
	if len(data) == 0 && !opts.Close {
		return store.AppendResult{}, store.ErrEmptyBody
	}
	ctx := context.Background()

	reqCT := ""
	if opts.ContentType != "" {
		reqCT = normalizeCT(opts.ContentType)
	}
	closeArg, hasProd, pid, epochArg, seqArg := "0", "0", "", "0", "0"
	if opts.Close {
		closeArg = "1"
	}
	if opts.HasAllProducerHeaders() {
		hasProd, pid = "1", opts.ProducerId
		epochArg = strconv.FormatInt(*opts.ProducerEpoch, 10)
		seqArg = strconv.FormatInt(*opts.ProducerSeq, 10)
	}

	for attempt := 0; attempt < maxAppendRetries; attempt++ {
		snap, err := s.client.HMGet(ctx, metaKey(path), fTail, fCT).Result()
		if err != nil {
			return store.AppendResult{}, err
		}
		tailStr, _ := snap[0].(string)
		ctStr, _ := snap[1].(string)
		if tailStr == "" {
			return store.AppendResult{}, store.ErrStreamNotFound
		}
		base, err := store.ParseOffset(tailStr)
		if err != nil {
			return store.AppendResult{}, err
		}

		frames := []any{}
		newTail := base
		valOnly := "0"
		var frameErr error
		if len(data) > 0 {
			frames, newTail, frameErr = buildFrames(ctStr, base, data, false)
			if frameErr != nil {
				// JSON-mode parse failure: run the validation chain anyway so
				// closed/producer/seq errors keep spec precedence.
				frames, newTail, valOnly = []any{}, base, "1"
			}
		}

		args := append([]any{
			s.nowNsArg(), notifyChannel(path), reqCT, opts.Seq, closeArg,
			hasProd, pid, epochArg, seqArg,
			base.String(), newTail.String(), valOnly,
		}, frames...)

		raw, err := appendScript.Run(ctx, s.client, keysFor(path), args...).Result()
		if err != nil {
			return store.AppendResult{}, err
		}
		r, err := decodeScriptReply(raw)
		if err != nil {
			return store.AppendResult{}, err
		}
		if r.Status == stRetry {
			continue
		}
		res, err := s.mapAppendReply(r)
		if err == nil && r.Status == stValOnly {
			// All validations passed; surface the JSON error (with no write).
			if r.ProducerRes == int64(store.ProducerResultDuplicate) {
				return res, nil
			}
			return store.AppendResult{}, frameErr
		}
		return res, err
	}
	return store.AppendResult{}, fmt.Errorf("append to %s: too much contention", path)
}

// mapAppendReply translates a script reply into (AppendResult, sentinel).
func (s *Store) mapAppendReply(r *scriptReply) (store.AppendResult, error) {
	tail := store.Offset{}
	if r.Tail != "" {
		var err error
		if tail, err = store.ParseOffset(r.Tail); err != nil {
			return store.AppendResult{}, err
		}
	}
	switch r.Status {
	case stOK, stValOnly:
		return store.AppendResult{
			Offset:         tail,
			ProducerResult: store.ProducerResult(r.ProducerRes),
			LastSeq:        r.LastSeq,
			StreamClosed:   r.Closed,
		}, nil
	case stNotFound:
		return store.AppendResult{}, store.ErrStreamNotFound
	case stSoftDel:
		return store.AppendResult{}, store.ErrStreamSoftDeleted
	case stClosed:
		return store.AppendResult{Offset: tail, StreamClosed: true}, store.ErrStreamClosed
	case stCTMismatch:
		return store.AppendResult{}, store.ErrContentTypeMismatch
	case stSeqConflict:
		return store.AppendResult{}, store.ErrSequenceConflict
	case stStaleEpoch:
		return store.AppendResult{Offset: tail, CurrentEpoch: r.CurrentEpoch}, store.ErrStaleEpoch
	case stEpochSeq:
		return store.AppendResult{Offset: tail}, store.ErrInvalidEpochSeq
	case stSeqGap:
		return store.AppendResult{Offset: tail, ExpectedSeq: r.ExpectedSeq, ReceivedSeq: r.ReceivedSeq}, store.ErrProducerSeqGap
	default:
		return store.AppendResult{}, fmt.Errorf("append.lua: unexpected status %q", r.Status)
	}
}

// Read returns messages with end offset > offset, stitching fork chains.
// It refreshes the sliding TTL (Read counts as access).
func (s *Store) Read(path string, offset store.Offset) ([]store.Message, bool, error) {
	ctx := context.Background()
	raw, err := readScript.Run(ctx, s.client, keysFor(path), s.nowNsArg(), lexLowerBound(offset)).Result()
	if err != nil {
		return nil, false, err
	}
	status, rest, err := decodeStatusReply(raw)
	if err != nil {
		return nil, false, err
	}
	switch status {
	case stNotFound, stSoftDel:
		// Read hides soft-deleted streams as not-found (Get surfaces them).
		return nil, false, store.ErrStreamNotFound
	case stOK:
	default:
		return nil, false, fmt.Errorf("read.lua: unexpected status %q", status)
	}
	if len(rest) < 2 {
		return nil, false, fmt.Errorf("read.lua: malformed OK reply")
	}
	metaFields, err := flatToMap(rest[0])
	if err != nil {
		return nil, false, err
	}
	meta, err := metaFromFields(path, metaFields)
	if err != nil || meta == nil {
		return nil, false, fmt.Errorf("read.lua: bad metadata: %w", err)
	}
	members, err := toStrings(rest[1])
	if err != nil {
		return nil, false, err
	}
	own, err := decodeFrames(members)
	if err != nil {
		return nil, false, err
	}

	messages := own
	if meta.ForkedFrom != "" && offset.LessThan(meta.ForkOffset) {
		inherited, err := s.readInherited(ctx, meta, offset)
		if err != nil {
			return nil, false, err
		}
		if len(inherited) > 0 {
			messages = concatMessages(inherited, own)
		}
	}

	var upToDate bool
	if len(messages) > 0 {
		upToDate = messages[len(messages)-1].Offset.Equal(meta.CurrentOffset)
	} else {
		upToDate = offset.Equal(meta.CurrentOffset) || meta.CurrentOffset.Equal(store.ZeroOffset)
	}
	return messages, upToDate, nil
}

func decodeFrames(members []string) ([]store.Message, error) {
	if len(members) == 0 {
		return nil, nil
	}
	msgs := make([]store.Message, 0, len(members))
	for _, m := range members {
		o, data, err := decodeFrame(m)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, store.Message{Data: data, Offset: o})
	}
	return msgs, nil
}

// readInherited reads the inherited range of a fork (offset < ForkOffset)
// from its source chain, capped at the fork point so post-fork source
// appends stay invisible.
func (s *Store) readInherited(ctx context.Context, forkMeta *store.StreamMetadata, offset store.Offset) ([]store.Message, error) {
	srcMeta, err := s.fetchMeta(ctx, forkMeta.ForkedFrom)
	if err != nil {
		return nil, err
	}
	if srcMeta == nil {
		return nil, nil // source vanished: mirror upstream's silent skip
	}
	msgs, err := s.readForkChain(ctx, forkMeta.ForkedFrom, srcMeta, offset)
	if err != nil {
		return nil, err
	}
	capped := msgs[:0]
	for _, m := range msgs {
		if m.Offset.LessThanOrEqual(forkMeta.ForkOffset) {
			capped = append(capped, m)
		}
	}
	return capped, nil
}

// readForkChain mirrors MemoryStore.readForkedStream: own frames > offset,
// preceded by inherited frames from the recursive source chain capped at
// this stream's fork point. It deliberately ignores SoftDeleted and expiry
// on sources (forks must read through soft-deleted parents) and never
// touches their TTLs.
func (s *Store) readForkChain(ctx context.Context, path string, meta *store.StreamMetadata, offset store.Offset) ([]store.Message, error) {
	members, err := s.client.ZRangeByLex(ctx, msgKey(path), &redis.ZRangeBy{
		Min: lexLowerBound(offset),
		Max: "+",
	}).Result()
	if err != nil {
		return nil, err
	}
	own, err := decodeFrames(members)
	if err != nil {
		return nil, err
	}
	if meta.ForkedFrom == "" || !offset.LessThan(meta.ForkOffset) {
		return own, nil
	}
	inherited, err := s.readInherited(ctx, meta, offset)
	if err != nil {
		return nil, err
	}
	if len(inherited) == 0 {
		return own, nil
	}
	return concatMessages(inherited, own), nil
}

func concatMessages(a, b []store.Message) []store.Message {
	out := make([]store.Message, 0, len(a)+len(b))
	out = append(out, a...)
	return append(out, b...)
}

// Delete removes a stream: soft-delete when forks reference it, hard
// delete with cascading GC up the fork chain otherwise. The cascade spans
// cluster slots and is not atomic (documented window: a crash mid-cascade
// leaves the parent's refcount high until another delete re-walks it).
func (s *Store) Delete(path string) error {
	ctx := context.Background()
	status, rest, err := s.runStatusScript(ctx, deleteScript, keysFor(path), notifyChannel(path))
	if err != nil {
		return err
	}
	switch status {
	case stNotFound:
		return store.ErrStreamNotFound
	case stSoftDel:
		return store.ErrStreamSoftDeleted
	case stSoftDeleted:
		return nil
	case stDeleted:
		parent := ""
		if len(rest) > 0 {
			parent, _ = rest[0].(string)
		}
		if parent != "" {
			return s.releaseRef(ctx, parent, path)
		}
		return nil
	default:
		return fmt.Errorf("delete.lua: unexpected status %q", status)
	}
}

// releaseRef decrements the fork refcount on parent (for child), cascading
// hard-deletes up the chain while soft-deleted parents hit refcount zero.
func (s *Store) releaseRef(ctx context.Context, parent, child string) error {
	for parent != "" {
		status, rest, err := s.runStatusScript(ctx, decrRefScript, keysFor(parent),
			s.nowNsArg(), notifyChannel(parent), child)
		if err != nil {
			return err
		}
		switch status {
		case stGone, stOK:
			return nil
		case stUnderflow:
			return store.ErrRefCountUnderflow
		case stCascade:
			grand := ""
			if len(rest) > 0 {
				grand, _ = rest[0].(string)
			}
			child, parent = parent, grand
		default:
			return fmt.Errorf("decr_ref.lua: unexpected status %q", status)
		}
	}
	return nil
}

// CloseStream closes a stream without appending. Idempotent; does not set
// ClosedBy and does not refresh the sliding TTL (mirrors MemoryStore).
func (s *Store) CloseStream(path string) (*store.CloseResult, error) {
	r, err := s.runCloseScript(context.Background(), path, "0", "", "0", "0")
	if err != nil {
		return nil, err
	}
	switch r.Status {
	case stNotFound:
		return nil, store.ErrStreamNotFound
	case stOK:
		tail, err := store.ParseOffset(r.Tail)
		if err != nil {
			return nil, err
		}
		return &store.CloseResult{FinalOffset: tail, AlreadyClosed: r.AlreadyClosed}, nil
	default:
		return nil, fmt.Errorf("close.lua: unexpected status %q", r.Status)
	}
}

// CloseStreamWithProducer closes a stream using producer headers for
// idempotent sequencing (closedBy tuple dedup).
func (s *Store) CloseStreamWithProducer(path string, opts store.CloseProducerOptions) (*store.CloseProducerResult, error) {
	r, err := s.runCloseScript(context.Background(), path, "1", opts.ProducerId,
		strconv.FormatInt(opts.ProducerEpoch, 10), strconv.FormatInt(opts.ProducerSeq, 10))
	if err != nil {
		return nil, err
	}
	if r.Status == stNotFound {
		return nil, store.ErrStreamNotFound
	}
	tail, err := store.ParseOffset(r.Tail)
	if err != nil {
		return nil, err
	}
	res := &store.CloseProducerResult{
		FinalOffset:    tail,
		ProducerResult: store.ProducerResult(r.ProducerRes),
		CurrentEpoch:   r.CurrentEpoch,
		ExpectedSeq:    r.ExpectedSeq,
		ReceivedSeq:    r.ReceivedSeq,
		LastSeq:        r.LastSeq,
		StreamClosed:   r.Closed,
		AlreadyClosed:  r.AlreadyClosed,
	}
	switch r.Status {
	case stOK:
		return res, nil
	case stClosed:
		return res, store.ErrStreamClosed
	case stStaleEpoch:
		return res, store.ErrStaleEpoch
	case stEpochSeq:
		return res, store.ErrInvalidEpochSeq
	case stSeqGap:
		return res, store.ErrProducerSeqGap
	default:
		return nil, fmt.Errorf("close.lua: unexpected status %q", r.Status)
	}
}

func (s *Store) runCloseScript(ctx context.Context, path, hasProd, pid, epoch, seq string) (*scriptReply, error) {
	raw, err := closeScript.Run(ctx, s.client, keysFor(path),
		s.nowNsArg(), notifyChannel(path), hasProd, pid, epoch, seq).Result()
	if err != nil {
		return nil, err
	}
	return decodeScriptReply(raw)
}

func (s *Store) runStatusScript(ctx context.Context, script *redis.Script, keys []string, args ...any) (string, []any, error) {
	raw, err := script.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return "", nil, err
	}
	return decodeStatusReply(raw)
}

// FormatResponse formats messages per the stream's content type (JSON array
// wrapper vs raw concatenation). Not part of store.Store, but the handler
// relies on it for drop-in parity with the Caddy stores.
func (s *Store) FormatResponse(path string, messages []store.Message) ([]byte, error) {
	ct, err := s.client.HGet(context.Background(), metaKey(path), fCT).Result()
	if errors.Is(err, redis.Nil) {
		return nil, store.ErrStreamNotFound
	}
	if err != nil {
		return nil, err
	}
	if store.IsJSONContentType(ct) {
		return store.FormatJSONResponse(messages), nil
	}
	var buf []byte
	for _, msg := range messages {
		buf = append(buf, msg.Data...)
	}
	return buf, nil
}

// Close releases the store's resources, closing the underlying client.
func (s *Store) Close() error {
	return s.client.Close()
}
