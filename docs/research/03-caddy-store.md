# 03 — The Caddy store layer

**Source:** `/Users/auk000v/dev/durable-streams/packages/caddy-plugin/store/` (read in full on 2026-06-10).
**Purpose:** catalog the storage contract that chronicle's Redis 8 store must satisfy, byte for byte where it matters. The Caddy plugin ships two `Store` implementations — `MemoryStore` (testing) and `FileStore` (production, segment files + a bbolt metadata sidecar) — behind one interface. Chronicle adds a third backend; everything in §1–§2 is the portable contract, §10 separates what is filesystem incidental.

Package layout (line counts as of this snapshot):

| File | Lines | Role |
|---|---|---|
| `store.go` | 400 | `Store` interface, all shared types, errors, content-type + JSON helpers |
| `offset.go` | 155 | `Offset` type, parsing, comparison, sentinels |
| `segment.go` | 309 | Segment file format, reader/writer, crash-recovery scan |
| `memory_store.go` | 1074 | In-memory implementation + shared `longPollManager` and `processJSONAppend` |
| `file_store.go` | 1412 | Production implementation, fork logic, recovery |
| `bbolt.go` | 710 | bbolt-backed metadata persistence (`BboltMetadataStore`) |
| `filepool.go` | 257 | LRU pools of `*os.File` handles (writer + reader) |
| `*_test.go` | ~2,400 | Behavioral contract (see §9) |

---

## 1. The `Store` interface — verbatim

Chronicle's Redis store must implement exactly this shape. Copied verbatim from `store.go` including doc comments:

```go
// Store is the interface for durable stream storage
type Store interface {
	// Create creates a new stream. Returns ErrStreamExists if stream exists with
	// different config, or nil if stream exists with same config (idempotent).
	// The bool return value indicates if the stream was newly created (true) or
	// already existed with matching config (false).
	Create(path string, opts CreateOptions) (*StreamMetadata, bool, error)

	// Get returns metadata for a stream, or ErrStreamNotFound if not found
	Get(path string) (*StreamMetadata, error)

	// Has returns true if the stream exists
	Has(path string) bool

	// Delete removes a stream. Returns ErrStreamNotFound if not found.
	Delete(path string) error

	// Append adds data to a stream. Returns AppendResult with the new offset.
	// Returns ErrStreamNotFound if stream doesn't exist.
	// Returns ErrSequenceConflict if seq is provided and <= last seq.
	// Returns ErrContentTypeMismatch if content type doesn't match.
	// Returns ErrStaleEpoch if producer epoch is less than current.
	// Returns ErrInvalidEpochSeq if new epoch doesn't start at seq 0.
	// Returns ErrProducerSeqGap if producer seq is greater than lastSeq + 1.
	// Returns ErrPartialProducer if only some producer headers are provided.
	// Returns ErrStreamClosed if stream is closed (unless opts.Close is true for close-only).
	Append(path string, data []byte, opts AppendOptions) (AppendResult, error)

	// CloseStream closes a stream without appending data.
	// Returns the final offset and whether it was already closed.
	// This is an idempotent operation - closing an already-closed stream succeeds.
	CloseStream(path string) (*CloseResult, error)

	// CloseStreamWithProducer closes a stream without appending data, using producer headers
	// for idempotent sequencing. Returns the final offset and producer validation result.
	CloseStreamWithProducer(path string, opts CloseProducerOptions) (*CloseProducerResult, error)

	// Read reads messages from a stream starting at the given offset.
	// Returns messages, whether we're up to date (at tail), and any error.
	// Returns ErrStreamNotFound if stream doesn't exist.
	Read(path string, offset Offset) ([]Message, bool, error)

	// WaitForMessages waits for new messages after the given offset.
	// Returns when messages are available, timeout expires, context is cancelled,
	// or stream is closed.
	// If messages exist at the offset, returns immediately.
	// timedOut is true if we returned due to timeout with no messages.
	// streamClosed is true if the stream was closed during or before the wait.
	WaitForMessages(ctx context.Context, path string, offset Offset, timeout time.Duration) (messages []Message, timedOut bool, streamClosed bool, err error)

	// GetCurrentOffset returns the current tail offset for a stream
	GetCurrentOffset(path string) (Offset, error)

	// Close releases any resources held by the store
	Close() error
}
```

Note: both concrete stores also expose `FormatResponse(path string, messages []Message) ([]byte, error)` (JSON → array wrapper, otherwise raw concatenation). It is **not** part of the `Store` interface, but the handler relies on it; chronicle should provide it for drop-in parity.

### 1.1 Error sentinels — verbatim

Tests compare with `==` / `!=` (e.g. `if err != ErrStreamNotFound`), so these must be returned as **unwrapped sentinel values**, never wrapped with `fmt.Errorf("%w", ...)` at the store boundary:

```go
// Common errors
var (
	ErrStreamNotFound      = errors.New("stream not found")
	ErrStreamExpired       = errors.New("stream has expired")
	ErrStreamExists        = errors.New("stream already exists")
	ErrConfigMismatch      = errors.New("stream configuration mismatch")
	ErrSequenceConflict    = errors.New("sequence number conflict")
	ErrContentTypeMismatch = errors.New("content type mismatch")
	ErrEmptyBody           = errors.New("empty body not allowed")
	ErrInvalidOffset       = errors.New("invalid offset")
	ErrEmptyJSONArray      = errors.New("empty JSON array not allowed")
	ErrInvalidJSON         = errors.New("invalid JSON")
	ErrStreamClosed        = errors.New("stream is closed")
)

// Producer validation errors
var (
	ErrStaleEpoch      = errors.New("producer epoch is stale")
	ErrInvalidEpochSeq = errors.New("new epoch must start at sequence 0")
	ErrProducerSeqGap  = errors.New("producer sequence gap detected")
	ErrPartialProducer = errors.New("all producer headers must be provided together")
)

// Fork-related errors
var (
	ErrStreamSoftDeleted    = errors.New("stream is soft-deleted")
	ErrInvalidForkOffset    = errors.New("fork offset beyond source stream length")
	ErrInvalidForkSubOffset = errors.New("fork sub-offset overshoots or is invalid")
	ErrRefCountUnderflow    = errors.New("reference count underflow")
)
```

Also in `segment.go` (filesystem-specific but part of the package vocabulary):

```go
var (
	// ErrMessageTooLarge is returned when a message exceeds MaxMessageSize
	ErrMessageTooLarge = errors.New("message too large")

	// ErrCorruptedSegment is returned when segment file appears corrupted
	ErrCorruptedSegment = errors.New("corrupted segment file")
)
```

**Dead vocabulary:** `ErrEmptyBody`, `ErrStreamExpired`, and `ErrInvalidOffset` are defined but never returned by either store implementation, nor referenced by `handler.go` (verified by grep). Expired streams surface as `ErrStreamNotFound`; empty-body and offset validation happen in the HTTP handler before the store is called. Chronicle should still define them for shape parity but is not expected to produce them from the store.

### 1.2 Supporting types — verbatim

```go
// ProducerState tracks the epoch and sequence for an idempotent producer
type ProducerState struct {
	Epoch       int64 // Client-declared epoch
	LastSeq     int64 // Last accepted sequence number
	LastUpdated int64 // Unix timestamp of last update
}

// ProducerResult indicates the outcome of producer validation
type ProducerResult int

const (
	ProducerResultNone      ProducerResult = iota // No producer headers provided
	ProducerResultAccepted                        // New data accepted
	ProducerResultDuplicate                       // Duplicate detected (204)
)

// AppendResult contains the result of an append operation
type AppendResult struct {
	Offset         Offset
	ProducerResult ProducerResult
	CurrentEpoch   int64 // Current epoch on stale epoch error
	ExpectedSeq    int64 // Expected seq on gap error
	ReceivedSeq    int64 // Received seq on gap error
	LastSeq        int64 // Highest accepted seq (for duplicates and success)
	StreamClosed   bool  // Stream is now closed (either by this request or previously)
}

// CloseResult contains the result of a close operation
type CloseResult struct {
	FinalOffset   Offset
	AlreadyClosed bool
}

// CloseProducerOptions contains producer headers for close-only operations.
type CloseProducerOptions struct {
	ProducerId    string
	ProducerEpoch int64
	ProducerSeq   int64
}

// CloseProducerResult contains the result of a close-only operation with producer headers.
type CloseProducerResult struct {
	FinalOffset    Offset
	ProducerResult ProducerResult
	CurrentEpoch   int64 // Current epoch on stale epoch error
	ExpectedSeq    int64 // Expected seq on gap error
	ReceivedSeq    int64 // Received seq on gap error
	LastSeq        int64 // Highest accepted seq (for duplicates and success)
	StreamClosed   bool  // Stream is now closed
	AlreadyClosed  bool  // Stream was already closed
}

// ClosedByProducer tracks which producer closed the stream for idempotent duplicate detection
type ClosedByProducer struct {
	ProducerId string
	Epoch      int64
	Seq        int64
}

// CreateOptions contains options for creating a stream
type CreateOptions struct {
	ContentType   string
	TTLSeconds    *int64
	ExpiresAt     *time.Time
	InitialData   []byte
	Closed        bool    // Create stream in closed state
	ForkedFrom    string  // Source stream path (fork creation)
	ForkOffset    *Offset // Fork offset (nil = source's current tail)
	ForkSubOffset *uint64 // Sub-position past ForkOffset (nil = 0). Bytes for non-JSON, message count for JSON.
}

// AppendOptions contains options for appending to a stream
type AppendOptions struct {
	Seq         string // Stream-Seq header value for coordination
	ContentType string // Content-Type to validate against stream
	Close       bool   // Close stream after append (Stream-Closed: true)

	// Idempotent producer fields (all must be set together, or none)
	ProducerId    string // Producer-Id header
	ProducerEpoch *int64 // Producer-Epoch header
	ProducerSeq   *int64 // Producer-Seq header
}

// HasProducerHeaders returns true if any producer headers are set
func (o AppendOptions) HasProducerHeaders() bool {
	return o.ProducerId != "" || o.ProducerEpoch != nil || o.ProducerSeq != nil
}

// HasAllProducerHeaders returns true if all producer headers are set
func (o AppendOptions) HasAllProducerHeaders() bool {
	return o.ProducerId != "" && o.ProducerEpoch != nil && o.ProducerSeq != nil
}

// Message represents a single message in a stream
type Message struct {
	Data   []byte
	Offset Offset
}

// StreamMetadata contains metadata about a stream
type StreamMetadata struct {
	Path                string
	ContentType         string
	CurrentOffset       Offset
	LastSeq             string // Last Stream-Seq value
	TTLSeconds          *int64
	ExpiresAt           *time.Time
	CreatedAt           time.Time
	LastAccessedAt      time.Time
	Producers           map[string]*ProducerState // Producer ID -> state
	Closed              bool                      // Stream is closed (no more appends allowed)
	ClosedBy            *ClosedByProducer         // Producer that closed the stream (for idempotent duplicate detection)
	ForkedFrom          string                    // Source stream path (empty if not a fork)
	ForkOffset          Offset                    // Internal divergence point: offsets < ForkOffset come from source. For JSON forks created with a sub-offset this is advanced past the user-supplied offset; ForkOffsetRequested holds the original.
	ForkOffsetRequested *Offset                   // The user-supplied Stream-Fork-Offset (nil if omitted). Differs from ForkOffset only for JSON forks created with sub-offset > 0; used for idempotent re-creation matching.
	ForkSubOffset       uint64                    // User-supplied Stream-Fork-Sub-Offset value: bytes for non-JSON forks, flattened message count for JSON forks (0 = no sub-offset slice). Stored verbatim for idempotent re-creation matching.
	RefCount            int32                     // Number of forks referencing this stream
	SoftDeleted         bool                      // Logically deleted but retained for fork readers
}
```

`StreamMetadata` carries two behavior methods chronicle must reproduce exactly:

```go
// IsExpired checks if the stream has expired based on TTL or ExpiresAt
func (m *StreamMetadata) IsExpired() bool
// — true if (ExpiresAt != nil && now > *ExpiresAt)
//   OR (TTLSeconds != nil && now > LastAccessedAt + TTLSeconds*time.Second)

// ConfigMatches checks if another set of options matches this stream's config
func (m *StreamMetadata) ConfigMatches(opts CreateOptions) bool
```

`ConfigMatches` compares, in order: content type (base media type, case-insensitive, empty normalized to `application/octet-stream`); `TTLSeconds` (nil-ness and value); `ExpiresAt` (nil-ness and `time.Equal`); `Closed`; `ForkedFrom`; and for forks, the **user-supplied** fork offset (`ForkOffsetRequested`, falling back to `ForkOffset` for pre-PR metadata) and the raw user-supplied `ForkSubOffset` (nil and 0 are equivalent).

Package-level helpers (also used by the handler — keep exported): `ContentTypeMatches(a, b string) bool`, `ExtractMediaType(ct string) string`, `IsJSONContentType(ct string) bool` (true iff lowercased base media type == `"application/json"`), `FormatJSONResponse(messages []Message) []byte`.

---

## 2. Offset semantics (`offset.go`)

```go
// Offset represents a position within a stream.
// Format: "0000000000000000_0000000000000000" (16 digits each, zero-padded)
// The format is lexicographically sortable.
type Offset struct {
	ReadSeq    uint64 // For future log rotation support
	ByteOffset uint64 // Bytes of actual data (not framing)
}

var ZeroOffset = Offset{ReadSeq: 0, ByteOffset: 0}

// NowOffset is a sentinel value indicating "current tail position".
// Uses max uint64 values which are guaranteed never to be valid stream offsets.
var NowOffset = Offset{ReadSeq: ^uint64(0), ByteOffset: ^uint64(0)}
```

| Aspect | Behavior |
|---|---|
| Wire encoding | `String()` → `fmt.Sprintf("%016d_%016d", ReadSeq, ByteOffset)`, e.g. `0000000000000000_0000000000000011`. **Lexicographic string order == semantic order** (asserted by `TestOffsetLexicographicOrder`) — a gift for Redis (string compares in Lua, sorted-set members, etc.). Note `%016d` does not truncate: values needing >16 digits print wider, which still sorts correctly only by length-aware compare; in practice ByteOffset stays well below 10^16. |
| Parsing | `ParseOffset(s)`: `""` → ZeroOffset; `"-1"` → ZeroOffset ("start from beginning"); `"now"` → NowOffset (skip historical data); otherwise strict `digits_digits` (exactly one underscore, not at either end, digits only, min length 3 — anti-injection validation), parsed with `strconv.ParseUint(_, 10, 64)`. Non-padded forms like `"0_11"` are accepted. Anything else errors. |
| Comparison | `Compare(a, b)`: ReadSeq first, then ByteOffset; returns -1/0/1. Methods `LessThan`, `LessThanOrEqual`, `Equal`, plus `IsZero()` and `IsNow()`. |
| Arithmetic | `Add(bytes uint64)` increments ByteOffset only, ReadSeq unchanged. |
| ReadSeq | Reserved for future log rotation. **Both implementations always write ReadSeq=0.** Chronicle should do the same (but preserve the field on the wire). |

### 2.1 What an offset means physically

Offsets are **opaque resume tokens**: `Message.Offset` is the position **after** that message — the value a client passes back to read everything that follows. `Read(path, off)` returns messages whose offset is strictly greater than `off`. `CurrentOffset` in metadata is the tail (offset after the last message). Equality with `CurrentOffset` means "caller is at the tail".

The two implementations disagree about the unit, which proves clients must never do arithmetic on offsets:

| Backend | ByteOffset counts | Read mapping |
|---|---|---|
| `MemoryStore` | **payload bytes only** (`CurrentOffset.Add(uint64(len(data)))` for binary, `Add(len(msgData))` per flattened JSON element) | linear scan over `[]Message`, keep `msg.Offset.ByteOffset > offset.ByteOffset` (ReadSeq ignored in this comparison) |
| `FileStore` | **payload + 4-byte length prefix per message** (`Add(uint64(n))` where `n` is the full framed write) — i.e. the offset **is the literal byte position in the segment file** | `SegmentReader.SeekToOffset(offset.ByteOffset)` then sequential reads to EOF; for forks, logical = physical + `ForkOffset.ByteOffset` |

The `offset.go` comment "Bytes of actual data (not framing)" is accurate only for `MemoryStore`; `FileStore` includes framing. The conformance suite passes for both, so the contract is only: monotonically increasing, dense enough to seek by, zero-start, lexicographically sortable string form. **Chronicle can choose its own physical interpretation** (e.g. cumulative payload bytes, which matches MemoryStore and is the natural fit for Redis) as long as it is self-consistent and `Message.Offset`/`CurrentOffset` are the post-message positions.

A read offset that points mid-message is effectively undefined behavior: FileStore would misparse framing (recoverable garbage check via the 64 MB cap), MemoryStore skips to the next message boundary. Clients only ever echo server-minted offsets.

---

## 3. Segment model (`segment.go`) and how reads map offsets to data

Filesystem-specific, but it defines the *message* model chronicle must mirror.

```
Segment file format ("data.seg" per stream directory):
  [4-byte big-endian length][data bytes]   repeated, no separators
For JSON mode, each JSON value is stored as a separate message.
For binary mode, all data of one append is stored as a single message.
```

Constants: `SegmentFileName = "data.seg"`, `LengthPrefixSize = 4`, `MaxMessageSize = 64 * 1024 * 1024` (64 MB; `WriteMessage` returns `ErrMessageTooLarge` above it; a read that sees a length > 64 MB returns `ErrCorruptedSegment`). Empty messages (length 0) are legal at this layer.

Key signatures:

```go
func WriteMessage(w io.Writer, data []byte) (int, error)        // returns framed byte count
func ReadMessage(r io.Reader) ([]byte, error)
func NewSegmentReader(path string) (*SegmentReader, error)       // 64KB bufio
func (r *SegmentReader) ReadMessages(startOffset Offset) ([]Message, Offset, error)
func NewSegmentWriter(path string) (*SegmentWriter, error)       // O_APPEND|O_CREATE|O_WRONLY 0644
func (w *SegmentWriter) WriteMessage(data []byte) (Offset, error)
func (w *SegmentWriter) WriteMessages(messages [][]byte) (Offset, error)
func ScanSegment(path string) (Offset, error)                    // missing file -> ZeroOffset, nil
func ScanSegmentFile(file *os.File) (Offset, error)
```

Read path: seek to `startOffset.ByteOffset`, then read framed messages until EOF. Each returned `Message.Offset = previousOffset + (4 + len(data))`, with `ReadSeq` carried through from `startOffset`. So messages self-describe their *end* position.

Recovery: `ScanSegment` walks frames and stops at the first incomplete/oversized frame, returning the offset of the last complete message — used by `RecoverStore`/`RecoverStoreWithEvents(dataDir, onEvent)` to truncate torn tails after a crash and reconcile bbolt's `CurrentOffset` with the file (the **segment file is the source of truth**, bbolt is advisory). `RecoveryEvent{StreamPath, SegmentPath, OriginalSize, RecoveredSize, DiscardedBytes}` reports each repair. Tests (`TestRecoverStore_TruncatesCorruptSegmentTail`) prove that after truncation the stream accepts new appends and reads return only complete messages.

**Redis translation:** none of this framing is needed — a Redis list/stream of message payloads with stored end-offsets replaces frames, and Redis's own durability (AOF/replication) replaces `ScanSegment`. What must survive translation: per-message granularity (JSON elements are individual messages), the 64 MB max-message bound, and offsets-as-end-positions.

---

## 4. JSON mode (shared `processJSONAppend`, `memory_store.go`)

```go
func processJSONAppend(data []byte, allowEmpty bool) ([][]byte, error)
```

- Input must satisfy `json.Valid` → else `ErrInvalidJSON`.
- If the trimmed body starts with `[`: unmarshal as `[]json.RawMessage` and **flatten one level** — each element becomes its own message. An empty array is allowed only when `allowEmpty` is true (Create/PUT with `InitialData`); on Append/POST it returns `ErrEmptyJSONArray`.
- Any other JSON value: stored as a single message, **whitespace-trimmed** (`bytes.TrimSpace`).
- Reads format as `[` + `join(messages, ",")` + `]`; zero messages → `[]`.

Binary (non-JSON) appends store the entire body as one message, verbatim.

---

## 5. Operation semantics (the portable contract)

### 5.1 Create

Order of checks (identical in both stores; chronicle must preserve it):

1. Existing stream?
   - expired → delete it in place, proceed as a fresh create (`created=true`).
   - `SoftDeleted` → `ErrStreamExists` (soft-deleted paths block re-creation).
   - `ConfigMatches(opts)` → idempotent success: return existing metadata, `created=false`, `err=nil`.
   - else → `ErrConfigMismatch`.
2. Fork validation (when `opts.ForkedFrom != ""`): source must exist (`ErrStreamNotFound`), not be soft-deleted (`ErrStreamSoftDeleted`), not be expired (`ErrStreamNotFound`); an explicit `opts.ContentType` must equal the source's (case-insensitive) **before** the refcount is taken, else `ErrContentTypeMismatch`; fork offset = `opts.ForkOffset` or source `CurrentOffset`, validated `ZeroOffset <= forkOffset <= source.CurrentOffset` else `ErrInvalidForkOffset`; sub-offset resolved (§5.6); finally source `RefCount++` (rolled back on any later failure).
3. Default content type: `application/octet-stream` (or inherited from fork source).
4. Metadata initialized with `CreatedAt = LastAccessedAt = now`, `Closed = opts.Closed`, `CurrentOffset = ZeroOffset` (non-fork) or the fork offset.
5. `InitialData` appended via the normal append path with `allowEmpty=true` (so `[]` is a legal initial body producing zero messages).
6. Return `(meta, true, nil)`.

### 5.2 Append — validation pipeline (order is load-bearing)

```
1. opts.HasProducerHeaders() && !opts.HasAllProducerHeaders()  -> ErrPartialProducer
2. (if producer headers) acquire per-(stream,producer) mutex, then the store lock
3. stream missing                                              -> ErrStreamNotFound
4. meta.SoftDeleted                                            -> ErrStreamSoftDeleted
5. meta.IsExpired()                                            -> ErrStreamNotFound
6. meta.LastAccessedAt = now            (TTL sliding-window refresh)
7. meta.Closed:
     - producer headers match meta.ClosedBy tuple exactly      -> duplicate success:
         AppendResult{Offset: CurrentOffset, ProducerResult: Duplicate,
                      LastSeq: *opts.ProducerSeq, StreamClosed: true}, nil
     - otherwise -> AppendResult{Offset: CurrentOffset, StreamClosed: true}, ErrStreamClosed
8. opts.ContentType != "" && !ContentTypeMatches(...)          -> ErrContentTypeMismatch
9. producer validation (§6) — BEFORE Stream-Seq, so retries dedupe at the
   transport layer even if Stream-Seq would conflict; duplicates return
   (CurrentOffset, Duplicate, LastSeq) with nil error and NO append
10. Stream-Seq: opts.Seq != "" && meta.LastSeq != "" && opts.Seq <= meta.LastSeq
                                                               -> ErrSequenceConflict
    (plain Go string comparison — lexicographic, opaque tokens)
11. write data (JSON flatten or single binary message); advance CurrentOffset
12. record meta.LastSeq (if Seq given) and producer state (if accepted)
13. opts.Close: meta.Closed = true; record meta.ClosedBy from producer tuple
    if all producer headers present; notify long-poll waiters of closure
14. notify long-poll waiters of new data
15. return AppendResult{Offset: newOffset, ProducerResult, LastSeq, StreamClosed}
```

On producer-validation errors the result still carries `Offset = CurrentOffset` plus the diagnostic fields (`CurrentEpoch` for stale epoch; `ExpectedSeq`/`ReceivedSeq` for gaps).

### 5.3 Read

1. Missing → `ErrStreamNotFound`. Expired → `ErrStreamNotFound` (MemoryStore additionally flips `SoftDeleted=true` if `RefCount > 0`, so fork readers keep working after source expiry). SoftDeleted → `ErrStreamNotFound` (note: **Read** hides soft-deleted as not-found, whereas **Get/Delete/Append** surface `ErrStreamSoftDeleted` — Get maps to 410 Gone at the handler).
2. Refresh `LastAccessedAt` (TTL sliding window — Read counts as access).
3. FileStore fast path: `offset == CurrentOffset` → `(nil, true, nil)` without touching the segment.
4. Collect messages with offset > requested (across the fork chain, §5.6).
5. `upToDate`:
   - messages returned → `lastMessage.Offset.Equal(meta.CurrentOffset)`
   - none returned → `offset.Equal(meta.CurrentOffset) || meta.CurrentOffset.Equal(ZeroOffset)` (an empty stream is always "up to date").

`NowOffset` never reaches `Read` — the handler resolves `"now"` to the tail via `GetCurrentOffset` first.

### 5.4 WaitForMessages (long-poll)

1. Fast path: stream exists, `meta.Closed`, and `offset == CurrentOffset` → `(nil, false, true, nil)` immediately.
2. `Read(path, offset)`; error propagates; any messages → `(messages, false, false, nil)`.
3. Fork guard: if the stream is a fork and `offset < ForkOffset` (inherited range) → return `(nil, false, false, nil)` immediately — source appends never notify fork waiters, so waiting would hang.
4. Register a buffered `chan struct{}` (cap 1) with the in-process `longPollManager` keyed by path; wait on `select { ch / timer / ctx }`:
   - **ch fired**: if stream now closed → re-Read; if no messages and at tail → `(nil, false, true, nil)`, else return messages. Otherwise re-Read and return `(messages, false, false, err)`.
   - **timeout**: `(nil, true, streamClosedSnapshot, nil)`.
   - **ctx done**: `(nil, false, false, ctx.Err())`.

`longPollManager.notify` is fire-and-forget (`select ... default` non-blocking send). Append, close, and delete all notify. **This is in-process state** — for chronicle (potentially multi-instance over shared Redis), the equivalent is Redis pub/sub (or keyspace notifications) per stream path; the observable semantics above are the contract.

### 5.5 Close

- `CloseStream`: missing/expired → `ErrStreamNotFound`; otherwise set `Closed=true` (idempotent), notify waiters, return `{FinalOffset: CurrentOffset, AlreadyClosed: priorClosed}`. Does **not** set `ClosedBy`.
- `CloseStreamWithProducer`: takes the per-producer lock; missing/expired → `ErrStreamNotFound`. If already closed: exact `ClosedBy` tuple match → duplicate success `{Duplicate, LastSeq: opts.ProducerSeq, StreamClosed: true, AlreadyClosed: true}`, nil; otherwise `(..., StreamClosed: true, AlreadyClosed: true)`, `ErrStreamClosed`. If open: run §6 validation; duplicates return without closing (`AlreadyClosed=false` since the stream was open); accepted → commit producer state, `Closed=true`, set `ClosedBy{ProducerId, Epoch, Seq}`, notify, return success.

### 5.6 Forks (read-time layering, not copy)

- A fork stores only its **own** messages; offsets `< ForkOffset` resolve recursively through `ForkedFrom` (chains supported), with inherited messages capped at `msg.Offset <= ForkOffset` so post-fork source appends are invisible.
- FileStore translates offsets for the fork's own segment: physical = logical − `ForkOffset.ByteOffset` on seek, logical = physical + `ForkOffset.ByteOffset` on return.
- Fork reads **ignore SoftDeleted on the source** — forks must read through soft-deleted parents.
- Sub-offset (`ForkSubOffset > 0`): JSON source → resolved by walking `subOffset` flattened messages past `forkOffset`; the fork's internal `ForkOffset` is advanced to that message boundary (overshoot → `ErrInvalidForkSubOffset`). Binary source → the first `subOffset` bytes of the message starting at `forkOffset` are **materialized as the fork's first own message** (FileStore advances `CurrentOffset` by `4 + len(prefix)` — framing included; MemoryStore by `len(prefix)`).
- Fork expiry resolution: explicit `TTLSeconds` on the fork wins; else explicit `ExpiresAt`; else inherit source's TTL; else inherit source's `ExpiresAt`; else none. Forks have independent lifetimes — no capping at source expiry.

---

## 6. Idempotent producer state machine

Per-stream `Producers map[string]*ProducerState` keyed by `ProducerId`. `validateProducer` (identical in both stores) — the table chronicle must reproduce, ideally atomically in a Lua script:

| Existing state | Incoming `(epoch, seq)` | Outcome |
|---|---|---|
| none | seq ≠ 0 | `ErrProducerSeqGap`, result `{ExpectedSeq: 0, ReceivedSeq: seq}` |
| none | seq == 0 | **Accepted**; new state `{epoch, LastSeq: 0, LastUpdated: now}` |
| epoch < state.Epoch | any | `ErrStaleEpoch` (zombie fencing), result `{CurrentEpoch: state.Epoch}` |
| epoch > state.Epoch | seq ≠ 0 | `ErrInvalidEpochSeq` |
| epoch > state.Epoch | seq == 0 | **Accepted** (epoch bump); state reset `{epoch, LastSeq: 0}` |
| epoch == state.Epoch | seq ≤ state.LastSeq | **Duplicate** — nil error, no write, result `{Duplicate, LastSeq: state.LastSeq}` (HTTP 204) |
| epoch == state.Epoch | seq == state.LastSeq + 1 | **Accepted**; state `{epoch, LastSeq: seq}` |
| epoch == state.Epoch | seq > state.LastSeq + 1 | `ErrProducerSeqGap`, result `{ExpectedSeq: LastSeq+1, ReceivedSeq: seq}` |

State is committed only when the append/close actually succeeds (validate returns a candidate `*ProducerState`; the caller stores it after the write). Persisted durably: FileStore writes producer state into bbolt via `UpdateAppendState` (atomically with offset, LastSeq, and closed/ClosedBy). The `ClosedBy` tuple provides duplicate detection for the *closing* request even after the stream is closed (§5.2 step 7, §5.5).

Serialization: a lazily-created, **never-evicted** `map["{streamPath}:{producerId}"]*sync.Mutex` ensures validation+write is atomic per producer even when HTTP requests race (acquired before the store-wide lock). In Redis this collapses into script atomicity, but note the lock also covers the duplicate-of-ClosedBy checks.

---

## 7. Expiry / TTL and deletion semantics

### 7.1 Expiry

- **Two mechanisms:** `TTLSeconds` is a **sliding window** measured from `LastAccessedAt`; `ExpiresAt` is absolute. Either being past makes `IsExpired()` true.
- `LastAccessedAt` is refreshed by **Append and Read only** — not by `Get`, `Has`, or `GetCurrentOffset`.
- Expiry is **lazy**: every operation checks `IsExpired()` and reports `ErrStreamNotFound` (never `ErrStreamExpired`). `Create` on an expired path deletes the leftovers and recreates fresh (`created=true`).
- `FileStore` optionally runs a background sweep (`FileStoreConfig.CleanupInterval > 0`): every tick, hard-delete all expired streams (writer-pool eviction + bbolt delete + cache delete + rename-then-async-`RemoveAll`). Note the sweep does **not** honor RefCount/soft-delete (it bypasses `deleteWithCascade`) — MemoryStore's Read-path mitigation (expired + `RefCount>0` → flip to `SoftDeleted`) is the more careful behavior; chronicle should protect fork sources from expiry-sweeps the same way.
- `MemoryStore` has no background cleanup; expired entries linger until `Get`-after-expiry, `Create`, etc.

For Redis: native key TTLs can implement the sliding window (PEXPIRE refresh on Append/Read) — but all of a stream's keys (data, metadata, producer state) must expire **together**, and fork-source streams must not be allowed to vanish while `RefCount > 0`.

### 7.2 Deletion — no tombstones, but soft-delete for forks

- `Delete` on a missing path → `ErrStreamNotFound`; on an already soft-deleted path → `ErrStreamSoftDeleted` (handler: 410 Gone).
- `RefCount > 0` (fork sources): **soft delete** — `SoftDeleted=true`, data retained for fork readers. Soft-deleted streams: invisible to `Has`; `Get/Append/Delete` → `ErrStreamSoftDeleted`; `Read` → `ErrStreamNotFound`; `Create` of the same path → `ErrStreamExists`. Fork-chain reads still traverse them.
- `RefCount == 0`: **hard delete with cascading GC** — remove data; notify long-poll waiters (so blocked polls wake and re-check); if this stream was itself a fork, decrement the parent's refcount; if the parent hits 0 **and** is soft-deleted, recursively hard-delete the parent. A negative refcount is clamped to 0 and reported as `ErrRefCountUnderflow`.
- **Recreate-after-delete is fully allowed** and yields a brand-new stream: offset back to zero, producers, LastSeq, closed flag all reset. There is no tombstone for hard-deleted paths. (The handler layer may add HTTP-level caching nuances, but the store contract is clean recreate.)

---

## 8. Concurrency / locking model

| Store | Model |
|---|---|
| `MemoryStore` | One global `sync.RWMutex` over the streams map. `Create/Delete/Append/CloseStream/CloseStreamWithProducer` take the write lock; `Get/Has/GetCurrentOffset` take the read lock; **`Read` takes the full write lock** (it mutates `LastAccessedAt` and possibly `SoftDeleted`). Per-producer mutexes (§6) wrap producer appends/closes, acquired *before* the global lock. `longPollManager` has its own mutex. |
| `FileStore` | One global `metaCacheMu sync.RWMutex` over `metaCache`/`dirCache`; writes (Create/Delete/Append/Close*) hold it exclusively for the **entire** operation including the segment fsync — appends to different streams serialize on it (a known throughput ceiling, fine to improve upon in chronicle as long as observable semantics hold). `Read` takes RLock, then briefly Lock to refresh `LastAccessedAt`. Per-producer mutexes as above. `BboltMetadataStore` adds its own `RWMutex` + bbolt transactions. `FilePool`/`ReaderPool` each have a mutex. |

Write/persistence ordering in FileStore appends: segment write + fsync **first**, then bbolt metadata update — and bbolt errors are deliberately swallowed (`// Log error but don't fail - the file is the source of truth; on recovery, we'll reconcile`). Equivalent guidance for chronicle: data write and metadata update should be one atomic Redis script, eliminating the reconcile step entirely.

`Close()` of the store: FileStore stops the cleanup goroutine (waits for it), closes the writer pool then bbolt. MemoryStore's is a no-op. Neither store guards data operations against use-after-`Close()` (bbolt returns "store is closed" errors; segment appends would fail on closed handles) — there is no `ErrStoreClosed` sentinel in the contract.

---

## 9. Behavioral contract distilled from the tests

The `_test.go` files pin these behaviors (chronicle's store tests should mirror them 1:1):

- **Exact sentinel identity**: tests use `err != ErrStreamNotFound`, `err != ErrConfigMismatch`, `err != ErrSequenceConflict`, `err != ErrContentTypeMismatch`, `err != ErrStreamClosed`, `err != ErrMessageTooLarge` — return the sentinels unwrapped.
- **Create**: new → `created=true`; same config again → `created=false`, nil error; different config → `ErrConfigMismatch`. Create with `InitialData` yields nonzero `CurrentOffset` and the data is readable from `ZeroOffset` as one message (binary). Create with `Closed: true` → metadata `Closed`, append → `ErrStreamClosed`.
- **Append/Read**: append returns nonzero offset; read from `ZeroOffset` returns the message with `upToDate=true`; read from the returned tail offset → 0 messages, `upToDate=true`. JSON array append `[{"id":1},{"id":2}]` → 2 messages; `FormatResponse` reproduces `[{"id":1},{"id":2}]` exactly.
- **Stream-Seq**: same seq again → `ErrSequenceConflict`; higher seq accepted ("seq1" < "seq2" string compare).
- **Close**: `CloseStream` → `AlreadyClosed=false` first, `true` second; append after close → `ErrStreamClosed`; `Append` with `Close: true` → `result.StreamClosed=true` and metadata `Closed`.
- **Expiry**: TTL=1s stream — `Get`/`Has`/`Append`/`Read` all flip to `ErrStreamNotFound`/`false` after ~1.1 s; `ExpiresAt` behaves identically; streams without expiry never expire; FileStore background cleanup (500 ms interval) physically removes expired streams from the cache while leaving non-expiring streams intact.
- **Long-poll**: waiter at `ZeroOffset` wakes on append with exactly the new message, `timedOut=false`; waiter at tail with 100 ms timeout → `timedOut=true`, 0 messages, nil error.
- **Persistence (FileStore)**: close store, reopen on the same dir → streams, data, offsets, dir names, closed state, and `ClosedBy{ProducerId, Epoch, Seq}` all survive. Chronicle equivalent: everything in §1.2's `StreamMetadata` must round-trip through Redis (bbolt serializes it as JSON with `Producers`, `Closed`, `ClosedBy`, fork fields, `RefCount`, `SoftDeleted`; timestamps as Unix seconds; offsets as their string form).
- **Recovery**: corrupt/torn segment tails are truncated to the last complete message; subsequent appends continue from there; `RecoveryEvent` reports original/recovered/discarded byte counts.
- **Offsets**: string format, parse special cases (`""`, `"-1"`, `"now"`), strict rejection of `0,11` / `0&11` / `0=11` / `0?11` / `12345` / `abc_def`, round-trip, compare order, and lexicographic-equals-semantic ordering.
- **Segment/filepool** (FS-specific): framed write/read round-trips including empty and 1 MB payloads; >64 MB → `ErrMessageTooLarge`; writers append across reopen; pools cache handles, LRU-evict beyond max, `Remove` of unknown path is a silent no-op.

Note `Delete` is only directly tested for: success, `Has`→false afterwards, and `ErrStreamNotFound` on missing; the fork/soft-delete/cascade behaviors are covered by the (server conformance) suite rather than store unit tests.

---

## 10. Filesystem-specific vs portable

| Concern | FS-specific (do **not** port) | Portable contract (must port) |
|---|---|---|
| `Store` interface, error sentinels, options/result structs | — | everything in §1 verbatim |
| Offset type & wire format | physical meaning = file byte position (FileStore) | string format, parsing, compare, sentinels, end-position semantics, monotonicity |
| Segment framing, `data.seg`, `LengthPrefixSize` | yes | per-message granularity; `MaxMessageSize` = 64 MB bound |
| `FilePool`/`ReaderPool` LRU handle caches | yes (Redis connection pooling replaces it) | — |
| fsync-per-append, `SegmentWriter.Sync` | yes | durability *intent*: append acknowledged ⇒ durable |
| bbolt sidecar (`BboltMetadataStore`), JSON serialization, dir-name mapping (`urlencode~unixnano~rand4hex`) | yes | the *fields* persisted (full `StreamMetadata` incl. producers, ClosedBy, fork fields, RefCount, SoftDeleted) and that data+metadata commit consistently |
| `ScanSegment` truncation recovery, `RecoverStore` | yes | crash consistency: no torn/partial messages observable |
| Rename-then-async-`RemoveAll` deletion | yes | delete is logically immediate; recreate allowed instantly |
| `longPollManager` in-process channels | yes (single-node only) | WaitForMessages observable semantics (§5.4); multi-instance chronicle needs Redis pub/sub |
| Per-producer `sync.Mutex` map (never GC'd) | implementation detail | atomicity of validate+write per (stream, producer) |
| Global store mutex | implementation detail | linearizable per-stream ops; `Create` idempotency race-free |
| Background cleanup goroutine | timing detail | lazy expiry on every op is the hard requirement; sweeping is hygiene |
| JSON flatten/format, content-type matching, ConfigMatches, producer state machine, fork layering & refcounts, expiry rules | — | all portable, port exactly |

### Redis-mapping observations (forward pointers for the design doc)

1. The lexicographically sortable offset string is directly usable as a Redis sorted-set score-by-lex member or hash field; or store messages in a LIST with a parallel cumulative-offset index. MemoryStore's payload-bytes offset scheme is the cleanest to replicate (no framing constant leaks into offsets).
2. Every mutating pipeline in §5.2 must be one Lua script (or Redis 8 function) per append: existence + soft-delete + expiry + closed + content-type + producer-state CAS + Stream-Seq check + write + metadata update — that single script replaces both the global mutex and the per-producer mutex.
3. `IsExpired`/`LastAccessedAt` refresh maps to checking stored expiry fields inside the script (don't rely solely on Redis key TTL, because Create-over-expired must atomically observe expiry and recreate, and fork sources with RefCount>0 must survive).
4. Long-poll: subscribe to a per-path channel before the initial read to avoid lost-wakeup, mirroring the register-then-re-read pattern in `WaitForMessages`.
5. `RefCount`/`SoftDeleted` cascade (§7.2) needs the same script-level atomicity; `DecrementRefCount` returning `(newRefCount, softDeleted)` is the shape to copy from `bbolt.go`.
