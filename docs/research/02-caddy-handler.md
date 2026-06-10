# Research 02 — The Caddy plugin HTTP layer

Source of truth: `/Users/auk000v/dev/durable-streams/packages/caddy-plugin/`
(`handler.go`, `module.go`, `cmd/caddy/main.go`, plus the parts of `store/` the
handler depends on directly). Chronicle mirrors this layer name-for-name so it
can track the upstream codebase.

The plugin lives in Go package `durablestreams` and splits cleanly into:

| File | Role |
| --- | --- |
| `module.go` | Caddy module registration, `Handler` config struct, provisioning, Caddyfile parsing |
| `handler.go` | The entire protocol HTTP layer: routing, method handlers, long-poll, SSE, error mapping |
| `cmd/caddy/main.go` | Custom Caddy binary with a `dev` subcommand that runs an in-memory server |
| `store/` | `Store` interface + `MemoryStore` / `FileStore` implementations, `Offset`, errors, JSON helpers |
| `webhook/` | Optional webhook subscription system (`Manager`, `Routes`) |

---

## 1. Protocol header constants (`handler.go`)

```go
const (
	HeaderStreamNextOffset      = "Stream-Next-Offset"
	HeaderStreamCursor          = "Stream-Cursor"
	HeaderStreamUpToDate        = "Stream-Up-To-Date"
	HeaderStreamSeq             = "Stream-Seq"
	HeaderStreamTTL             = "Stream-TTL"
	HeaderStreamExpiresAt       = "Stream-Expires-At"
	HeaderStreamClosed          = "Stream-Closed"
	HeaderStreamSSEDataEncoding = "Stream-SSE-Data-Encoding"

	// Idempotent producer headers
	HeaderProducerId          = "Producer-Id"
	HeaderProducerEpoch       = "Producer-Epoch"
	HeaderProducerSeq         = "Producer-Seq"
	HeaderProducerExpectedSeq = "Producer-Expected-Seq"
	HeaderProducerReceivedSeq = "Producer-Received-Seq"
)

// Fork headers (request headers only — not set on responses)
const (
	HeaderStreamForkedFrom    = "Stream-Forked-From"
	HeaderStreamForkOffset    = "Stream-Fork-Offset"
	HeaderStreamForkSubOffset = "Stream-Fork-Sub-Offset"
)
```

Package-level vars in `handler.go`:

```go
var sseLineTerminators = regexp.MustCompile(`\r\n|\r|\n`)        // SSE injection defense
var cursorEpoch = time.Date(2024, 10, 9, 0, 0, 0, 0, time.UTC)  // cursor interval epoch
const cursorIntervalSeconds = 20
const (
	minJitterSeconds = 1
	maxJitterSeconds = 3600
)
var nonNegativeIntegerRegex = regexp.MustCompile(`^[0-9]+$`)
var ttlRegex = regexp.MustCompile(`^[1-9][0-9]*$|^0$`)
var subOffsetRegex = regexp.MustCompile(`^[1-9][0-9]*$|^0$`)
```

---

## 2. The `Handler` type and Caddy module plumbing (`module.go`)

```go
type Handler struct {
	// DataDir is the directory for storing stream data.
	// If empty, uses in-memory storage (for testing).
	DataDir string `json:"data_dir,omitempty"`

	// MaxFileHandles is the maximum number of open file handles to cache
	MaxFileHandles int `json:"max_file_handles,omitempty"`

	// LongPollTimeout is the default timeout for long-poll requests
	LongPollTimeout caddy.Duration `json:"long_poll_timeout,omitempty"`

	// SSEReconnectInterval is how often SSE connections should reconnect
	SSEReconnectInterval caddy.Duration `json:"sse_reconnect_interval,omitempty"`

	// WebhookCallbackURL enables the webhook subscription system if set
	WebhookCallbackURL string `json:"webhook_callback_url,omitempty"`

	store          store.Store
	logger         *zap.Logger
	webhookManager *webhook.Manager
	webhookRoutes  *webhook.Routes
}
```

Lifecycle / registration functions:

```go
func init()                                                  // caddy.RegisterModule(Handler{}); httpcaddyfile.RegisterHandlerDirective("durable_streams", parseCaddyfile)
func (Handler) CaddyModule() caddy.ModuleInfo                // ID: "http.handlers.durable_streams"
func (h *Handler) Provision(ctx caddy.Context) error
func (h *Handler) Validate() error                           // no-op, returns nil
func (h *Handler) Cleanup() error                            // webhookManager.Shutdown(); store.Close()
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error)
func parseIntArg(s string) (int, error)                      // fmt.Sscanf(s, "%d", &val)
```

Interface guards (chronicle should keep equivalents where applicable):

```go
var (
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
```

### Provision behavior

1. `h.logger = ctx.Logger()`.
2. Defaults applied only when zero-valued:

| Field | Default |
| --- | --- |
| `MaxFileHandles` | `100` |
| `LongPollTimeout` | `caddy.Duration(30 * time.Second)` |
| `SSEReconnectInterval` | `caddy.Duration(60 * time.Second)` |
| `DataDir` | `""` → `store.NewMemoryStore()` |

3. If `DataDir != ""`: run `store.RecoverStoreWithEvents(h.DataDir, fn)` (logs a
   warning per truncated corrupt segment tail), then
   `store.NewFileStore(store.FileStoreConfig{DataDir, MaxFileHandles})`.
4. If `WebhookCallbackURL != ""`: build `webhook.NewManager(h.WebhookCallbackURL,
   getTailOffset, h.logger, nil)` and `webhook.NewRoutes(h.webhookManager)`.
   `getTailOffset` is a closure `func(path string) string` returning
   `meta.CurrentOffset.String()` or `"-1"` on store error.

### Caddyfile directive

```caddyfile
durable_streams {
    data_dir /var/lib/durable-streams
    max_file_handles 100
    long_poll_timeout 30s
    sse_reconnect_interval 60s
    webhook_callback_url https://example.com/callbacks
}
```

Subdirectives: `data_dir`, `max_file_handles` (via `parseIntArg`),
`long_poll_timeout` and `sse_reconnect_interval` (via `caddy.ParseDuration`),
`webhook_callback_url`. Unknown subdirectives error with
`d.Errf("unknown subdirective: %s", d.Val())`.

---

## 3. Request lifecycle — `ServeHTTP`

```go
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error
```

Order of operations (every request, before any dispatch):

1. **CORS headers** — set unconditionally:
   - `Access-Control-Allow-Origin: *`
   - `Access-Control-Allow-Methods: GET, POST, PUT, DELETE, HEAD, OPTIONS`
   - `Access-Control-Allow-Headers: Content-Type, Stream-Seq, Stream-TTL, Stream-Expires-At, Stream-Closed, If-None-Match, Producer-Id, Producer-Epoch, Producer-Seq, Stream-Forked-From, Stream-Fork-Offset, Stream-Fork-Sub-Offset, Authorization`
   - `Access-Control-Expose-Headers: Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, Stream-Closed, ETag, Location, Producer-Epoch, Producer-Seq, Producer-Expected-Seq, Producer-Received-Seq`
2. **Browser security headers** (protocol §10.7): `X-Content-Type-Options: nosniff`,
   `Cross-Origin-Resource-Policy: cross-origin`.
3. **OPTIONS preflight** → `204 No Content`, return.
4. **Webhook routes** — if `h.webhookRoutes != nil` and
   `h.webhookRoutes.HandleRequest(w, r)` returns true, the request was consumed.
5. **Stream path** = `r.URL.Path` **verbatim** (the route prefix, e.g.
   `/v1/stream/foo`, is part of the stream identity — there is no prefix
   stripping).
6. Debug log of method/path/query, then **method dispatch**:

| Method | Handler |
| --- | --- |
| `PUT` | `handleCreate` |
| `HEAD` | `handleHead` |
| `GET` | `handleRead` |
| `POST` | `handleAppend` |
| `DELETE` | `handleDelete` |
| anything else | `405 Method Not Allowed` |

7. Any non-nil error from a handler funnels into `h.writeError(w, err)`.
   `ServeHTTP` itself always returns `nil` (never delegates to `next`).

### Error machinery

```go
type httpError struct {
	status  int
	message string
}

func (e *httpError) Error() string
func newHTTPError(status int, message string) *httpError
func (h *Handler) writeError(w http.ResponseWriter, err error)
```

`writeError` unwraps with `errors.As`; an `*httpError` becomes
`http.Error(w, message, status)` (plain-text body). Anything else is logged at
error level (`"internal error"`) and becomes a generic
`500 internal server error`. **Exception:** several append/close error paths
need response headers set alongside the status, so they call `http.Error`
directly inside the handler and return `nil` (stale epoch, seq gap, stream
closed, soft-deleted create).

---

## 4. `handleCreate` — `PUT`

```go
func (h *Handler) handleCreate(w http.ResponseWriter, r *http.Request, path string) error
```

Request parsing:

- `Content-Type`, `Stream-TTL`, `Stream-Expires-At`, `Stream-Closed`
  (`closedStr == "true"` only; anything else is false).
- Fork headers: `Stream-Forked-From`, `Stream-Fork-Offset`, and
  `Stream-Fork-Sub-Offset` — the sub-offset uses `r.Header.Values()` to
  distinguish *present-but-empty* from *absent* (empty-but-present is an
  error via `parseSubOffset`).
- `Stream-TTL` and `Stream-Expires-At` together → `400` ("cannot specify both…").
- TTL parsed by `parseTTL` (strict digits, no leading zeros except `"0"`);
  failure → `400` with the parse error's message.
- `Stream-Expires-At` parsed with `time.Parse(time.RFC3339, …)`; failure →
  `400 "invalid Stream-Expires-At format"`.
- Body: read fully **only when `r.ContentLength > 0`** (a chunked PUT with
  unknown length would *not* be read — subtle, mirror exactly or note the
  deviation). Read failure → `400 "failed to read body"`.
- `Stream-Fork-Offset` parsed via `store.ParseOffset`; failure → `400`.
- `Stream-Fork-Sub-Offset` without `Stream-Forked-From` → `400`.

Store call: `h.store.Create(path, store.CreateOptions{...})` returning
`(meta *store.StreamMetadata, wasCreated bool, err error)`.

Error mapping (`errors.Is`):

| Store error | Status / message |
| --- | --- |
| `ErrStreamNotFound` | `404 source stream not found` |
| `ErrInvalidForkOffset` | `400 fork offset beyond source stream length` |
| `ErrInvalidForkSubOffset` | `400 fork sub-offset overshoots or is invalid` |
| `ErrStreamSoftDeleted` | `409 source stream was deleted but still has active forks` |
| `ErrStreamExists` | `409 stream already exists` |
| `ErrConfigMismatch` | `409 stream exists with different configuration` |
| `ErrContentTypeMismatch` | `409 fork content type does not match source stream` |
| other | bubbles to `writeError` → `500` |

Success path:

- If `meta.SoftDeleted` → raw `409` with body
  `"stream was deleted but still has active forks — path cannot be reused until all forks are removed"`, return nil.
- Response headers: `Content-Type: meta.ContentType`,
  `Stream-Next-Offset: meta.CurrentOffset.String()`, and
  `Stream-Closed: true` if `meta.Closed`.
- Webhook notifications **only when `wasCreated`**: `OnStreamCreated(path)`
  and additionally `OnStreamAppend(path)` if `len(initialData) > 0`.
- `wasCreated == true` → build absolute `Location` and respond `201 Created`:
  - scheme: `http`, upgraded to `https` if `r.TLS != nil`, **overridden** by
    `X-Forwarded-Proto` when present;
  - host: `r.Host`, overridden by `X-Forwarded-Host` when present;
  - `Location = fmt.Sprintf("%s://%s%s", scheme, host, r.URL.Path)`.
- `wasCreated == false` (idempotent re-create with matching config) → `200 OK`.

---

## 5. `handleHead` — `HEAD`

```go
func (h *Handler) handleHead(w http.ResponseWriter, r *http.Request, path string) error
```

`h.store.Get(path)`; `ErrStreamNotFound` → `404 stream not found`,
`ErrStreamSoftDeleted` → `410 stream has been deleted`. On success, `200 OK`
with headers only:

- `Content-Type`, `Stream-Next-Offset` (current tail), `Cache-Control: no-store`
- `Stream-TTL` if `meta.TTLSeconds != nil` (decimal seconds)
- `Stream-Expires-At` if `meta.ExpiresAt != nil` (RFC 3339)
- `Stream-Closed: true` if closed

There is also an unused-by-HEAD helper kept on the handler:

```go
// isSoftDeleted checks if a stream is soft-deleted and writes 410 Gone if so.
func (h *Handler) isSoftDeleted(w http.ResponseWriter, meta *store.StreamMetadata) bool
```

---

## 6. `handleRead` — `GET` (catch-up, long-poll, SSE entry)

```go
func (h *Handler) handleRead(w http.ResponseWriter, r *http.Request, path string) error
```

### 6.1 Existence + query parsing

1. `h.store.Get(path)` → `404` / `410` mapping as in HEAD.
2. `offset` query parameter, parsed with care:
   - `query["offset"]` (not `Get`) to distinguish *provided* from *missing*;
   - more than one `offset` param → `400 multiple offset parameters not allowed`;
   - provided-but-empty (`?offset=`) → `400 offset parameter cannot be empty`;
   - missing → `store.ParseOffset("")` → `ZeroOffset` (read from start);
   - invalid format → `400 invalid offset`.
3. `live` and `cursor` query params read with `query.Get`.
4. `live=long-poll` without an offset param → `400 offset required for long-poll mode`.
   `live=sse` without an offset param → `400 offset required for SSE mode`.

### 6.2 Offset semantics (`store.ParseOffset`)

```go
type Offset struct {
	ReadSeq    uint64
	ByteOffset uint64
}
var ZeroOffset = Offset{0, 0}
var NowOffset  = Offset{^uint64(0), ^uint64(0)} // sentinel for "now"

func ParseOffset(s string) (Offset, error) // "" and "-1" → ZeroOffset; "now" → NowOffset;
                                           // else strict `digits_digits` (one underscore)
func (o Offset) String() string            // fmt.Sprintf("%016d_%016d", ReadSeq, ByteOffset)
func (o Offset) IsZero() bool
func (o Offset) IsNow() bool
func (o Offset) Add(bytes uint64) Offset
func Compare(a, b Offset) int
func (o Offset) LessThan(other Offset) bool
func (o Offset) LessThanOrEqual(other Offset) bool
func (o Offset) Equal(other Offset) bool
```

### 6.3 SSE branch (first)

If `live == "sse"`:

- Base64 auto-detection from the **stream's** content type:
  `ct := strings.ToLower(store.ExtractMediaType(meta.ContentType))`;
  text-compatible means `strings.HasPrefix(ct, "text/") || ct == "application/json"`;
  `useBase64 = !isTextCompatible`.
- `offset=now` is converted to `meta.CurrentOffset` *before* entering SSE.
- Delegate: `return h.handleSSE(w, r, path, sseOffset, cursor, useBase64)`.

### 6.4 `offset=now` catch-up (non-long-poll)

If `offset.IsNow()` and `live != "long-poll"` → immediate empty response:

- `Content-Type`, `Stream-Next-Offset: meta.CurrentOffset`, `Stream-Up-To-Date: true`,
  `Stream-Closed: true` if closed, `Cache-Control: no-store`.
- **No ETag** (deliberate: `no-store` + ETag confuses some CDNs).
- Body: `[]` for JSON streams (`store.IsJSONContentType`), empty otherwise. `200 OK`.

For long-poll, `offset=now` instead becomes `effectiveOffset = meta.CurrentOffset`
and falls through to the wait logic.

### 6.5 Read + long-poll wait

`messages, _, err := h.store.Read(path, effectiveOffset)` (the `upToDate` return
is ignored here; it is recomputed from metadata). `nextOffset` = last message's
offset, or `meta.CurrentOffset` when no messages.

**Wait condition** (exact):

```go
shouldWait := liveMode == "long-poll" && len(messages) == 0 &&
	(isNowOffset || effectiveOffset.Equal(meta.CurrentOffset))
```

Inside the wait branch:

- **Closed stream short-circuit**: if `meta.Closed`, respond immediately
  `204 No Content` with `Content-Type`, `Stream-Next-Offset` (tail),
  `Stream-Up-To-Date: true`, `Stream-Closed: true`,
  `Stream-Cursor: generateResponseCursor(cursor)`.
- Otherwise:

```go
timeout := time.Duration(h.LongPollTimeout)
ctx, cancel := context.WithTimeout(r.Context(), timeout)
defer cancel()
messages, timedOut, streamClosed, err = h.store.WaitForMessages(ctx, path, effectiveOffset, timeout)
```

  Note the timeout is supplied **twice** — as a context deadline *and* as the
  store-level timer. Either may fire.

- `err` is `context.Canceled` (client disconnect) or `context.DeadlineExceeded`
  → `204` with `Stream-Next-Offset: effectiveOffset`, `Stream-Up-To-Date: true`,
  `Stream-Cursor`, plus `Stream-Closed: true` if a re-`Get` shows the stream
  closed meanwhile. Any other error bubbles to `500`.
- `streamClosed == true` → same `204` shape with `Stream-Closed: true`.
- `timedOut == true` → same `204` shape, `Stream-Closed` only if re-`Get` says so.
- New messages → update `nextOffset` and fall through to the normal response.

### 6.6 Normal data response

1. Re-fetch tail: `currentMeta, _ := h.store.Get(path)`;
   `upToDate := nextOffset.Equal(currentMeta.CurrentOffset)`.
   (Sharp edge: the error is ignored — if the stream were deleted mid-request
   this dereferences nil. Chronicle should mirror behavior but may want to
   harden.)
2. Headers: `Content-Type`, `Stream-Next-Offset: nextOffset`;
   `Stream-Up-To-Date: true` iff at tail; `Stream-Closed: true` iff
   `currentMeta.Closed && upToDate`.
3. `Stream-Cursor: generateResponseCursor(cursor)` **only for `live=long-poll`**
   responses (CDN cache-collision prevention).
4. `ETag: "<nextOffset>"` (double-quoted, e.g. `"0000000000000000_0000000000000042"`),
   always set on this path.
5. `Cache-Control: public, max-age=60, stale-while-revalidate=300` **only when**
   `!upToDate && len(messages) > 0` (historical chunk).
6. `If-None-Match` exact-string compare against the quoted ETag → `304 Not Modified`
   (no weak validators, no comma lists).
7. Body via `h.formatResponse(...)`, `200 OK`, single `w.Write(body)`.

### 6.7 Response formatting

```go
func (h *Handler) formatResponse(path string, messages []store.Message, contentType string) ([]byte, error)
```

- JSON streams: `store.FormatJSONResponse(messages)` — one pre-sized buffer,
  `[` + comma-joined raw message bytes + `]`; empty slice → `[]`.
- Non-JSON: total length computed first, single `make([]byte, 0, total)`
  allocation, raw concatenation. No chunked/streamed body for plain GET — the
  whole response is buffered then written once.

---

## 7. Cursor generation (long-poll + SSE control events)

```go
func generateCursor() string
func generateResponseCursor(clientCursor string) string
```

- Cursor = decimal interval number since `cursorEpoch` (2024-10-09T00:00:00Z) in
  `cursorIntervalSeconds`-sized (20 s) buckets:
  `intervalNumber = (nowMs - epochMs) / (20 * 1000)`.
- `generateResponseCursor` enforces monotonic progression:
  - no client cursor → current interval;
  - unparsable client cursor or `clientInterval < currentInterval` → current interval;
  - client at-or-ahead → `clientInterval + jitterIntervals`. Despite the
    "random jitter" comment, the implementation is deterministic:
    `jitterSeconds = 1 + (3600-1)/2 = 1800`, so `jitterIntervals = 1800/20 = 90`
    (i.e. client cursor advances by 90 intervals = 30 minutes).

---

## 8. Long-poll waiter mechanism (store layer)

The `Store` contract the handler relies on:

```go
// WaitForMessages waits for new messages after the given offset.
// Returns when messages are available, timeout expires, context is cancelled,
// or stream is closed. If messages exist at the offset, returns immediately.
WaitForMessages(ctx context.Context, path string, offset Offset, timeout time.Duration) (messages []Message, timedOut bool, streamClosed bool, err error)
```

Both `MemoryStore` and `FileStore` share one implementation pattern via a
`longPollManager` (defined in `memory_store.go`, reused by `file_store.go`):

```go
type longPollManager struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

func (m *longPollManager) register(path string, ch chan struct{})
func (m *longPollManager) unregister(path string, ch chan struct{})
func (m *longPollManager) notify(path string)        // non-blocking send to every waiter
func (m *longPollManager) notifyClosed(path string)  // alias for notify
```

Semantics chronicle must reproduce (over Redis pub/sub instead of in-process
channels):

- A waiter is a **buffered `chan struct{}` of capacity 1** registered per
  stream path; `WaitForMessages` does
  `ch := make(chan struct{}, 1); register; defer unregister`.
- `notify` iterates all waiters for the path with a **non-blocking send**
  (`select { case ch <- struct{}{}: default: }`) — wakeups are advisory
  edge-triggers, not data carriers; the waiter always re-reads the store.
- `notify` fires on append and on delete; `notifyClosed` (== `notify`) fires on
  close. Fork streams are *not* notified by source-stream appends, and
  `WaitForMessages` returns immediately (no wait) when the requested offset is
  in a fork's inherited range.
- `WaitForMessages` flow: (1) immediate `streamClosed` return if closed and
  offset == tail; (2) immediate read — return messages if any; (3) register
  waiter; (4) `select` over wake channel / `time.NewTimer(timeout)` /
  `ctx.Done()`. On wake it re-checks closed state and re-reads; on timer it
  returns `timedOut=true` plus current closed state; on context it returns
  `ctx.Err()`.

---

## 9. SSE implementation — `handleSSE`

```go
func (h *Handler) handleSSE(w http.ResponseWriter, r *http.Request, path string, offset store.Offset, cursor string, useBase64 bool) error
```

### Setup

- `h.store.Get(path)` once up front; this `meta` provides the content type for
  all `formatResponse` calls during the connection (closed state is re-fetched
  each loop iteration instead).
- Response headers: `Content-Type: text/event-stream`, `Cache-Control: no-cache`,
  `Connection: keep-alive`, `X-Accel-Buffering: no`, plus
  `Stream-SSE-Data-Encoding: base64` when `useBase64`.
- `w.(http.Flusher)` required; if unavailable →
  `500 streaming not supported`. `WriteHeader(200)` + immediate `Flush()`.
- `reconnectTimer := time.NewTimer(time.Duration(h.SSEReconnectInterval))` —
  when it fires the handler simply `return nil`, closing the connection "to
  allow CDN collapsing". **There are no keep-alive comments/pings** — the
  reconnect interval (default 60 s) is the only liveness mechanism; clients
  are expected to reconnect with their last offset.

### Main loop

`for { select { <-ctx.Done(): return nil; <-reconnectTimer.C: return nil; default: ... } }`
— i.e. a poll loop; cancellation/reconnect are only observed between
iterations. Each iteration:

1. `messages, upToDate, err := h.store.Read(path, currentOffset)`.
2. Re-fetch `currentMeta, _ := h.store.Get(path)`;
   `streamIsClosed := currentMeta != nil && currentMeta.Closed`.
3. **If messages exist** — emit a `data` event then a `control` event:

   ```
   event: data
   data:<payload line(s)>

   event: control
   data:<control JSON>

   ```

   - Payload comes from `h.formatResponse(path, messages, meta.ContentType)`.
   - `useBase64`: single `data:` line containing
     `base64.StdEncoding.EncodeToString(body)`.
   - Text: body split on `sseLineTerminators` (`\r\n|\r|\n`) and each line
     emitted as `data:%s\n` — **no space after the colon**, deliberately,
     because SSE clients strip exactly one leading space (adding one would
     corrupt data beginning with a space). Splitting on all three terminator
     forms blocks SSE event-injection.
   - `currentOffset` advances to the last message's offset.
   - Control JSON (`map[string]interface{}` → `json.Marshal`):
     - always `"streamNextOffset": currentOffset.String()`;
     - if closed **and** client now at tail: `"streamClosed": true` only
       (cursor omitted; `upToDate` implied per protocol) — then `Flush` and
       **return** (connection closes);
     - otherwise `"streamCursor": generateResponseCursor(cursor)` and
       `"upToDate": true` when the `Read` said so.
   - `Flush()` after every data+control pair; `sentInitialControl = true`.
4. **Else if `!sentInitialControl`** — emit the initial control event even for
   an empty stream: `streamNextOffset` = `currentMeta.CurrentOffset`; if the
   stream is already closed and the client is at tail → `streamClosed: true`
   and return; otherwise `streamCursor` + unconditional `upToDate: true`.
5. **Else if `streamIsClosed`** — close arrived after the initial control with
   no new data (close-only request): if `currentOffset` equals the tail, emit
   the final control `{streamNextOffset, streamClosed: true}`, flush, return.
   (A close that lands together with a final append is handled by branch 3 on
   the same iteration — see the in-code comment about draining the final
   append before emitting `streamClosed`, otherwise a caught-up live reader
   would silently lose the final append.)
6. **Sleep/wake**: 

   ```go
   timeout := 100 * time.Millisecond
   waitCtx, cancel := context.WithTimeout(ctx, timeout)
   h.store.WaitForMessages(waitCtx, path, currentOffset, timeout)
   cancel()
   ```

   Return values are deliberately ignored — `WaitForMessages` is used purely
   as an interruptible 100 ms sleep (wakes early on notify). Effective idle
   poll rate is ~10/s per SSE connection.

---

## 10. `handleAppend` — `POST` (incl. idempotent producers and close)

```go
func (h *Handler) handleAppend(w http.ResponseWriter, r *http.Request, path string) error
```

### Parsing and validation order

1. `h.store.Get(path)` → `404` / `410` mapping.
2. `closeStream := r.Header.Get(HeaderStreamClosed) == "true"`.
3. `contentType := r.Header.Get("Content-Type")`.
4. Body read fully (`io.ReadAll`); failure → `400 failed to read body`.
5. Producer headers extracted: `Producer-Id`, `Producer-Epoch`, `Producer-Seq`.
   - All-or-none: any subset present but not all →
     `400 "all producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided together"`.
   - Epoch/seq validated with `isValidIntegerString` (regex `^[0-9]+$` — so
     negatives and signs are rejected at the HTTP layer) then
     `strconv.ParseInt(s, 10, 64)`; failures →
     `400 invalid Producer-Epoch: must be an integer` /
     `400 invalid Producer-Seq: must be an integer`.

### Close-only path (`len(body) == 0 && closeStream`)

Content-Type validation is skipped per protocol §5.2.

- **With producer headers** → `h.store.CloseStreamWithProducer(path,
  store.CloseProducerOptions{ProducerId, ProducerEpoch, ProducerSeq})`:
  - `ErrStreamNotFound` → `404`
  - `ErrStaleEpoch` → set `Producer-Epoch: result.CurrentEpoch`, then
    `http.Error(w, "producer epoch is stale", 403)`
  - `ErrInvalidEpochSeq` → `400 new epoch must start at sequence 0`
  - `ErrProducerSeqGap` → set `Producer-Expected-Seq` / `Producer-Received-Seq`
    from result, then `http.Error(w, "producer sequence gap detected", 409)`
  - `ErrStreamClosed` → set `Stream-Closed: true`, then
    `http.Error(w, "stream is closed", 409)`
  - Success → `204` with `Stream-Next-Offset: result.FinalOffset`,
    `Stream-Closed: true`, `Producer-Epoch` (echo), `Producer-Seq: result.LastSeq`.
- **Without producer headers** → `h.store.CloseStream(path)` (idempotent):
  `404` on not-found; success → `204` with `Stream-Next-Offset` +
  `Stream-Closed: true`.

### Data append path

- Empty body without `Stream-Closed: true` → `400 empty body not allowed`.
- Missing `Content-Type` → `400 Content-Type header is required`.
- `!store.ContentTypeMatches(meta.ContentType, contentType)` →
  `409 content type mismatch` (checked at HTTP layer *before* the store call;
  comparison ignores case and parameters, empty normalizes to
  `application/octet-stream`).
- Store call:

```go
opts := store.AppendOptions{
	Seq:         r.Header.Get(HeaderStreamSeq),
	ContentType: contentType,
	Close:       closeStream,
}
// + ProducerId / ProducerEpoch / ProducerSeq when all present
result, err := h.store.Append(path, body, opts)
```

Error mapping:

| Store error | Response |
| --- | --- |
| `ErrStreamClosed` | headers `Stream-Closed: true`, `Stream-Next-Offset: result.Offset`; `http.Error(409, "stream is closed")` |
| `ErrSequenceConflict` | `409 sequence number conflict` |
| `ErrContentTypeMismatch` | `409 content type mismatch` |
| `ErrInvalidJSON` | `400 invalid JSON` |
| `ErrEmptyJSONArray` | `400 empty JSON array not allowed` |
| `ErrPartialProducer` | `400` (same all-or-none message) |
| `ErrStaleEpoch` | headers `Stream-Next-Offset`, `Producer-Epoch: result.CurrentEpoch`; `http.Error(403, "producer epoch is stale")` — zombie fencing |
| `ErrInvalidEpochSeq` | `400 new epoch must start at sequence 0` |
| `ErrProducerSeqGap` | headers `Stream-Next-Offset`, `Producer-Expected-Seq: result.ExpectedSeq`, `Producer-Received-Seq: result.ReceivedSeq`; `http.Error(409, "producer sequence gap detected")` |
| other | `500` |

Note: the store populates `result.Offset` (and epoch/seq fields) even on error
returns — the handler reads them when building error responses.

Success path:

- `Stream-Next-Offset: result.Offset`; `Stream-Closed: true` if
  `result.StreamClosed`.
- If producer headers were used: echo `Producer-Epoch` and set
  `Producer-Seq: result.LastSeq` (highest accepted seq, per PROTOCOL.md).
- `result.ProducerResult == store.ProducerResultDuplicate` → `204 No Content`
  (no webhook notification), return.
- Otherwise notify `h.webhookManager.OnStreamAppend(path)` (if configured), then:
  - producer append (new write) → **`200 OK`** (distinguishes from duplicate 204);
  - non-producer append → **`204 No Content`**.

---

## 11. `handleDelete` — `DELETE`

```go
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, path string) error
```

`h.store.Delete(path)`; `ErrStreamNotFound` → `404`, `ErrStreamSoftDeleted` →
`410 stream has been deleted`. Success → notify
`webhookManager.OnStreamDeleted(path)` (if configured), `204 No Content`.

---

## 12. Header-parsing helpers (exact signatures)

```go
func isValidIntegerString(s string) bool       // regex ^[0-9]+$ (non-negative only)
func parseTTL(s string) (int64, error)         // regex ^[1-9][0-9]*$|^0$ then ParseInt; no leading zeros/sign/float
func parseSubOffset(s string) (uint64, error)  // same digit-only regex, ParseUint
```

---

## 13. Store-layer surface the handler depends on

Everything below must exist (same names) for chronicle's Redis store to slot in
behind the handler unchanged.

### Interface

```go
type Store interface {
	Create(path string, opts CreateOptions) (*StreamMetadata, bool, error)
	Get(path string) (*StreamMetadata, error)
	Has(path string) bool
	Delete(path string) error
	Append(path string, data []byte, opts AppendOptions) (AppendResult, error)
	CloseStream(path string) (*CloseResult, error)
	CloseStreamWithProducer(path string, opts CloseProducerOptions) (*CloseProducerResult, error)
	Read(path string, offset Offset) ([]Message, bool, error)
	WaitForMessages(ctx context.Context, path string, offset Offset, timeout time.Duration) (messages []Message, timedOut bool, streamClosed bool, err error)
	GetCurrentOffset(path string) (Offset, error)
	Close() error
}
```

### Error variables (`errors.Is` targets)

```go
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

	ErrStaleEpoch      = errors.New("producer epoch is stale")
	ErrInvalidEpochSeq = errors.New("new epoch must start at sequence 0")
	ErrProducerSeqGap  = errors.New("producer sequence gap detected")
	ErrPartialProducer = errors.New("all producer headers must be provided together")

	ErrStreamSoftDeleted    = errors.New("stream is soft-deleted")
	ErrInvalidForkOffset    = errors.New("fork offset beyond source stream length")
	ErrInvalidForkSubOffset = errors.New("fork sub-offset overshoots or is invalid")
	ErrRefCountUnderflow    = errors.New("reference count underflow")
)
```

### Result / option types

```go
type ProducerState struct { Epoch, LastSeq, LastUpdated int64 }

type ProducerResult int
const (
	ProducerResultNone ProducerResult = iota
	ProducerResultAccepted
	ProducerResultDuplicate
)

type AppendResult struct {
	Offset         Offset
	ProducerResult ProducerResult
	CurrentEpoch   int64
	ExpectedSeq    int64
	ReceivedSeq    int64
	LastSeq        int64
	StreamClosed   bool
}

type CloseResult struct {
	FinalOffset   Offset
	AlreadyClosed bool
}

type CloseProducerOptions struct {
	ProducerId    string
	ProducerEpoch int64
	ProducerSeq   int64
}

type CloseProducerResult struct {
	FinalOffset    Offset
	ProducerResult ProducerResult
	CurrentEpoch   int64
	ExpectedSeq    int64
	ReceivedSeq    int64
	LastSeq        int64
	StreamClosed   bool
	AlreadyClosed  bool
}

type CreateOptions struct {
	ContentType   string
	TTLSeconds    *int64
	ExpiresAt     *time.Time
	InitialData   []byte
	Closed        bool
	ForkedFrom    string
	ForkOffset    *Offset
	ForkSubOffset *uint64
}

type AppendOptions struct {
	Seq           string
	ContentType   string
	Close         bool
	ProducerId    string
	ProducerEpoch *int64
	ProducerSeq   *int64
}
func (o AppendOptions) HasProducerHeaders() bool
func (o AppendOptions) HasAllProducerHeaders() bool

type Message struct {
	Data   []byte
	Offset Offset
}

type StreamMetadata struct {
	Path                string
	ContentType         string
	CurrentOffset       Offset
	LastSeq             string
	TTLSeconds          *int64
	ExpiresAt           *time.Time
	CreatedAt           time.Time
	LastAccessedAt      time.Time
	Producers           map[string]*ProducerState
	Closed              bool
	ClosedBy            *ClosedByProducer
	ForkedFrom          string
	ForkOffset          Offset
	ForkOffsetRequested *Offset
	ForkSubOffset       uint64
	RefCount            int32
	SoftDeleted         bool
}
func (m *StreamMetadata) IsExpired() bool
func (m *StreamMetadata) ConfigMatches(opts CreateOptions) bool
```

### Content-type / JSON helpers used by the handler

```go
func ContentTypeMatches(a, b string) bool     // case-insensitive media type, params ignored, "" → application/octet-stream
func ExtractMediaType(ct string) string       // strips ";..." parameters
func IsJSONContentType(ct string) bool        // media type == "application/json" (lowercased)
func FormatJSONResponse(messages []Message) []byte
```

---

## 14. Dev-mode binary — `cmd/caddy/main.go`

`package main`. Imports `caddycmd "github.com/caddyserver/caddy/v2/cmd"`, blank
imports `modules/standard` and the plugin package.

```go
const defaultCaddyfile = `{
	admin off
	auto_https off
}

:4437 {
	route /v1/stream/* {
		durable_streams
	}
}
`

func main()        // os.Args[1] == "dev" → runDevMode(); else caddycmd.Main()
func runDevMode()  // banner prints; temp Caddyfile; os.Args = {argv0, "run", "--config", tmpfile}; caddycmd.Main()
```

Dev-mode facts chronicle's dev server should match:

- Port **4437**, endpoint prefix `/v1/stream/*`, **in-memory** storage (no
  `data_dir` subdirective → `NewMemoryStore`), admin API and auto-HTTPS off.
- Because Caddy's `route` doesn't strip the prefix and the handler uses
  `r.URL.Path` directly, **stream paths include the `/v1/stream/...` prefix**.
- The temp Caddyfile is written to `os.CreateTemp("", "Caddyfile.*")` and
  removed on exit; dev mode is implemented purely by rewriting `os.Args` to
  `run --config <tempfile>` before calling `caddycmd.Main()`.

---

## 15. Status-code matrix (quick reference)

| Operation | Outcome | Status | Notable headers |
| --- | --- | --- | --- |
| OPTIONS | preflight | 204 | CORS set |
| PUT | created | 201 | `Location`, `Stream-Next-Offset`, `Content-Type`, `Stream-Closed?` |
| PUT | idempotent match | 200 | `Stream-Next-Offset`, `Content-Type` |
| PUT | bad TTL/expiry/fork headers, both TTL+expiry | 400 | — |
| PUT | exists w/ different config, soft-deleted path, fork CT mismatch | 409 | — |
| PUT | fork source missing | 404 | — |
| HEAD | ok | 200 | `Stream-Next-Offset`, `Stream-TTL?`, `Stream-Expires-At?`, `Stream-Closed?`, `Cache-Control: no-store` |
| HEAD/GET/POST/DELETE | missing | 404 | — |
| HEAD/GET/POST/DELETE | soft-deleted | 410 | — |
| GET | data | 200 | `Stream-Next-Offset`, `Stream-Up-To-Date?`, `Stream-Closed?`, `ETag`, `Stream-Cursor` (long-poll), `Cache-Control` (historical) |
| GET | `If-None-Match` hit | 304 | `ETag` |
| GET `offset=now` (catch-up) | empty | 200 | `Stream-Up-To-Date: true`, `Cache-Control: no-store`, no ETag; body `[]` if JSON |
| GET long-poll | timeout / cancel / closed-at-tail | 204 | `Stream-Next-Offset`, `Stream-Up-To-Date: true`, `Stream-Cursor`, `Stream-Closed?` |
| GET | bad offset / dup offset / empty offset / live mode without offset | 400 | — |
| POST | non-producer append ok | 204 | `Stream-Next-Offset`, `Stream-Closed?` |
| POST | producer append, new write | 200 | + `Producer-Epoch`, `Producer-Seq` |
| POST | producer duplicate | 204 | + `Producer-Epoch`, `Producer-Seq` |
| POST | close-only ok | 204 | `Stream-Next-Offset`, `Stream-Closed: true` (+ producer echoes) |
| POST | stream closed | 409 | `Stream-Closed: true`, `Stream-Next-Offset` |
| POST | seq conflict / CT mismatch | 409 | — |
| POST | producer seq gap | 409 | `Producer-Expected-Seq`, `Producer-Received-Seq`, `Stream-Next-Offset` |
| POST | stale epoch | 403 | `Producer-Epoch` (current), `Stream-Next-Offset` |
| POST | empty body, missing CT, bad producer headers, bad JSON, epoch-seq≠0 | 400 | — |
| DELETE | ok | 204 | — |
| any | unknown method | 405 | — |
| any | unmapped store error | 500 | body `internal server error` |

---

## 16. Subtleties chronicle must preserve

1. **Path = stream identity, prefix included.** No stripping of the route
   prefix; conformance tests address streams by full URL path.
2. **`r.ContentLength > 0` gate on PUT body.** Chunked PUT bodies (length −1)
   are silently ignored as initial data.
3. **`Stream-Fork-Sub-Offset` present-but-empty detection** via
   `Header.Values()` — empty value is a 400, absent header is fine.
4. **`offset` query param multiplicity and emptiness** are validated before
   parsing; missing offset means `ZeroOffset`, but live modes *require* the
   param.
5. **`"-1"` and `""` both parse to `ZeroOffset`; `"now"` is a max-uint64
   sentinel** converted to the tail before SSE / catch-up / long-poll logic.
6. **Long-poll wait only when caught up** (or `offset=now`); a closed stream at
   tail short-circuits to 204 without waiting; the timeout is passed both as
   context deadline and store timer; context cancellation and deadline both
   yield the same 204 shape.
7. **Waiter wakeups are advisory**: buffered chan of 1, non-blocking notify,
   re-read after wake. Append, close, and delete all notify. Fork waiters are
   not notified by source appends.
8. **Cursor math is deterministic** (+90 intervals when the client cursor is
   at/ahead), 20-second buckets since 2024-10-09T00:00:00Z.
9. **SSE has no ping/comment keep-alive** — the reconnect timer (default 60 s)
   closes the connection; idle loops sleep in 100 ms `WaitForMessages` calls
   whose results are discarded.
10. **SSE `data:` lines have no space after the colon**, and text payloads are
    split on `\r\n|\r|\n` to prevent event injection; binary streams are
    base64-encoded with `Stream-SSE-Data-Encoding: base64` set on the response.
11. **Final-append drain before `streamClosed`**: the SSE loop never emits the
    closing control from the sleep branch; data appended atomically with a
    close must flow out as a `data` event first.
12. **`streamClosed` control omits `streamCursor` and `upToDate`** per
    protocol; normal control events carry `streamCursor` and optional
    `upToDate`.
13. **Producer status-code asymmetry**: producer new write → 200; producer
    duplicate → 204; non-producer append → 204. Duplicates skip webhook
    notification.
14. **Error responses that need headers bypass `writeError`** (direct
    `http.Error` after `w.Header().Set`): stale epoch (403), seq gap (409),
    closed (409), soft-deleted create (409 with long body).
15. **ETag is the quoted next offset**; `If-None-Match` is exact string
    equality; `offset=now` responses intentionally omit ETag and use
    `Cache-Control: no-store`; historical non-tail chunks get
    `public, max-age=60, stale-while-revalidate=300`.
16. **Response bodies are fully buffered** (single pre-sized allocation in
    `formatResponse` / `FormatJSONResponse`) and written with one `w.Write`;
    only SSE streams incrementally with explicit `Flush()`.
17. **`currentMeta, _ := h.store.Get(path)` ignores errors** in the read path
    and in post-wait closed checks; a delete racing a read can nil-deref in
    upstream. Mirror or harden deliberately.
18. **Webhook hooks** fire on create (`OnStreamCreated` + `OnStreamAppend` if
    initial data), append (non-duplicate only), and delete; webhook routes are
    checked before stream dispatch in `ServeHTTP`.
