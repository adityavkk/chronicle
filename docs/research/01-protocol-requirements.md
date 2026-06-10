# Durable Streams Protocol — Requirements Catalog for chronicle

**Source of truth:** `durable-streams/PROTOCOL.md` ("DRAFT: The Durable Streams Protocol", Version 1.0, ElectricSQL, 2025).
**Purpose:** Exhaustive, implementation-ready catalog of every protocol requirement for chronicle, a Go implementation of the Durable Streams protocol backed by Redis 8 that mirrors the official Caddy implementation (`packages/caddy-plugin`).
**Convention:** Spec section references appear as `(§x.y)`. RFC 2119 keywords (MUST / SHOULD / MAY) are reproduced with the spec's exact force. Header names are verbatim from the spec.

---

## Contents

1. [Protocol version and negotiation](#1-protocol-version-and-negotiation)
2. [Stream model and lifecycle](#2-stream-model-and-lifecycle)
3. [Offset model](#3-offset-model)
4. [HTTP operations](#4-http-operations)
   - 4.1 PUT — Create stream
   - 4.2 POST — Append to stream
   - 4.3 POST — Close stream (close-only)
   - 4.4 DELETE — Delete stream
   - 4.5 HEAD — Stream metadata
   - 4.6 GET — Read (catch-up)
   - 4.7 GET — Read (long-poll)
   - 4.8 GET — Read (SSE)
5. [Stream closure and EOF semantics across modes](#5-stream-closure-and-eof-semantics-across-modes)
6. [TTL and expiry](#6-ttl-and-expiry)
7. [Content modes: byte mode vs JSON mode](#7-content-modes-byte-mode-vs-json-mode)
8. [Caching, ETag, cursors, and collapsing](#8-caching-etag-cursors-and-collapsing)
9. [Browser security headers and CORS](#9-browser-security-headers-and-cors)
10. [Fork semantics](#10-fork-semantics)
11. [Idempotent producers (OPTIONAL feature)](#11-idempotent-producers-optional-feature)
12. [Reserved subscription APIs and delivery (§6, §7)](#12-reserved-subscription-apis-and-delivery-6-7)
13. [Security considerations](#13-security-considerations)
14. [IANA: default port and header registry](#14-iana-default-port-and-header-registry)
15. [Spec cross-reference errata](#15-spec-cross-reference-errata)
16. [Consolidated MUST/SHOULD/MAY table](#16-consolidated-mustshouldmay-table)

---

## 1. Protocol version and negotiation

- The spec document declares **Version 1.0** (front matter; document titled "DRAFT: The Durable Streams Protocol", dated 2025-01-XX, author ElectricSQL).
- **There is no on-the-wire version negotiation mechanism in the protocol.** No version header, no version query parameter, no handshake. The protocol is identified purely by its HTTP methods, query parameters, and `Stream-*` headers applied to a stream URL (§3).
- The protocol does **not** prescribe a URL structure (§3): servers may organize streams under any URL scheme (`/v1/stream/{path}`, `/streams/{id}`, etc.). Any versioning in the URL path is an implementation choice, not protocol.
- Compatibility across versions/implementations is governed by the extensibility rules (§11):
  - Extensions **SHOULD** be pure supersets of the base protocol (§11).
  - Extensions **MUST NOT** break base protocol semantics; clients that do not understand extension parameters or headers **MUST** be able to operate using only base protocol features (§11.1, "Backward Compatibility").
  - New parameters and headers **SHOULD** be optional with sensible defaults/fallbacks (§11.1, "Pure Superset").
  - Extensions **SHOULD** work with any client version implementing the base protocol; extension negotiation **MAY** be handled through headers or query parameters, but base operations **MUST** remain functional without extension support (§11.1, "Version Independence").
  - Authentication extensions (API keys, OAuth tokens, custom auth headers/params) are explicitly permitted (§11.2).
- **Implication for chronicle:** implement spec 1.0 exactly; any chronicle-specific additions (e.g., Redis-specific admin endpoints) must be additive, optional, and invisible to base-protocol clients.

## 2. Stream model and lifecycle

A stream is a URL-addressable, append-only byte stream (§2, §4) with these properties (§4):

- **Durability** — once written and acknowledged, bytes persist until the stream is deleted or expired.
- **Immutability by position** — bytes at a given offset never change; data is only appended.
- **Ordering** — bytes are strictly ordered by offset.
- **Content type** — each stream has a MIME content type fixed at creation (§2, §4).
- **TTL/expiry** — optional sliding TTL window (resets on read/write) or absolute expiry time (§4; details in [Section 6](#6-ttl-and-expiry)).
- **Retention** — servers **MAY** implement retention policies dropping data older than a certain age while the stream continues. If a stream is deleted, a new stream **SHOULD NOT** be created at the same URL (§4).
- **Stream state** — a stream is either **open** (accepts appends) or **closed** (no further appends). Streams start open; the open→closed transition is **durable** (persisted, survives restarts) and **monotonic** (a closed stream can never reopen) (§4, §4.1).

Terminology fixed by the spec (§2):

| Term | Definition |
| --- | --- |
| Stream | URL-addressable, append-only byte stream; durable, immutable by position |
| Offset | Opaque, lexicographically sortable token identifying a position within a stream |
| Tail offset | The offset immediately after the last byte; where new appends will be written |
| Closed stream | Terminal state: appends rejected, reads observe EOF; durable and monotonic |
| Fork | Stream created referencing a source stream and a divergence offset; inherits source data without copying; reads transparently stitch source + fork data |
| Fork offset | Divergence point: data before it comes from the source; data at/after it comes from the fork's own storage |
| Source stream | The stream a fork inherits from; may itself be a fork (fork chains) |
| Reference count | Number of forks referencing a stream as source; governs full-delete vs soft-delete |
| Soft-deleted stream | Deleted by owner but retained because forks reference its data; returns `410 Gone` for all client-facing operations (`GET`, `HEAD`, `POST`, `DELETE`) while data is retained internally for fork reads |

Operations overview (§3): **Create** (PUT), **Append** (POST), **Close** (POST with `Stream-Closed: true`), **Read** (GET — catch-up, long-poll, SSE), **Delete** (DELETE), **Head** (HEAD).

**Independent read/write implementation** (§3): servers **MAY** implement the read and write paths independently (e.g., read-only sync server with out-of-band write injection). Servers that do not support appends for a given stream **SHOULD** return `405 Method Not Allowed` or `501 Not Implemented` to `POST` (§5.2); same options for unsupported `DELETE` (§5.4).

**`Stream-Closed` header value parsing rule (§4.1)** — applies everywhere the header is accepted on requests:

- The value `true` (case-insensitive) is the only value treated as "header present".
- Any other value (`false`, `yes`, `1`, empty string, …) **MUST** be treated as if the header were absent.
- Servers **SHOULD NOT** return error responses for non-`true` values; they simply ignore the header.

## 3. Offset model

### 3.1 Properties (§8)

1. **Opaque** — clients **MUST NOT** interpret offset structure or meaning. (They also **MUST NOT** construct or modify offset values — restated in §8 "Fork offset validity".)
2. **Lexicographically sortable** — for any two valid offsets of the same stream, lexicographic comparison determines relative position. Clients **MAY** compare offsets lexicographically to determine ordering.
3. **Persistent** — offsets remain valid for the lifetime of the stream (until deletion or expiration).
4. **Unique** — each offset identifies exactly one position; no two positions may share an offset.
5. **Strictly increasing** — offsets assigned to appended data **MUST** be lexicographically greater than all previously assigned offsets. Servers **MUST NOT** use schemes that can produce duplicate or non-monotonic offsets (e.g., raw UTC timestamps). ULID-style identifiers (timestamp + random, guaranteed monotonic) are acceptable.

### 3.2 Wire format (§8)

- Offset tokens are opaque, **case-sensitive strings**; internal structure is implementation-defined.
- Offsets are single tokens and **MUST NOT** contain `,`, `&`, `=`, `?`, or `/` (avoids conflicts with URL query syntax).
- Servers **SHOULD** use URL-safe characters; clients **MUST** properly URL-encode offset values in query parameters regardless.
- Servers **SHOULD** keep offsets reasonably short (**under 256 characters**) since they appear in every request URL.
- Opacity enables server-side optimizations (e.g., offsets encoding chunk-file identifiers for serving catch-up directly from object storage).
- The reference implementations use offsets shaped like `0000000000000001_0000000000000042` (seen in §6/§7 examples) — chronicle may choose its own shape within the rules above.

### 3.3 Sentinel values (§8)

Two reserved sentinel offset values exist. Servers **MUST NOT** ever generate the strings `-1` or `now` as real stream offsets (in `Stream-Next-Offset` headers or SSE control events) so clients can always distinguish sentinels from real offsets.

**`-1` — stream beginning:**

- Represents the beginning of the stream; semantically equivalent to omitting the `offset` parameter.
- Clients **MAY** use `offset=-1` explicitly; servers **MUST** recognize `-1` as a valid offset returning data from the beginning.

**`now` — current tail position** (skip all existing data; only future data). Behavior by mode:

- **Catch-up** (`offset=now`, no `live` param):
  - **MUST** return `200 OK` with an empty body appropriate to content type: `[]` (empty JSON array) for `application/json` streams; 0 bytes for all others.
  - **MUST** include `Stream-Next-Offset` set to the current tail position.
  - **MUST** include `Stream-Up-To-Date: true`.
  - **SHOULD** return `Cache-Control: no-store` (prevents caching the tail offset).
  - The response **MUST** contain no data messages, regardless of stream content.
- **Long-poll** (`offset=now&live=long-poll`):
  - **MUST** immediately begin waiting for new data (no initial empty response) — saves a round trip.
  - New data within timeout → `200 OK` with the data; timeout → `204 No Content` with `Stream-Up-To-Date: true`.
  - `Stream-Next-Offset` **MUST** be set to the tail position.
- **SSE** (`offset=now&live=sse`):
  - **MUST** immediately begin the SSE stream from the tail position; no historical data is sent.
  - First control event **MUST** include the tail offset in `streamNextOffset`.
  - If no data has arrived, first control event **MUST** include `upToDate: true`; if data arrives before the first control event, `upToDate` reflects current state.
- **Closed streams** (`offset=now` on a closed stream, any mode):
  - Servers **MUST** return immediately with the closure signal (no waiting).
  - Response **MUST** include `Stream-Closed: true` and `Stream-Up-To-Date: true`; `Stream-Next-Offset` **MUST** be the stream's final (tail) offset.
  - Catch-up: `200 OK` empty body (or `[]` for JSON). Long-poll: `204 No Content` immediately. SSE: single control event with `streamClosed: true` and `upToDate: true`, then connection closes.

### 3.4 `Stream-Next-Offset` semantics

- Returned on: PUT 201/200 (tail after any initial content, §5.1); POST success (new tail, §5.2); POST close-only 204 (unchanged tail, §5.3); POST 409-closed (final offset of the closed stream, §5.2); HEAD 200 (tail, §5.5); GET 200 (next offset to read from, §5.6); long-poll 204 (current tail, **MUST**, §5.7); SSE control events as `streamNextOffset` (**MUST**, §5.8).
- Clients **MUST** use the `Stream-Next-Offset` value returned in responses for subsequent read requests, and **SHOULD** persist offsets locally (browser local storage, database) for resumability (§8).

### 3.5 Sub-offset addressing (§8, §4.2)

- `Stream-Fork-Sub-Offset` introduces a **separate addressing dimension** alongside the opaque offset — it is *not* part of the offset value, never appears in any response, and does not violate offset opacity/uniqueness/monotonicity.
- Servers internally resolve `(offset, suboffset)` to a precise position; all offsets returned to clients remain server-minted opaque tokens.
- Future protocol revisions **MAY** extend sub-offset addressing to read operations with the same content-type-driven semantics.

### 3.6 Offsets and forked streams (§8)

- Forks use the **same offset space** as their source — there is **no offset translation**. Reading a fork from `-1` yields offsets identical to the source up to the divergence point, then offsets minted by the fork's own appends.
- **Fork offset validity:** `Stream-Fork-Offset` **MUST** be an offset previously returned by the server (via `Stream-Next-Offset`). Servers are **NOT REQUIRED** to validate that a fork offset corresponds to a valid internal storage position; if a client supplies a constructed offset, behavior is undefined (fork reads **MAY** return corrupted data or errors). Servers **MAY** validate alignment and reject invalid offsets with `400 Bad Request`, but this is not required.

## 4. HTTP operations

All operations apply to `{stream-url}` — any URL the implementation chooses (§5).

### 4.1 PUT — Create stream (§5.1)

```
PUT {stream-url}
```

Creates a new stream. **Idempotent "create or ensure exists"**: if a stream already exists at the URL, the server **MUST** either:

- return `200 OK` if the existing stream's configuration (content type, TTL/expiry, **and closure status**) matches the request, or
- return `409 Conflict` if it does not.

**Closure-status matching for idempotent PUT (§5.1):**

| Request | Existing stream state | Result |
| --- | --- | --- |
| `PUT` (no `Stream-Closed`), matching config | open | `200 OK` |
| `PUT` (no `Stream-Closed`) | closed | `409 Conflict` (closure status mismatch) |
| `PUT` + `Stream-Closed: true`, matching config | closed | `200 OK` |
| `PUT` + `Stream-Closed: true` | open | `409 Conflict` (closure status mismatch) |

**Request headers (all optional):**

- `Content-Type: <stream-content-type>` — sets the stream's content type. If omitted, the server **MAY** default to `application/octet-stream`.
- `Stream-TTL: <seconds>` — sliding TTL window in seconds (full rules in [Section 6](#6-ttl-and-expiry)). Value **MUST** be a non-negative integer in decimal notation **without** leading zeros, plus signs, decimal points, or scientific notation (`3600` valid; `+3600`, `03600`, `3600.0`, `3.6e3` invalid).
- `Stream-Expires-At: <rfc3339>` — absolute expiry as an RFC 3339 timestamp. If **both** `Stream-TTL` and `Stream-Expires-At` are supplied, servers **SHOULD** reject with `400 Bad Request`; implementations **MAY** instead define a deterministic precedence rule but **MUST** document it.
- `Stream-Closed: true` — create the stream in the **closed** state; any body becomes the complete and final content (atomic "create and close"). Examples: empty body → empty immediately-closed stream ("completed with no output", error placeholders); with body → single-shot stream (cached responses, pre-computed results).
- `Stream-Forked-From: <source-path>` — creates a **fork** instead of an empty stream (see [Section 10](#10-fork-semantics)).
- `Stream-Fork-Offset: <offset>` — (requires `Stream-Forked-From`) divergence point; defaults to the source's current tail offset if omitted. Servers **MUST** return `400 Bad Request` if the offset exceeds the source stream's tail.
- `Stream-Fork-Sub-Offset: <integer>` — (requires `Stream-Forked-From`) non-negative integer refining the divergence point (see [Section 10](#10-fork-semantics)).

**Request body (optional):** initial stream bytes — the first content of the stream.

**Response codes:**

| Code | Meaning |
| --- | --- |
| `201 Created` | Stream created successfully |
| `200 OK` | Stream already exists with matching configuration (idempotent success) |
| `409 Conflict` | Stream exists with different configuration; also: fork content-type mismatch, target path in use with different config, source soft-deleted, re-creation of a soft-deleted path (§4.2/§5.4) |
| `400 Bad Request` | Invalid headers/parameters (including conflicting TTL/expiry, bad fork offset/sub-offset) |
| `404 Not Found` | `Stream-Forked-From` source does not exist (fork creation only) |
| `429 Too Many Requests` | Rate limit exceeded |

**Response headers (on 201 or 200):**

- `Location: {stream-url}` — servers **SHOULD** include on `201 Created`.
- `Content-Type: <stream-content-type>` — the stream's content type.
- `Stream-Next-Offset: <offset>` — the tail offset after any initial content.
- `Stream-Closed: true` — present when the stream was created in the closed state.

### 4.2 POST — Append to stream (§5.2)

```
POST {stream-url}
```

Appends bytes to the end of an existing stream. Supports full-body and streaming (chunked) appends; optionally closes the stream atomically. Servers not supporting appends for a stream **SHOULD** return `405 Method Not Allowed` or `501 Not Implemented`.

**Request headers:**

- `Content-Type: <stream-content-type>`
  - **MUST** match the stream's existing content type when a body is provided; servers **MUST** return `409 Conflict` when the content type is valid but does not match the stream's configured type.
  - **MAY** be omitted when the body is empty (close-only requests). When the body is empty, servers **MUST NOT** reject based on `Content-Type` and **MAY** ignore it entirely (keeps close-only robust when HTTP libraries attach default content types).
- `Transfer-Encoding: chunked` (optional) — streaming body. Servers **SHOULD** support HTTP/1.1 chunked encoding and HTTP/2 streaming semantics.
- `Stream-Seq: <string>` (optional) — monotonic, lexicographic **writer sequence number** for coordination.
  - Values are opaque strings compared with **simple byte-wise lexicographic ordering** (**MUST**).
  - Scoped per authenticated writer identity or per stream, implementation's choice — servers **MUST** document the scope they enforce.
  - If provided and ≤ the last appended sequence (lexicographic), the server **MUST** return `409 Conflict`. Sequence numbers **MUST** be strictly increasing.
- `Stream-Closed: true` (optional) — close the stream after the append completes, atomically (body appended as final data + transition to closed in the same step).
  - Empty body (`Content-Length: 0` or none) + `Stream-Closed: true` → close without appending. This is **the only case where an empty POST body is valid**.
  - **Close-only requests are idempotent**: if already closed and the request has `Stream-Closed: true` + empty body, servers **SHOULD** return `204 No Content` with `Stream-Closed: true`.
  - **Append-and-close is NOT idempotent** (absent producer headers): if already closed and the request has a body but no idempotent-producer headers, servers **MUST** return `409 Conflict` with `Stream-Closed: true` (the body cannot be appended). With producer headers matching the closing `(producerId, epoch, seq)` tuple, treat as deduplicated success (§5.2.1; see [Section 11](#11-idempotent-producers-optional-feature)).
- Producer headers (`Producer-Id`, `Producer-Epoch`, `Producer-Seq`) — OPTIONAL feature, [Section 11](#11-idempotent-producers-optional-feature).

**Request body:** bytes to append. Servers **MUST** reject empty-body POSTs (`Content-Length: 0` or no body) with `400 Bad Request` **unless** `Stream-Closed: true` is present.

**Response codes:**

| Code | Meaning |
| --- | --- |
| `204 No Content` | Append successful (or stream already closed when closing idempotently). Note: with producer headers, a *new* write returns `200 OK` and `204` means *duplicate* — see Section 11. |
| `400 Bad Request` | Malformed request: invalid header syntax, missing `Content-Type` (with a body), empty body without `Stream-Closed: true` |
| `404 Not Found` | Stream does not exist |
| `405 Method Not Allowed` / `501 Not Implemented` | Append not supported for this stream |
| `409 Conflict` | Content-type mismatch, `Stream-Seq` regression, or stream is closed (append without `Stream-Closed: true`) |
| `410 Gone` | Stream is soft-deleted |
| `413 Payload Too Large` | Body exceeds server limits |
| `429 Too Many Requests` | Rate limit exceeded |

**Response headers (on success):**

- `Stream-Next-Offset: <offset>` — the new tail offset after the append.
- `Stream-Closed: true` — present when the stream is now closed (by this request or previously).

**Response headers (on 409 Conflict due to closed stream)** — servers **MUST** return all of:

- `409 Conflict` status
- `Stream-Closed: true` header
- `Stream-Next-Offset: <offset>` — the final offset of the closed stream

Clients can thereby detect "stream already closed" programmatically without parsing the body. Servers **SHOULD** keep the 409 body empty or use a standardized error format; clients **SHOULD NOT** rely on parsing the body for the rejection reason.

**Error precedence (§5.2)** — when multiple conflicts apply, servers **SHOULD** check in this order so clients receive `Stream-Closed: true`:

1. Stream closed → `409 Conflict` with `Stream-Closed: true`
2. Content-type mismatch → `409 Conflict`
3. Sequence regression → `409 Conflict`

### 4.3 POST — Close stream (close-only) (§5.3)

```
POST {stream-url}
Stream-Closed: true
```

(empty body) — the canonical close-only operation. For atomic "append final message and close", include a body per §5.2.

**Response codes:**

| Code | Meaning |
| --- | --- |
| `204 No Content` | Stream closed successfully (or already closed — idempotent) |
| `404 Not Found` | Stream does not exist |
| `405 Method Not Allowed` / `501 Not Implemented` | Append/close not supported for this stream |

**Response headers:**

- `Stream-Next-Offset: <offset>` — the tail offset (unchanged; no data appended).
- `Stream-Closed: true` — confirms the stream is now closed.

### 4.4 DELETE — Delete stream (§5.4)

```
DELETE {stream-url}
```

Deletes the stream and all its data. In-flight reads may terminate; subsequent requests get `404 Not Found`.

**Response codes:**

| Code | Meaning |
| --- | --- |
| `204 No Content` | Stream deleted successfully |
| `404 Not Found` | Stream does not exist |
| `405 Method Not Allowed` / `501 Not Implemented` | Delete not supported for this stream |
| `410 Gone` | Stream is already soft-deleted (deleting a soft-deleted stream **MUST** return `410 Gone`) |

**Soft-delete (§5.4, §4.2):** when a stream has active forks (reference count > 0), the server **MUST** transition it to soft-deleted rather than fully removing it:

- `410 Gone` for all direct operations (`GET`, `HEAD`, `POST`, `DELETE`).
- Path blocked from re-creation via `PUT` → `409 Conflict`.
- Data preserved internally for fork readers (transparent to fork clients).
- When the last referencing fork is deleted, data is cleaned up via cascading garbage collection; cleanup **MAY** occur asynchronously — clients **SHOULD NOT** assume the source returns `404` immediately after the fork's `DELETE` response.

### 4.5 HEAD — Stream metadata (§5.5)

```
HEAD {stream-url}
```

Checks stream existence and returns metadata without a body. The **canonical way** to find the tail offset, TTL/expiry information, and closure status (e.g., to learn closure before reaching the tail, §5.6).

**Response codes:**

| Code | Meaning |
| --- | --- |
| `200 OK` | Stream exists |
| `404 Not Found` | Stream does not exist |
| `410 Gone` | Stream is soft-deleted |
| `429 Too Many Requests` | Rate limit exceeded |

**Response headers (on 200):**

- `Content-Type: <stream-content-type>` — the stream's content type.
- `Stream-Next-Offset: <offset>` — the tail offset.
- `Stream-TTL: <seconds>` (optional) — the stream's TTL window.
- `Stream-Expires-At: <rfc3339>` (optional) — absolute expiry time, if applicable.
- `Stream-Closed: true` (optional) — present when closed; absence indicates the stream is open.
- `Cache-Control` — see caching guidance below.

**Caching guidance:** servers **SHOULD** make HEAD responses effectively non-cacheable, e.g., `Cache-Control: no-store` (recommended, avoids stale tail offsets/closure status); `Cache-Control: private, max-age=0, must-revalidate` is a permitted (**MAY**) alternative.

**TTL note:** HEAD requests do **not** reset the sliding TTL countdown (§5.1).

### 4.6 GET — Read stream, catch-up (§5.6)

```
GET {stream-url}?offset=<offset>
```

Returns bytes starting from the specified offset, up to a server-defined maximum chunk size. Used to replay stream content from a known position.

**Query parameters:**

- `offset` (optional) — start offset token. If omitted, defaults to stream start (offset `-1`).

**Response codes:**

| Code | Meaning |
| --- | --- |
| `200 OK` | Data available (or empty body if offset equals tail) |
| `304 Not Modified` | `If-None-Match` matched current `ETag` (§10.1; see [Section 8](#8-caching-etag-cursors-and-collapsing)) |
| `400 Bad Request` | Malformed offset or invalid parameters |
| `404 Not Found` | Stream does not exist |
| `410 Gone` | Offset is before the earliest retained position (retention/compaction), **or** stream is soft-deleted |
| `429 Too Many Requests` | Rate limit exceeded |

**At-tail behavior:** for non-live reads with no data beyond the requested offset, servers **SHOULD** return `200 OK` with an empty body and `Stream-Next-Offset` equal to the requested offset. If the stream is closed, this response **MUST** also include `Stream-Closed: true` (EOF).

**Response headers (on 200):**

- `Cache-Control` — derived from TTL/expiry (see [Section 8](#8-caching-etag-cursors-and-collapsing); spec text says "see Section 9" — stale cross-reference, the rules live in §10).
- `ETag: {internal_stream_id}:{start_offset}:{end_offset}` — entity tag for cache validation.
- `Stream-Cursor: <cursor>` — **optional for catch-up**, required for live modes when the stream is open. Servers **MAY** include it on catch-up reads and **MAY** omit it when `Stream-Closed` is true. Clients **MUST** tolerate its absence when `Stream-Closed` is present.
- `Stream-Next-Offset: <offset>` — the next offset to read from.
- `Stream-Up-To-Date: true`
  - **MUST** be present (value `true`) when the response includes all data available at response-generation time (requested offset reached the tail, nothing more exists).
  - **SHOULD NOT** be present when returning partial data due to server-defined chunk-size limits (more data exists beyond what was returned).
  - Clients **MAY** use it to decide when to switch to live tailing.
  - **Does NOT imply EOF** — more data may be appended later. Only `Stream-Closed: true` means no more data ever.
- `Stream-Closed: true`
  - **MUST** be present when the stream is closed **and** the client has reached the final offset at response-generation time. Covers: (a) responses returning the final chunk when the stream is already closed at generation time; (b) empty-body responses where the requested offset equals the tail of a closed stream (**the canonical EOF signal**).
  - When present, clients can conclude no more data will ever arrive (EOF).
  - **SHOULD NOT** be present when returning partial data from a closed stream (more data remains before the final offset); it will appear on the subsequent request that reaches the final offset.
  - **Timing:** if a stream is closed *after* the final chunk was served (or cached), that chunk won't carry `Stream-Closed: true`; clients discover closure by requesting the next offset and receiving an empty body with `Stream-Closed: true`. This is the expected flow with caching.
  - Clients needing closure status before reaching the tail **SHOULD** use `HEAD` (§5.5).

**Response body:** bytes from the stream starting at the requested offset, up to a server-defined maximum chunk size. (JSON-mode bodies are always JSON arrays — see [Section 7](#7-content-modes-byte-mode-vs-json-mode).)

### 4.7 GET — Read stream, live long-poll (§5.7)

```
GET {stream-url}?offset=<offset>&live=long-poll[&cursor=<cursor>]
```

If no data is available at the offset, the server waits up to a timeout for new data.

**Query parameters:**

- `offset` (**required** — MUST be provided)
- `live=long-poll` (**required**)
- `cursor` (optional) — echo of the last `Stream-Cursor` header value from a previous response; used for CDN/proxy collapsing keys.

**Response codes:**

| Code | Meaning |
| --- | --- |
| `200 OK` | Data became available within the timeout |
| `204 No Content` | Timeout expired with no new data |
| `400 Bad Request` | Invalid parameters |
| `404 Not Found` | Stream does not exist |
| `429 Too Many Requests` | Rate limit exceeded |

**Response headers (on 200):** same as catch-up (§5.6) **plus** `Stream-Cursor: <cursor>` — servers **MUST** include it (see §10.1 cursor rules in [Section 8](#8-caching-etag-cursors-and-collapsing)).

**Response headers (on 204):**

- `Stream-Next-Offset: <offset>` — **MUST** be included; the current tail offset.
- `Stream-Up-To-Date: true` — **MUST** be included.
- `Stream-Cursor: <cursor>` — **MUST** be included when the stream is open; **MAY** be omitted when `Stream-Closed` is true (no further polling expected). Clients **MUST** tolerate its absence when `Stream-Closed` is present.
- `Stream-Closed: true` — **MUST** be present when the stream is closed. `204 No Content` + `Stream-Closed: true` = EOF.

**Closed-stream behavior:** when the stream is closed and the client is already at the tail offset, servers **MUST NOT** wait for the long-poll timeout and **MUST** immediately return `204 No Content` with `Stream-Closed: true` and `Stream-Up-To-Date: true` (no hanging connections for data that will never arrive).

**Response body (on 200):** the new bytes that arrived during the long-poll period.

**Timeout:** implementation-defined. Servers **MAY** accept a `timeout` query parameter (seconds) as a future extension; not required by the base protocol.

**Long-poll on forked streams (§5.7):**

- Offset in the inherited range (before fork offset): data already exists in the source — servers **MUST** return it immediately without waiting.
- Offset at the fork's tail: servers **MUST** wait only for the fork's own appends. Appends to the source stream after fork creation **MUST NOT** unblock waiters on the fork.

### 4.8 GET — Read stream, live SSE (§5.8)

```
GET {stream-url}?offset=<offset>&live=sse
```

Returns data as a Server-Sent Events stream. SSE mode supports **all** content types.

**Query parameters:** `offset` (**required**), `live=sse` (**required**).

**Response codes:**

| Code | Meaning |
| --- | --- |
| `200 OK` | Streaming body (SSE format) |
| `400 Bad Request` | Invalid parameters |
| `404 Not Found` | Stream does not exist |
| `429 Too Many Requests` | Rate limit exceeded |

**Response headers:**

- `Content-Type: text/event-stream` — SSE responses **MUST** use this.
- `stream-sse-data-encoding: base64` — **MUST** be included when the stream's configured content type is neither `text/*` nor `application/json` (binary streams). Clients **MUST** check for this header and decode data events accordingly.

**Encoding rule:** for `content-type: text/*` or `application/json` streams, data events carry UTF-8 text directly. For any other content type, servers **MUST** automatically base64-encode data-event payloads and include `stream-sse-data-encoding: base64`.

**Event framing — two event types:**

- `event: data` — emitted for each batch of data; each payload line prefixed `data:`.
  - Binary streams (`stream-sse-data-encoding: base64` present): payload is bytes encoded with **standard base64 per RFC 4648** (alphabet `A–Z a–z 0–9 + /`).
    - Servers **MAY** split base64 text across multiple `data:` lines within one SSE event.
    - Clients **MUST** concatenate the event's `data:` lines (per SSE rules) and **MUST** remove all `\n` and `\r` characters inserted between lines before base64-decoding.
    - The resulting string **MUST** be valid base64 with length a multiple of 4 (or empty).
    - A 0-byte payload **MUST** be encoded as the empty string.
  - Base64 affects **only** `event: data` payloads — `event: control` events remain plain JSON, never encoded.
  - For `application/json` streams, implementations **MAY** batch multiple logical messages into one SSE `data` event by streaming a JSON array across multiple `data:` lines.
- `event: control` — emitted **after every data event**. JSON object; field names are camelCase: `streamNextOffset`, `streamCursor`, `upToDate`, `streamClosed`.
  - **MUST** include `streamNextOffset`.
  - **MUST** include `streamCursor` when the stream is open; **MAY** omit when `streamClosed` is true (no reconnection expected).
  - **MUST** include `upToDate: true` when the client is caught up with all available data. `streamClosed: true` implies `upToDate: true`, so `upToDate` **MAY** be omitted when `streamClosed` is true.
  - **MUST** include `streamClosed: true` when the stream is closed and all data up to the final offset has been sent.

**Wire examples (verbatim from §5.8):**

```
event: data
data: [
data: {"k":"v"},
data: {"k":"w"},
data: ]

event: control
data: {"streamNextOffset":"123456_789","streamCursor":"abc"}
```

Final data with closure (note `streamCursor` omitted):

```
event: data
data: [
data: {"k":"final"}
data: ]

event: control
data: {"streamNextOffset":"123456_999","streamClosed":true}
```

Binary stream with automatic base64:

```
event: data
data: AQIDBAUG
data: BwgJCg==

event: control
data: {"streamNextOffset":"123456_789","streamCursor":"abc"}
```

**Client compatibility:** clients **MUST** tolerate the absence of `streamCursor` (SSE) and `Stream-Cursor` (HTTP headers) when `streamClosed` / `Stream-Closed` is present.

**Closure behavior in SSE mode:**

- The final `control` event **MUST** include `streamClosed: true`.
- After emitting the final control event, servers **MUST** close the SSE connection.
- Clients receiving `streamClosed: true` **MUST NOT** reconnect.
- If the stream is already closed when the SSE connection is established and the client's offset is at the tail: servers **MUST** immediately emit a `control` event with `streamClosed: true` and `upToDate: true`, then **MUST** close the connection.

**Connection lifecycle (reconnect interval):**

- Servers **SHOULD** close SSE connections roughly every **~60 seconds** to enable CDN collapsing (also §10.2).
- Clients **MUST** reconnect using the last received `streamNextOffset` from a control event.
- Clients **MUST NOT** reconnect if the last control event included `streamClosed: true`.

**SSE on forked streams (§5.8):** delivers inherited source data, then the fork's own data, then waits for new fork appends. Source appends after the fork point are never delivered.

## 5. Stream closure and EOF semantics across modes

Closure provides an explicit EOF signal distinguishing "no data yet" from "no more data ever" (§4.1). Use cases: proxied HTTP responses that finished streaming, completed job outputs/workflows, finalized conversation histories/documents.

**Properties (§4.1):**

- **Durable** — persisted, survives server restarts.
- **Monotonic** — once closed, never reopened.
- **Idempotent** — closing an already-closed stream succeeds (or returns a stable "already closed" response).
- **Observable** — readers detect closure as EOF.
- **Atomic with final append** — append final message + close in one operation.
- After closure, all existing data remains fully readable; only new appends are rejected.

**Ways to close:** `PUT` + `Stream-Closed: true` (create closed, §5.1); `POST` + body + `Stream-Closed: true` (append-and-close, §5.2); `POST` + empty body + `Stream-Closed: true` (close-only, §5.3); idempotent-producer close (§5.2.1, [Section 11](#11-idempotent-producers-optional-feature)).

**EOF signaling matrix (§5.7):** clients treat any of the following as EOF —

| Mode | EOF signal |
| --- | --- |
| Catch-up | `200 OK` with empty body and `Stream-Closed: true` |
| Long-poll | `204 No Content` with `Stream-Closed: true` |
| SSE | `control` event with `streamClosed: true` |

`Stream-Closed` / `streamClosed` is the **definitive** EOF signal in every mode. `Stream-Up-To-Date` / `upToDate` alone is **not** EOF — it only means caught up *now*; more may arrive.

**Append to a closed stream:** `409 Conflict` + `Stream-Closed: true` + `Stream-Next-Offset: <final offset>` (§5.2). Close-only against a closed stream: idempotent `204 No Content` + `Stream-Closed: true` (§5.2, §5.3).

**Closure discovery with caching (§10.1):** the closure signal is a distinct request/response at the tail offset — cached final chunks never go stale; clients always make one more request to discover EOF (flow detailed in [Section 8](#8-caching-etag-cursors-and-collapsing)).

## 6. TTL and expiry

### 6.1 `Stream-TTL` — sliding window (§5.1, §4)

- Set at creation: `Stream-TTL: <seconds>`. Stream expires after being **idle** (no reads or writes) for this duration; each read or write resets the countdown to the full value.
- **Syntax (MUST):** non-negative integer, decimal notation, no leading zeros, no plus signs, no decimal points, no scientific notation. Valid: `3600`. Invalid: `+3600`, `03600`, `3600.0`, `3.6e3` → `400 Bad Request`.
- **What resets the countdown:**
  - Reads and writes that **reach the origin server** — TTL resets are a server-side concern.
  - For live modes (long-poll, SSE): TTL resets **when the server begins processing the request**, not when data is delivered or the response completes → a stream with active live readers never expires, even with no new data.
- **What does NOT reset it:**
  - `HEAD` requests (§5.1).
  - Reads served from intermediate caches (e.g., CDN catch-up hits under `Cache-Control: public, max-age=60`) — they never reach the server.
- Reported back on `HEAD 200` as `Stream-TTL: <seconds>` (§5.5).

### 6.2 `Stream-Expires-At` — absolute expiry (§5.1)

- `Stream-Expires-At: <rfc3339>` — absolute expiry time, RFC 3339 timestamp.
- If **both** `Stream-TTL` and `Stream-Expires-At` are supplied on PUT: servers **SHOULD** reject with `400 Bad Request`; **MAY** instead define a deterministic precedence rule but **MUST** document it.
- Reported back on `HEAD 200` as `Stream-Expires-At: <rfc3339>` (§5.5).

### 6.3 Fork TTL/expiry inheritance (§4.2)

| Source | Fork request | Fork gets | Rationale |
| --- | --- | --- | --- |
| No expiry | No expiry | No expiry | Nothing to inherit or set |
| No expiry | TTL | Own TTL | Fork's own sliding window |
| No expiry | Expires-At | Own Expires-At | Fork's own hard deadline |
| TTL | No expiry | Inherit source's TTL value | Same sliding window, refreshed independently |
| TTL | TTL | Requested TTL | Fork's own sliding window |
| TTL | Expires-At | Requested Expires-At | Fork's own hard deadline |
| Expires-At | No expiry | Inherit source's Expires-At | Prevents unbounded retention |
| Expires-At | TTL | Requested TTL | Fork lives independently |
| Expires-At | Expires-At | Requested Expires-At | Fork can outlive parent |

### 6.4 Related lifecycle rules

- Offsets remain valid until stream deletion **or expiration** (§8).
- After deletion, a new stream **SHOULD NOT** be created at the same URL (§4).
- Retention policies (dropping old data on a live stream) are permitted (**MAY**, §4); reads at offsets before the earliest retained position return `410 Gone` (§5.6).

## 7. Content modes: byte mode vs JSON mode

### 7.1 Byte mode (default) (§9)

- The protocol supports arbitrary MIME content types; most operate at the **byte level** — message framing and interpretation are left to clients.
- Example types: `application/ndjson`, `application/x-protobuf`, `text/plain`, custom types. Default when omitted on PUT: server **MAY** use `application/octet-stream` (§5.1).
- The content type is fixed at creation and returned on reads (`Content-Type` response header).
- **Content-type matching rules:**
  - POST with body: `Content-Type` **MUST** match the stream's configured type; mismatch → `409 Conflict` (§5.2). Servers **MUST** validate this to prevent type-confusion attacks (§12.4).
  - POST with empty body (close-only): servers **MUST NOT** reject based on `Content-Type` and **MAY** ignore it (§5.2).
  - PUT idempotency: content type is part of the configuration compared for `200 OK` vs `409 Conflict` (§5.1).
  - Fork creation: `Content-Type` optional; if provided it **MUST** match the source's type, else `409 Conflict`; if omitted, the fork inherits the source's type (§4.2).
- **SSE interaction:** `text/*` and `application/json` stream data goes over SSE as UTF-8 text; **all other types MUST be base64-encoded** with `stream-sse-data-encoding: base64` (§5.8, §9).

### 7.2 JSON mode — `Content-Type: application/json` (§9.1)

Special semantics for message boundaries and batching:

- **Message boundaries (§9.1.1):** servers **MUST** preserve message boundaries. Each POST stores messages as a distinct unit; GET responses **MUST** return a JSON array containing all messages from the requested offset range.
- **Array flattening (§9.1.2):** when a POST body is a JSON array, servers **MUST** flatten **exactly one level**, treating each element as a separate message. Examples (verbatim):
  - body `{"event": "created"}` → one message `{"event": "created"}`
  - body `[{"event": "a"}, {"event": "b"}]` → two messages `{"event": "a"}`, `{"event": "b"}`
  - body `[[1,2], [3,4]]` → two messages `[1,2]`, `[3,4]`
  - body `[[[1,2,3]]]` → one message `[[1,2,3]]`
  - Client libraries **MAY** auto-wrap single values in arrays for batching (e.g., `append({"x": 1})` sends `[{"x": 1}]`, server stores `{"x": 1}`).
- **Empty arrays (§9.1.3):**
  - POST body `[]` → servers **MUST** reject with `400 Bad Request` (no-op, likely client bug).
  - PUT body `[]` → **valid**; creates an empty stream (no initial messages).
- **Validation (§9.1.4):** servers **MUST** validate appended data is valid JSON; failure → `400 Bad Request` with an appropriate error message.
- **Response format (§9.1.5):** GET responses **MUST** return `Content-Type: application/json` with a body that is a JSON array of all messages in the range, e.g. `[{"event":"created"},{"event":"updated"}]`. If no messages exist in the range, servers **MUST** return the empty JSON array `[]`.
- **`offset=now` catch-up on a JSON stream:** body **MUST** be `[]` (§8 sentinels).
- **Forked JSON streams (§9.1):** reads spanning the fork boundary (inherited + fork messages) **MUST** wrap all messages in a **single** JSON array. Fork inherits source content type if none specified.
- **Fork sub-offset unit:** for `application/json` source streams, `Stream-Fork-Sub-Offset` counts **flattened messages** past the anchor offset; for all other types it counts **decoded entity body bytes** (§4.2; see [Section 10](#10-fork-semantics)).

## 8. Caching, ETag, cursors, and collapsing

### 8.1 Cache-Control (§10.1)

- Shared, non-user-specific streams — catch-up/long-poll reads **SHOULD** return:
  `Cache-Control: public, max-age=60, stale-while-revalidate=300`
- Streams that may contain user-specific or confidential data — **SHOULD** use `private` instead of `public` (rely on CDN configs that respect `Authorization`/cache keys):
  `Cache-Control: private, max-age=60, stale-while-revalidate=300`
- `HEAD` responses: **SHOULD** be effectively non-cacheable — `Cache-Control: no-store` recommended; `private, max-age=0, must-revalidate` permitted (§5.5).
- `offset=now` catch-up responses: **SHOULD** return `Cache-Control: no-store` (§8).
- Sensitive data responses: `Cache-Control: no-store` **SHOULD** be used (§12.7).
- Long-poll `204 No Content` responses: CDNs/proxies **SHOULD NOT** cache them in most cases. Long-poll `200 OK` responses are safe to cache when keyed by `offset`, `cursor`, and authentication credentials (§10.1).

### 8.2 Caching and stream closure (§10.1)

Catch-up chunks remain fully cacheable, **including the chunk at the tail** — whether a chunk is final is unknown until the client requests the next offset. Closure discovery flow:

1. Client reads data, receives `Stream-Next-Offset: X` (tail).
2. Client requests offset `X`.
3. Stream closed → `200 OK`, **empty body**, `Stream-Closed: true`.
4. Stream open → `200 OK`, empty body, `Stream-Up-To-Date: true` (or long-poll/SSE waits for data).

Design guarantees: all data chunks cacheable; the closure signal is a distinct request/response at the tail offset; cached chunks never become stale due to closure.

### 8.3 ETag / If-None-Match (§10.1, §5.6)

- ETag format (§5.6): `ETag: {internal_stream_id}:{start_offset}:{end_offset}`.
- Servers **MUST** generate `ETag` headers for GET responses, **except** `offset=now` responses.
- Clients **MAY** send `If-None-Match` with the ETag on repeat catch-up requests; on a match with the current ETag, servers **MUST** respond `304 Not Modified` with no body (essential for fast loading / bandwidth).
- **ETag and closure:** ETags **MUST** vary with the stream's closure status — when a stream is closed without new data, the ETag **MUST** change so clients don't get a `304` that hides the closure signal. Implementations **SHOULD** include a closure indicator in the ETag (e.g., append `:c` when closed).

### 8.4 Query parameter ordering (§10.1)

Clients **SHOULD** order query parameters lexicographically by key name — consistent URL serialization improves CDN cache hit rates.

### 8.5 Cursor mechanism — `Stream-Cursor` / `cursor` / `streamCursor` (§10.1)

Purpose: collapsing (multiple clients waiting on the same data collapse into one upstream request) and preventing infinite CDN cache loops (clients receiving the same cached empty response forever).

- Servers **MUST** generate cursors on **all live-mode responses**: long-poll → `Stream-Cursor` response header; SSE → `streamCursor` field in `control` events. (Exception: **MAY** omit when `Stream-Closed`/`streamClosed` is true; clients **MUST** tolerate absence in that case — §5.6/§5.7/§5.8.)
- Clients **SHOULD** echo `Stream-Cursor` as `cursor=<cursor>` in subsequent long-poll requests; clients **MUST** include the received cursor value as the `cursor` query parameter in subsequent requests (creates fresh cache keys as time progresses).
- **Cursor generation algorithm:**
  1. **Interval-based calculation:** time is divided into fixed intervals (default **20 seconds**) counted from an epoch (default **October 9, 2024 00:00:00 UTC**). Cursor value = the interval number as a decimal string.
  2. For each live response, the server returns the current interval number as the cursor.
  3. **Monotonic progression (MUST):** cursors never go backwards. If a client's `cursor` query parameter is **≥** the current interval number, the server **MUST** return a cursor **strictly greater** than the client's cursor, by adding **random jitter of 1–3600 seconds**. This guarantees monotonicity and prevents cache cycles.
- Example flow (§10.1): initial request → `Stream-Cursor: 1000`; client echoes `cursor=1000`; if still in the same interval, server returns a jittered, advanced cursor such as `Stream-Cursor: 1050`.

### 8.6 SSE collapsing (§10.2)

SSE connections **SHOULD** be closed by the server approximately every **60 seconds**, letting new clients collapse onto edge requests rather than holding long-lived origin connections.

## 9. Browser security headers and CORS

> Note: the task brief referenced "spec section 10.7"; no such section exists. Browser security headers live in **§12.7**. Spec §10 is "Caching and Collapsing".

When serving streams to browser clients, servers **SHOULD** include the following headers to prevent MIME-sniffing attacks, cross-origin embedding exploits, and cache-related vulnerabilities (§12.7):

- `X-Content-Type-Options: nosniff`
  - **SHOULD** be included **on all responses**. Prevents browsers MIME-sniffing the response and executing it as a different content type (e.g., binary data interpreted as HTML/JavaScript).
- `Cross-Origin-Resource-Policy: cross-origin` (or `same-origin` / `same-site`)
  - **SHOULD** be included to explicitly control cross-origin embedding: `cross-origin` allows cross-origin `fetch()` access; `same-site` restricts to the registrable domain; `same-origin` is strict same-origin. Prevents Cross-Origin Read Blocking (CORB) issues and Spectre-like side-channel attacks.
- `Cache-Control: no-store`
  - **SHOULD** be included on `HEAD` responses and on responses with sensitive or user-specific stream data (prevents proxy/CDN caching). Public, non-sensitive historical reads **MAY** use `Cache-Control: public, max-age=60, stale-while-revalidate=300` per §10.
- `Content-Disposition: attachment` (optional)
  - **MAY** be included for `application/octet-stream` responses to prevent inline rendering on direct navigation.

Rationale (§12.7): defense-in-depth for stream URLs accessed outside programmatic fetch (direct navigation, malicious `<script>`/`<img>` embedding).

**CORS:** the spec defines **no `Access-Control-*` (CORS) requirements** — the only cross-origin control it specifies is `Cross-Origin-Resource-Policy`. CORS configuration (allowing browser clients on other origins to read `Stream-*` response headers via `Access-Control-Expose-Headers`, preflight handling for custom request headers, etc.) is implementation-defined for chronicle; it falls under permitted protocol extensions (§11.1) and must not alter base semantics.

## 10. Fork semantics

Forking creates a new stream referencing a source stream's data up to a divergence offset, without copying (§2, §4.2). A fork is a **variant of stream creation** — `PUT` with extra headers. Once created, the fork is an independent stream: own URL, accepts appends, can be closed or deleted without affecting the source. Reads return inherited data followed by fork-local data; the stitching mechanism (copy-on-fork, pointer-based, etc.) is an implementation detail.

### 10.1 Fork creation headers (§4.2, §5.1)

- `Stream-Forked-From: <source-path>` — path component of the source stream's URL, **relative to the same server**. Presence turns the PUT into a fork. **Cross-service forking is not supported** — source must be on the same server.
- `Stream-Fork-Offset: <offset>` — divergence point in the source. The fork inherits all source data **up to (but not including)** this offset. Omitted → defaults to the source's **current tail offset**. Offset exceeding the source's tail → `400 Bad Request` (**MUST**).
- `Stream-Fork-Sub-Offset: <integer>` (optional) — non-negative integer refining the divergence point **past** `Stream-Fork-Offset`. Interpreted per the **source stream's content type**:
  - `application/json` streams: number of **flattened messages** to inherit past the anchor (flattening per §9.1.2; the spec's "Section 7.1" pointer here is stale — see [Section 15](#15-spec-cross-reference-errata)).
  - All other content types: number of **decoded entity body bytes** to inherit past the anchor, exclusive of any internal server framing and independent of HTTP transfer/content encoding in transit.
  - Default `0`; sub-offset `0` is equivalent to omitting the header.
  - A separate addressing dimension, not part of the offset value (see [Section 3.5](#35-sub-offset-addressing-8-42)).

**Content type:** optional when forking — omitted → fork inherits the source's content type; provided → **MUST** match the source's type, else `409 Conflict` (**MUST**).

**TTL/expiry:** forks have independent lifetimes and can outlive their source; inheritance matrix in [Section 6.3](#63-fork-ttlexpiry-inheritance-42).

### 10.2 Fork creation errors (§4.2)

Standard §5.1 creation errors apply (e.g., `409` for config mismatch, `400` for invalid TTL/expiry), plus:

| Condition | Status |
| --- | --- |
| Source stream not found (`Stream-Forked-From` path does not exist) | `404 Not Found` |
| Fork offset beyond stream length (exceeds source's current tail) | `400 Bad Request` |
| Invalid offset format (malformed `Stream-Fork-Offset`) | `400 Bad Request` |
| Sub-offset overshoot or invalid (malformed, negative, or names a position past the next data boundary) | `400 Bad Request` |
| `Content-Type` mismatch with source | `409 Conflict` |
| Target path already in use (stream exists at target URL with different config) | `409 Conflict` |
| Source is soft-deleted (deleted but still has forks) | `409 Conflict` |

### 10.3 Idempotent fork creation (§4.2)

Same idempotency rules as regular creation (§5.1): if a stream already exists at the target URL with matching configuration — **including `Stream-Forked-From` and `Stream-Fork-Offset`** — the server **MUST** return `200 OK`; differing configuration → **MUST** return `409 Conflict`.

### 10.4 Closed-stream forking (§4.2)

Closed streams **MAY** be forked. The fork starts in the **open** state regardless of the source's closed status (enables forking from historical points in completed streams).

### 10.5 Producer state and fork boundaries (§4.2)

- Forks **MUST NOT** inherit idempotent producer state (§5.2.1) or per-writer `Stream-Seq` state (§5.2) from the source.
- A fork is a new stream from a writer-state perspective; producers writing to the fork **MUST** re-bootstrap their state on the fork (typically by bumping their epoch).
- Applies to **all** forks, including those created with `Stream-Fork-Sub-Offset` whose boundary lies inside a producer batch on the source: the source's producer state is unchanged, and the fork's writer-state-fresh shape ensures retries against the fork cannot collide with the partial inherited data.

### 10.6 Soft-delete, reference counting, and lifecycle (§4.2, §5.4)

- `DELETE` on a stream with active forks (reference count > 0) → server **MUST** transition it to **soft-deleted** (not full removal):
  - Direct client access to the URL returns `410 Gone` for **all** operations (`GET`, `HEAD`, `POST`, `DELETE`).
  - The path is blocked from re-creation via `PUT` → `409 Conflict`.
  - Data is retained internally so fork reads can stitch inherited data — transparent to fork clients.
- `DELETE` on an already soft-deleted stream → **MUST** return `410 Gone` (consistent with the other direct operations).
- When the last referencing fork is deleted, the server cleans up the soft-deleted stream's data. Cleanup **MAY** be asynchronous — clients **SHOULD NOT** assume the source 404s immediately after the fork's `DELETE` response.
- **Cascading GC:** if deleting a fork drops its source's refcount to zero and the source is itself soft-deleted, the source is cleaned up too; the cascade continues up the fork chain. Cascade cleanup **MAY** also be asynchronous.

### 10.7 Reading forks

- **Offsets:** same offset space as the source, no translation; fork-offset validity rules in [Section 3.6](#36-offsets-and-forked-streams-8).
- **Long-poll (§5.7):** inherited-range offsets **MUST** be served immediately; at the fork tail, only fork appends unblock waiters — source appends after fork creation **MUST NOT**.
- **SSE (§5.8):** inherited data → fork data → wait for fork appends; source appends past the fork point are never delivered.
- **JSON mode (§9.1):** boundary-spanning reads **MUST** return one single JSON array wrapping inherited + fork messages.

## 11. Idempotent producers (OPTIONAL feature)

Kafka-style idempotent producers for exactly-once write semantics (§5.2.1): fire-and-forget writes with server-side deduplication, eliminating duplicates from client retries.

### 11.1 Design (§5.2.1)

- **Client-provided producer IDs** — zero RTT, no handshake.
- **Client-declared epochs, server-validated fencing** — client increments epoch on restart; server validates monotonicity and fences stale epochs.
- **Per-batch sequence numbers** — separate from `Stream-Seq`; used for retry safety.
- **Two-layer sequence design:** transport layer = `Producer-Id` + `Producer-Epoch` + `Producer-Seq` (retry safety); application layer = `Stream-Seq` (cross-restart ordering, lexicographic).

### 11.2 Request headers (§5.2.1)

All three producer headers **MUST** be provided together or none at all; partial sets → `400 Bad Request` (**MUST**).

- `Producer-Id: <string>` — client-supplied stable identifier (e.g., `"order-service-1"`, UUID), identifying the logical producer across restarts. **MUST** be a non-empty string; empty → `400 Bad Request`.
- `Producer-Epoch: <integer>` — client-declared epoch, starting at 0; incremented on producer restart to establish a new session. Server validates monotonic non-decrease. **MUST** be a non-negative integer ≤ 2^53−1 (JavaScript interoperability).
- `Producer-Seq: <integer>` — monotonically increasing sequence number per epoch; starts at 0 for each new epoch; applies **per batch (per HTTP request)**, not per message. **MUST** be a non-negative integer ≤ 2^53−1.

### 11.3 Response headers (§5.2.1)

- `Producer-Epoch: <integer>` — echoed back on success (200/204); on stale epoch (403), carries the **current server epoch**.
- `Producer-Seq: <integer>` — on success (200/204), the **highest accepted sequence** for the `(stream, producerId, epoch)` tuple (lets clients confirm pipelined requests and recover after crashes).
- `Producer-Expected-Seq: <integer>` — on 409 Conflict (sequence gap), the expected sequence.
- `Producer-Received-Seq: <integer>` — on 409 Conflict (sequence gap), the received sequence.

### 11.4 Validation logic (§5.2.1, verbatim)

```
# Epoch validation (client-declared, server-validated)
if epoch < state.epoch:
  → 403 Forbidden
  → Headers: Producer-Epoch: <current epoch>

if epoch > state.epoch:
  if seq != 0:
    → 400 Bad Request (new epoch must start at seq=0)
  → Accept: update state.epoch = epoch, state.lastSeq = 0
  → 200 OK (new epoch established)

# Same epoch: sequence validation
if seq <= state.lastSeq:
  → 204 No Content (duplicate, idempotent success)

if seq == state.lastSeq + 1:
  → Accept, update state.lastSeq = seq
  → 200 OK

if seq > state.lastSeq + 1:
  → 409 Conflict
  → Headers: Producer-Expected-Seq: <lastSeq + 1>, Producer-Received-Seq: <seq>
```

### 11.5 Response codes with producer headers (§5.2.1)

| Code | Meaning |
| --- | --- |
| `200 OK` | Append successful (**new data**) — note the contrast with plain appends, which return 204 |
| `204 No Content` | **Duplicate** append (idempotent success; data already exists) |
| `400 Bad Request` | Invalid producer headers (non-integer values, empty `Producer-Id`, partial header set, epoch increase with seq ≠ 0) |
| `403 Forbidden` | Stale producer epoch (**zombie fencing**); response includes `Producer-Epoch` with current server epoch |
| `409 Conflict` | Sequence gap; response includes `Producer-Expected-Seq` and `Producer-Received-Seq` |

### 11.6 Bootstrap, restart, auto-claim (§5.2.1)

1. **Initial start:** producer sends `(epoch=0, seq=0)`; server accepts and establishes state.
2. **Restart:** producer increments local epoch (0→1), resets seq to 0, sends `(epoch=1, seq=0)`; server sees epoch > state.epoch and accepts.
3. **Zombie fencing:** old producer still sending `(epoch=0, seq=N)` gets `403 Forbidden` with `Producer-Epoch: 1`.

**Auto-claim flow (ephemeral/serverless producers without persisted epoch):** start fresh with `(epoch=0, seq=0)`; on `403` with `Producer-Epoch: 5`, retry with `(epoch=6, seq=0)` to claim the producer ID. Opt-in client behavior; use with caution.

### 11.7 Concurrency and atomicity (§5.2.1)

- **Concurrency (MUST):** servers **MUST** serialize validation + append per `(stream, producerId)` pair — HTTP requests can arrive out of order; without serialization, seq=1 arriving before seq=0 would cause false sequence gaps.
- **Atomicity (SHOULD):** for persistent storage, servers **SHOULD** commit producer-state updates and log appends atomically (single transaction). Non-atomic implementations have a crash window (data appended → crash before state update → retry re-accepted → duplicate data). Recovery for non-atomic stores: clients bump epoch after a crash (trades "exactly once within epoch" for "at least once across crashes"). Stores **SHOULD** document their atomicity guarantees clearly.
  - **Chronicle note:** Redis 8 Lua scripts / MULTI-EXEC transactions can provide the atomic commit of producer state + append in one step.

### 11.8 Producer state cleanup (§5.2.1)

Servers **MAY** implement TTL-based cleanup of producer state:

- In-memory stores: 7-day TTL recommended; clean up on stream access.
- Persistent stores: retain as long as the stream data exists (stronger guarantee).
- After state expiry the producer is treated as new — a zombie alive past TTL can write again (acceptable for testing; persistent stores should use longer retention).

### 11.9 Stream closure with idempotent producers (§5.2.1)

- **Close with final append:** body + producer headers + `Stream-Closed: true` → append deduplicated normally, stream closes atomically with the final append.
- **Close without append:** `Stream-Closed: true` + empty body; producer headers optional, but if provided the close is still idempotent.
- **Duplicate close:** if the stream was already closed by the **same `(producerId, epoch, seq)` tuple**, servers **SHOULD** return `204 No Content` with `Stream-Closed: true`.
- **Append to a closed stream from an idempotent producer:**
  - `(producerId, epoch, seq)` matches the request that closed the stream → `204 No Content` (duplicate/idempotent success) with `Stream-Closed: true`.
  - Otherwise → `409 Conflict` with `Stream-Closed: true`.
- Forks never inherit producer state (§4.2; see [Section 10.5](#105-producer-state-and-fork-boundaries-42)).

## 12. Reserved subscription APIs and delivery (§6, §7)

Subscriptions are durable cursors that wake workers when linked streams have pending events. They are part of the spec but separable from the core stream operations; chronicle can stage them after core protocol support. Captured here for completeness.

### 12.1 Reserved prefix and addressing (§6, §6.1)

- Control APIs live under the reserved prefix: `{stream-url}/__ds/subscriptions/:id`.
- Servers **MUST** route the `__ds` prefix **before** normal stream operations; application stream paths whose first stream-root-relative segment is `__ds` are reserved for Durable Streams control APIs.
- Stream paths in subscription request/response bodies are **stream-root-relative** (e.g., `events/abc`, `wake/pool`).
- Subscription `id` is client-provided, unique within the `__ds` namespace. One cursor per subscription stream. Each stream link has: `path`, `link_type` (`glob` from `pattern`, `explicit` from `streams[]`), `acked_offset` (last processed offset, **inclusive**).
- If a stream is linked both explicitly and by glob, `explicit` takes precedence in serialized responses; removing the explicit link does not remove a still-matching glob link.

### 12.2 Create or re-confirm — `PUT /__ds/subscriptions/:id` (§6.2)

Body fields: `type` (required: `webhook` | `pull-wake`), `pattern` (glob over stream-root-relative paths; `*` matches one segment, `**` matches zero or more), `streams` (explicit paths, additive to `pattern`), `webhook.url` (required for webhook type), `wake_stream` (required for pull-wake type), `lease_ttl_ms` (1 s – 10 min; default 30 s), `description`. At least one of `pattern` or `streams` **MUST** be present.

Responses: `201` created; `200` re-confirmed with identical configuration; `409` same ID, different configuration. Servers **MUST** hash the normalized configuration (type, pattern, normalized streams[], delivery config, lease_ttl_ms, description) and compare hashes for idempotent re-confirmation.

- Webhook deliveries are signed with an **asymmetric** key; responses **MUST NOT** include shared webhook signing secrets.
- Webhook URL validation (also §12.8): production URLs **MUST** use `https://`; development **MAY** use `http://localhost` / `http://127.0.0.x`; RFC 1918, link-local, loopback, and other local targets **MUST** be rejected outside the localhost exception.
- On creation with a `pattern`, the server **MUST** eagerly backfill matching existing streams **at their current tail offset** (no historical replay by default). Streams discovered later via a matching append are linked **before** that append for wake purposes.

### 12.3 Read / delete / membership (§6.3, §6.4)

- `GET /__ds/subscriptions/:id` → JSON document with `id`, `subscription_id`, `type`, `pattern`, `streams[]` (path, link_type, acked_offset), `webhook` (may include `signing` metadata: `alg`, `kid`, `jwks_url`), `wake_stream`, `lease_ttl_ms`, `created_at`, `status` (`active` | `failed` while webhook retry is scheduled), `description`. Receivers **MUST** use the `kid` from each webhook signature header when selecting a verification key.
- `DELETE /__ds/subscriptions/:id` → tombstones the subscription, `204 No Content`. In-flight callback/ack/release requests for a deleted subscription **MUST** fail and **MUST NOT** advance cursors.
- `POST /__ds/subscriptions/:id/streams` with `{ "streams": [...] }` → `204`; links added at current tail offset; adding an already-linked stream is idempotent.
- `DELETE /__ds/subscriptions/:id/streams/:path` (`:path` URL-encoded, may contain slashes) → `204`; deleting an absent explicit link is idempotent; removes only the explicit link (glob link remains).

### 12.4 JWKS discovery (§6.5)

- Servers supporting webhook subscriptions **MUST** expose signing public keys as a JWKS at `GET {stream-url}/__ds/jwks.json` → `200`, `Content-Type: application/jwk-set+json`, `Cache-Control: public, max-age=300` (example shows Ed25519 OKP keys).
- Lives under `__ds` (not `/.well-known/`) because servers can be embedded under arbitrary sub-roots. Receivers **SHOULD** rely on the `jwks_url` from subscription metadata.
- Each JWK `kid` **MUST** be stable for the key's lifetime and **MUST** be unique within the set; **SHOULD** be derived from key material (e.g., JWK thumbprint). Private keys **MUST** stay secret, **SHOULD** persist across restarts in production. During rotation, old public keys **MUST** stay published until all deliveries that could have been signed with them are outside the accepted replay window.

### 12.5 Delivery model (§7)

- A subscription is **idle** when no lease is held and no wake is in flight. Pending work = any linked stream's tail offset > its `acked_offset`. Pending work creates a new wake generation unless a wake is in flight or a lease is held.
- Every wake has a unique `wake_id` and a monotonically increasing `generation` scoped to the subscription. Acks are **last-processed inclusive**: acking offset `N` means the next read starts after `N`.

**Webhook delivery (§7.1):**

- `POST {webhook.url}` with JSON body (`subscription_id`, `wake_id`, `generation`, `streams[]` with `acked_offset`/`tail_offset`/`has_pending`, `callback_url`, `callback_token`) and header `Webhook-Signature: t=<timestamp>,kid=<key-id>,ed25519=<base64url-signature>`.
- Signature: Ed25519 over `<timestamp>.<raw_body>` (`t` = decimal Unix timestamp; raw body = exact request bytes; signature unpadded base64url). Receivers **SHOULD** verify against the raw body and reject timestamps outside a small replay window (e.g., five minutes).
- Synchronous completion: handler returns `{ "done": true }` → server **MUST** auto-ack the tail offsets from that wake snapshot and release the lease; if newer events arrived after the snapshot, the subscription **MUST** be woken again with a new `wake_id` and `generation`.
- Asynchronous completion: handler calls `POST /__ds/subscriptions/:id/callback` with `Authorization: Bearer <callback_token>` and body `{ wake_id, generation, acks: [{stream, offset}], done }`. Success → `{ "ok": true, "next_wake": false }`. Stale `wake_id`/`generation` → **MUST** return `409` with `{ "error": { "code": "FENCED" } }`.
- Callback tokens are scoped to subscription + generation (not service JWTs).
- Retries: exponential backoff **1 s → 60 s with 20% jitter**; retry metadata including `next_attempt_at` **MUST** be persisted across (Durable Object) eviction so a freshly-loaded instance honors the prior schedule.

**Pull-wake (§7.2):**

- The subscription writes wake events (`{type:"wake", subscription_id, stream, generation, ts}`) to its configured `wake_stream` — an ordinary durable stream that **MUST** be created explicitly by the application.
- Workers race to claim: `POST /__ds/subscriptions/:id/claim` with `Authorization: Bearer <service-jwt>` and `{ "worker": <name> }`. Success → `{ wake_id, generation, token, streams[], lease_ttl_ms }`. Lease held elsewhere → `409` with `{ "error": { "code": "ALREADY_CLAIMED", "current_holder", "generation" } }`.
- Ack: `POST /__ds/subscriptions/:id/ack` with claim token; doubles as **heartbeat** — without `done: true` it extends the lease; with `done: true` it applies acks, releases the lease, returns `{ "ok": true, "next_wake": true|false }`.
- Release without acking: `POST /__ds/subscriptions/:id/release` with `{ wake_id, generation }` → `204`. If pending work remains after release, the server **MUST** write another wake event. Stale release/ack → **MUST** return `409 FENCED`.

**Generation fencing and leases (§7.3):**

- Servers **MUST** reject a callback, ack, or release unless **all** match current state: (1) bearer token valid for the subscription; (2) token generation matches current generation; (3) request `generation` matches current generation; (4) request `wake_id` matches the current wake.
- `lease_ttl_ms` bounds worker liveness: webhook leases start when a wake is issued, extended by valid callbacks; pull-wake leases start on successful claim, extended by valid non-`done` acks. On lease expiry the server **MUST** clear the holder and wake token, and **MUST** schedule another wake if pending work remains.

## 13. Security considerations (§12)

- **Authentication/authorization (§12.1):** explicitly out of scope. Clients **SHOULD** implement standard HTTP auth primitives (Basic [RFC7617], Bearer [RFC6750], Digest [RFC7616]). Implementations **MUST** provide appropriate access controls preventing unauthorized stream creation/modification/deletion, by any mechanism (including auth extensions per §11.2).
- **Multi-tenant safety (§12.2):** if stream URLs are guessable, servers **MUST** enforce access controls even when using shared caches. Servers **SHOULD** validate and sanitize stream URLs against path traversal and enforce URL component limits.
- **Untrusted content (§12.3):** clients **MUST** treat stream contents as untrusted input and **MUST NOT** evaluate or execute stream data without validation (log-injection concern).
- **Content-type validation (§12.4):** servers **MUST** validate appended content types against the stream's declared type (type-confusion prevention).
- **Rate limiting (§12.5):** servers **SHOULD** rate-limit; `429 Too Many Requests` signals exhaustion.
- **Sequence validation (§12.6):** servers **MUST** reject `Stream-Seq` regressions to maintain stream integrity.
- **Browser security headers (§12.7):** see [Section 9](#9-browser-security-headers-and-cors).
- **Webhook SSRF prevention (§12.8):** **MUST** require HTTPS for production webhook URLs (HTTP **MAY** be allowed for localhost in dev); **SHOULD** block RFC 1918, link-local, loopback; **SHOULD** block cloud metadata endpoints (e.g., `169.254.169.254`); **MAY** allowlist domains.
- **Callback token security (§12.9):** callback/claim tokens **MUST** be passed via the `Authorization` header (avoids logging exposure); **SHOULD** be signed (e.g., HMAC JWTs) carrying subscription identity, generation, expiry; signatures **MUST** be validated on every callback, ack, and release.
- **Webhook signature security (§12.10):** receivers **SHOULD** verify signatures before processing, select keys by `kid` from the JWKS, reject timestamps outside the replay window; shared endpoints **SHOULD** also check the expected server key set and `subscription_id`.
- **TLS (§12.11):** all protocol operations **MUST** be over HTTPS (TLS) in production.

## 14. IANA: default port and header registry

### 14.1 Default port (§13.1)

- Standalone Durable Streams servers default to **4437/tcp** (4437/udp reserved for future use; chosen from IANA unassigned range 4434–4440).
- Standalone implementations **SHOULD** default to 4437 when no port is configured; when embedded in an existing web server/framework, **SHOULD** use the host server's port instead.

### 14.2 Registered HTTP headers (§13.2)

| Header | Description (spec wording) |
| --- | --- |
| `Stream-TTL` | Sliding time-to-live window for streams (seconds); resets on read or write |
| `Stream-Expires-At` | Absolute expiry time for streams (RFC 3339 timestamp) |
| `Stream-Seq` | Writer sequence number for coordination (opaque string) |
| `Stream-Cursor` | Cursor for CDN collapsing (opaque string) |
| `Stream-Next-Offset` | Next offset for subsequent reads (opaque string) |
| `Stream-Up-To-Date` | Indicates up-to-date response (presence header) |
| `Stream-Closed` | Indicates stream is closed / end-of-stream (presence header, value `true`) |
| `Stream-Forked-From` | Source stream path for forked streams, used on `PUT` requests (opaque string) |
| `Stream-Fork-Offset` | Divergence point offset for forked streams, used on `PUT` requests (opaque string) |
| `Stream-Fork-Sub-Offset` | Sub-position refinement past `Stream-Fork-Offset`, used on `PUT` (non-negative integer; bytes for non-JSON, message count for JSON) |
| `Webhook-Signature` | Ed25519 signature for webhook notification verification (§7.1) |

Producer headers (`Producer-Id`, `Producer-Epoch`, `Producer-Seq`, `Producer-Expected-Seq`, `Producer-Received-Seq`) and `stream-sse-data-encoding` are defined in the spec body (§5.2.1, §5.8) but are not in the §13.2 registration table.

## 15. Spec cross-reference errata

Stale internal references in PROTOCOL.md (the content is unambiguous; only the pointers are wrong). Useful when reading the spec side-by-side:

| Location | Says | Should be |
| --- | --- | --- |
| §4.2 fork sub-offset, JSON semantics | "flattened messages (Section 7.1)" | §9.1.2 (Array Flattening) — §7.1 is Webhook Delivery |
| §4.2 fork sub-offset, addressing dimension | "see Section 6" | §8 (Offsets) — §6 is Reserved Subscription APIs |
| §5.6 response headers, `Cache-Control` | "see Section 9" | §10 (Caching and Collapsing) — §9 is Content Types |
| Task brief (not spec) | "spec section 10.7" for CORS/browser security | §12.7 (Browser Security Headers); the spec has no §10.7 and defines no CORS (`Access-Control-*`) requirements |

## 16. Consolidated MUST/SHOULD/MAY table

Server-side requirements unless marked **(client)**. Force is the spec's exact RFC 2119 keyword.

### Core model, creation, deletion

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 1 | MUST | Treat `Stream-Closed` request header as present only when value is exactly `true` (case-insensitive); treat other values as absent | §4.1 |
| 2 | SHOULD NOT | Return errors for non-`true` `Stream-Closed` values (ignore instead) | §4.1 |
| 3 | MAY | Implement retention policies dropping data older than a given age | §4 |
| 4 | SHOULD NOT | Create a new stream at the URL of a deleted stream | §4 |
| 5 | MAY | Implement read and write paths independently | §3 |
| 6 | MUST | PUT on existing stream: `200 OK` if config (content type, TTL/expiry, closure status) matches, `409 Conflict` if not | §5.1 |
| 7 | MUST | Compare request `Stream-Closed` against the stream's closure status for idempotent PUT matching | §5.1 |
| 8 | MAY | Default content type to `application/octet-stream` when omitted on PUT | §5.1 |
| 9 | MUST | Enforce `Stream-TTL` syntax: non-negative decimal integer, no leading zeros/plus/decimal point/scientific notation | §5.1 |
| 10 | SHOULD | Reject PUT with both `Stream-TTL` and `Stream-Expires-At` with `400` (MAY define documented precedence instead — then MUST document) | §5.1 |
| 11 | SHOULD | Include `Location: {stream-url}` on `201 Created` | §5.1 |
| 12 | MUST | Soft-delete (not remove) a stream with reference count > 0 on DELETE | §5.4 |
| 13 | MUST | Return `410 Gone` for DELETE (and GET/HEAD/POST) on a soft-deleted stream | §5.4, §4.2 |
| 14 | MAY | Perform soft-delete cleanup / cascade GC asynchronously | §4.2, §5.4 |
| 15 | SHOULD NOT | **(client)** Assume source returns `404` immediately after deleting its last fork | §4.2 |
| 16 | SHOULD | Return `405` or `501` for unsupported POST (append/close) or DELETE | §5.2, §5.4 |

### Append path

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 17 | MUST | Reject body-bearing POST whose valid `Content-Type` mismatches the stream's type with `409 Conflict` | §5.2, §12.4 |
| 18 | MUST NOT | Reject empty-body POST based on `Content-Type` (MAY ignore it entirely) | §5.2 |
| 19 | SHOULD | Support HTTP/1.1 chunked encoding and HTTP/2 streaming for appends | §5.2 |
| 20 | MUST | Compare `Stream-Seq` byte-wise lexicographically; reject ≤ last sequence with `409`; require strictly increasing | §5.2, §12.6 |
| 21 | MUST | Document the `Stream-Seq` scope enforced (per writer identity or per stream) | §5.2 |
| 22 | MUST | Reject empty-body POST without `Stream-Closed: true` with `400 Bad Request` | §5.2 |
| 23 | SHOULD | Return `204` + `Stream-Closed: true` for close-only POST on an already-closed stream (idempotent) | §5.2, §5.3 |
| 24 | MUST | Return `409` + `Stream-Closed: true` for append-and-close (body, no producer headers) on a closed stream | §5.2 |
| 25 | MUST | On 409-because-closed: include `Stream-Closed: true` and `Stream-Next-Offset` (final offset) | §5.2 |
| 26 | SHOULD | Keep 409 bodies empty or standardized; **(client)** SHOULD NOT parse body for rejection reason | §5.2 |
| 27 | SHOULD | Check conflicts in order: closed → content-type mismatch → sequence regression | §5.2 |

### Idempotent producers (OPTIONAL feature — every MUST)

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 28 | MUST | Require all three producer headers together; partial set → `400 Bad Request` | §5.2.1 |
| 29 | MUST | `Producer-Id` non-empty string; empty → `400` | §5.2.1 |
| 30 | MUST | `Producer-Epoch` and `Producer-Seq`: non-negative integers ≤ 2^53−1 | §5.2.1 |
| 31 | MUST | epoch < state.epoch → `403 Forbidden` with `Producer-Epoch: <current epoch>` (zombie fencing) | §5.2.1 |
| 32 | MUST | epoch > state.epoch with seq ≠ 0 → `400 Bad Request` (new epoch must start at seq=0) | §5.2.1 |
| 33 | MUST | epoch > state.epoch with seq = 0 → accept, update state, `200 OK` | §5.2.1 |
| 34 | MUST | Same epoch, seq ≤ lastSeq → `204 No Content` (duplicate, no re-append) | §5.2.1 |
| 35 | MUST | Same epoch, seq = lastSeq+1 → accept, `200 OK` | §5.2.1 |
| 36 | MUST | Same epoch, seq > lastSeq+1 → `409` with `Producer-Expected-Seq` and `Producer-Received-Seq` | §5.2.1 |
| 37 | MUST | Echo `Producer-Epoch` and return highest-accepted `Producer-Seq` on success (200/204) | §5.2.1 |
| 38 | MUST | Serialize validation + append per `(stream, producerId)` pair | §5.2.1 |
| 39 | SHOULD | Commit producer state + append atomically (persistent storage); document atomicity guarantees | §5.2.1 |
| 40 | MAY | TTL-based producer-state cleanup (7-day recommendation for in-memory) | §5.2.1 |
| 41 | SHOULD | Duplicate close (same `(producerId, epoch, seq)` that closed the stream) → `204` + `Stream-Closed: true` | §5.2.1 |
| 42 | MUST NOT | Forks inherit producer state or `Stream-Seq` state; producers MUST re-bootstrap on the fork | §4.2 |

### Reads, closure, EOF

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 43 | SHOULD | Catch-up at tail, no data: `200 OK`, empty body, `Stream-Next-Offset` = requested offset | §5.6 |
| 44 | MUST | Add `Stream-Closed: true` to the at-tail empty response when the stream is closed (canonical EOF) | §5.6 |
| 45 | MUST | Set `Stream-Up-To-Date: true` when the response contains all currently-available data | §5.6 |
| 46 | SHOULD NOT | Set `Stream-Up-To-Date` on partial responses (chunk-size limited) | §5.6 |
| 47 | MUST | Set `Stream-Closed: true` when stream is closed and client reached the final offset at generation time | §5.6 |
| 48 | SHOULD NOT | Set `Stream-Closed` on partial data from a closed stream | §5.6 |
| 49 | SHOULD | **(client)** Use `HEAD` to learn closure status before reaching the tail | §5.6 |
| 50 | MUST | **(client)** Use returned `Stream-Next-Offset` for subsequent reads; SHOULD persist offsets locally | §8 |
| 51 | MUST | Long-poll: require `offset`; `204 No Content` on timeout with `Stream-Next-Offset` + `Stream-Up-To-Date: true` | §5.7 |
| 52 | MUST | Long-poll 200/204: include `Stream-Cursor` when stream open (MAY omit when closed) | §5.7 |
| 53 | MUST | **(client)** Tolerate missing `Stream-Cursor`/`streamCursor` when `Stream-Closed`/`streamClosed` present | §5.6–5.8 |
| 54 | MUST NOT / MUST | Long-poll on closed stream at tail: not wait; immediately `204` + `Stream-Closed: true` + `Stream-Up-To-Date: true` | §5.7 |
| 55 | MAY | Accept a `timeout` query parameter for long-poll (future extension) | §5.7 |
| 56 | MUST / MUST NOT | Fork long-poll: serve inherited-range data immediately; source appends after fork creation never unblock fork waiters | §5.7 |

### SSE mode

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 57 | MUST | Use `Content-Type: text/event-stream` on SSE responses | §5.8 |
| 58 | MUST | Base64-encode data events and send `stream-sse-data-encoding: base64` for content types other than `text/*` / `application/json`; **(client)** MUST check the header and decode | §5.8 |
| 59 | MUST | **(client)** Concatenate `data:` lines and strip `\n`/`\r` before base64-decoding; text must be valid base64, length multiple of 4 (or empty); 0-byte payload = empty string | §5.8 |
| 60 | MUST | Emit a `control` event after every `data` event, containing `streamNextOffset` | §5.8 |
| 61 | MUST | Include `streamCursor` in control events while open (MAY omit when `streamClosed: true`) | §5.8 |
| 62 | MUST | Include `upToDate: true` when caught up (MAY omit when `streamClosed: true`, which implies it) | §5.8 |
| 63 | MUST | Include `streamClosed: true` in the final control event when closed and all data sent; then close the connection | §5.8 |
| 64 | MUST | Closed stream + offset at tail on connect: immediately emit control `{streamClosed: true, upToDate: true}` and close | §5.8 |
| 65 | MUST NOT | **(client)** Reconnect after `streamClosed: true`; otherwise MUST reconnect with last `streamNextOffset` | §5.8 |
| 66 | SHOULD | Close SSE connections roughly every ~60 s (CDN collapsing) | §5.8, §10.2 |

### Offsets

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 67 | MUST NOT | **(client)** Interpret, construct, or modify offsets | §8 |
| 68 | MUST | Mint strictly-increasing, unique, lexicographically sortable offsets; no duplicate/non-monotonic schemes (e.g., raw timestamps) | §8 |
| 69 | MUST NOT | Offsets contain `,` `&` `=` `?` `/`; SHOULD be URL-safe and under 256 chars; **(client)** MUST URL-encode in query params | §8 |
| 70 | MUST | Recognize `-1` as stream beginning (≡ omitted offset) | §8 |
| 71 | MUST | `offset=now` catch-up: `200` empty body (`[]` for JSON), `Stream-Next-Offset` = tail, `Stream-Up-To-Date: true`, no data; SHOULD `Cache-Control: no-store` | §8 |
| 72 | MUST | `offset=now` long-poll: wait immediately (no initial empty response); `Stream-Next-Offset` = tail | §8 |
| 73 | MUST | `offset=now` SSE: start at tail; first control event carries tail offset (+ `upToDate: true` if no data) | §8 |
| 74 | MUST | `offset=now` on closed stream (any mode): return immediately with `Stream-Closed: true`, `Stream-Up-To-Date: true`, final offset | §8 |
| 75 | MUST NOT | Generate `-1` or `now` as real offsets | §8 |

### JSON mode

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 76 | MUST | Preserve message boundaries; GET returns a JSON array of messages in range | §9.1.1, §9.1.5 |
| 77 | MUST | Flatten exactly one array level on POST (each element = one message) | §9.1.2 |
| 78 | MUST | Reject POST `[]` with `400`; accept PUT `[]` (empty stream) | §9.1.3 |
| 79 | MUST | Validate appended JSON; invalid → `400` with error message | §9.1.4 |
| 80 | MUST | GET responses: `Content-Type: application/json`; empty range → `[]` | §9.1.5 |
| 81 | MUST | Fork-boundary-spanning JSON reads return one single JSON array | §9.1 |

### Caching, cursors, collapsing

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 82 | SHOULD | Catch-up/long-poll: `Cache-Control: public, max-age=60, stale-while-revalidate=300` (shared) or `private, ...` (user-specific) | §10.1 |
| 83 | SHOULD | HEAD responses effectively non-cacheable (`no-store` recommended; `private, max-age=0, must-revalidate` allowed) | §5.5 |
| 84 | MUST | Generate `ETag` for GET responses except `offset=now`; format `{internal_stream_id}:{start_offset}:{end_offset}` | §10.1, §5.6 |
| 85 | MUST | Matching `If-None-Match` → `304 Not Modified`, no body | §10.1 |
| 86 | MUST | ETag varies with closure status (changes on close without new data); SHOULD encode closure marker (e.g., `:c`) | §10.1 |
| 87 | SHOULD | **(client)** Order query params lexicographically by key | §10.1 |
| 88 | MUST | Generate cursors on all live-mode responses (long-poll header, SSE control field) | §10.1 |
| 89 | MUST | Cursor monotonicity: client cursor ≥ current interval → return strictly greater cursor (jitter 1–3600 s); never go backwards | §10.1 |
| 90 | MUST | **(client)** Echo received cursor as `cursor` query param on subsequent requests | §10.1 |
| 91 | SHOULD NOT | CDNs/proxies cache long-poll `204` responses (200s cacheable keyed by offset+cursor+auth) | §10.1 |

### Forks

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 92 | MUST | Fork `Content-Type` (if provided) match the source's; else `409` (omitted → inherit) | §4.2 |
| 93 | MUST | `400` when `Stream-Fork-Offset` exceeds source tail, is malformed, or sub-offset is malformed/negative/overshoots the next data boundary | §4.2, §5.1 |
| 94 | MUST | `404` when fork source path does not exist; `409` when target path in use with different config or source soft-deleted | §4.2 |
| 95 | MUST | Idempotent fork PUT: matching config (incl. `Stream-Forked-From`, `Stream-Fork-Offset`) → `200`; differing → `409` | §4.2 |
| 96 | MAY | Fork closed streams (fork starts open) | §4.2 |
| 97 | MUST | `Stream-Fork-Offset` be a server-returned offset; servers NOT REQUIRED to validate storage alignment (MAY reject with `400`) | §8 |
| 98 | MUST | Apply fork TTL/expiry inheritance table (incl. inheriting source TTL/Expires-At when fork requests none) | §4.2 |

### Extensibility & security

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 99 | MUST NOT | Extensions break base protocol semantics; base operations MUST work without extension support | §11.1 |
| 100 | SHOULD | Extensions be additive/pure-superset, optional, version-independent | §11, §11.1 |
| 101 | MUST | Provide access controls against unauthorized create/modify/delete (mechanism free) | §12.1 |
| 102 | MUST | Enforce access controls when URLs are guessable, even with shared caches; SHOULD sanitize URLs (path traversal, length) | §12.2 |
| 103 | MUST | **(client)** Treat stream contents as untrusted; never evaluate/execute without validation | §12.3 |
| 104 | SHOULD | Rate-limit (signal via `429`) | §12.5 |
| 105 | SHOULD | Send `X-Content-Type-Options: nosniff` on all responses; `Cross-Origin-Resource-Policy` on stream responses; `Cache-Control: no-store` on HEAD/sensitive responses; MAY send `Content-Disposition: attachment` for octet-stream | §12.7 |
| 106 | MUST | HTTPS (TLS) for all operations in production | §12.11 |
| 107 | SHOULD | Standalone servers default to port 4437/tcp; embedded deployments use the host server's port | §13.1 |

### Subscriptions (§6–§7, if implemented)

| # | Force | Requirement | Spec |
| --- | --- | --- | --- |
| 108 | MUST | Route reserved `__ds` prefix before normal stream operations | §6 |
| 109 | MUST | Require at least one of `pattern`/`streams`; hash normalized config for idempotent re-confirmation (201/200/409) | §6.2 |
| 110 | MUST | Validate webhook URLs: HTTPS in production (localhost HTTP for dev); reject RFC 1918/link-local/loopback; never return shared signing secrets | §6.2, §12.8 |
| 111 | MUST | Eagerly backfill pattern-matching existing streams at their current tail offset | §6.2 |
| 112 | MUST | Fail in-flight callback/ack/release for deleted subscriptions without advancing cursors | §6.3 |
| 113 | MUST | Expose webhook signing keys as JWKS at `__ds/jwks.json`; stable unique `kid`s; publish old keys through the replay window | §6.5 |
| 114 | MUST | Auto-ack snapshot tails on webhook `{done: true}`; re-wake with new `wake_id`/`generation` if newer events exist | §7.1 |
| 115 | MUST | Return `409 {"error":{"code":"FENCED"}}` for stale `wake_id`/`generation`; reject unless token valid + token generation + request generation + `wake_id` all match | §7.1, §7.3 |
| 116 | MUST | Persist webhook retry metadata (incl. `next_attempt_at`) across eviction; backoff 1–60 s with 20% jitter | §7.1 |
| 117 | MUST | On lease expiry: clear holder and wake token; schedule another wake if pending work remains; write another wake event after release with pending work | §7.2, §7.3 |
| 118 | MUST | Pass callback/claim tokens via `Authorization` header; validate token signatures on every callback/ack/release | §12.9 |

---

*End of requirements catalog. Generated from PROTOCOL.md (1563 lines) in full; no external sources used.*
