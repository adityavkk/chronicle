/**
 * Core typed contracts for dsui. Everything else (client, store, components)
 * depends on these shapes. Keep this file dependency-free.
 *
 * Vocabulary maps directly onto the Durable Streams (chronicle) HTTP protocol:
 *  - An *offset* is an opaque cursor string into a stream. Special values
 *    "-1" (beginning) and "now" (current tail) are understood by the server.
 *  - A read returns a *batch* of messages plus `Stream-Next-Offset`, the offset
 *    to pass on the next read to resume exactly where you left off.
 */

/** A saved Durable Streams server target. */
export interface Connection {
	/** Stable id (uuid-ish); used as the localStorage / selection key. */
	readonly id: string;
	/** Human label shown in the switcher. */
	readonly name: string;
	/** Origin, no trailing slash, e.g. "http://localhost:4437". */
	readonly baseUrl: string;
	/** Stream route root, default "/v1/stream". No trailing slash. */
	readonly streamRoot: string;
	/** Wall-clock ms when this connection was created. */
	readonly createdAt: number;
	/** Wall-clock ms this connection was last made active, or null if never. */
	readonly lastUsedAt: number | null;
}

/**
 * Runtime config served by the Go binary at /dsui-config.json. Optional fields
 * because the file may be absent (pure `vite dev`) or partial.
 */
export interface DsuiConfig {
	/** A Durable Streams server URL to prefill as a connection, if any. */
	readonly defaultServer: string | null;
}

/** Result of a connectivity probe against a {@link Connection}. */
export interface ConnectionProbe {
	readonly ok: boolean;
	/** HTTP status of the probe request, or 0 if the request never completed. */
	readonly status: number;
	/** Round-trip time in ms. */
	readonly latencyMs: number;
	/** Present when ok is false. */
	readonly error?: string;
}

/**
 * Per-connection probe lifecycle. "checking" is distinct from a completed probe
 * so the UI can show an in-flight pulse on the status dot. Absent (not in the
 * map) means "not yet probed" — rendered as an unknown/idle dot.
 */
export type ProbeStatus =
	| { readonly state: "checking" }
	| { readonly state: "done"; readonly probe: ConnectionProbe };

/** Stream content classification derived from Content-Type. */
export type StreamKind = "json" | "text" | "binary";

/** A stream discovered via the registry (or manually added). */
export interface StreamInfo {
	/** Stream path under streamRoot, e.g. "orders/created". No leading slash. */
	readonly path: string;
	/** Raw Content-Type as reported by the server, if known. */
	readonly contentType: string | null;
	/** Derived kind from contentType. */
	readonly kind: StreamKind;
	/** ISO timestamp from the registry event value, if present. */
	readonly createdAt: string | null;
	/** True when this stream was typed in by the user, not seen in __registry__. */
	readonly manual: boolean;
}

/**
 * One renderable row in the messages grid.
 *
 * For JSON streams: one row per array element. The protocol does NOT expose a
 * per-element offset (it returns a batch + a single Stream-Next-Offset), so we
 * surface the element `index` within the batch and let the caller show the
 * honest batch offset range. For text/binary: a single row for the chunk.
 */
export interface GridRow {
	/** Zero-based index within the read batch. */
	readonly index: number;
	/** Byte size of this element (new Blob([...]).size). */
	readonly byteSize: number;
	/** One-line, truncated preview for the grid cell. */
	readonly preview: string;
	/** Render hint for the inspector. */
	readonly kind: StreamKind;
	/**
	 * The decoded value.
	 *  - json: the parsed element (unknown — guard before deep use).
	 *  - text: the chunk as a string.
	 *  - binary: the raw bytes.
	 */
	readonly value: unknown;
}

/** The HTTP response headers the protocol disclosure cares about. */
export interface ProtocolHeaders {
	readonly streamNextOffset: string | null;
	readonly streamClosed: string | null;
	readonly streamUpToDate: string | null;
	readonly etag: string | null;
	readonly contentType: string | null;
}

/**
 * A captured record of the last HTTP exchange, for the "Under the hood"
 * protocol disclosure. Self-contained so it can be rendered without re-fetching.
 */
export interface HttpExchange {
	readonly method: string;
	/** Full URL including query string. */
	readonly url: string;
	readonly requestHeaders: Readonly<Record<string, string>>;
	/** 0 if the request failed before a response (network error). */
	readonly status: number;
	readonly statusText: string;
	readonly responseHeaders: Readonly<Record<string, string>>;
	/** The protocol-significant subset, pre-extracted for convenience. */
	readonly protocol: ProtocolHeaders;
	/** Wall-clock ms when the exchange completed. */
	readonly at: number;
	/** Round-trip duration in ms. */
	readonly durationMs: number;
	/** Set when status === 0 (network/CORS failure, etc.). */
	readonly error?: string;
}

/** The outcome of reading a stream at a given offset. */
export interface ReadResult {
	/** The stream that was read. */
	readonly path: string;
	/** Derived kind for this read. */
	readonly kind: StreamKind;
	/** The offset that was requested ("-1", "now", or an opaque cursor). */
	readonly requestedOffset: string;
	/** Stream-Next-Offset from the response; pass it back to resume. */
	readonly nextOffset: string | null;
	/** True when the server reports the stream is closed. */
	readonly closed: boolean;
	/** True when the server reports the read reached the tail. */
	readonly upToDate: boolean;
	/** Decoded rows ready for the grid. */
	readonly rows: readonly GridRow[];
	/** Raw response body bytes, for the per-stream "Raw" view. */
	readonly rawBytes: Uint8Array;
	/** The HTTP exchange that produced this result. */
	readonly exchange: HttpExchange;
}

/* ----------------------------------------------------------------------------
 * Write / fork / live-tail contracts (Feature: stream operations)
 *
 * These shapes are the inputs the dsClient write/tail methods take and the
 * outcomes they return. They map directly onto the chronicle HTTP surface:
 *   - CREATE  PUT  /v1/stream/{path}      (Content-Type, Stream-TTL, …)
 *   - APPEND  POST /v1/stream/{path}      (body + optional Producer-* headers)
 *   - CLOSE   POST … Stream-Closed: true
 *   - DELETE  DELETE /v1/stream/{path}
 *   - FORK    PUT … Stream-Forked-From + Stream-Fork-Offset
 *   - TAIL    GET  …?offset=X&live=long-poll | live=sse
 *
 * Like the rest of the client, the methods that take these resolve to typed
 * outcomes and never throw; they all carry the captured {@link HttpExchange}.
 * ------------------------------------------------------------------------- */

/**
 * The wire Content-Type a stream is created with. The server fixes a stream's
 * type at creation; reads/appends are then shaped by it. Other content types
 * are accepted as a free-form string for forward compatibility.
 */
export type StreamContentType =
	| "text/plain"
	| "application/json"
	| "application/octet-stream"
	| (string & {});

/**
 * Where a new stream forks from. A fork is a CREATE that inherits the source
 * stream's data up to {@link offset} (+ optional {@link subOffset}) and then
 * diverges. Sent as Stream-Forked-From / Stream-Fork-Offset / Stream-Fork-Sub-Offset.
 */
export interface ForkSource {
	/** The source stream path to fork from. No leading slash. */
	readonly fromPath: string;
	/** Opaque offset cursor in the source up to which data is inherited. */
	readonly offset: string;
	/** Optional sub-offset within the fork point (a batch element index). */
	readonly subOffset?: number;
}

/**
 * Options for creating (or forking) a stream via PUT /v1/stream/{path}.
 * `closed` creates an already-closed stream; `fork` makes this a fork CREATE.
 */
export interface CreateStreamOptions {
	/** Stream path to create. No leading slash. */
	readonly path: string;
	/** Content-Type header, which fixes the stream's type. */
	readonly contentType: StreamContentType;
	/** Optional Stream-TTL, e.g. "1h", "30m". */
	readonly ttl?: string;
	/** Optional Stream-Expires-At as an RFC3339 timestamp. */
	readonly expiresAt?: string;
	/** When true, create the stream already closed (Stream-Closed: true). */
	readonly closed?: boolean;
	/** When present, this CREATE is a fork of an existing stream. */
	readonly fork?: ForkSource;
}

/**
 * Idempotent-producer identity sent on an append. `epoch` fences older
 * producers; `seq` dedupes and orders writes from this producer.
 */
export interface ProducerIdentity {
	/** Producer-Id: a stable producer name. */
	readonly id: string;
	/** Producer-Epoch: increments to fence older producers with the same id. */
	readonly epoch: number;
	/** Producer-Seq: per-producer monotonic sequence number for dedupe. */
	readonly seq: number;
}

/**
 * Options for an append/publish via POST /v1/stream/{path}.
 *
 * `body` is the data to publish. For application/json streams it is the JSON
 * ARRAY batch text (each element = one message); for text/binary it is the raw
 * payload. `closeAfter` appends-and-closes atomically (Stream-Closed: true).
 */
export interface AppendOptions {
	/** The request body: a JSON-array string, raw text, or raw bytes. */
	readonly body: string | Uint8Array;
	/** Optional idempotent-producer identity (Producer-* headers). */
	readonly producer?: ProducerIdentity;
	/** When true, close the stream in the same request. */
	readonly closeAfter?: boolean;
	/** Override the request Content-Type; defaults to the stream's type. */
	readonly contentType?: StreamContentType;
}

/**
 * A protocol-level descriptor of a single HTTP operation the UI performs,
 * independent of whether it has run yet. It is the input to the curl helper
 * ({@link "./curl".toCurl}) and the shape every write action is built from, so
 * the exact equivalent curl can be shown next to the control before it runs.
 */
export interface Operation {
	/** HTTP method, uppercase (GET / HEAD / PUT / POST / DELETE). */
	readonly method: string;
	/** The full URL, including any query string. */
	readonly url: string;
	/** Request headers as a plain record (preserve insertion order). */
	readonly headers: Readonly<Record<string, string>>;
	/**
	 * Request body, if any. A string is sent verbatim; bytes are noted in curl
	 * as binary rather than inlined. Absent for GET/HEAD/DELETE.
	 */
	readonly body?: string | Uint8Array;
}

/**
 * Producer-conflict detail surfaced when the server rejects an append because
 * the producer sequence did not match (Producer-Expected-Seq / -Received-Seq).
 */
export interface ProducerConflict {
	/** The seq the server expected next from this producer. */
	readonly expectedSeq: number | null;
	/** The seq the server actually received. */
	readonly receivedSeq: number | null;
}

/**
 * The outcome of a write/fork/close/delete operation. Never thrown — returned.
 * `ok` reflects a 2xx; on failure, `error` (and possibly {@link conflict})
 * explain why. The {@link operation} descriptor + {@link exchange} let the UI
 * show the equivalent curl and the under-the-hood transcript for what ran.
 */
export interface WriteResult {
	/** True when the server returned a 2xx. */
	readonly ok: boolean;
	/** Stream-Next-Offset after the write, if the server returned one. */
	readonly nextOffset: string | null;
	/** Location header from a CREATE (201), if present. */
	readonly location: string | null;
	/** Producer-conflict detail when a producer-seq mismatch was reported. */
	readonly conflict: ProducerConflict | null;
	/** A short human error, present when ok is false. */
	readonly error: string | null;
	/** The operation descriptor that was sent (for the curl helper). */
	readonly operation: Operation;
	/** The captured HTTP exchange (for the protocol disclosure). */
	readonly exchange: HttpExchange;
}

/**
 * How a live tail follows a stream.
 *  - "catchup":   plain paged reads (the existing read path).
 *  - "long-poll": GET …&live=long-poll, looping on Stream-Next-Offset.
 *  - "sse":       an EventSource on …&live=sse.
 */
export type TailMode = "catchup" | "long-poll" | "sse";

/**
 * Lifecycle of a live-tail connection, for the tail UI's status affordance.
 *  - "idle":         not tailing.
 *  - "connecting":   opening (first request / EventSource handshake).
 *  - "live":         connected and actively receiving / waiting at the tail.
 *  - "reconnecting": a transient error; backing off before retrying.
 *  - "closed":       the stream is closed; no more data will arrive.
 *  - "error":        a terminal error stopped the tail.
 */
export type TailStatus =
	| { readonly state: "idle" }
	| { readonly state: "connecting" }
	| { readonly state: "live"; readonly atOffset: string | null }
	| { readonly state: "reconnecting"; readonly attempt: number; readonly reason: string }
	| { readonly state: "closed" }
	| { readonly state: "error"; readonly message: string };

/**
 * A batch of rows delivered by a live tail, plus the cursor to resume from.
 * `upToDate` is true when the tail has caught up and is waiting for new data.
 */
export interface TailBatch {
	/** Decoded rows newly received in this delivery. */
	readonly rows: readonly GridRow[];
	/** The next offset cursor to resume from, if the server returned one. */
	readonly nextOffset: string | null;
	/** True when this delivery reached the current tail (nothing newer yet). */
	readonly upToDate: boolean;
	/** The exchange that produced this batch (long-poll only; null for SSE). */
	readonly exchange: HttpExchange | null;
}

/** A handle returned by a tail opener; call it to stop and clean up. */
export type TailStopper = () => void;

/* ----------------------------------------------------------------------------
 * Toast / notification contracts
 * ------------------------------------------------------------------------- */

/** The visual + semantic tone of a toast. */
export type ToastKind = "info" | "success" | "warning" | "error";

/**
 * A single transient notification. Created by the store's addToast action and
 * rendered by the Toaster. Auto-dismisses after {@link durationMs} unless it is
 * 0 (sticky). An optional one-shot {@link action} (e.g. "Copy as curl", "View")
 * is rendered as a button inside the toast.
 */
export interface Toast {
	/** Stable id used as the render key and the dismiss handle. */
	readonly id: string;
	/** Tone, driving the icon + color. */
	readonly kind: ToastKind;
	/** Short headline. */
	readonly title: string;
	/** Optional secondary line. */
	readonly message?: string;
	/** Auto-dismiss delay in ms; 0 keeps it until dismissed. */
	readonly durationMs: number;
	/** Wall-clock ms when the toast was created. */
	readonly createdAt: number;
	/** Optional inline action button. */
	readonly action?: ToastAction;
}

/** An inline action button rendered inside a toast. */
export interface ToastAction {
	/** Button label. */
	readonly label: string;
	/** Invoked on click; the toast is dismissed afterward. */
	readonly onAction: () => void;
}

/** Active UI theme. "system" defers to prefers-color-scheme. */
export type Theme = "system" | "light" | "dark";

/** A typed parse failure that the UI can render inline instead of throwing. */
export interface ParseError {
	readonly kind: "parse-error";
	readonly message: string;
}

/** Result of a guarded parse: either a value or a typed error. */
export type ParseOutcome<T> =
	| { readonly ok: true; readonly value: T }
	| { readonly ok: false; readonly error: ParseError };
