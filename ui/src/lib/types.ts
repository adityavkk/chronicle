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
	/**
	 * Base URL the dsui binary's built-in webhook-capture endpoint is reachable
	 * at (e.g. "http://localhost:4438"), or null under pure `vite dev`. A webhook
	 * subscription's webhook_url is built as `${captureBase}/__hooks/{id}`; the
	 * browser opens an EventSource on `${captureBase}/__hooks/{id}/stream` to see
	 * captured deliveries. This is a tool feature, not part of the protocol.
	 */
	readonly captureBase: string | null;
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

/**
 * One visited read position for the per-stream, session-only read-cursor
 * history. Offsets are opaque cursors the server hands out and the protocol has
 * no backward read, so the client keeps the sequence of positions it has read
 * this session — turning opaque offsets into navigable breadcrumbs. Bounded and
 * ephemeral (reset on stream/connection switch, never persisted).
 */
export interface ReadHistoryEntry {
	/** The stream this position belongs to (all entries share the selected one). */
	readonly path: string;
	/** The offset that was requested ("-1", "now", or an opaque cursor). */
	readonly requestedOffset: string;
	/** Stream-Next-Offset returned by that read; the cursor to resume from. */
	readonly nextOffset: string | null;
	/** Number of rows the read decoded (the honest batch size, pre-cap). */
	readonly rowCount: number;
	/** Wall-clock ms when the read completed. */
	readonly at: number;
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

/* ----------------------------------------------------------------------------
 * Subscription control plane (the reserved /__ds/* surface)
 *
 * Subscriptions are the chronicle fan-out plane: a subscription links a set of
 * streams (by glob pattern and/or an explicit list) and delivers wakes either by
 * POSTing a signed notification to a webhook URL ("webhook") or by appending a
 * wake event to a wake stream that workers claim ("pull-wake"). The shapes below
 * map onto the verified server contract (webhook/wire.go + webhook/types.go):
 *
 *   - CREATE  PUT    /__ds/subscriptions/{id}            (201 new / 200 match / 409 conflict)
 *   - GET     GET    /__ds/subscriptions/{id}            (404 when absent)
 *   - DELETE  DELETE /__ds/subscriptions/{id}            (204)
 *   - ADD     POST   /__ds/subscriptions/{id}/streams    (204)
 *   - REMOVE  DELETE /__ds/subscriptions/{id}/streams/{path}  (204)
 *   - CLAIM   POST   /__ds/subscriptions/{id}/claim       (200 / 409 ALREADY_CLAIMED)
 *   - ACK     POST   /__ds/subscriptions/{id}/ack         (200 / 409 FENCED)
 *   - RELEASE POST   /__ds/subscriptions/{id}/release     (204 / 409 FENCED)
 *   - CALLBACK POST  /__ds/subscriptions/{id}/callback    (200 / 409 FENCED)
 *   - JWKS    GET    /__ds/jwks.json
 *
 * IMPORTANT: there is no list-all endpoint. The UI tracks known subscription ids
 * client-side, persisted per connection (see the store), exactly as it does for
 * the connection list. These shapes carry only what the server actually
 * serializes; runtime-only fields (phase / generation / wake_id) appear in the
 * claim/wake plane, not the GET response, so they are modeled as optional.
 *
 * Like the rest of the client, the methods that take/return these resolve to
 * typed outcomes and never throw; they all carry the captured {@link HttpExchange}.
 * ------------------------------------------------------------------------- */

/** How a subscription delivers wakes (DispatchType). */
export type SubscriptionType = "webhook" | "pull-wake";

/** How a stream came to be linked to a subscription. Explicit wins on display. */
export type LinkType = "glob" | "explicit";

/** Serialized delivery status: normal, or a webhook retry is scheduled. */
export type SubscriptionStatus = "active" | "failed";

/**
 * Runtime wake/lease state. Not serialized in the GET response (the server keeps
 * it internal), so it is optional everywhere the UI surfaces it.
 *  - "idle":   no lease held and no wake in flight; eligible to wake.
 *  - "waking": a wake was issued/POSTed but not yet claimed/completed.
 *  - "live":   a worker holds the lease, or a webhook callback was received
 *              without done=true.
 */
export type SubscriptionPhase = "idle" | "waking" | "live";

/**
 * Webhook signing metadata (asymmetric Ed25519; never a shared secret). A
 * receiver selects the verification key by {@link kid} from the
 * Webhook-Signature header and fetches the public key from {@link jwksUrl}.
 */
export interface WebhookSigning {
	/** Signature algorithm, the lowercase "ed25519" (distinct from JWK "EdDSA"). */
	readonly alg: string;
	/** Stable key id ("ds_<base64url-thumbprint>"). */
	readonly kid: string;
	/** Absolute URL of the JWKS document (…/streams/__ds/jwks.json). */
	readonly jwksUrl: string;
}

/** The webhook block of a webhook-type subscription. */
export interface WebhookConfig {
	/** Delivery target URL. */
	readonly url: string;
	/** Signing metadata, present once the server has minted a key. */
	readonly signing: WebhookSigning | null;
}

/**
 * One serialized stream link as returned in the GET / create response. The
 * server only serializes path / link_type / acked_offset here; the richer
 * tail_offset / has_pending live in {@link WakeStreamSnapshot} (claim + wake).
 */
export interface StreamLink {
	/** Stream-root-relative path of the linked stream. */
	readonly path: string;
	/** How the link was formed (glob match vs explicit add). */
	readonly linkType: LinkType;
	/** Opaque, inclusive cursor of the last processed offset for this stream. */
	readonly ackedOffset: string;
}

/**
 * A per-stream snapshot returned in a claim response and a webhook wake
 * notification: a link plus the current tail and whether work is pending.
 */
export interface WakeStreamSnapshot {
	readonly path: string;
	readonly linkType: LinkType;
	/** Opaque, inclusive cursor of the last processed offset. */
	readonly ackedOffset: string;
	/** The stream's current tail cursor (opaque). */
	readonly tailOffset: string;
	/** True when tailOffset > ackedOffset (unprocessed data exists). */
	readonly hasPending: boolean;
}

/**
 * A subscription as the UI models it: the serialized GET / create fields, plus
 * optional runtime fields the UI may learn from a claim. `wakeStream` is set for
 * pull-wake subscriptions, `webhook` for webhook subscriptions.
 */
export interface Subscription {
	/** Client-provided id, unique within the __ds namespace. */
	readonly id: string;
	/** Delivery type. */
	readonly type: SubscriptionType;
	/** Glob pattern matching stream paths, or null when only explicit streams. */
	readonly pattern: string | null;
	/** Linked streams with their acked cursors. */
	readonly streams: readonly StreamLink[];
	/** Webhook block (webhook type only), or null. */
	readonly webhook: WebhookConfig | null;
	/** Wake stream path (pull-wake type only), or null. */
	readonly wakeStream: string | null;
	/** Lease TTL in ms (1000–600000; default 30000). */
	readonly leaseTtlMs: number;
	/** RFC3339 creation timestamp, or null when the server omitted it. */
	readonly createdAt: string | null;
	/** Serialized delivery status. */
	readonly status: SubscriptionStatus;
	/** Optional human-readable label. */
	readonly description: string | null;
	/** Runtime phase, when the UI has learned it (not in the GET response). */
	readonly phase?: SubscriptionPhase;
	/** Current fencing generation, when known (from a claim). */
	readonly generation?: number;
}

/**
 * Options for creating (or re-confirming) a subscription via PUT
 * /__ds/subscriptions/{id}. At least one of {@link pattern} or {@link streams}
 * is required. {@link webhookUrl} is required for type "webhook";
 * {@link wakeStream} for type "pull-wake".
 */
export interface CreateSubscriptionOptions {
	/** Client-provided subscription id (the {id} path segment). */
	readonly id: string;
	/** Delivery type. */
	readonly type: SubscriptionType;
	/** Glob pattern to match stream paths (* = one segment, ** = zero or more). */
	readonly pattern?: string;
	/** Explicit stream paths to link at their current tail. */
	readonly streams?: readonly string[];
	/** Webhook delivery URL (type "webhook"). */
	readonly webhookUrl?: string;
	/** Wake stream path (type "pull-wake"). */
	readonly wakeStream?: string;
	/** Lease TTL in ms; the server clamps to 1000–600000, 0 → 30000 default. */
	readonly leaseTtlMs?: number;
	/** Optional human-readable label. */
	readonly description?: string;
}

/**
 * The successful result of a pull-wake claim (POST …/claim). Carries the Bearer
 * {@link token} the worker uses for ack/release, the fencing {@link generation}
 * + {@link wakeId}, and the per-stream snapshots with pending work.
 */
export interface WakeClaim {
	/** Unique id of this wake (prevents replay within a generation). */
	readonly wakeId: string;
	/** Monotonic fencing generation; tokens are valid only for it. */
	readonly generation: number;
	/** Bearer token (signed JWT) for ack/release of this claim. */
	readonly token: string;
	/** Per-stream snapshots (tail + has_pending) at claim time. */
	readonly streams: readonly WakeStreamSnapshot[];
	/** Lease TTL in ms granted for this claim. */
	readonly leaseTtlMs: number;
}

/** A single offset acknowledgment in an ack/callback body. */
export interface OffsetAck {
	/** The stream-root-relative path being acked. */
	readonly stream: string;
	/** The opaque offset processed (inclusive). */
	readonly offset: string;
}

/**
 * The body of an ack (POST …/ack) or webhook callback (POST …/callback). Fences
 * on (generation, wakeId). {@link done} true releases the lease and applies the
 * acks; absent/false extends the lease as a heartbeat.
 */
export interface AckRequest {
	/** The wake being acked. */
	readonly wakeId: string;
	/** The generation the claim/wake was issued under. */
	readonly generation: number;
	/** Offsets processed, applied when done. */
	readonly acks: readonly OffsetAck[];
	/** True to release + apply; absent/false to heartbeat-extend the lease. */
	readonly done?: boolean;
}

/** The success body of an ack / callback (POST …/ack | …/callback). */
export interface AckResult {
	/** Always true on a 2xx ack. */
	readonly ok: boolean;
	/** Whether the server determined a new wake is due. */
	readonly nextWake: boolean;
}

/**
 * The outcome of a subscription control-plane operation. Like {@link WriteResult}
 * it is returned, never thrown, and carries the {@link Operation} descriptor + the
 * captured {@link HttpExchange} for copy-as-curl and the under-the-hood panel.
 *
 * `fenced` is the typed surfacing of the protocol's 409 cases: a "FENCED" ack/
 * release/callback (stale generation/wake/token, or deleted subscription) or an
 * "ALREADY_CLAIMED" claim. `errorCode` is the wire code when the server sent an
 * `{"error":{"code":…}}` envelope. `value` is the parsed body for ops that return
 * one (a {@link Subscription} from create/get, a {@link WakeClaim} from claim, an
 * {@link AckResult} from ack/callback); null for 204 ops and failures.
 */
export interface SubscriptionResult<T> {
	/** True when the server returned a 2xx. */
	readonly ok: boolean;
	/** The parsed body for this op, or null (204 / failure / unparseable). */
	readonly value: T | null;
	/** True when the op was rejected by fencing (409 FENCED / ALREADY_CLAIMED). */
	readonly fenced: boolean;
	/** True when the subscription no longer exists (410 SUBSCRIPTION_GONE) — terminal. */
	readonly gone: boolean;
	/**
	 * A fresh token the server rolled, when present: the `token` field of a 2xx
	 * ack/callback body (near-expiry rotation) or the retry token returned with a
	 * 401 TOKEN_EXPIRED. Null when the server sent none. The store adopts it so
	 * subsequent heartbeats/acks/release use the current token instead of locking out.
	 */
	readonly refreshedToken: string | null;
	/** The wire error code from an {"error":{"code":…}} body, when present. */
	readonly errorCode: string | null;
	/** A short human error, present when ok is false. */
	readonly error: string | null;
	/** The operation descriptor that was sent (for the curl helper). */
	readonly operation: Operation;
	/** The captured HTTP exchange (for the protocol disclosure). */
	readonly exchange: HttpExchange;
}

/* ----------------------------------------------------------------------------
 * Metrics (Prometheus text-exposition parsing)
 *
 * The server exposes Prometheus metrics on a separate listener (the
 * --metrics-listen address, e.g. :9090) at GET /metrics, in the text exposition
 * format (Content-Type: text/plain; version=0.0.4). lib/metrics.ts parses that
 * text into the typed snapshot below; the client just fetches the raw text.
 * ------------------------------------------------------------------------- */

/** The metric type as declared by a `# TYPE name kind` comment line. */
export type MetricType = "counter" | "gauge" | "histogram" | "summary" | "untyped";

/** One parsed sample: a metric name, its label set, and a numeric value. */
export interface MetricSample {
	/**
	 * The base metric name (without the _bucket/_sum/_count suffix a histogram or
	 * summary series carries). e.g. "chronicle_wake_delivery_seconds".
	 */
	readonly name: string;
	/**
	 * The full series name as it appeared on the line, including any suffix
	 * (e.g. "chronicle_wake_delivery_seconds_bucket"). Distinguishes the
	 * component series of a histogram/summary family.
	 */
	readonly series: string;
	/** Label set as a plain record (le / quantile included verbatim). */
	readonly labels: Readonly<Record<string, string>>;
	/** The parsed numeric value (NaN / +Inf / -Inf are preserved). */
	readonly value: number;
}

/** A metric family: its declared HELP/TYPE plus every parsed sample. */
export interface Metric {
	/** The base metric name. */
	readonly name: string;
	/** Declared type from the `# TYPE` line, or "untyped" when none was given. */
	readonly type: MetricType;
	/** The HELP text from the `# HELP` line, or null. */
	readonly help: string | null;
	/** All samples belonging to this family, in document order. */
	readonly samples: readonly MetricSample[];
}

/** A parsed Prometheus exposition document: metric families keyed by name. */
export interface MetricsSnapshot {
	/** Metric families in first-seen order. */
	readonly metrics: readonly Metric[];
	/** Wall-clock ms when the snapshot was parsed. */
	readonly parsedAt: number;
}

/* ----------------------------------------------------------------------------
 * Wake monitor (webhook capture + pull-wake wake events)
 *
 * The browser cannot receive an inbound webhook, so for a "webhook" subscription
 * the dsui binary hosts a capture endpoint that chronicle POSTs signed wakes to;
 * the binary relays each one to the browser over SSE as a {@link CaptureDelivery}.
 * For a "pull-wake" subscription there is no webhook — wakes are durable events
 * appended to the subscription's wake_stream, which the UI tails (reusing the
 * live-tail machinery) and decodes into {@link WakeEvent}s.
 *
 * These shapes are dependency-free contracts; the parsing lives in lib/wakes.ts.
 * ------------------------------------------------------------------------- */

/**
 * One captured webhook delivery as the dsui binary relays it over SSE (mirrors
 * the Go `Delivery` struct in cmd/dsui/capture.go). The {@link body} is the
 * exact raw bytes the server POSTed, kept verbatim so signature verification
 * (which is over the unmodified body) stays honest.
 */
export interface CaptureDelivery {
	/** Monotonic per-bucket sequence number (1-based), stamped on receipt. */
	readonly seq: number;
	/** Capture time in Unix milliseconds. */
	readonly receivedAt: number;
	/** HTTP method of the delivery (normally POST). */
	readonly method: string;
	/** The raw Webhook-Signature header value, or "" when none was sent. */
	readonly signature: string;
	/** The delivery's Content-Type header (normally application/json). */
	readonly contentType: string;
	/** Full request headers, each value joined by ", ". */
	readonly headers: Readonly<Record<string, string>>;
	/** The exact raw request body bytes as a string. */
	readonly body: string;
}

/**
 * The decoded `Webhook-Signature` header: `t=<ts>,kid=<id>,ed25519=<sig>`. Any
 * part may be absent (a delivery may carry no signature at all); the UI shows
 * what is present without verifying — it is a display + honesty aid, and full
 * Ed25519 verification needs the JWKS, which the UI links to rather than embeds.
 */
export interface WebhookSignatureParts {
	/** The `t=` Unix timestamp (seconds), or null when absent / unparseable. */
	readonly timestamp: number | null;
	/** The `kid=` key id identifying the JWKS key, or null when absent. */
	readonly kid: string | null;
	/** The unpadded base64url Ed25519 signature, or null when absent. */
	readonly ed25519: string | null;
	/** The raw header value, kept verbatim for display. */
	readonly raw: string;
}

/**
 * A decoded wake notification (the JSON body of a webhook delivery; mirrors the
 * server's WakeNotification). Carries the fencing fields, the per-stream
 * snapshots, and the callback target the receiver acks to. Parsed leniently:
 * missing optional fields degrade rather than throw.
 */
export interface WakeNotification {
	readonly subscriptionId: string;
	readonly wakeId: string;
	readonly generation: number;
	readonly streams: readonly WakeStreamSnapshot[];
	/** Absolute URL to POST the callback ack to, or null when absent. */
	readonly callbackUrl: string | null;
	/** Bearer token for the callback, or null when absent. */
	readonly callbackToken: string | null;
}

/**
 * A pull-wake wake event, as appended to the subscription's wake_stream (mirrors
 * the server's wake event: `{type:"wake", subscription_id, stream, generation,
 * ts}`). Parsed from a tailed grid row's decoded JSON value.
 */
export interface WakeEvent {
	readonly subscriptionId: string;
	/** Stream-root-relative path of the stream with pending data. */
	readonly stream: string;
	readonly generation: number;
	/** Unix timestamp in milliseconds. */
	readonly ts: number;
}
