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
