/**
 * A fully-typed, fetch-based client for a single Durable Streams (chronicle)
 * server. The only network primitive used is the platform `fetch` — no axios,
 * no wrappers. Every request captures an {@link HttpExchange} so the UI's
 * "Under the hood" protocol disclosure can show exactly what went over the wire.
 *
 * The client is deliberately robust: 404, empty bodies, malformed JSON, and
 * network/CORS failures all resolve to typed results rather than throwing.
 */

import {
	jsonByteSize,
	kindFromContentType,
	parseJsonArray,
	parseRegistryBody,
	previewOf,
	reduceRegistry,
} from "./guards";
import type {
	AppendOptions,
	Connection,
	ConnectionProbe,
	CreateStreamOptions,
	GridRow,
	HttpExchange,
	Operation,
	ProducerConflict,
	ProtocolHeaders,
	ReadResult,
	StreamInfo,
	TailBatch,
	TailStatus,
	TailStopper,
	WriteResult,
} from "./types";

/** The registry stream path used for discovery (no native list-all endpoint). */
export const REGISTRY_PATH = "__registry__";

/** Build the absolute URL for a stream path under a connection. */
export function streamUrl(conn: Connection, path: string, offset?: string): string {
	const cleanPath = path.replace(/^\/+/, "");
	const base = `${conn.baseUrl}${conn.streamRoot}/${encodeStreamPath(cleanPath)}`;
	if (offset === undefined) return base;
	const u = new URL(base);
	u.searchParams.set("offset", offset);
	return u.toString();
}

/** Encode each path segment but keep slashes as path separators. */
function encodeStreamPath(path: string): string {
	return path
		.split("/")
		.map((seg) => encodeURIComponent(seg))
		.join("/");
}

/** Flatten a Headers object into a plain, lowercased-key record. */
function headersToRecord(headers: Headers): Record<string, string> {
	const out: Record<string, string> = {};
	headers.forEach((value, key) => {
		out[key.toLowerCase()] = value;
	});
	return out;
}

/** Extract the protocol-significant headers from a response. */
function protocolHeaders(headers: Headers): ProtocolHeaders {
	return {
		streamNextOffset: headers.get("Stream-Next-Offset"),
		streamClosed: headers.get("Stream-Closed"),
		streamUpToDate: headers.get("Stream-Up-To-Date"),
		etag: headers.get("ETag"),
		contentType: headers.get("Content-Type"),
	};
}

/** Interpret a protocol boolean-ish header ("true"/"1"/present) loosely. */
function headerTrue(value: string | null): boolean {
	if (value === null) return false;
	const v = value.trim().toLowerCase();
	return v === "true" || v === "1" || v === "yes";
}

/** Parse an integer header value, or null if absent / not a number. */
function headerInt(value: string | null): number | null {
	if (value === null) return null;
	const n = Number.parseInt(value.trim(), 10);
	return Number.isFinite(n) ? n : null;
}

/** Build the URL for a stream live-tail read (?offset=…&live=mode). */
function tailUrl(
	conn: Connection,
	path: string,
	offset: string,
	live: "long-poll" | "sse",
): string {
	const u = new URL(streamUrl(conn, path, offset));
	u.searchParams.set("live", live);
	return u.toString();
}

/**
 * Extract producer-conflict detail from a response, or null when the response
 * did not report one. The server surfaces Producer-Expected-Seq /
 * Producer-Received-Seq when an append's producer sequence did not match.
 */
function producerConflictFrom(headers: Headers): ProducerConflict | null {
	const expected = headers.get("Producer-Expected-Seq");
	const received = headers.get("Producer-Received-Seq");
	if (expected === null && received === null) return null;
	return { expectedSeq: headerInt(expected), receivedSeq: headerInt(received) };
}

/** A short human error for a non-2xx write response. */
function writeErrorLabel(status: number, statusText: string): string {
	const text = statusText.trim();
	if (status === 0) return "network request failed";
	return text === "" ? `request failed (${status})` : `${status} ${text}`;
}

interface ExchangeInit {
	readonly method: string;
	readonly url: string;
	readonly requestHeaders: Record<string, string>;
}

/** Build a captured exchange from a completed response. */
function exchangeFromResponse(init: ExchangeInit, res: Response, startedAt: number): HttpExchange {
	const responseHeaders = headersToRecord(res.headers);
	return {
		method: init.method,
		url: init.url,
		requestHeaders: init.requestHeaders,
		status: res.status,
		statusText: res.statusText,
		responseHeaders,
		protocol: protocolHeaders(res.headers),
		at: Date.now(),
		durationMs: Math.round(performance.now() - startedAt),
	};
}

/** Build a captured exchange for a request that never produced a response. */
function exchangeFromError(init: ExchangeInit, startedAt: number, error: unknown): HttpExchange {
	return {
		method: init.method,
		url: init.url,
		requestHeaders: init.requestHeaders,
		status: 0,
		statusText: "",
		responseHeaders: {},
		protocol: {
			streamNextOffset: null,
			streamClosed: null,
			streamUpToDate: null,
			etag: null,
			contentType: null,
		},
		at: Date.now(),
		durationMs: Math.round(performance.now() - startedAt),
		error: error instanceof Error ? error.message : "network request failed",
	};
}

/** A response plus its captured exchange, or an exchange-only failure. */
interface FetchOutcome {
	readonly exchange: HttpExchange;
	readonly response: Response | null;
}

/** Header description used when capturing requests. CORS is open server-side. */
const ACCEPT_HEADER = { Accept: "*/*" } as const;

/** A typed client bound to one {@link Connection}. */
export interface DsClient {
	readonly connection: Connection;
	testConnection(signal?: AbortSignal): Promise<ConnectionProbe>;
	listStreams(signal?: AbortSignal): Promise<{ streams: StreamInfo[]; exchange: HttpExchange }>;
	readStream(path: string, offset: string, signal?: AbortSignal): Promise<ReadResult>;
	headStream(path: string, signal?: AbortSignal): Promise<HttpExchange>;

	/* ---- Write / fork / lifecycle (all resolve to a typed WriteResult) ---- */

	/** Create a stream (PUT). When opts.fork is set, this is a fork CREATE. */
	createStream(opts: CreateStreamOptions, signal?: AbortSignal): Promise<WriteResult>;
	/** Append/publish to a stream (POST). Sets Producer-* / Stream-Closed if given. */
	appendMessages(path: string, opts: AppendOptions, signal?: AbortSignal): Promise<WriteResult>;
	/** Close a stream (POST with Stream-Closed: true and an empty body). */
	closeStream(path: string, signal?: AbortSignal): Promise<WriteResult>;
	/** Delete a stream (DELETE). Soft-deletes if forks exist, else hard delete. */
	deleteStream(path: string, signal?: AbortSignal): Promise<WriteResult>;
	/** Fork a stream into newPath at the given source offset (a fork CREATE). */
	forkStream(
		newPath: string,
		fromPath: string,
		offset: string,
		subOffset?: number,
		signal?: AbortSignal,
	): Promise<WriteResult>;

	/* ---- Live tail (each returns a stopper; never throws to the caller) --- */

	/**
	 * Follow a stream by long-polling. Loops GET …&live=long-poll, honoring
	 * Stream-Next-Offset (to advance) and Stream-Up-To-Date (to re-poll at the
	 * tail), with backoff on transient failures and an internal AbortController.
	 * Returns a stopper that aborts the loop and stops emitting.
	 */
	openLongPoll(
		path: string,
		fromOffset: string,
		onBatch: (batch: TailBatch) => void,
		onState: (status: TailStatus) => void,
	): TailStopper;

	/**
	 * Follow a stream over Server-Sent Events. Opens an EventSource on
	 * …&live=sse, decoding each event into a row, with reconnect handling on
	 * error. Returns a stopper that closes the EventSource.
	 */
	openSse(
		path: string,
		fromOffset: string,
		onMessage: (batch: TailBatch) => void,
		onState: (status: TailStatus) => void,
	): TailStopper;
}

/** Create a {@link DsClient} for the given connection. */
export function createClient(connection: Connection): DsClient {
	async function doFetch(
		method: string,
		url: string,
		requestHeaders: Record<string, string>,
		signal: AbortSignal | undefined,
		body?: string | Uint8Array,
	): Promise<FetchOutcome> {
		const startedAt = performance.now();
		const init: ExchangeInit = { method, url, requestHeaders };
		try {
			const requestInit: RequestInit = { method, headers: requestHeaders };
			if (signal !== undefined) requestInit.signal = signal;
			// A Uint8Array is a valid runtime BodyInit; the DOM lib types are narrower.
			if (body !== undefined) requestInit.body = body as BodyInit;
			const response = await fetch(url, requestInit);
			return { exchange: exchangeFromResponse(init, response, startedAt), response };
		} catch (err) {
			return { exchange: exchangeFromError(init, startedAt, err), response: null };
		}
	}

	/**
	 * Run a single write-style request and shape it into a {@link WriteResult}.
	 * Builds the matching {@link Operation} descriptor (for the curl helper) and
	 * never throws — every failure (4xx/5xx/network) becomes ok:false.
	 */
	async function doWrite(
		method: string,
		url: string,
		headers: Record<string, string>,
		signal: AbortSignal | undefined,
		body?: string | Uint8Array,
	): Promise<WriteResult> {
		const operation: Operation =
			body === undefined ? { method, url, headers } : { method, url, headers, body };
		const { exchange, response } = await doFetch(method, url, headers, signal, body);
		if (response === null) {
			return {
				ok: false,
				nextOffset: null,
				location: null,
				conflict: null,
				error: exchange.error ?? "network request failed",
				operation,
				exchange,
			};
		}
		const conflict = producerConflictFrom(response.headers);
		return {
			ok: response.ok,
			nextOffset: exchange.protocol.streamNextOffset,
			location: response.headers.get("Location"),
			conflict,
			error: response.ok ? null : writeErrorLabel(response.status, response.statusText),
			operation,
			exchange,
		};
	}

	/** Create (or fork) a stream via PUT. Shared by createStream + forkStream. */
	function createStream(opts: CreateStreamOptions, signal?: AbortSignal): Promise<WriteResult> {
		const url = streamUrl(connection, opts.path);
		const headers = createHeaders(opts);
		return doWrite("PUT", url, headers, signal);
	}

	return {
		connection,

		async testConnection(signal) {
			// Probe with a HEAD against the registry stream. Any HTTP response
			// (even 404 — registry may not exist yet) means the server is up.
			const url = streamUrl(connection, REGISTRY_PATH);
			const { exchange, response } = await doFetch("HEAD", url, { ...ACCEPT_HEADER }, signal);
			if (response === null) {
				const probe: ConnectionProbe = {
					ok: false,
					status: 0,
					latencyMs: exchange.durationMs,
					error: exchange.error ?? "unreachable",
				};
				return probe;
			}
			return { ok: true, status: response.status, latencyMs: exchange.durationMs };
		},

		async listStreams(signal) {
			const url = streamUrl(connection, REGISTRY_PATH, "-1");
			const { exchange, response } = await doFetch("GET", url, { ...ACCEPT_HEADER }, signal);

			// No response, or registry absent/empty: honest empty tree.
			if (response === null || response.status === 404 || response.status === 204) {
				return { streams: [], exchange };
			}
			if (!response.ok) {
				return { streams: [], exchange };
			}

			const body = await safeText(response);
			const events = parseRegistryBody(body);
			const current = reduceRegistry(events);

			const streams: StreamInfo[] = [];
			for (const ev of current.values()) {
				streams.push({
					path: ev.path,
					contentType: ev.contentType,
					kind: kindFromContentType(ev.contentType),
					createdAt: ev.createdAt,
					manual: false,
				});
			}
			streams.sort((a, b) => a.path.localeCompare(b.path));
			return { streams, exchange };
		},

		async readStream(path, offset, signal) {
			const url = streamUrl(connection, path, offset);
			const { exchange, response } = await doFetch("GET", url, { ...ACCEPT_HEADER }, signal);

			const kind = kindFromContentType(exchange.protocol.contentType);
			const base = {
				path,
				kind,
				requestedOffset: offset,
				nextOffset: exchange.protocol.streamNextOffset,
				closed: headerTrue(exchange.protocol.streamClosed),
				upToDate: headerTrue(exchange.protocol.streamUpToDate),
			} as const;

			if (response === null || !response.ok) {
				const empty: ReadResult = {
					...base,
					rows: [],
					rawBytes: new Uint8Array(0),
					exchange,
				};
				return empty;
			}

			const buffer = await safeArrayBuffer(response);
			const rawBytes = new Uint8Array(buffer);
			const rows = decodeRows(kind, rawBytes);

			const result: ReadResult = { ...base, rows, rawBytes, exchange };
			return result;
		},

		async headStream(path, signal) {
			const url = streamUrl(connection, path);
			const { exchange } = await doFetch("HEAD", url, { ...ACCEPT_HEADER }, signal);
			return exchange;
		},

		createStream,

		async appendMessages(path, opts, signal) {
			const url = streamUrl(connection, path);
			const headers = appendHeaders(opts);
			return doWrite("POST", url, headers, signal, opts.body);
		},

		async closeStream(path, signal) {
			const url = streamUrl(connection, path);
			// A POST that only carries Stream-Closed: true and an empty body.
			const headers: Record<string, string> = { ...ACCEPT_HEADER, "Stream-Closed": "true" };
			return doWrite("POST", url, headers, signal, "");
		},

		async deleteStream(path, signal) {
			const url = streamUrl(connection, path);
			return doWrite("DELETE", url, { ...ACCEPT_HEADER }, signal);
		},

		forkStream(newPath, fromPath, offset, subOffset, signal) {
			const fork = subOffset === undefined ? { fromPath, offset } : { fromPath, offset, subOffset };
			return createStream({ path: newPath, contentType: "application/octet-stream", fork }, signal);
		},

		openLongPoll(path, fromOffset, onBatch, onState) {
			return runLongPoll(connection, path, fromOffset, onBatch, onState, doFetch);
		},

		openSse(path, fromOffset, onMessage, onState) {
			return runSse(connection, path, fromOffset, onMessage, onState);
		},
	};
}

/* ----------------------------------------------------------------------------
 * Write header builders (pure given the inputs)
 * ------------------------------------------------------------------------- */

/** Build the PUT request headers for a CREATE / FORK from its options. */
function createHeaders(opts: CreateStreamOptions): Record<string, string> {
	const headers: Record<string, string> = {
		...ACCEPT_HEADER,
		"Content-Type": opts.contentType,
	};
	if (opts.ttl !== undefined) headers["Stream-TTL"] = opts.ttl;
	if (opts.expiresAt !== undefined) headers["Stream-Expires-At"] = opts.expiresAt;
	if (opts.closed === true) headers["Stream-Closed"] = "true";
	if (opts.fork !== undefined) {
		headers["Stream-Forked-From"] = opts.fork.fromPath;
		headers["Stream-Fork-Offset"] = opts.fork.offset;
		if (opts.fork.subOffset !== undefined) {
			headers["Stream-Fork-Sub-Offset"] = String(opts.fork.subOffset);
		}
	}
	return headers;
}

/** Build the POST request headers for an APPEND from its options. */
function appendHeaders(opts: AppendOptions): Record<string, string> {
	const headers: Record<string, string> = { ...ACCEPT_HEADER };
	if (opts.contentType !== undefined) headers["Content-Type"] = opts.contentType;
	if (opts.producer !== undefined) {
		headers["Producer-Id"] = opts.producer.id;
		headers["Producer-Epoch"] = String(opts.producer.epoch);
		headers["Producer-Seq"] = String(opts.producer.seq);
	}
	if (opts.closeAfter === true) headers["Stream-Closed"] = "true";
	return headers;
}

/* ----------------------------------------------------------------------------
 * Live tail — long-poll loop
 *
 * A long-poll read blocks up to the server's timeout and returns either new
 * data (advance via Stream-Next-Offset) or, at the tail, Stream-Up-To-Date with
 * the same offset (re-poll). On a transient failure we back off and retry; a
 * closed stream ends the loop. The loop owns an AbortController so the returned
 * stopper can cut an in-flight poll immediately.
 * ------------------------------------------------------------------------- */

/** Backoff schedule (ms) for reconnect attempts; caps at the last entry. */
const BACKOFF_MS: readonly number[] = [500, 1000, 2000, 4000, 8000];

function backoffFor(attempt: number): number {
	const idx = Math.min(attempt, BACKOFF_MS.length - 1);
	return BACKOFF_MS[idx] ?? 8000;
}

type DoFetchFn = (
	method: string,
	url: string,
	headers: Record<string, string>,
	signal: AbortSignal | undefined,
	body?: string | Uint8Array,
) => Promise<FetchOutcome>;

function runLongPoll(
	conn: Connection,
	path: string,
	fromOffset: string,
	onBatch: (batch: TailBatch) => void,
	onState: (status: TailStatus) => void,
	doFetch: DoFetchFn,
): TailStopper {
	const controller = new AbortController();
	let stopped = false;
	let attempt = 0;
	let offset = fromOffset;

	function stop(): void {
		if (stopped) return;
		stopped = true;
		controller.abort();
		onState({ state: "idle" });
	}

	async function loop(): Promise<void> {
		onState({ state: "connecting" });
		while (!stopped) {
			const url = tailUrl(conn, path, offset, "long-poll");
			const { exchange, response } = await doFetch(
				"GET",
				url,
				{ ...ACCEPT_HEADER },
				controller.signal,
			);
			if (stopped) return;

			if (response === null || !response.ok) {
				// Network error or non-2xx: back off and retry (unless aborted).
				if (exchange.status === 0 && controller.signal.aborted) return;
				attempt += 1;
				const reason = exchange.error ?? writeErrorLabel(exchange.status, exchange.statusText);
				onState({ state: "reconnecting", attempt, reason });
				await delay(backoffFor(attempt), controller.signal);
				continue;
			}

			attempt = 0;
			const kind = kindFromContentType(exchange.protocol.contentType);
			const buffer = await safeArrayBuffer(response);
			const rows = decodeRows(kind, new Uint8Array(buffer));
			const next = exchange.protocol.streamNextOffset;
			const upToDate = headerTrue(exchange.protocol.streamUpToDate);
			const closed = headerTrue(exchange.protocol.streamClosed);

			if (rows.length > 0) {
				onBatch({ rows, nextOffset: next, upToDate, exchange });
			}
			if (next !== null) offset = next;

			if (closed) {
				onState({ state: "closed" });
				return;
			}
			onState({ state: "live", atOffset: offset });
			// At the tail (or no advance), avoid hammering: re-poll lets the
			// server's long-poll block; a tiny yield prevents a hot loop if the
			// server returns immediately with no advance.
			if (upToDate || next === null) {
				await delay(50, controller.signal);
			}
		}
	}

	void loop();
	return stop;
}

/** Resolve after ms, or immediately if the signal aborts. Never rejects. */
function delay(ms: number, signal: AbortSignal): Promise<void> {
	return new Promise((resolve) => {
		if (signal.aborted) {
			resolve();
			return;
		}
		const id = globalThis.setTimeout(() => {
			signal.removeEventListener("abort", onAbort);
			resolve();
		}, ms);
		function onAbort(): void {
			globalThis.clearTimeout(id);
			resolve();
		}
		signal.addEventListener("abort", onAbort, { once: true });
	});
}

/* ----------------------------------------------------------------------------
 * Live tail — Server-Sent Events
 *
 * The browser EventSource carries offset + live=sse on the URL (no custom
 * headers needed). Each `message` event is decoded as one row using the
 * stream's kind, which we don't know up front, so SSE rows are decoded as text
 * unless the data parses as JSON. EventSource auto-reconnects; we surface that
 * as a "reconnecting" status and tear down on stop.
 * ------------------------------------------------------------------------- */

function runSse(
	conn: Connection,
	path: string,
	fromOffset: string,
	onMessage: (batch: TailBatch) => void,
	onState: (status: TailStatus) => void,
): TailStopper {
	const url = tailUrl(conn, path, fromOffset, "sse");
	let stopped = false;
	let opened = false;

	const EventSourceCtor = globalThis.EventSource;
	if (EventSourceCtor === undefined) {
		onState({ state: "error", message: "EventSource is not available in this environment" });
		return () => {};
	}

	onState({ state: "connecting" });
	const es = new EventSourceCtor(url);

	es.onopen = (): void => {
		if (stopped) return;
		opened = true;
		onState({ state: "live", atOffset: null });
	};

	es.onmessage = (ev: MessageEvent<string>): void => {
		if (stopped) return;
		const row = decodeSseEvent(ev.data, ev.lastEventId);
		onMessage({
			rows: [row],
			nextOffset: nonEmptyOrNull(ev.lastEventId),
			upToDate: false,
			exchange: null,
		});
	};

	es.onerror = (): void => {
		if (stopped) return;
		// EventSource reconnects on its own while readyState is CONNECTING.
		if (es.readyState === EventSourceCtor.CLOSED) {
			onState({ state: "error", message: "the SSE connection closed" });
		} else {
			onState({
				state: "reconnecting",
				attempt: 1,
				reason: opened ? "connection dropped" : "could not connect",
			});
		}
	};

	return (): void => {
		if (stopped) return;
		stopped = true;
		es.close();
		onState({ state: "idle" });
	};
}

/** Decode one SSE event payload into a grid row (JSON if it parses, else text). */
function decodeSseEvent(data: string, lastEventId: string): GridRow {
	const parsed = parseJsonArray(data);
	if (parsed.ok && parsed.value.length === 1) {
		const el = parsed.value[0];
		return {
			index: 0,
			byteSize: jsonByteSize(el),
			preview: previewOf(el),
			kind: "json",
			value: el,
		};
	}
	void lastEventId;
	return {
		index: 0,
		byteSize: new Blob([data]).size,
		preview: previewOf(data),
		kind: "text",
		value: data,
	};
}

/** Trim a string and return null when empty, for optional id headers. */
function nonEmptyOrNull(s: string): string | null {
	const t = s.trim();
	return t === "" ? null : t;
}

/** Decode raw response bytes into grid rows according to the stream kind. */
function decodeRows(kind: "json" | "text" | "binary", bytes: Uint8Array): GridRow[] {
	if (bytes.byteLength === 0) return [];

	if (kind === "json") {
		const text = new TextDecoder().decode(bytes);
		const parsed = parseJsonArray(text);
		if (!parsed.ok) {
			// Malformed JSON: fall back to a single text row so nothing is lost.
			return [textRow(text)];
		}
		return parsed.value.map((el, index) => ({
			index,
			byteSize: jsonByteSize(el),
			preview: previewOf(el),
			kind: "json" as const,
			value: el,
		}));
	}

	if (kind === "text") {
		const text = new TextDecoder().decode(bytes);
		return [textRow(text)];
	}

	// binary: one row, value is the raw bytes (inspector renders hex).
	return [
		{
			index: 0,
			byteSize: bytes.byteLength,
			preview: `<binary ${bytes.byteLength} bytes>`,
			kind: "binary" as const,
			value: bytes,
		},
	];
}

function textRow(text: string): GridRow {
	return {
		index: 0,
		byteSize: new Blob([text]).size,
		preview: previewOf(text),
		kind: "text",
		value: text,
	};
}

/** Read a response body as text, swallowing read errors to "". */
async function safeText(response: Response): Promise<string> {
	try {
		return await response.text();
	} catch {
		return "";
	}
}

/** Read a response body as bytes, swallowing read errors to an empty buffer. */
async function safeArrayBuffer(response: Response): Promise<ArrayBuffer> {
	try {
		return await response.arrayBuffer();
	} catch {
		return new ArrayBuffer(0);
	}
}
