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
import { parseMetrics } from "./metrics";
import {
	buildAckBody,
	buildCreateBody,
	parseAckResult,
	parseErrorCode,
	parseSubscription,
	parseWakeClaim,
} from "./subscriptions";
import type {
	AckResult,
	AppendOptions,
	Connection,
	ConnectionProbe,
	CreateStreamOptions,
	CreateSubscriptionOptions,
	GridRow,
	HttpExchange,
	MetricsSnapshot,
	Operation,
	ProducerConflict,
	ProtocolHeaders,
	ReadResult,
	StreamInfo,
	Subscription,
	SubscriptionResult,
	TailBatch,
	TailStatus,
	TailStopper,
	WakeClaim,
	WriteResult,
} from "./types";

/** The reserved control-plane prefix for subscriptions (no streamRoot). */
export const SUBSCRIPTIONS_PREFIX = "/__ds/subscriptions";

/** The well-known JWKS path for webhook-signature verification (no streamRoot). */
export const JWKS_PATH = "/__ds/jwks.json";

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

/**
 * Build the absolute URL for a subscription control-plane endpoint. Per the
 * Durable Streams spec the reserved `__ds` prefix is STREAM-ROOT-RELATIVE
 * (`{stream-url}/__ds/subscriptions/:id`), so it is built as
 * baseUrl + streamRoot + the fixed prefix — the same shape as a stream URL, not
 * the bare origin. `suffix` is an already-shaped tail such as "/streams",
 * "/claim", "/ack", "/release", "/callback", "/wake_stream", or
 * "/streams/<encoded-path>".
 */
export function subscriptionUrl(conn: Connection, id: string, suffix = ""): string {
	return `${conn.baseUrl}${conn.streamRoot}${SUBSCRIPTIONS_PREFIX}/${encodeURIComponent(id)}${suffix}`;
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
 * Build a {@link TailUrlFor} for a pull-wake subscription's wake_stream. It is on
 * the reserved /__ds/* surface (…/subscriptions/{id}/wake_stream), so the URL is
 * built from {@link subscriptionUrl} rather than streamUrl, then carries the same
 * ?offset=…&live=… query the stream tail uses.
 */
function wakeStreamUrlFor(
	conn: Connection,
	id: string,
): (offset: string, live: "long-poll" | "sse") => string {
	const base = subscriptionUrl(conn, id, "/wake_stream");
	return (offset, live) => {
		const u = new URL(base);
		u.searchParams.set("offset", offset);
		u.searchParams.set("live", live);
		return u.toString();
	};
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

	/**
	 * Append a discovery event to the registry stream (__registry__) so the
	 * navigator surfaces (operation "upsert") or drops (operation "deleted") a
	 * stream. The server exposes no stream-listing API, so the client maintains
	 * this convention stream. Best-effort; resolves to a WriteResult, never throws.
	 */
	writeRegistryEvent(
		path: string,
		contentType: string | null,
		operation: "upsert" | "deleted",
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

	/* ---- Subscription control plane (reserved /__ds/* surface) ----------
	 *
	 * Each resolves to a typed {@link SubscriptionResult}, never throwing. The
	 * protocol's 409 cases are surfaced typed: a CONFIG_CONFLICT on create, an
	 * ALREADY_CLAIMED on claim, or a FENCED on ack/release/callback all set
	 * `fenced`/`errorCode` rather than looking like an opaque failure. There is no
	 * list-all endpoint — the store tracks known ids client-side.
	 */

	/** Create or re-confirm a subscription (PUT). 201 new / 200 match / 409 conflict. */
	createSubscription(
		opts: CreateSubscriptionOptions,
		signal?: AbortSignal,
	): Promise<SubscriptionResult<Subscription>>;
	/** Fetch a subscription's current view (GET). 404 → ok:false, value:null. */
	getSubscription(id: string, signal?: AbortSignal): Promise<SubscriptionResult<Subscription>>;
	/** Tombstone a subscription (DELETE). 204 → ok:true, value:null. */
	deleteSubscription(id: string, signal?: AbortSignal): Promise<SubscriptionResult<null>>;
	/** Add explicit stream links to a subscription (POST …/streams). 204. */
	addSubscriptionStreams(
		id: string,
		streams: readonly string[],
		signal?: AbortSignal,
	): Promise<SubscriptionResult<null>>;
	/** Remove an explicit stream link (DELETE …/streams/{path}). 204, idempotent. */
	removeSubscriptionStream(
		id: string,
		path: string,
		signal?: AbortSignal,
	): Promise<SubscriptionResult<null>>;

	/**
	 * Tail a pull-wake subscription's wake_stream by long-polling. The wake_stream
	 * lives on the reserved /__ds/* surface (GET …/subscriptions/{id}/wake_stream),
	 * NOT under streamRoot, so it has its own opener; the loop machinery (offset
	 * advance, backoff, decode) is shared with {@link openLongPoll}. Each row is a
	 * wake event JSON object the caller decodes via lib/wakes.
	 */
	openWakeStreamLongPoll(
		id: string,
		fromOffset: string,
		onBatch: (batch: TailBatch) => void,
		onState: (status: TailStatus) => void,
	): TailStopper;

	/** Tail a subscription's wake_stream over SSE (the /__ds/* analogue of openSse). */
	openWakeStreamSse(
		id: string,
		fromOffset: string,
		onMessage: (batch: TailBatch) => void,
		onState: (status: TailStatus) => void,
	): TailStopper;

	/* ---- Pull-wake worker plane (claim → ack/heartbeat → release) -------- */

	/** Claim a pull-wake lease (POST …/claim). 200 → WakeClaim; 409 ALREADY_CLAIMED → fenced. */
	claimWake(
		id: string,
		worker: string,
		signal?: AbortSignal,
	): Promise<SubscriptionResult<WakeClaim>>;
	/**
	 * Ack a pull-wake claim (POST …/ack) with the Bearer token from the claim.
	 * `done:true` releases + applies acks; absent/false heartbeat-extends the
	 * lease. 409 FENCED (stale generation/wake/token) → fenced.
	 */
	ackWake(
		id: string,
		token: string,
		req: {
			wakeId: string;
			generation: number;
			acks: readonly { stream: string; offset: string }[];
			done?: boolean;
		},
		signal?: AbortSignal,
	): Promise<SubscriptionResult<AckResult>>;
	/**
	 * Voluntarily release a pull-wake lease without acking (POST …/release) with
	 * the Bearer token. 204 → ok:true; 409 FENCED → fenced.
	 */
	releaseWake(
		id: string,
		token: string,
		req: { wakeId: string; generation: number },
		signal?: AbortSignal,
	): Promise<SubscriptionResult<null>>;

	/**
	 * Ack a WEBHOOK wake on the callback path (POST …/callback) with the
	 * callback_token from the received wake notification. The body shape matches
	 * {@link ackWake} ({wake_id, generation, acks, done}); `done:true` releases the
	 * lease and applies the acks. 200 → AckResult; 409 FENCED (stale token/wake/
	 * generation) → fenced. This is the asynchronous-ack half of the webhook
	 * contract — the receiver calls it instead of returning {"done":true} inline.
	 */
	callbackWake(
		id: string,
		token: string,
		req: {
			wakeId: string;
			generation: number;
			acks: readonly { stream: string; offset: string }[];
			done?: boolean;
		},
		signal?: AbortSignal,
	): Promise<SubscriptionResult<AckResult>>;

	/* ---- Metrics (Prometheus text on the separate --metrics-listen) ----- */

	/**
	 * Fetch + parse the Prometheus metrics document from an explicit metrics URL
	 * (the --metrics-listen address's /metrics, e.g. http://host:9090/metrics).
	 * It is a separate origin from the stream handler, so the URL is passed in
	 * rather than derived. Resolves to the parsed snapshot or null on failure,
	 * always with the captured exchange.
	 */
	fetchMetrics(
		metricsUrl: string,
		signal?: AbortSignal,
	): Promise<{ snapshot: MetricsSnapshot | null; exchange: HttpExchange }>;
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
		const headers = createHeaders(opts, connection.streamRoot);
		return doWrite("PUT", url, headers, signal);
	}

	/**
	 * Run a single subscription control-plane request and shape it into a typed
	 * {@link SubscriptionResult}. Never throws: a network error, a non-2xx, or a
	 * fencing 409 all resolve. `parse` maps the (already-read) response body text
	 * to the typed value on a 2xx; pass `null` for 204 ops. The 409 cases are
	 * surfaced via `fenced` + `errorCode` from the `{"error":{"code":…}}` body.
	 */
	async function doSubscriptionOp<T>(
		method: string,
		url: string,
		headers: Record<string, string>,
		body: string | undefined,
		parse: (bodyText: string) => T | null,
		signal: AbortSignal | undefined,
	): Promise<SubscriptionResult<T>> {
		const operation: Operation =
			body === undefined ? { method, url, headers } : { method, url, headers, body };
		const { exchange, response } = await doFetch(method, url, headers, signal, body);
		if (response === null) {
			return {
				ok: false,
				value: null,
				fenced: false,
				errorCode: null,
				error: exchange.error ?? "network request failed",
				operation,
				exchange,
			};
		}
		const text = await safeText(response);
		if (response.ok) {
			return {
				ok: true,
				value: parse(text),
				fenced: false,
				errorCode: null,
				error: null,
				operation,
				exchange,
			};
		}
		const errorCode = parseErrorCode(jsonOrNull(text));
		// A 409 with FENCED (ack/release/callback) or ALREADY_CLAIMED (claim) is the
		// protocol's fencing signal; surface it typed rather than as a flat failure.
		const fenced =
			response.status === 409 && (errorCode === "FENCED" || errorCode === "ALREADY_CLAIMED");
		return {
			ok: false,
			value: null,
			fenced,
			errorCode,
			error: writeErrorLabel(response.status, response.statusText),
			operation,
			exchange,
		};
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

		async forkStream(newPath, fromPath, offset, subOffset, signal) {
			// HEAD the source once to learn two things the fork must respect:
			//  1. its content type — a fork's type MUST match the source or the
			//     server rejects with 409 ("fork content type does not match source");
			//  2. its current tail offset — the valid fork range is
			//     0 <= forkOffset <= source.CurrentOffset, and the literal "now"
			//     sentinel resolves ABOVE CurrentOffset, so the server rejects it with
			//     400 ("fork offset beyond source stream length"). Resolve "now"/blank
			//     to the concrete Stream-Next-Offset (the tail) so "fork at latest"
			//     inherits everything instead of failing.
			const probe = await doFetch(
				"HEAD",
				streamUrl(connection, fromPath),
				{ ...ACCEPT_HEADER },
				signal,
			);
			const sourceType = probe.exchange.protocol.contentType ?? "application/octet-stream";
			const wantsTail = offset.trim() === "" || offset.trim() === "now";
			const resolvedOffset =
				wantsTail && probe.exchange.protocol.streamNextOffset !== null
					? probe.exchange.protocol.streamNextOffset
					: offset;
			const fork =
				subOffset === undefined
					? { fromPath, offset: resolvedOffset }
					: { fromPath, offset: resolvedOffset, subOffset };
			return createStream({ path: newPath, contentType: sourceType, fork }, signal);
		},

		async writeRegistryEvent(path, contentType, operation, signal) {
			const url = streamUrl(connection, REGISTRY_PATH);
			const headers: Record<string, string> = {
				...ACCEPT_HEADER,
				"Content-Type": "application/json",
			};
			const event = {
				type: "stream",
				key: path,
				value: { path, contentType, createdAt: new Date().toISOString() },
				headers: { operation },
			};
			const body = JSON.stringify([event]);
			// The registry is an ordinary JSON stream maintained client-side. Append
			// the event; if the registry stream does not exist yet, create it (PUT)
			// and retry the append once. Best-effort — a failed write never blocks
			// the original operation that triggered it.
			let result = await doWrite("POST", url, headers, signal, body);
			if (!result.ok) {
				await doWrite("PUT", url, headers, signal);
				result = await doWrite("POST", url, headers, signal, body);
			}
			return result;
		},

		openLongPoll(path, fromOffset, onBatch, onState) {
			const urlFor: TailUrlFor = (offset, live) => tailUrl(connection, path, offset, live);
			return runLongPoll(urlFor, fromOffset, onBatch, onState, doFetch);
		},

		openSse(path, fromOffset, onMessage, onState) {
			const urlFor: TailUrlFor = (offset, live) => tailUrl(connection, path, offset, live);
			return runSse(urlFor, fromOffset, onMessage, onState);
		},

		openWakeStreamLongPoll(id, fromOffset, onBatch, onState) {
			return runLongPoll(wakeStreamUrlFor(connection, id), fromOffset, onBatch, onState, doFetch);
		},

		openWakeStreamSse(id, fromOffset, onMessage, onState) {
			return runSse(wakeStreamUrlFor(connection, id), fromOffset, onMessage, onState);
		},

		/* ---- Subscription control plane ----------------------------------- */

		createSubscription(opts, signal) {
			const url = subscriptionUrl(connection, opts.id);
			const headers: Record<string, string> = {
				...ACCEPT_HEADER,
				"Content-Type": "application/json",
			};
			const body = JSON.stringify(
				buildCreateBody({
					type: opts.type,
					...(opts.pattern !== undefined ? { pattern: opts.pattern } : {}),
					...(opts.streams !== undefined ? { streams: opts.streams } : {}),
					...(opts.webhookUrl !== undefined ? { webhookUrl: opts.webhookUrl } : {}),
					...(opts.wakeStream !== undefined ? { wakeStream: opts.wakeStream } : {}),
					...(opts.leaseTtlMs !== undefined ? { leaseTtlMs: opts.leaseTtlMs } : {}),
					...(opts.description !== undefined ? { description: opts.description } : {}),
				}),
			);
			return doSubscriptionOp(
				"PUT",
				url,
				headers,
				body,
				(t) => parseSubscription(jsonOrNull(t)),
				signal,
			);
		},

		getSubscription(id, signal) {
			const url = subscriptionUrl(connection, id);
			return doSubscriptionOp(
				"GET",
				url,
				{ ...ACCEPT_HEADER },
				undefined,
				(t) => parseSubscription(jsonOrNull(t)),
				signal,
			);
		},

		deleteSubscription(id, signal) {
			const url = subscriptionUrl(connection, id);
			return doSubscriptionOp("DELETE", url, { ...ACCEPT_HEADER }, undefined, () => null, signal);
		},

		addSubscriptionStreams(id, streams, signal) {
			const url = subscriptionUrl(connection, id, "/streams");
			const headers: Record<string, string> = {
				...ACCEPT_HEADER,
				"Content-Type": "application/json",
			};
			const body = JSON.stringify({ streams: [...streams] });
			return doSubscriptionOp("POST", url, headers, body, () => null, signal);
		},

		removeSubscriptionStream(id, path, signal) {
			// The path is the stream-root-relative path, URL-encoded as a single
			// segment after /streams/ (its own slashes are encoded too).
			const clean = path.trim().replace(/^\/+/, "");
			const url = subscriptionUrl(connection, id, `/streams/${encodeURIComponent(clean)}`);
			return doSubscriptionOp("DELETE", url, { ...ACCEPT_HEADER }, undefined, () => null, signal);
		},

		claimWake(id, worker, signal) {
			const url = subscriptionUrl(connection, id, "/claim");
			const headers: Record<string, string> = {
				...ACCEPT_HEADER,
				"Content-Type": "application/json",
			};
			const body = JSON.stringify({ worker });
			return doSubscriptionOp(
				"POST",
				url,
				headers,
				body,
				(t) => parseWakeClaim(jsonOrNull(t)),
				signal,
			);
		},

		ackWake(id, token, req, signal) {
			const url = subscriptionUrl(connection, id, "/ack");
			const headers: Record<string, string> = {
				...ACCEPT_HEADER,
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			};
			const body = JSON.stringify(
				buildAckBody({
					wakeId: req.wakeId,
					generation: req.generation,
					acks: req.acks,
					...(req.done !== undefined ? { done: req.done } : {}),
				}),
			);
			return doSubscriptionOp(
				"POST",
				url,
				headers,
				body,
				(t) => parseAckResult(jsonOrNull(t)),
				signal,
			);
		},

		releaseWake(id, token, req, signal) {
			const url = subscriptionUrl(connection, id, "/release");
			const headers: Record<string, string> = {
				...ACCEPT_HEADER,
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			};
			const body = JSON.stringify({ wake_id: req.wakeId, generation: req.generation });
			return doSubscriptionOp("POST", url, headers, body, () => null, signal);
		},

		callbackWake(id, token, req, signal) {
			// The webhook ack path mirrors …/ack exactly (same body, Bearer token)
			// but POSTs to …/callback, which the server routes to the same fencing.
			const url = subscriptionUrl(connection, id, "/callback");
			const headers: Record<string, string> = {
				...ACCEPT_HEADER,
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			};
			const body = JSON.stringify(
				buildAckBody({
					wakeId: req.wakeId,
					generation: req.generation,
					acks: req.acks,
					...(req.done !== undefined ? { done: req.done } : {}),
				}),
			);
			return doSubscriptionOp(
				"POST",
				url,
				headers,
				body,
				(t) => parseAckResult(jsonOrNull(t)),
				signal,
			);
		},

		/* ---- Metrics ------------------------------------------------------ */

		async fetchMetrics(metricsUrl, signal) {
			const { exchange, response } = await doFetch("GET", metricsUrl, { ...ACCEPT_HEADER }, signal);
			if (response === null || !response.ok) {
				return { snapshot: null, exchange };
			}
			const text = await safeText(response);
			return { snapshot: parseMetrics(text), exchange };
		},
	};
}

/** JSON.parse a body text to unknown, or null when it is empty / not JSON. */
function jsonOrNull(text: string): unknown {
	const trimmed = text.trim();
	if (trimmed === "") return null;
	try {
		return JSON.parse(trimmed) as unknown;
	} catch {
		return null;
	}
}

/* ----------------------------------------------------------------------------
 * Write header builders (pure given the inputs)
 * ------------------------------------------------------------------------- */

/** Build the PUT request headers for a CREATE / FORK from its options. */
function createHeaders(opts: CreateStreamOptions, streamRoot: string): Record<string, string> {
	const headers: Record<string, string> = {
		...ACCEPT_HEADER,
		"Content-Type": opts.contentType,
	};
	if (opts.ttl !== undefined) headers["Stream-TTL"] = opts.ttl;
	if (opts.expiresAt !== undefined) headers["Stream-Expires-At"] = opts.expiresAt;
	if (opts.closed === true) headers["Stream-Closed"] = "true";
	if (opts.fork !== undefined) {
		// Stream-Forked-From must be the source stream's full request path
		// (e.g. /v1/stream/orders), not the bare stream path — the server keys
		// streams by their full path and 404s ("source stream not found") on a
		// bare name.
		const cleanFrom = opts.fork.fromPath.trim().replace(/^\/+/, "");
		headers["Stream-Forked-From"] = `${streamRoot}/${encodeStreamPath(cleanFrom)}`;
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

/**
 * A builder for the live-read URL at a given offset + mode. Stream tails build it
 * via {@link tailUrl}; the subscription wake_stream builds it from the reserved
 * /__ds/* path instead (it is not under streamRoot), so the loop/EventSource
 * machinery is shared by passing the URL builder in.
 */
type TailUrlFor = (offset: string, live: "long-poll" | "sse") => string;

function runLongPoll(
	urlFor: TailUrlFor,
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
			const url = urlFor(offset, "long-poll");
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
	urlFor: TailUrlFor,
	fromOffset: string,
	onMessage: (batch: TailBatch) => void,
	onState: (status: TailStatus) => void,
): TailStopper {
	const url = urlFor(fromOffset, "sse");
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

	// chronicle sends NAMED SSE events: `event: data` (a JSON array batch of
	// messages) and `event: control` (offset / up-to-date metadata). The default
	// `onmessage` only fires for UNNAMED events, so named events were silently
	// dropped — the "tail shows no new messages" bug. Listen by name, and keep
	// onmessage as a fallback for servers that emit unnamed events.
	const onData = (ev: Event): void => {
		if (stopped) return;
		onMessage({
			rows: decodeSseBatch((ev as MessageEvent<string>).data),
			nextOffset: null,
			upToDate: false,
			exchange: null,
		});
	};
	const onControl = (ev: Event): void => {
		if (stopped) return;
		const ctrl = parseSseControl((ev as MessageEvent<string>).data);
		onState({ state: "live", atOffset: ctrl.nextOffset });
	};
	es.addEventListener("data", onData);
	es.addEventListener("control", onControl);
	es.onmessage = (ev: MessageEvent<string>): void => {
		if (stopped) return;
		onMessage({
			rows: decodeSseBatch(ev.data),
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

/**
 * Decode an SSE `data` event payload into grid rows. chronicle frames a batch as
 * a JSON array of messages, so this returns one row per element (mirroring how a
 * catch-up read is decoded); a non-JSON payload becomes a single text row.
 */
function decodeSseBatch(data: string): GridRow[] {
	const parsed = parseJsonArray(data);
	if (parsed.ok) {
		return parsed.value.map((el, i) => ({
			index: i,
			byteSize: jsonByteSize(el),
			preview: previewOf(el),
			kind: "json" as const,
			value: el,
		}));
	}
	return [
		{
			index: 0,
			byteSize: new Blob([data]).size,
			preview: previewOf(data),
			kind: "text" as const,
			value: data,
		},
	];
}

/** Parse an SSE `control` event payload (offset + up-to-date metadata). */
function parseSseControl(data: string): { nextOffset: string | null; upToDate: boolean } {
	try {
		const parsed: unknown = JSON.parse(data);
		if (parsed !== null && typeof parsed === "object") {
			const rec = parsed as Record<string, unknown>;
			return {
				nextOffset: typeof rec.streamNextOffset === "string" ? rec.streamNextOffset : null,
				upToDate: rec.upToDate === true,
			};
		}
	} catch {
		// Malformed control payload — treat as no metadata.
	}
	return { nextOffset: null, upToDate: false };
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
