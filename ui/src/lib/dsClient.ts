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
	Connection,
	ConnectionProbe,
	GridRow,
	HttpExchange,
	ProtocolHeaders,
	ReadResult,
	StreamInfo,
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
}

/** Create a {@link DsClient} for the given connection. */
export function createClient(connection: Connection): DsClient {
	async function doFetch(
		method: string,
		url: string,
		requestHeaders: Record<string, string>,
		signal: AbortSignal | undefined,
	): Promise<FetchOutcome> {
		const startedAt = performance.now();
		const init: ExchangeInit = { method, url, requestHeaders };
		try {
			const requestInit: RequestInit = { method, headers: requestHeaders };
			if (signal !== undefined) requestInit.signal = signal;
			const response = await fetch(url, requestInit);
			return { exchange: exchangeFromResponse(init, response, startedAt), response };
		} catch (err) {
			return { exchange: exchangeFromError(init, startedAt, err), response: null };
		}
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
	};
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
