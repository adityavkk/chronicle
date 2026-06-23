/**
 * Pure helpers backing the "Under the hood" protocol disclosure (Feature 4).
 *
 * The goal of the disclosure is progressive disclosure of the Durable Streams
 * HTTP protocol: show the real exchange that dsClient captured, and — for a
 * beginner — explain in plain language what each protocol-significant header
 * means and how an offset works. None of that explanatory knowledge belongs in
 * a component, so it lives here as data + small pure functions that are trivial
 * to unit-test and reuse (the workspace disclosure and the inspector both read
 * from this module).
 *
 * No DOM, no store, no I/O.
 */

import type { HttpExchange } from "./types";

/**
 * One protocol-significant response header, paired with a short, plain-language
 * explanation aimed at someone meeting Durable Streams for the first time. The
 * `value` is read from a captured {@link HttpExchange.protocol}; `null` means
 * the server did not send it on this response (an honest, expected case).
 */
export interface ProtocolHeaderRow {
	/** The wire header name, exactly as it appears over HTTP. */
	readonly name: string;
	/** The value the server sent, or null when the header was absent. */
	readonly value: string | null;
	/** A one-line plain-language explanation of what this header is for. */
	readonly note: string;
}

/**
 * Build the ordered list of protocol-significant headers for an exchange, each
 * with its explanation. Order is meaningful: the resume cursor first (it is the
 * one a reader acts on), then the two state flags, then caching + typing.
 */
export function protocolHeaderRows(exchange: HttpExchange): readonly ProtocolHeaderRow[] {
	const p = exchange.protocol;
	return [
		{
			name: "Stream-Next-Offset",
			value: p.streamNextOffset,
			note: "The cursor to send as ?offset on your next read to resume exactly where this batch ended. This is how you page forward through a stream.",
		},
		{
			name: "Stream-Up-To-Date",
			value: p.streamUpToDate,
			note: "Present when this read reached the current tail — there is nothing newer to fetch right now.",
		},
		{
			name: "Stream-Closed",
			value: p.streamClosed,
			note: "Present when the stream has been closed and will never receive more data. Reads past the end stay empty.",
		},
		{
			name: "ETag",
			value: p.etag,
			note: "An entity tag identifying this exact response, used for conditional reads and caching.",
		},
		{
			name: "Content-Type",
			value: p.contentType,
			note: "How the body is shaped. application/json means the body is a JSON array of messages; otherwise it is rendered as text or as raw bytes.",
		},
	];
}

/** The protocol-significant header names, lowercased, for set membership. */
const SIGNIFICANT_HEADER_NAMES: ReadonlySet<string> = new Set([
	"stream-next-offset",
	"stream-up-to-date",
	"stream-closed",
	"etag",
	"content-type",
]);

/** True when a response header name is one the protocol disclosure highlights. */
export function isSignificantHeader(name: string): boolean {
	return SIGNIFICANT_HEADER_NAMES.has(name.toLowerCase());
}

/**
 * Split a flat response-header record into the protocol-significant subset and
 * the rest, each sorted by name. Used by the inspector Headers view so the few
 * headers that matter for resuming sort to the top.
 */
export function partitionHeaders(headers: Readonly<Record<string, string>>): {
	readonly significant: readonly (readonly [string, string])[];
	readonly other: readonly (readonly [string, string])[];
} {
	const significant: [string, string][] = [];
	const other: [string, string][] = [];
	for (const [k, v] of Object.entries(headers)) {
		if (isSignificantHeader(k)) significant.push([k, v]);
		else other.push([k, v]);
	}
	const byName = (a: readonly [string, string], b: readonly [string, string]): number =>
		a[0].localeCompare(b[0]);
	significant.sort(byName);
	other.sort(byName);
	return { significant, other };
}

/**
 * A short, human classification of an exchange's status for a status pill:
 * "ok" (1xx–3xx), "err" (4xx/5xx), or "fail" (status 0 — the request never
 * produced a response, e.g. a network or CORS error).
 */
export type ExchangeOutcome = "ok" | "err" | "fail";

/** Classify an exchange for the status pill. */
export function exchangeOutcome(exchange: HttpExchange): ExchangeOutcome {
	if (exchange.status === 0) return "fail";
	return exchange.status < 400 ? "ok" : "err";
}

/** A human label for the status line ("200 OK", "404 Not Found", "network error"). */
export function statusLabel(exchange: HttpExchange): string {
	if (exchange.status === 0) return exchange.error ?? "network error";
	const text = exchange.statusText.trim();
	return text === "" ? String(exchange.status) : `${exchange.status} ${text}`;
}

/**
 * Plain-language primer on what an offset is, tailored to the offset that was
 * actually requested. Special-cases the two reserved cursors so a beginner sees
 * "the beginning" / "the current tail" instead of a cryptic literal.
 */
export function explainOffset(requestedOffset: string): string {
	const o = requestedOffset.trim();
	if (o === "-1") {
		return "Offset -1 is the reserved cursor for the beginning of the stream — this read started from the very first message.";
	}
	if (o === "now") {
		return "Offset now is the reserved cursor for the current tail — this read started from the newest position, skipping past history.";
	}
	return "An offset is an opaque cursor into the stream. The server returns the next cursor in Stream-Next-Offset; you hand it back to resume.";
}

/**
 * Reproduce an exchange as a copy-pastable curl command. Headers other than the
 * implicit Accept are emitted as -H flags; a non-GET/HEAD method adds -X. The
 * URL is single-quoted so query strings with & survive a shell paste.
 */
export function toCurl(exchange: HttpExchange): string {
	const parts: string[] = ["curl"];
	const method = exchange.method.toUpperCase();
	if (method === "HEAD") {
		parts.push("-I");
	} else if (method !== "GET") {
		parts.push("-X", method);
	}
	for (const [name, value] of Object.entries(exchange.requestHeaders)) {
		parts.push("-H", shellQuote(`${name}: ${value}`));
	}
	parts.push(shellQuote(exchange.url));
	return parts.join(" ");
}

/** Single-quote a string for a POSIX shell, escaping embedded single quotes. */
function shellQuote(s: string): string {
	return `'${s.replace(/'/g, "'\\''")}'`;
}

/**
 * Split a full URL into its origin+path and an ordered list of query params, so
 * the disclosure can render the query — where the offset lives — as a readable
 * key/value list instead of an opaque string. Falls back to the whole URL as
 * the base with no params if it cannot be parsed.
 */
export function splitUrl(url: string): {
	readonly base: string;
	readonly query: readonly (readonly [string, string])[];
} {
	try {
		const u = new URL(url);
		const query: [string, string][] = [];
		u.searchParams.forEach((value, key) => {
			query.push([key, value]);
		});
		const base = `${u.origin}${u.pathname}`;
		return { base, query };
	} catch {
		return { base: url, query: [] };
	}
}
