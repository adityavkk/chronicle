/**
 * Hand-written runtime type guards and parsers. No schema library: the wire
 * shapes are small and stable enough to validate by hand, which keeps the
 * runtime dependency surface to just preact + signals.
 *
 * Everything here is defensive — the server may return 404s, empty bodies, or
 * malformed JSON, and the UI must degrade gracefully rather than throw.
 */

import type { ParseOutcome, StreamKind } from "./types";

/** Narrow unknown to a plain object record. */
export function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Narrow unknown to a non-empty string. */
export function isNonEmptyString(value: unknown): value is string {
	return typeof value === "string" && value.length > 0;
}

/** Classify a Content-Type header into a render kind. */
export function kindFromContentType(contentType: string | null | undefined): StreamKind {
	if (!contentType) return "binary";
	const ct = contentType.toLowerCase();
	if (ct.includes("application/json") || ct.includes("+json")) return "json";
	if (
		ct.startsWith("text/") ||
		ct.includes("charset") ||
		ct.includes("xml") ||
		ct.includes("csv")
	) {
		return "text";
	}
	return "binary";
}

/**
 * A single event in the __registry__ stream. Shape (per protocol):
 *   { "type":"stream", "key":"<path>",
 *     "value": { "path", "contentType", "createdAt" },
 *     "headers": { "operation": "upsert" | "deleted" } }
 */
export interface RegistryEvent {
	readonly key: string;
	readonly path: string;
	readonly contentType: string | null;
	readonly createdAt: string | null;
	readonly operation: "upsert" | "deleted";
}

function asOperation(value: unknown): "upsert" | "deleted" {
	return value === "deleted" ? "deleted" : "upsert";
}

/**
 * Parse one raw registry event object into a {@link RegistryEvent}, or return
 * null if it does not look like a stream event we understand.
 */
export function parseRegistryEvent(raw: unknown): RegistryEvent | null {
	if (!isRecord(raw)) return null;

	// The canonical key is `key`; fall back to value.path if absent.
	const value = isRecord(raw.value) ? raw.value : undefined;
	const keyCandidate = raw.key;
	const pathCandidate = value?.path;

	const key = isNonEmptyString(keyCandidate)
		? keyCandidate
		: isNonEmptyString(pathCandidate)
			? pathCandidate
			: null;
	if (key === null) return null;

	const path = isNonEmptyString(pathCandidate) ? pathCandidate : key;

	const contentTypeRaw = value?.contentType;
	const contentType = isNonEmptyString(contentTypeRaw) ? contentTypeRaw : null;

	const createdAtRaw = value?.createdAt;
	const createdAt = isNonEmptyString(createdAtRaw) ? createdAtRaw : null;

	const headers = isRecord(raw.headers) ? raw.headers : undefined;
	const operation = asOperation(headers?.operation);

	return { key, path, contentType, createdAt, operation };
}

/**
 * Parse a registry body into events. Tolerant of two encodings:
 *   1. A JSON array of event objects.
 *   2. Newline-delimited JSON (one object per line).
 * Unrecognized or malformed entries are skipped, not fatal.
 */
export function parseRegistryBody(body: string): RegistryEvent[] {
	const trimmed = body.trim();
	if (trimmed.length === 0) return [];

	const events: RegistryEvent[] = [];

	// Try a single JSON array first.
	if (trimmed.startsWith("[")) {
		const parsed = tryJson(trimmed);
		if (parsed.ok && Array.isArray(parsed.value)) {
			for (const item of parsed.value) {
				const ev = parseRegistryEvent(item);
				if (ev) events.push(ev);
			}
			return events;
		}
	}

	// Otherwise treat as newline-delimited JSON.
	for (const line of trimmed.split("\n")) {
		const ln = line.trim();
		if (ln.length === 0) continue;
		const parsed = tryJson(ln);
		if (!parsed.ok) continue;
		const ev = parseRegistryEvent(parsed.value);
		if (ev) events.push(ev);
	}
	return events;
}

/**
 * Reduce a stream of registry events to the current live set of stream paths.
 * Later events win; a "deleted" operation removes the path.
 */
export function reduceRegistry(events: readonly RegistryEvent[]): Map<string, RegistryEvent> {
	const current = new Map<string, RegistryEvent>();
	for (const ev of events) {
		if (ev.operation === "deleted") {
			current.delete(ev.path);
		} else {
			current.set(ev.path, ev);
		}
	}
	return current;
}

/**
 * Parse a read body that is expected to be a JSON array (JSON streams). The
 * protocol says a JSON-typed read returns a JSON array of messages.
 */
export function parseJsonArray(body: string): ParseOutcome<unknown[]> {
	const trimmed = body.trim();
	if (trimmed.length === 0) {
		return { ok: true, value: [] };
	}
	const parsed = tryJson(trimmed);
	if (!parsed.ok) {
		return { ok: false, error: { kind: "parse-error", message: parsed.message } };
	}
	if (Array.isArray(parsed.value)) {
		return { ok: true, value: parsed.value };
	}
	// A single JSON object/value is wrapped so the grid still renders one row.
	return { ok: true, value: [parsed.value] };
}

/** JSON.parse wrapped to a typed outcome instead of a throw. */
export function tryJson(
	text: string,
): { ok: true; value: unknown } | { ok: false; message: string } {
	try {
		return { ok: true, value: JSON.parse(text) as unknown };
	} catch (err) {
		return { ok: false, message: err instanceof Error ? err.message : "invalid JSON" };
	}
}

/** Byte size of a JSON-serializable element, via Blob (UTF-8 accurate). */
export function jsonByteSize(value: unknown): number {
	try {
		return new Blob([JSON.stringify(value) ?? ""]).size;
	} catch {
		return 0;
	}
}

/** Collapse any value into a single-line preview string, truncated. */
export function previewOf(value: unknown, max = 160): string {
	let s: string;
	if (typeof value === "string") {
		s = value;
	} else {
		try {
			s = JSON.stringify(value) ?? String(value);
		} catch {
			s = String(value);
		}
	}
	s = s.replace(/\s+/g, " ").trim();
	return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}
