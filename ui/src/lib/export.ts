/**
 * Pure serializers for exporting the loaded batch / tail buffer, plus one small
 * DOM-guarded download helper.
 *
 * The formatters (`rowsToNdjson`, `rowsToCsv`) and the filename / mime helpers
 * are plain functions over plain data, exactly like the rest of `lib/`, so they
 * are unit-tested directly. `downloadBlob` is the single, deliberate DOM touch
 * in this module — the same kind of documented exception as `dsClient` (the only
 * `fetch`) and `config` (the only config load). It is guarded for a missing
 * `document` / object-URL API so it is safe to call under SSR or in tests, where
 * it simply returns `false` instead of throwing.
 *
 * Content kind matters. JSON streams have one row per array element, so NDJSON
 * (one value per line) and a fixed-column CSV (index, byteSize, time, value) are
 * the natural shapes. Text/binary streams are unframed — a read returns one
 * concatenated row — so the exact-bytes "raw body" export (offered separately
 * from the paged grid) is usually what a power user wants there; the formatters
 * still produce sensible output for them (the text as a JSON string / a base64
 * cell) so a tailed text/binary stream, where each append is its own row, is
 * losslessly exportable too.
 */

import { extractTimestamp, formatTimeFull } from "./messages";
import type { GridRow, StreamKind } from "./types";

/* ---------------------------------------------------------------------------
 * MIME types + extensions
 * ------------------------------------------------------------------------ */

/** MIME type for a newline-delimited JSON download. */
export const NDJSON_MIME = "application/x-ndjson";
/** MIME type for a CSV download (UTF-8, RFC-4180 shaped). */
export const CSV_MIME = "text/csv;charset=utf-8";

/** The MIME type for a "Save raw body" download, by stream kind. */
export function rawMimeForKind(kind: StreamKind): string {
	switch (kind) {
		case "json":
			return "application/json";
		case "text":
			return "text/plain;charset=utf-8";
		case "binary":
			return "application/octet-stream";
	}
}

/** The file extension (no dot) for a "Save raw body" download, by stream kind. */
export function rawExtensionForKind(kind: StreamKind): string {
	switch (kind) {
		case "json":
			return "json";
		case "text":
			return "txt";
		case "binary":
			return "bin";
	}
}

/* ---------------------------------------------------------------------------
 * Filenames
 * ------------------------------------------------------------------------ */

/** Longest offset segment kept in a filename (opaque cursors can be long). */
const MAX_OFFSET_LEN = 40;

/** Replace anything outside a safe filename set with a single dash, collapsing runs. */
function sanitize(raw: string): string {
	return raw.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/-{2,}/g, "-");
}

/** True when a sanitized segment carries no real value (empty or only separators). */
function isOnlySeparators(segment: string): boolean {
	return segment === "" || /^[-.]+$/.test(segment);
}

/**
 * Build a download filename from the stream path + offset so successive exports
 * are distinguishable, e.g. `orders-created@-1.ndjson`. Both are sanitized to
 * safe filename characters; the offset is faithful (so the meaningful `-1`
 * earliest sentinel survives) and length-capped. A path that is empty / only
 * separators falls back to `stream`, such an offset to `0`.
 */
export function buildExportFilename(path: string, offset: string, extension: string): string {
	const cleanedPath = sanitize(path).replace(/^-+|-+$/g, "");
	const safePath = cleanedPath === "" ? "stream" : cleanedPath;
	const cleanedOffset = sanitize(offset).slice(0, MAX_OFFSET_LEN);
	const safeOffset = isOnlySeparators(cleanedOffset) ? "0" : cleanedOffset;
	const ext = extension.replace(/^\.+/, "");
	return `${safePath}@${safeOffset}.${ext}`;
}

/* ---------------------------------------------------------------------------
 * Base64 (pure — no btoa, so it is deterministic in node + the browser)
 * ------------------------------------------------------------------------ */

const B64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/** Standard base64-encode raw bytes. Used to make binary rows JSON/CSV-safe. */
export function bytesToBase64(bytes: Uint8Array): string {
	let out = "";
	for (let i = 0; i < bytes.length; i += 3) {
		const b0 = bytes[i] ?? 0;
		const b1 = bytes[i + 1];
		const b2 = bytes[i + 2];
		const triple = (b0 << 16) | ((b1 ?? 0) << 8) | (b2 ?? 0);
		out += B64_ALPHABET.charAt((triple >> 18) & 63);
		out += B64_ALPHABET.charAt((triple >> 12) & 63);
		out += b1 === undefined ? "=" : B64_ALPHABET.charAt((triple >> 6) & 63);
		out += b2 === undefined ? "=" : B64_ALPHABET.charAt(triple & 63);
	}
	return out;
}

/* ---------------------------------------------------------------------------
 * NDJSON
 * ------------------------------------------------------------------------ */

/** Render the bytes of a binary row, defensively coercing a non-Uint8Array. */
function bytesOf(value: unknown): Uint8Array {
	return value instanceof Uint8Array ? value : new Uint8Array(0);
}

/** One NDJSON line for a row: the decoded value, kind-aware, never throwing. */
function ndjsonLine(row: GridRow): string {
	try {
		if (row.kind === "binary") return JSON.stringify(bytesToBase64(bytesOf(row.value)));
		if (row.kind === "text") return JSON.stringify(String(row.value));
		const json = JSON.stringify(row.value);
		// JSON.stringify(undefined) is undefined; keep every line a valid value.
		return json === undefined ? "null" : json;
	} catch {
		return JSON.stringify(String(row.value));
	}
}

/**
 * Serialize rows to newline-delimited JSON — one decoded value per line, with a
 * trailing newline. JSON rows emit their parsed value; text rows emit the text
 * as a JSON string; binary rows emit a base64 JSON string (lossless). Returns an
 * empty string for no rows.
 */
export function rowsToNdjson(rows: readonly GridRow[]): string {
	if (rows.length === 0) return "";
	return `${rows.map(ndjsonLine).join("\n")}\n`;
}

/* ---------------------------------------------------------------------------
 * CSV (RFC-4180)
 * ------------------------------------------------------------------------ */

/** The CSV columns, in order. */
const CSV_HEADER: readonly string[] = ["index", "byteSize", "time", "value"];

/** Quote + escape a field per RFC-4180: wrap and double-quote only when needed. */
function csvField(value: string): string {
	if (/[",\r\n]/.test(value)) return `"${value.replace(/"/g, '""')}"`;
	return value;
}

/** The flattened value cell for a row: compact JSON / the text / a base64 blob. */
function csvCell(row: GridRow): string {
	if (row.kind === "text") return String(row.value);
	if (row.kind === "binary") return bytesToBase64(bytesOf(row.value));
	try {
		const json = JSON.stringify(row.value);
		return json === undefined ? "" : json;
	} catch {
		return String(row.value);
	}
}

/**
 * Serialize rows to RFC-4180 CSV with a header row and `\r\n` record separators.
 * Columns: index, byteSize, time (ISO event time for JSON rows that carry one,
 * else blank), and a flattened value cell (compact JSON for JSON rows, the text
 * for text rows, base64 for binary rows). Fields are quote-escaped per RFC-4180.
 */
export function rowsToCsv(rows: readonly GridRow[]): string {
	const lines = [CSV_HEADER.join(",")];
	for (const row of rows) {
		const time = row.kind === "json" ? formatTimeFull(extractTimestamp(row.value)) : "";
		lines.push(
			[String(row.index), String(row.byteSize), csvField(time), csvField(csvCell(row))].join(","),
		);
	}
	return `${lines.join("\r\n")}\r\n`;
}

/* ---------------------------------------------------------------------------
 * Download (the one DOM touch — guarded)
 * ------------------------------------------------------------------------ */

/**
 * Trigger a client-side download of `data` as `filename` with the given MIME
 * type, via a Blob + object URL and a transient `<a download>`. No library, no
 * server. Guarded for a missing `document` / object-URL API (SSR, tests): in
 * that case it does nothing and returns `false`. Returns `true` when the
 * download was initiated. Like the clipboard CopyButton, it degrades silently
 * rather than throwing when the platform API is unavailable.
 */
export function downloadBlob(filename: string, mime: string, data: string | Uint8Array): boolean {
	if (typeof document === "undefined") return false;
	const urlApi = globalThis.URL;
	if (urlApi === undefined || typeof urlApi.createObjectURL !== "function") return false;

	// Copy a Uint8Array into a fresh ArrayBuffer-backed view so it satisfies
	// BlobPart regardless of the source's (possibly Shared) backing buffer.
	let part: BlobPart;
	if (typeof data === "string") {
		part = data;
	} else {
		const copy = new Uint8Array(data.byteLength);
		copy.set(data);
		part = copy;
	}
	const blob = new Blob([part], { type: mime });
	const url = urlApi.createObjectURL(blob);
	try {
		const anchor = document.createElement("a");
		anchor.href = url;
		anchor.download = filename;
		anchor.rel = "noopener";
		anchor.style.display = "none";
		document.body.appendChild(anchor);
		anchor.click();
		anchor.remove();
	} finally {
		urlApi.revokeObjectURL(url);
	}
	return true;
}
