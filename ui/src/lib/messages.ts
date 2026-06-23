/**
 * Pure helpers for the Messages workspace toolbar + grid. No DOM, no store, no
 * I/O — so they are trivially unit-testable and reusable. Everything here is
 * about turning the user's toolbar choices into a concrete protocol offset, and
 * about surfacing content-aware details (timestamps, sizes) for the grid.
 */

import { isRecord } from "./guards";
import type { GridRow } from "./types";

/**
 * The starting position the user picked in the toolbar.
 *  - "earliest": read from the beginning (protocol offset "-1").
 *  - "latest":   read from the current tail (protocol offset "now").
 *  - "at":       read from an explicit, opaque offset cursor the user typed.
 */
export type StartMode = "earliest" | "latest" | "at";

/** The protocol offset for the special "beginning" position. */
export const OFFSET_EARLIEST = "-1";
/** The protocol offset for the current tail. */
export const OFFSET_LATEST = "now";

/** Sensible row-cap choices for the grid. */
export const ROW_CAP_OPTIONS: readonly number[] = [25, 50, 100, 250, 500, 1000];
/** Default number of rows to keep from a batch. */
export const DEFAULT_ROW_CAP = 100;

/**
 * Resolve the toolbar's {@link StartMode} (+ a typed custom offset) into the
 * concrete offset string the protocol understands. For "at", a blank custom
 * offset falls back to the beginning so a Read is never sent with an empty
 * cursor.
 */
export function resolveOffset(mode: StartMode, customOffset: string): string {
	switch (mode) {
		case "earliest":
			return OFFSET_EARLIEST;
		case "latest":
			return OFFSET_LATEST;
		case "at": {
			const trimmed = customOffset.trim();
			return trimmed === "" ? OFFSET_EARLIEST : trimmed;
		}
	}
}

/** Clamp a requested row cap to a positive integer, defaulting on bad input. */
export function clampRowCap(value: number, fallback: number = DEFAULT_ROW_CAP): number {
	if (!Number.isFinite(value) || value <= 0) return fallback;
	return Math.floor(value);
}

/** A short human label for a {@link StartMode}, for the disclosure / titles. */
export function describeStartMode(mode: StartMode, customOffset: string): string {
	switch (mode) {
		case "earliest":
			return "Earliest (offset -1)";
		case "latest":
			return "Latest (offset now)";
		case "at":
			return `At offset ${customOffset.trim() === "" ? "-1" : customOffset.trim()}`;
	}
}

/** Field names commonly carrying a record's event time, in priority order. */
const TIME_FIELDS: readonly string[] = [
	"timestamp",
	"time",
	"ts",
	"createdAt",
	"created_at",
	"eventTime",
	"event_time",
	"datetime",
	"date",
	"@timestamp",
];

/**
 * Best-effort extraction of an event timestamp from a JSON grid row's value.
 * The protocol does not mandate a time field, so this only succeeds when the
 * element is an object carrying a recognizable time-ish field. Accepts ISO
 * strings and epoch numbers (seconds or milliseconds). Returns epoch ms, or
 * null when no usable timestamp is present.
 */
export function extractTimestamp(value: unknown): number | null {
	if (!isRecord(value)) return null;
	for (const field of TIME_FIELDS) {
		const raw = value[field];
		const ms = coerceTimestamp(raw);
		if (ms !== null) return ms;
	}
	return null;
}

/** Coerce a single field value into epoch ms, or null if it is not time-ish. */
function coerceTimestamp(raw: unknown): number | null {
	if (typeof raw === "number" && Number.isFinite(raw)) {
		// Heuristic: 10-digit-ish values are seconds; larger are already ms.
		// Anything below ~10^11 is treated as seconds (year ~5138 boundary).
		return raw < 1e11 ? Math.round(raw * 1000) : Math.round(raw);
	}
	if (typeof raw === "string" && raw.trim() !== "") {
		const parsed = Date.parse(raw);
		if (!Number.isNaN(parsed)) return parsed;
	}
	return null;
}

/** Format epoch ms as a compact local time string for the grid time column. */
export function formatTime(ms: number | null): string {
	if (ms === null) return "";
	try {
		const d = new Date(ms);
		if (Number.isNaN(d.getTime())) return "";
		return d.toLocaleTimeString(undefined, {
			hour: "2-digit",
			minute: "2-digit",
			second: "2-digit",
		});
	} catch {
		return "";
	}
}

/** Full ISO-ish timestamp for the inspector's metadata block. */
export function formatTimeFull(ms: number | null): string {
	if (ms === null) return "";
	try {
		const d = new Date(ms);
		if (Number.isNaN(d.getTime())) return "";
		return d.toISOString();
	} catch {
		return "";
	}
}

/** Human byte-size label shared by the grid and inspector. */
export function formatBytes(n: number): string {
	if (n < 1024) return `${n} B`;
	if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
	return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

/**
 * True when any row in a batch carries an extractable timestamp — used to
 * decide whether the grid shows its Time column at all (keeps it dense when no
 * stream has times).
 */
export function batchHasTimes(rows: readonly GridRow[]): boolean {
	return rows.some((r) => r.kind === "json" && extractTimestamp(r.value) !== null);
}

/* ---------------------------------------------------------------------------
 * In-grid row filtering (issue #53)
 *
 * Pure, client-side matching over the already-loaded batch and the live tail
 * buffer — there is no server query, just narrowing the rows in memory. Three
 * query shapes, all case-insensitive by default:
 *
 *  - substring (the default): the trimmed query must appear in the row's
 *    "haystack" — its preview, the full stringified decoded value, and (for
 *    JSON rows) the formatted time. Matching the full value, not just the
 *    160-char preview, lets a search reach text the preview truncated.
 *  - /regex/flags: a query wrapped in slashes compiles to a RegExp tested
 *    against the same haystack. g/y flags are stripped so .test() is stateless
 *    across rows; an invalid pattern does NOT empty the grid (it is inert until
 *    the user finishes typing, and the input surfaces the error).
 *  - field:value: a leading bare identifier + ':' targets one decoded JSON
 *    field (dotted paths like user.id descend objects). The ':' must not be
 *    followed by '/' or ':' so a URL like http://… stays a substring search.
 *    An empty value matches "field exists".
 *
 * The matcher is split so a component compiles the query once (useComputed) and
 * runs matchCompiled per row; rowMatches is the issue-named convenience that
 * compiles + matches in one call (used directly by the tests).
 * ------------------------------------------------------------------------ */

/** The classified shape of a filter query. */
export type FilterKind = "empty" | "substring" | "field" | "regex" | "invalid";

/** A query compiled once, then run against many rows. */
export interface CompiledQuery {
	/** The original, untrimmed query text. */
	readonly raw: string;
	/** Which matching strategy applies. */
	readonly kind: FilterKind;
	/** True when the query actually narrows rows (substring | field | regex). */
	readonly active: boolean;
	/** A human-readable reason when kind is "invalid" (e.g. a bad regex). */
	readonly error: string | null;
	/** Lowercased substring needle, for "substring" and "field" kinds. */
	readonly needle: string;
	/** Dotted JSON field path, for the "field" kind. */
	readonly field: string;
	/** Compiled pattern, for the "regex" kind. */
	readonly regex: RegExp | null;
}

/** Options shared by the matcher (kept small and explicit). */
export interface RowMatchOptions {
	/** Include the formatted time in the haystack (default true). */
	readonly includeTime?: boolean;
}

/** A query wrapped in slashes, optionally with trailing flags: /body/flags. */
const REGEX_FORM = /^\/(.+)\/([a-z]*)$/;
/**
 * A leading bare identifier + ':' targeting one JSON field. The negative
 * lookahead keeps URLs (http://…) and "::" out of field syntax so they fall
 * through to a normal substring search.
 */
const FIELD_FORM = /^([A-Za-z_$][\w.$-]*):(?![/:])(.*)$/;

/** Classify + compile a raw filter query. Pure; never throws. */
export function compileQuery(query: string): CompiledQuery {
	const trimmed = query.trim();
	const base = { raw: query, error: null, needle: "", field: "", regex: null } as const;
	if (trimmed === "") {
		return { ...base, kind: "empty", active: false };
	}

	const regexMatch = REGEX_FORM.exec(trimmed);
	if (regexMatch) {
		const body = regexMatch[1] ?? "";
		// Strip g/y: a stateful lastIndex would make .test() flip-flop per row.
		const flags = (regexMatch[2] ?? "").replace(/[gy]/g, "");
		try {
			const regex = new RegExp(body, flags);
			return { ...base, kind: "regex", active: true, regex };
		} catch (err) {
			const message = err instanceof Error ? err.message : "invalid regular expression";
			return { ...base, kind: "invalid", active: false, error: message };
		}
	}

	const fieldMatch = FIELD_FORM.exec(trimmed);
	if (fieldMatch) {
		const field = fieldMatch[1] ?? "";
		const value = fieldMatch[2] ?? "";
		return { ...base, kind: "field", active: true, field, needle: value.trim().toLowerCase() };
	}

	return { ...base, kind: "substring", active: true, needle: trimmed.toLowerCase() };
}

/** Stringify a row's decoded value for searching (full, not preview-truncated). */
function stringifyValue(value: unknown): string {
	if (typeof value === "string") return value;
	try {
		return JSON.stringify(value) ?? String(value);
	} catch {
		return String(value);
	}
}

/**
 * The searchable text for a row: its preview, the full stringified decoded
 * value (so search reaches past the truncated preview), and the formatted time
 * for JSON rows. Binary rows contribute only their preview (the raw bytes are
 * not meaningfully searchable as text).
 */
export function rowHaystack(row: GridRow, opts: RowMatchOptions = {}): string {
	const includeTime = opts.includeTime ?? true;
	const parts: string[] = [row.preview];
	if (row.kind === "json") {
		parts.push(stringifyValue(row.value));
		if (includeTime) {
			const time = formatTime(extractTimestamp(row.value));
			if (time !== "") parts.push(time);
		}
	} else if (row.kind === "text" && typeof row.value === "string") {
		parts.push(row.value);
	}
	return parts.join("\n");
}

/** Resolve a dotted field path against a decoded value, or undefined. */
function resolveFieldPath(value: unknown, path: string): unknown {
	let current: unknown = value;
	for (const segment of path.split(".")) {
		if (!isRecord(current)) return undefined;
		current = current[segment];
	}
	return current;
}

/** Run a pre-compiled query against one row. Pure; never throws. */
export function matchCompiled(
	row: GridRow,
	query: CompiledQuery,
	opts: RowMatchOptions = {},
): boolean {
	switch (query.kind) {
		case "empty":
		case "invalid":
			// Not an active filter — never narrow the visible rows.
			return true;
		case "substring":
			return rowHaystack(row, opts).toLowerCase().includes(query.needle);
		case "regex":
			return query.regex?.test(rowHaystack(row, opts)) ?? false;
		case "field": {
			if (row.kind !== "json") return false;
			const found = resolveFieldPath(row.value, query.field);
			if (found === undefined) return false;
			if (query.needle === "") return true; // field-exists query
			return stringifyValue(found).toLowerCase().includes(query.needle);
		}
	}
}

/**
 * Does a row match a raw filter query? The issue-named entry point: compiles the
 * query and matches in one call. Components that filter many rows should
 * `compileQuery` once and call {@link matchCompiled} per row instead.
 */
export function rowMatches(row: GridRow, query: string, opts: RowMatchOptions = {}): boolean {
	return matchCompiled(row, compileQuery(query), opts);
}
