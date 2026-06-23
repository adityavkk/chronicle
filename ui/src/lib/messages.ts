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
