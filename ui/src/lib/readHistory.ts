/**
 * Pure helpers for the per-stream read-cursor history. No DOM, no store, no I/O.
 *
 * Offsets are opaque cursors and the protocol has no backward read, so the
 * client remembers the sequence of positions it has read this session. These
 * helpers turn that bookkeeping (append-with-cap) and the display of a cursor
 * (a short chip label) into plain functions over plain data, so they are
 * trivially unit-tested and the component stays pure layout.
 */

import { OFFSET_EARLIEST, OFFSET_LATEST } from "./messages";
import type { ReadHistoryEntry } from "./types";

/**
 * Append a visited read position to the capped history, newest last.
 *
 * Re-reading a cursor already in the trail — pressing Refresh, or clicking any
 * chip (the current one or an older one) — collapses onto that position rather
 * than adding a duplicate: the prior occurrence for the same (path, offset) is
 * dropped and the fresh visit re-appended as newest. So the strip stays a trail
 * of DISTINCT positions in most-recently-visited order, and clicking a chip
 * navigates to it instead of growing the strip. When the history would exceed
 * `cap`, the oldest entries are dropped.
 */
export function appendReadHistory(
	history: readonly ReadHistoryEntry[],
	entry: ReadHistoryEntry,
	cap: number,
): readonly ReadHistoryEntry[] {
	if (cap <= 0) return [];
	const base = history.filter(
		(h) => !(h.path === entry.path && h.requestedOffset === entry.requestedOffset),
	);
	const next = [...base, entry];
	return next.length > cap ? next.slice(next.length - cap) : next;
}

/**
 * A short, human label for an offset cursor shown on a history chip. The two
 * sentinel offsets read as words; a long opaque cursor is middle-truncated so
 * the chip stays compact while keeping its head and tail recognizable.
 */
export function offsetChipLabel(offset: string): string {
	if (offset === OFFSET_EARLIEST) return "earliest";
	if (offset === OFFSET_LATEST) return "latest";
	if (offset.length <= 12) return offset;
	return `${offset.slice(0, 6)}…${offset.slice(-4)}`;
}
