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
 * An immediate re-read of the same cursor (e.g. pressing Refresh, or clicking
 * the chip you are already on) collapses into the latest entry rather than
 * adding a duplicate chip, so the strip stays a trail of distinct positions.
 * When the history would exceed `cap`, the oldest entries are dropped.
 */
export function appendReadHistory(
	history: readonly ReadHistoryEntry[],
	entry: ReadHistoryEntry,
	cap: number,
): readonly ReadHistoryEntry[] {
	const last = history[history.length - 1];
	const isRepeat =
		last !== undefined &&
		last.path === entry.path &&
		last.requestedOffset === entry.requestedOffset;
	const base = isRepeat ? history.slice(0, -1) : history;
	const next = [...base, entry];
	if (cap <= 0) return [];
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
