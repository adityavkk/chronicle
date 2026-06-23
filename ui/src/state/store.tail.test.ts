import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { GridRow, TailBatch } from "../lib/types";
import {
	TAIL_BUFFER_CAP,
	clearTailBuffer,
	pushTailBatch,
	stopTail,
	tailDropped,
	tailPaused,
	tailRows,
} from "./store";

function makeRow(index: number): GridRow {
	return { index, byteSize: 8, preview: `r${index}`, kind: "text", value: `r${index}` };
}

function batch(rows: readonly GridRow[]): TailBatch {
	return { rows, nextOffset: null, upToDate: false, exchange: null };
}

// Capture the scheduled animation-frame callback so the test drives the clock
// deterministically (jsdom's real rAF would fire asynchronously).
let frame: FrameRequestCallback | null = null;
let rafCalls = 0;

beforeEach(() => {
	frame = null;
	rafCalls = 0;
	vi.stubGlobal("requestAnimationFrame", (cb: FrameRequestCallback): number => {
		frame = cb;
		rafCalls++;
		return 1;
	});
	vi.stubGlobal("cancelAnimationFrame", (): void => {
		frame = null;
	});
	tailRows.value = [];
	tailDropped.value = 0;
	tailPaused.value = false;
});

afterEach(() => {
	stopTail();
	vi.unstubAllGlobals();
	tailRows.value = [];
	tailDropped.value = 0;
	tailPaused.value = false;
});

describe("tail coalescer", () => {
	it("coalesces batches arriving within one frame into a single signal write", () => {
		pushTailBatch(batch([makeRow(0)]));
		pushTailBatch(batch([makeRow(1)]));
		pushTailBatch(batch([makeRow(2)]));
		// Nothing written to the signal yet — all three are buffered for one frame.
		expect(tailRows.value).toHaveLength(0);
		// Exactly one frame was scheduled for the burst (idempotent scheduling).
		expect(rafCalls).toBe(1);

		// The frame fires once and applies all three rows in a single write.
		frame?.(0);
		expect(tailRows.value).toHaveLength(3);
		expect(tailRows.value.map((r) => r.index)).toEqual([0, 1, 2]);
	});

	it("does not buffer rows while paused", () => {
		tailPaused.value = true;
		pushTailBatch(batch([makeRow(0)]));
		expect(rafCalls).toBe(0);
		frame?.(0);
		expect(tailRows.value).toHaveLength(0);
	});

	it("clears the pending frame (and scratch) in stopTail", () => {
		pushTailBatch(batch([makeRow(0)]));
		expect(frame).not.toBeNull();
		stopTail();
		// cancelAnimationFrame nulled our captured frame, and the scratch was
		// dropped, so a stale frame firing now would not resurrect the rows.
		expect(frame).toBeNull();
		frame?.(0);
		expect(tailRows.value).toHaveLength(0);
	});

	it("keeps tailDropped correct when a burst overflows the cap", () => {
		const big = Array.from({ length: TAIL_BUFFER_CAP + 5 }, (_, i) => makeRow(i));
		pushTailBatch(batch(big));
		frame?.(0);
		expect(tailRows.value).toHaveLength(TAIL_BUFFER_CAP);
		expect(tailDropped.value).toBe(5);
		// The newest rows are the ones retained.
		expect(tailRows.value.at(-1)?.index).toBe(TAIL_BUFFER_CAP + 4);
	});

	it("evicts in blocks across flushes so steady-state appends stay cheap", () => {
		// Fill exactly to the cap in one flush.
		pushTailBatch(batch(Array.from({ length: TAIL_BUFFER_CAP }, (_, i) => makeRow(i))));
		frame?.(0);
		expect(tailRows.value).toHaveLength(TAIL_BUFFER_CAP);
		expect(tailDropped.value).toBe(0);

		// One more row tips it over: a whole block ages out at once (not just one),
		// leaving room so the next appends fall through the no-eviction branch.
		pushTailBatch(batch([makeRow(TAIL_BUFFER_CAP)]));
		frame?.(0);
		expect(tailDropped.value).toBe(TAIL_BUFFER_CAP / 10);
		expect(tailRows.value.length).toBeLessThan(TAIL_BUFFER_CAP);
		// Invariant: everything received is either buffered or counted as dropped.
		expect(tailRows.value.length + tailDropped.value).toBe(TAIL_BUFFER_CAP + 1);
	});

	it("drops un-flushed scratch rows on clearTailBuffer", () => {
		pushTailBatch(batch([makeRow(0)]));
		clearTailBuffer();
		frame?.(0);
		expect(tailRows.value).toHaveLength(0);
		expect(tailDropped.value).toBe(0);
	});

	it("flushes synchronously when no requestAnimationFrame is available", () => {
		vi.stubGlobal("requestAnimationFrame", undefined);
		pushTailBatch(batch([makeRow(7)]));
		// With no rAF the scheduler falls back to a synchronous flush.
		expect(tailRows.value.map((r) => r.index)).toEqual([7]);
	});
});
