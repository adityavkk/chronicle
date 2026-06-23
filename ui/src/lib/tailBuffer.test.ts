import { describe, expect, it } from "vitest";
import { appendCapped } from "./tailBuffer";

/** Build [0,1,2,…,n-1] for terse fixtures. */
function seq(n: number, from = 0): number[] {
	return Array.from({ length: n }, (_, i) => from + i);
}

describe("appendCapped", () => {
	it("returns the same buffer for an empty append (no allocation needed)", () => {
		const current = seq(5);
		const r = appendCapped(current, [], 10, 2);
		expect(r.rows).toBe(current);
		expect(r.dropped).toBe(0);
	});

	it("concatenates without eviction while under the cap", () => {
		const r = appendCapped([0, 1, 2], [3, 4], 10, 2);
		expect(r.rows).toEqual([0, 1, 2, 3, 4]);
		expect(r.dropped).toBe(0);
	});

	it("fills exactly to the cap without dropping", () => {
		const r = appendCapped(seq(8), [8, 9], 10, 2);
		expect(r.rows).toHaveLength(10);
		expect(r.dropped).toBe(0);
	});

	it("evicts in blocks once over cap, so the result sits below the cap", () => {
		// cap 10, block 4: buffer full at 10, append 1 → must drop ≥1, rounded to a
		// block of 4. Result = 10 - 4 + 1 = 7 rows; oldest 4 aged out.
		const r = appendCapped(seq(10), [10], 10, 4);
		expect(r.dropped).toBe(4);
		expect(r.rows).toEqual([4, 5, 6, 7, 8, 9, 10]);
		expect(r.rows.length).toBeLessThanOrEqual(10);
	});

	it("never exceeds the cap even when the overflow is larger than a block", () => {
		// Appending 6 to a full buffer overflows by 6 > block(4): must drop 6.
		const r = appendCapped(seq(10), seq(6, 10), 10, 4);
		expect(r.rows).toHaveLength(10);
		expect(r.rows[0]).toBe(6);
		expect(r.rows.at(-1)).toBe(15);
		expect(r.dropped).toBe(6);
	});

	it("keeps only the newest cap rows when one burst exceeds the cap", () => {
		const r = appendCapped(seq(3), seq(25, 100), 10, 4);
		expect(r.rows).toHaveLength(10);
		// Newest 10 of the 25-row burst (100..124) → 115..124.
		expect(r.rows).toEqual(seq(10, 115));
		// Everything buffered (3) plus the older 15 of the burst aged out.
		expect(r.dropped).toBe(3 + 15);
	});

	it("keeps the running dropped total honest under a long one-row stream", () => {
		const cap = 50;
		const block = 5;
		let buf: readonly number[] = [];
		let dropped = 0;
		const totalSeen = 1000;
		for (let i = 0; i < totalSeen; i++) {
			const r = appendCapped(buf, [i], cap, block);
			buf = r.rows;
			dropped += r.dropped;
			expect(buf.length).toBeLessThanOrEqual(cap);
		}
		// Invariant: everything ever received is either still buffered or counted dropped.
		expect(dropped + buf.length).toBe(totalSeen);
		// The newest row is always retained, and the buffer is full at steady state.
		expect(buf.at(-1)).toBe(totalSeen - 1);
		expect(buf.length).toBe(cap);
	});

	it("amortizes eviction: a full buffer takes block-many cheap appends between drops", () => {
		const cap = 20;
		const block = 4;
		let buf: readonly number[] = seq(cap); // start full
		const dropEvents: number[] = [];
		for (let i = 0; i < 12; i++) {
			const r = appendCapped(buf, [cap + i], cap, block);
			buf = r.rows;
			if (r.dropped > 0) dropEvents.push(i);
		}
		// First append drops a block (buffer → cap-block+1), then ~block cheap
		// appends before the next eviction — not an eviction on every append.
		expect(dropEvents.length).toBeLessThan(12);
		expect(dropEvents[0]).toBe(0);
	});
});
