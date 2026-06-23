import { describe, expect, it } from "vitest";
import { OVERSCAN, ROW_HEIGHT, WINDOW_THRESHOLD, windowRange } from "./virtual";

describe("windowRange", () => {
	it("renders everything when the viewport is not measurable (pre-layout / jsdom)", () => {
		// clientHeight 0 → no windowing, so a list is never blank before measuring.
		expect(windowRange(0, 0, ROW_HEIGHT, 500)).toEqual({
			startIndex: 0,
			endIndex: 500,
			padTop: 0,
			padBottom: 0,
		});
	});

	it("is an empty range for an empty list", () => {
		expect(windowRange(0, 600, ROW_HEIGHT, 0)).toEqual({
			startIndex: 0,
			endIndex: 0,
			padTop: 0,
			padBottom: 0,
		});
	});

	it("renders the whole list plus overscan from the top", () => {
		// 600px / 30px = 20 visible rows, +1 partial, +OVERSCAN below; nothing above.
		const r = windowRange(0, 600, ROW_HEIGHT, 1000);
		expect(r.startIndex).toBe(0);
		expect(r.endIndex).toBe(20 + 1 + OVERSCAN);
		expect(r.padTop).toBe(0);
		expect(r.padBottom).toBe((1000 - r.endIndex) * ROW_HEIGHT);
	});

	it("windows a middle band with overscan on both sides", () => {
		// scrollTop 3000 → first visible row = 100; band is [100-8, 100+21+8).
		const r = windowRange(3000, 600, ROW_HEIGHT, 1000);
		expect(r.startIndex).toBe(100 - OVERSCAN);
		expect(r.endIndex).toBe(100 + 21 + OVERSCAN);
		expect(r.padTop).toBe(r.startIndex * ROW_HEIGHT);
		expect(r.padBottom).toBe((1000 - r.endIndex) * ROW_HEIGHT);
	});

	it("clamps the end to the total at the bottom of the list", () => {
		// Scrolled near the end: endIndex never exceeds total, padBottom is 0.
		const total = 1000;
		const r = windowRange((total - 20) * ROW_HEIGHT, 600, ROW_HEIGHT, total);
		expect(r.endIndex).toBe(total);
		expect(r.padBottom).toBe(0);
		expect(r.startIndex).toBeGreaterThan(0);
	});

	it("keeps the scrollbar geometry exact: padTop + rendered + padBottom === total height", () => {
		const total = 777;
		for (const scrollTop of [0, 1234, 9999, total * ROW_HEIGHT]) {
			const r = windowRange(scrollTop, 480, ROW_HEIGHT, total);
			const rendered = (r.endIndex - r.startIndex) * ROW_HEIGHT;
			expect(r.padTop + rendered + r.padBottom).toBe(total * ROW_HEIGHT);
			expect(r.startIndex).toBeGreaterThanOrEqual(0);
			expect(r.endIndex).toBeLessThanOrEqual(total);
		}
	});

	it("treats a negative scrollTop as the top (rubber-band guard)", () => {
		expect(windowRange(-50, 600, ROW_HEIGHT, 500)).toEqual(windowRange(0, 600, ROW_HEIGHT, 500));
	});

	it("exposes the row height and threshold the components share", () => {
		expect(ROW_HEIGHT).toBe(30);
		expect(WINDOW_THRESHOLD).toBeGreaterThan(0);
	});
});
