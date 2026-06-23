import { describe, expect, it } from "vitest";
import { nextRovingIndex, wrapIndex } from "./roving";

describe("wrapIndex", () => {
	it("leaves an in-range index unchanged", () => {
		expect(wrapIndex(2, 5)).toBe(2);
		expect(wrapIndex(0, 5)).toBe(0);
	});

	it("wraps past the end back to the start", () => {
		expect(wrapIndex(5, 5)).toBe(0);
		expect(wrapIndex(6, 5)).toBe(1);
	});

	it("wraps a negative index to the end", () => {
		expect(wrapIndex(-1, 5)).toBe(4);
		expect(wrapIndex(-2, 5)).toBe(3);
	});

	it("returns 0 for an empty list", () => {
		expect(wrapIndex(0, 0)).toBe(0);
		expect(wrapIndex(3, 0)).toBe(0);
	});
});

describe("nextRovingIndex", () => {
	it("ArrowDown advances and wraps at the bottom", () => {
		expect(nextRovingIndex("ArrowDown", 0, 4)).toBe(1);
		expect(nextRovingIndex("ArrowDown", 3, 4)).toBe(0);
	});

	it("ArrowUp retreats and wraps at the top", () => {
		expect(nextRovingIndex("ArrowUp", 2, 4)).toBe(1);
		expect(nextRovingIndex("ArrowUp", 0, 4)).toBe(3);
	});

	it("ArrowDown from -1 (no current focus) lands on the first item", () => {
		expect(nextRovingIndex("ArrowDown", -1, 4)).toBe(0);
	});

	it("Home and End jump to the ends", () => {
		expect(nextRovingIndex("Home", 2, 4)).toBe(0);
		expect(nextRovingIndex("End", 1, 4)).toBe(3);
	});

	it("returns null for a non-navigation key", () => {
		expect(nextRovingIndex("Enter", 1, 4)).toBeNull();
		expect(nextRovingIndex("a", 1, 4)).toBeNull();
		expect(nextRovingIndex("Tab", 1, 4)).toBeNull();
	});

	it("returns null for an empty list", () => {
		expect(nextRovingIndex("ArrowDown", 0, 0)).toBeNull();
		expect(nextRovingIndex("Home", 0, 0)).toBeNull();
	});
});
