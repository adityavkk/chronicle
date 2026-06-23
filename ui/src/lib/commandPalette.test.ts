import { describe, expect, it } from "vitest";
import { type Searchable, filterCommands, isPaletteHotkey, scoreCommand } from "./commandPalette";

const items: readonly Searchable[] = [
	{ label: "orders/created", keywords: "stream" },
	{ label: "orders/shipped", keywords: "stream" },
	{ label: "New stream", keywords: "create add" },
	{ label: "Toggle theme", keywords: "dark light mode appearance" },
];

describe("scoreCommand", () => {
	it("matches everything with score 0 and no ranges for an empty query", () => {
		const m = scoreCommand(items[0] as Searchable, "   ");
		expect(m).not.toBeNull();
		expect(m?.score).toBe(0);
		expect(m?.ranges).toEqual([]);
	});

	it("returns null when neither the label nor the keywords contain the query", () => {
		expect(scoreCommand({ label: "Metrics" }, "zzz")).toBeNull();
	});

	it("is case-insensitive and reports the matched range in the label", () => {
		const m = scoreCommand({ label: "orders/created" }, "CREATED");
		expect(m).not.toBeNull();
		expect(m?.ranges).toEqual([{ start: 7, end: 14 }]);
	});

	it("ranks a prefix hit above a word-boundary hit above a mid-word hit", () => {
		const prefix = scoreCommand({ label: "create stream" }, "create");
		const boundary = scoreCommand({ label: "the create step" }, "create");
		const midword = scoreCommand({ label: "recreated" }, "create");
		expect(prefix).not.toBeNull();
		expect(boundary).not.toBeNull();
		expect(midword).not.toBeNull();
		const ps = prefix?.score ?? 0;
		const bs = boundary?.score ?? 0;
		const ms = midword?.score ?? 0;
		expect(ps).toBeGreaterThan(bs);
		expect(bs).toBeGreaterThan(ms);
	});

	it("treats a keywords-only hit as a match below any label hit, with no ranges", () => {
		const labelHit = scoreCommand({ label: "Toggle theme", keywords: "dark" }, "theme");
		const keywordHit = scoreCommand({ label: "Toggle theme", keywords: "dark" }, "dark");
		expect(keywordHit).not.toBeNull();
		expect(keywordHit?.ranges).toEqual([]);
		expect(labelHit?.score ?? 0).toBeGreaterThan(keywordHit?.score ?? 0);
	});
});

describe("filterCommands", () => {
	it("returns all items in input order for an empty query", () => {
		const out = filterCommands(items, "");
		expect(out.map((m) => m.item.label)).toEqual([
			"orders/created",
			"orders/shipped",
			"New stream",
			"Toggle theme",
		]);
	});

	it("keeps only matching items, best-first", () => {
		const out = filterCommands(items, "orders");
		expect(out.map((m) => m.item.label)).toEqual(["orders/created", "orders/shipped"]);
	});

	it("matches a core action via its keywords when the label does not contain the query", () => {
		const out = filterCommands(items, "add");
		expect(out.map((m) => m.item.label)).toEqual(["New stream"]);
	});

	it("orders equal-score matches stably by input position", () => {
		// Both labels are word-boundary hits of "s" of equal length, so the tie
		// must resolve to input order.
		const tied: readonly Searchable[] = [{ label: "a s" }, { label: "b s" }];
		const out = filterCommands(tied, "s");
		expect(out.map((m) => m.item.label)).toEqual(["a s", "b s"]);
	});
});

describe("isPaletteHotkey", () => {
	it("is true for Cmd-K and Ctrl-K, lower or upper case", () => {
		expect(isPaletteHotkey({ key: "k", metaKey: true, ctrlKey: false })).toBe(true);
		expect(isPaletteHotkey({ key: "k", metaKey: false, ctrlKey: true })).toBe(true);
		expect(isPaletteHotkey({ key: "K", metaKey: true, ctrlKey: false })).toBe(true);
	});

	it("is false without a modifier or for another key", () => {
		expect(isPaletteHotkey({ key: "k", metaKey: false, ctrlKey: false })).toBe(false);
		expect(isPaletteHotkey({ key: "j", metaKey: true, ctrlKey: false })).toBe(false);
	});
});
