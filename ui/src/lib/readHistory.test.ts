import { describe, expect, it } from "vitest";
import { OFFSET_EARLIEST, OFFSET_LATEST } from "./messages";
import { appendReadHistory, offsetChipLabel } from "./readHistory";
import type { ReadHistoryEntry } from "./types";

function entry(over: Partial<ReadHistoryEntry> = {}): ReadHistoryEntry {
	return {
		path: "orders",
		requestedOffset: "-1",
		nextOffset: "42",
		rowCount: 3,
		at: 0,
		...over,
	};
}

describe("appendReadHistory", () => {
	it("appends a new position newest-last", () => {
		const a = entry({ requestedOffset: "-1" });
		const b = entry({ requestedOffset: "42" });
		const history = appendReadHistory(appendReadHistory([], a, 10), b, 10);
		expect(history.map((e) => e.requestedOffset)).toEqual(["-1", "42"]);
	});

	it("collapses an immediate re-read of the same cursor into the latest entry", () => {
		const first = entry({ requestedOffset: "42", rowCount: 3, at: 1 });
		const refreshed = entry({ requestedOffset: "42", rowCount: 5, at: 2 });
		const history = appendReadHistory(appendReadHistory([], first, 10), refreshed, 10);
		expect(history).toHaveLength(1);
		// The collapsed entry carries the fresh read's data, not the stale one.
		expect(history[0]?.rowCount).toBe(5);
		expect(history[0]?.at).toBe(2);
	});

	it("records a revisit of a non-adjacent cursor as a distinct entry", () => {
		let history = appendReadHistory([], entry({ requestedOffset: "-1" }), 10);
		history = appendReadHistory(history, entry({ requestedOffset: "42" }), 10);
		history = appendReadHistory(history, entry({ requestedOffset: "-1" }), 10);
		expect(history.map((e) => e.requestedOffset)).toEqual(["-1", "42", "-1"]);
	});

	it("does not collapse the same offset across different streams", () => {
		const a = entry({ path: "orders", requestedOffset: "42" });
		const b = entry({ path: "events", requestedOffset: "42" });
		const history = appendReadHistory(appendReadHistory([], a, 10), b, 10);
		expect(history).toHaveLength(2);
	});

	it("caps the history by dropping the oldest entries", () => {
		let history: readonly ReadHistoryEntry[] = [];
		for (let i = 0; i < 6; i++) {
			history = appendReadHistory(history, entry({ requestedOffset: `o${i}` }), 3);
		}
		expect(history.map((e) => e.requestedOffset)).toEqual(["o3", "o4", "o5"]);
	});

	it("returns an empty history for a non-positive cap", () => {
		expect(appendReadHistory([], entry(), 0)).toEqual([]);
	});
});

describe("offsetChipLabel", () => {
	it("renders the sentinel offsets as words", () => {
		expect(offsetChipLabel(OFFSET_EARLIEST)).toBe("earliest");
		expect(offsetChipLabel(OFFSET_LATEST)).toBe("latest");
	});

	it("passes short cursors through unchanged", () => {
		expect(offsetChipLabel("42")).toBe("42");
		expect(offsetChipLabel("abc123def456")).toBe("abc123def456");
	});

	it("middle-truncates a long opaque cursor", () => {
		expect(offsetChipLabel("0123456789abcdefghij")).toBe("012345…ghij");
	});
});
