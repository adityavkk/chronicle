import { cleanup, fireEvent, render, screen, within } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { GridRow, HttpExchange, ReadHistoryEntry, ReadResult, StreamInfo } from "../lib/types";
import {
	activeConnectionId,
	connections,
	lastRead,
	readHistory,
	rowsTruncated,
	selectedRow,
	selectedStreamPath,
	streams,
} from "../state/store";
import { MessagesWorkspace } from "./MessagesWorkspace";

function makeExchange(): HttpExchange {
	return {
		method: "GET",
		url: "http://localhost:4437/v1/stream/orders",
		requestHeaders: {},
		status: 200,
		statusText: "OK",
		responseHeaders: {},
		protocol: {
			streamNextOffset: "42",
			streamClosed: null,
			streamUpToDate: null,
			etag: null,
			contentType: "text/plain",
		},
		at: 0,
		durationMs: 1,
	};
}

function makeRow(index: number): GridRow {
	return { index, byteSize: 10, preview: `line ${index}`, kind: "text", value: `line ${index}` };
}

function makeRead(count: number): ReadResult {
	const rows = Array.from({ length: count }, (_, i) => makeRow(i));
	return {
		path: "orders",
		kind: "text",
		requestedOffset: "-1",
		nextOffset: "42",
		closed: false,
		upToDate: false,
		rows,
		rawBytes: new TextEncoder().encode("body"),
		exchange: makeExchange(),
	};
}

const stream: StreamInfo = {
	path: "orders",
	contentType: "text/plain",
	kind: "text",
	createdAt: null,
	manual: false,
};

function seed(count: number): void {
	connections.value = [
		{
			id: "c1",
			name: "Local",
			baseUrl: "http://localhost:4437",
			streamRoot: "/v1/stream",
			createdAt: 0,
			lastUsedAt: null,
		},
	];
	activeConnectionId.value = "c1";
	streams.value = [stream];
	selectedStreamPath.value = "orders";
	const read = makeRead(count);
	lastRead.value = read;
	selectedRow.value = read.rows[0] ?? null;
	rowsTruncated.value = false;
}

beforeEach(() => {
	seed(3);
});

afterEach(() => {
	cleanup();
	connections.value = [];
	activeConnectionId.value = null;
	streams.value = [];
	selectedStreamPath.value = null;
	lastRead.value = null;
	readHistory.value = [];
	selectedRow.value = null;
	rowsTruncated.value = false;
});

function historyEntry(over: Partial<ReadHistoryEntry> = {}): ReadHistoryEntry {
	return { path: "orders", requestedOffset: "-1", nextOffset: "42", rowCount: 3, at: 0, ...over };
}

describe("MessagesWorkspace grid", () => {
	it("exposes the rows as a single-select listbox with one option per row", () => {
		render(<MessagesWorkspace />);
		const list = screen.getByRole("listbox", { name: "Message rows" });
		expect(within(list).getAllByRole("option")).toHaveLength(3);
		// The active (first) row is reflected as the selected option.
		const selected = within(list).getAllByRole("option", { selected: true });
		expect(selected).toHaveLength(1);
	});

	it("shows rows that arrive AFTER mount (visibleRows stays reactive to lastRead)", async () => {
		// Reproduces the real-app sequence the other tests skip: the component mounts
		// BEFORE the async read lands. A useComputed that closed over the render-scoped
		// `read` const would freeze to this mount-time null and render "No rows match"
		// forever once the read arrived.
		lastRead.value = null;
		selectedRow.value = null;
		render(<MessagesWorkspace />);
		expect(screen.queryByRole("listbox", { name: "Message rows" })).toBeNull();

		// The read lands after mount.
		lastRead.value = makeRead(3);

		const list = await screen.findByRole("listbox", { name: "Message rows" });
		expect(within(list).getAllByRole("option")).toHaveLength(3);
		// And the (empty) filter must NOT report a no-match state.
		expect(screen.queryByText(/No rows match the filter/)).toBeNull();
	});

	it("keeps exactly one row in the Tab sequence (roving tabindex)", () => {
		render(<MessagesWorkspace />);
		// Scope to the listbox: the toolbar's Rows <select> also exposes native
		// <option> elements, which are not message rows.
		const list = screen.getByRole("listbox", { name: "Message rows" });
		const options = within(list).getAllByRole("option");
		const tabbable = options.filter((c) => c.getAttribute("tabindex") === "0");
		expect(tabbable).toHaveLength(1);
		// The active (first) row owns the tab stop.
		expect(tabbable[0]?.getAttribute("aria-selected")).toBe("true");
	});

	it("windows a large batch, rendering only a visible slice with one tab stop", () => {
		// A 1000-row batch (the max row cap) would be ~4–5k DOM nodes unwindowed.
		// jsdom has no layout, so give the scroller a real clientHeight and scroll
		// to engage the fixed-height windowing.
		seed(1000);
		const { container } = render(<MessagesWorkspace />);
		const scroller = container.querySelector(".dsui-grid__rows") as HTMLElement;
		Object.defineProperty(scroller, "clientHeight", { value: 300, configurable: true });
		Object.defineProperty(scroller, "scrollHeight", { value: 1000 * 30, configurable: true });
		fireEvent.scroll(scroller);

		const list = screen.getByRole("listbox", { name: "Message rows" });
		const options = within(list).getAllByRole("option");
		// Only a slice is rendered, not all 1000 rows.
		expect(options.length).toBeGreaterThan(0);
		expect(options.length).toBeLessThan(60);
		// Roving tabindex survives windowing: exactly one rendered row is tabbable.
		const tabbable = options.filter((o) => o.getAttribute("tabindex") === "0");
		expect(tabbable).toHaveLength(1);
		// The spacers preserve the scrollable height (sticky header stays put).
		expect(list.style.paddingBlockEnd).not.toBe("");
		expect(list.style.paddingBlockEnd).not.toBe("0px");
	});

	it("moves focus between rows with ArrowDown / ArrowUp / End", () => {
		render(<MessagesWorkspace />);
		const list = screen.getByRole("listbox", { name: "Message rows" });
		const options = within(list).getAllByRole("option");

		fireEvent.keyDown(options[0] as HTMLElement, { key: "ArrowDown" });
		expect(document.activeElement).toBe(options[1]);

		fireEvent.keyDown(options[1] as HTMLElement, { key: "ArrowUp" });
		expect(document.activeElement).toBe(options[0]);

		fireEvent.keyDown(options[0] as HTMLElement, { key: "End" });
		expect(document.activeElement).toBe(options[2]);
	});
});

describe("MessagesWorkspace filter", () => {
	function typeFilter(value: string): void {
		const input = screen.getByRole("searchbox", { name: "Filter messages" });
		fireEvent.input(input, { target: { value } });
	}

	it("narrows the visible rows to the matching subset", () => {
		render(<MessagesWorkspace />);
		expect(within(screen.getByRole("listbox")).getAllByRole("option")).toHaveLength(3);

		typeFilter("line 1");

		const options = within(screen.getByRole("listbox")).getAllByRole("option");
		expect(options).toHaveLength(1);
		// The batch-index # stays honest: the surviving row keeps its original
		// index (1), not a re-numbered filtered position.
		expect(options[0]?.getAttribute("aria-label")).toContain("Message 1");
	});

	it("shows a 'showing N of M' count while filtering", () => {
		render(<MessagesWorkspace />);
		typeFilter("line 1");
		expect(screen.getByText(/showing 1 of 3/)).toBeTruthy();
	});

	it("keeps exactly one tab stop over the filtered subset, even when the active row is hidden", () => {
		// The active (selected) row is index 0; filtering to "line 2" hides it.
		render(<MessagesWorkspace />);
		typeFilter("line 2");
		const options = within(screen.getByRole("listbox")).getAllByRole("option");
		expect(options).toHaveLength(1);
		// The lone visible row (index 2) must own the single tab stop so the list
		// stays reachable rather than orphaning the tab stop on the hidden row.
		expect(options[0]?.getAttribute("tabindex")).toBe("0");
		expect(options[0]?.getAttribute("aria-label")).toContain("Message 2");
	});

	it("shows a no-match note and a Clear control that restores every row", () => {
		render(<MessagesWorkspace />);
		typeFilter("nothing-matches-this");
		expect(screen.queryByRole("listbox")).toBeNull();
		expect(screen.getByText("No rows match the filter")).toBeTruthy();

		// Both the filter box and the no-match note offer a Clear control; either
		// restores the full batch.
		const clears = screen.getAllByRole("button", { name: "Clear filter" });
		fireEvent.click(clears[0] as HTMLElement);
		expect(within(screen.getByRole("listbox")).getAllByRole("option")).toHaveLength(3);
	});
});

describe("MessagesWorkspace read-cursor history", () => {
	it("renders no History strip until a position has been read", () => {
		readHistory.value = [];
		render(<MessagesWorkspace />);
		expect(screen.queryByRole("navigation", { name: "Read history" })).toBeNull();
	});

	it("renders a clickable chip per visited position, newest last", () => {
		readHistory.value = [
			historyEntry({ requestedOffset: "-1" }),
			historyEntry({ requestedOffset: "42" }),
			historyEntry({ requestedOffset: "now" }),
		];
		render(<MessagesWorkspace />);
		const strip = screen.getByRole("navigation", { name: "Read history" });
		const chips = within(strip).getAllByRole("button");
		expect(chips).toHaveLength(3);
		// Sentinel offsets read as words; the opaque cursor shows verbatim.
		expect(chips.map((c) => c.textContent)).toEqual(["earliest", "42", "latest"]);
	});

	it("marks only the newest chip as the current position", () => {
		readHistory.value = [
			historyEntry({ requestedOffset: "-1" }),
			historyEntry({ requestedOffset: "42" }),
		];
		render(<MessagesWorkspace />);
		const strip = screen.getByRole("navigation", { name: "Read history" });
		const current = within(strip)
			.getAllByRole("button")
			.filter((c) => c.getAttribute("aria-current") === "true");
		expect(current).toHaveLength(1);
		expect(current[0]?.textContent).toBe("42");
	});

	it("leads each chip's accessible name with its visible label (WCAG 2.5.3)", () => {
		readHistory.value = [
			historyEntry({ requestedOffset: "-1" }),
			historyEntry({ requestedOffset: "now" }),
		];
		render(<MessagesWorkspace />);
		// The visible word ("earliest"/"latest") must appear in the accessible
		// name so a speech-control user can target the chip by what they see.
		expect(screen.getByRole("button", { name: /^earliest, re-read/ })).toBeTruthy();
		expect(screen.getByRole("button", { name: /^latest, current position/ })).toBeTruthy();
	});

	it("offers a copy-cursor control on the batch-offset readout", () => {
		render(<MessagesWorkspace />);
		// The read seeded by `seed(3)` has requestedOffset "-1" and nextOffset "42".
		expect(screen.getByRole("button", { name: "Copy this batch's start cursor" })).toBeTruthy();
		expect(screen.getByRole("button", { name: "Copy the next-batch cursor" })).toBeTruthy();
	});
});
