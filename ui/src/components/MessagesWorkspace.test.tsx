import { cleanup, fireEvent, render, screen, within } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { GridRow, HttpExchange, ReadResult, StreamInfo } from "../lib/types";
import {
	activeConnectionId,
	connections,
	lastRead,
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
	selectedRow.value = null;
	rowsTruncated.value = false;
});

describe("MessagesWorkspace grid", () => {
	it("exposes the rows as a single-select listbox with one option per row", () => {
		render(<MessagesWorkspace />);
		const list = screen.getByRole("listbox", { name: "Message rows" });
		expect(within(list).getAllByRole("option")).toHaveLength(3);
		// The active (first) row is reflected as the selected option.
		const selected = within(list).getAllByRole("option", { selected: true });
		expect(selected).toHaveLength(1);
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
