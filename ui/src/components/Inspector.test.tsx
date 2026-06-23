import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { GridRow, HttpExchange, ReadResult } from "../lib/types";
import { lastRead, selectedRow } from "../state/store";
import { Inspector } from "./Inspector";

function makeExchange(): HttpExchange {
	return {
		method: "GET",
		url: "http://localhost:4437/v1/stream/orders",
		requestHeaders: {},
		status: 200,
		statusText: "OK",
		responseHeaders: { "content-type": "application/json", "stream-next-offset": "42" },
		protocol: {
			streamNextOffset: "42",
			streamClosed: null,
			streamUpToDate: null,
			etag: null,
			contentType: "application/json",
		},
		at: 0,
		durationMs: 1,
	};
}

function makeRow(): GridRow {
	return { index: 0, byteSize: 12, preview: '{"a":1}', kind: "json", value: { a: 1 } };
}

function makeRead(): ReadResult {
	return {
		path: "orders",
		kind: "json",
		requestedOffset: "-1",
		nextOffset: "42",
		closed: false,
		upToDate: false,
		rows: [makeRow()],
		rawBytes: new TextEncoder().encode('[{"a":1}]'),
		exchange: makeExchange(),
	};
}

beforeEach(() => {
	selectedRow.value = makeRow();
	lastRead.value = makeRead();
});

afterEach(() => {
	cleanup();
	selectedRow.value = null;
	lastRead.value = null;
});

describe("Inspector tabs", () => {
	it("wires each tab to its panel via aria-controls and the panel back via aria-labelledby", () => {
		render(<Inspector />);

		const selected = screen.getByRole("tab", { selected: true });
		const panel = screen.getByRole("tabpanel");
		const controls = selected.getAttribute("aria-controls");
		expect(controls).not.toBeNull();
		expect(panel.getAttribute("id")).toBe(controls);
		expect(panel.getAttribute("aria-labelledby")).toBe(selected.getAttribute("id"));
	});

	it("keeps exactly one tab in the Tab sequence (roving tabindex)", () => {
		render(<Inspector />);
		const tabs = screen.getAllByRole("tab");
		const tabbable = tabs.filter((t) => t.getAttribute("tabindex") === "0");
		expect(tabbable).toHaveLength(1);
		expect(tabbable[0]?.getAttribute("aria-selected")).toBe("true");
	});

	it("moves selection with ArrowRight / ArrowLeft", () => {
		render(<Inspector />);
		const tablist = screen.getByRole("tablist");

		// Normalize to Value first (activeTab is a module-level signal).
		fireEvent.click(screen.getByRole("tab", { name: "Value" }));
		expect(screen.getByRole("tab", { selected: true }).textContent).toBe("Value");

		fireEvent.keyDown(screen.getByRole("tab", { name: "Value" }), { key: "ArrowRight" });
		expect(screen.getByRole("tab", { selected: true }).textContent).toBe("Raw");

		fireEvent.keyDown(screen.getByRole("tab", { name: "Raw" }), { key: "ArrowLeft" });
		expect(screen.getByRole("tab", { selected: true }).textContent).toBe("Value");

		// ArrowLeft from the first tab wraps to the last.
		fireEvent.keyDown(screen.getByRole("tab", { name: "Value" }), { key: "ArrowLeft" });
		expect(screen.getByRole("tab", { selected: true }).textContent).toBe("Headers");
		expect(tablist).toBeTruthy();
	});
});
