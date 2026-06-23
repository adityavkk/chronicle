import { cleanup, fireEvent, render, screen, within } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { GridRow } from "../lib/types";
import {
	activeConnectionId,
	connections,
	selectedStreamPath,
	streams,
	tailMode,
	tailPaused,
	tailRows,
	tailStatus,
} from "../state/store";
import { TailPanel } from "./TailPanel";

function makeRow(index: number): GridRow {
	return {
		index,
		byteSize: 12,
		preview: `live ${index}`,
		kind: "text",
		value: `live ${index}`,
	};
}

function seed(): void {
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
	streams.value = [
		{ path: "orders", contentType: "text/plain", kind: "text", createdAt: null, manual: false },
	];
	selectedStreamPath.value = "orders";
	tailMode.value = "sse";
	tailStatus.value = { state: "live", atOffset: "42" };
	tailRows.value = [makeRow(0), makeRow(1)];
	tailPaused.value = false;
}

beforeEach(seed);

afterEach(() => {
	cleanup();
	connections.value = [];
	activeConnectionId.value = null;
	streams.value = [];
	selectedStreamPath.value = null;
	tailMode.value = "catchup";
	tailStatus.value = { state: "idle" };
	tailRows.value = [];
	tailPaused.value = false;
});

describe("TailPanel", () => {
	it("announces the live status through an aria-live status region", () => {
		render(<TailPanel />);
		const status = screen.getByRole("status");
		expect(status.textContent ?? "").toContain("Live");
		// "live" connecting/connected announces politely.
		expect(status.getAttribute("aria-live")).toBe("polite");
	});

	it("renders received rows as a single-select listbox of live options", () => {
		render(<TailPanel />);
		const list = screen.getByRole("listbox", { name: "Live message rows" });
		expect(within(list).getAllByRole("option")).toHaveLength(2);
	});

	it("offers Pause + Stop while connected and Clear", () => {
		render(<TailPanel />);
		expect(screen.getByRole("button", { name: /Pause/ })).toBeTruthy();
		expect(screen.getByRole("button", { name: /Stop/ })).toBeTruthy();
		expect(screen.getByRole("button", { name: /Clear/ })).toBeTruthy();
	});

	it("toggles the store's tailPaused when Pause is pressed", () => {
		render(<TailPanel />);
		const pause = screen.getByRole("button", { name: /Pause/ });
		expect(pause.getAttribute("aria-pressed")).toBe("false");
		fireEvent.click(pause);
		expect(tailPaused.value).toBe(true);
	});

	it("shows a Start affordance (not Stop) when the tail is idle", () => {
		tailStatus.value = { state: "idle" };
		tailRows.value = [];
		render(<TailPanel />);
		expect(screen.getByRole("button", { name: /Start tail/ })).toBeTruthy();
		expect(screen.queryByRole("button", { name: /^Stop$/ })).toBeNull();
	});

	it("windows the live list above the threshold, rendering only a visible slice", () => {
		// jsdom computes no layout, so windowRange would render everything (a
		// non-measurable viewport falls back to the full range). Give the scroller
		// a real clientHeight, then scroll to engage the fixed-height windowing.
		tailRows.value = Array.from({ length: 300 }, (_, i) => makeRow(i));
		const { container } = render(<TailPanel />);
		const scroller = container.querySelector(".dsui-tail__rows") as HTMLElement;
		Object.defineProperty(scroller, "clientHeight", { value: 300, configurable: true });
		Object.defineProperty(scroller, "scrollHeight", { value: 300 * 30, configurable: true });
		fireEvent.scroll(scroller);
		const options = screen.getAllByRole("option");
		// Only a small slice of the 300 rows is in the DOM.
		expect(options.length).toBeGreaterThan(0);
		expect(options.length).toBeLessThan(60);
		// The spacers stand in for the windowed-out rows, preserving scroll height.
		const body = screen.getByRole("listbox", { name: "Live message rows" });
		expect(body.style.paddingBlockEnd).not.toBe("");
		expect(body.style.paddingBlockEnd).not.toBe("0px");
	});

	it("renders every row at or below the windowing threshold", () => {
		tailRows.value = Array.from({ length: 30 }, (_, i) => makeRow(i));
		render(<TailPanel />);
		expect(screen.getAllByRole("option")).toHaveLength(30);
	});

	it("surfaces the error status assertively", () => {
		tailStatus.value = { state: "error", message: "the SSE connection closed" };
		render(<TailPanel />);
		const status = screen.getByRole("status");
		const text = status.textContent ?? "";
		expect(text).toContain("Error");
		expect(text).toContain("the SSE connection closed");
		expect(status.getAttribute("aria-live")).toBe("assertive");
	});

	it("filters the live buffer without dropping the connection and keeps seq honest", () => {
		render(<TailPanel />);
		expect(within(screen.getByRole("listbox")).getAllByRole("option")).toHaveLength(2);

		const input = screen.getByRole("searchbox", { name: "Filter live messages" });
		fireEvent.input(input, { target: { value: "live 1" } });

		const options = within(screen.getByRole("listbox")).getAllByRole("option");
		expect(options).toHaveLength(1);
		// Filtering hides row 0; the surviving row keeps its true arrival number
		// (seq 1 = dropped 0 + buffer position 1), not a re-numbered 0.
		expect(options[0]?.getAttribute("aria-label")).toContain("Live message 1");
		// The store buffer is untouched — filtering is purely a view concern.
		expect(tailRows.value).toHaveLength(2);
		expect(tailStatus.value.state).toBe("live");
	});

	it("shows a 'showing N of M' count and a no-match note with a clear", () => {
		render(<TailPanel />);
		const input = screen.getByRole("searchbox", { name: "Filter live messages" });

		fireEvent.input(input, { target: { value: "live 1" } });
		expect(screen.getByText(/showing 1 of 2/)).toBeTruthy();

		fireEvent.input(input, { target: { value: "no-such-row" } });
		expect(screen.queryByRole("listbox")).toBeNull();
		expect(screen.getByText("No messages match the filter")).toBeTruthy();

		fireEvent.input(input, { target: { value: "" } });
		expect(within(screen.getByRole("listbox")).getAllByRole("option")).toHaveLength(2);
	});
});
