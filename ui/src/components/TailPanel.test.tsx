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

	it("surfaces the error status assertively", () => {
		tailStatus.value = { state: "error", message: "the SSE connection closed" };
		render(<TailPanel />);
		const status = screen.getByRole("status");
		const text = status.textContent ?? "";
		expect(text).toContain("Error");
		expect(text).toContain("the SSE connection closed");
		expect(status.getAttribute("aria-live")).toBe("assertive");
	});
});
