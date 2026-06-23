import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { StreamInfo } from "../lib/types";
import {
	activeConnectionId,
	commandPaletteOpen,
	connections,
	selectedStreamPath,
	streams,
	subscriptionIds,
} from "../state/store";
import { CommandPalette } from "./CommandPalette";

// The palette is pure layout over store actions; in a render test we only care
// that it dispatches the right action, so stub the ones we assert on and keep
// the real signals (commandPaletteOpen, streams, …) so state behaves normally.
vi.mock("../state/store", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../state/store")>();
	return {
		...actual,
		selectStream: vi.fn(),
		selectSubscription: vi.fn(),
		openCreateDialog: vi.fn(),
		cycleTheme: vi.fn(),
	};
});

import { cycleTheme, openCreateDialog, selectStream, selectSubscription } from "../state/store";

function makeStream(path: string): StreamInfo {
	return { path, contentType: "application/json", kind: "json", createdAt: null, manual: false };
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
	streams.value = [makeStream("orders/created"), makeStream("users/signups")];
	subscriptionIds.value = ["orders-sub"];
	selectedStreamPath.value = null;
	commandPaletteOpen.value = true;
}

beforeEach(() => {
	seed();
});

afterEach(() => {
	cleanup();
	vi.clearAllMocks();
	connections.value = [];
	activeConnectionId.value = null;
	streams.value = [];
	subscriptionIds.value = [];
	selectedStreamPath.value = null;
	commandPaletteOpen.value = false;
});

describe("CommandPalette", () => {
	it("renders as a labelled modal dialog with a focused combobox search input", () => {
		render(<CommandPalette />);
		expect(screen.getByRole("dialog", { name: "Command palette" })).toBeTruthy();
		const input = screen.getByRole("combobox", { name: /search commands/i });
		expect(input).toBeTruthy();
		expect(document.activeElement).toBe(input);
	});

	it("lists core actions and every stream", () => {
		render(<CommandPalette />);
		expect(screen.getByRole("option", { name: /New stream/ })).toBeTruthy();
		expect(screen.getByRole("option", { name: /Toggle theme/ })).toBeTruthy();
		expect(screen.getByRole("option", { name: /orders\/created/ })).toBeTruthy();
		expect(screen.getByRole("option", { name: /users\/signups/ })).toBeTruthy();
	});

	it("substring-filters to a stream as the query is typed", () => {
		render(<CommandPalette />);
		fireEvent.input(screen.getByRole("combobox"), { target: { value: "signups" } });
		const options = screen.getAllByRole("option");
		expect(options).toHaveLength(1);
		expect(options[0]?.textContent).toContain("users/signups");
	});

	it("shows an empty state when nothing matches", () => {
		render(<CommandPalette />);
		fireEvent.input(screen.getByRole("combobox"), { target: { value: "zzzzz" } });
		expect(screen.queryAllByRole("option")).toHaveLength(0);
		expect(screen.getByText("No matching commands")).toBeTruthy();
	});

	it("moves the active option with ArrowDown (aria-selected roving)", () => {
		render(<CommandPalette />);
		const input = screen.getByRole("combobox");
		const before = screen.getAllByRole("option");
		expect(before[0]?.getAttribute("aria-selected")).toBe("true");
		fireEvent.keyDown(input, { key: "ArrowDown" });
		const after = screen.getAllByRole("option");
		expect(after[0]?.getAttribute("aria-selected")).toBe("false");
		expect(after[1]?.getAttribute("aria-selected")).toBe("true");
	});

	it("runs the active command on Enter and closes the palette", () => {
		render(<CommandPalette />);
		const input = screen.getByRole("combobox");
		fireEvent.input(input, { target: { value: "orders/created" } });
		fireEvent.keyDown(input, { key: "Enter" });
		expect(selectStream).toHaveBeenCalledWith("orders/created");
		expect(commandPaletteOpen.value).toBe(false);
	});

	it("runs a command on click", () => {
		render(<CommandPalette />);
		fireEvent.click(screen.getByRole("option", { name: /orders\/created/ }));
		expect(selectStream).toHaveBeenCalledWith("orders/created");
	});

	it("invokes the New stream action and closes", () => {
		render(<CommandPalette />);
		fireEvent.click(screen.getByRole("option", { name: /New stream/ }));
		expect(openCreateDialog).toHaveBeenCalledTimes(1);
		expect(commandPaletteOpen.value).toBe(false);
	});

	it("invokes the theme cycle action", () => {
		render(<CommandPalette />);
		fireEvent.click(screen.getByRole("option", { name: /Toggle theme/ }));
		expect(cycleTheme).toHaveBeenCalledTimes(1);
	});

	it("lists tracked subscriptions and invokes selectSubscription on click", () => {
		render(<CommandPalette />);
		const sub = screen.getByRole("option", { name: /orders-sub/ });
		expect(sub).toBeTruthy();
		fireEvent.click(sub);
		expect(selectSubscription).toHaveBeenCalledWith("orders-sub");
		expect(commandPaletteOpen.value).toBe(false);
	});

	it("keeps focus on the search input after typing and arrowing", () => {
		render(<CommandPalette />);
		const input = screen.getByRole("combobox");
		fireEvent.input(input, { target: { value: "orders" } });
		expect(document.activeElement).toBe(input);
		fireEvent.keyDown(input, { key: "ArrowDown" });
		expect(document.activeElement).toBe(input);
	});

	it("closes via the Modal shell on Escape", () => {
		render(<CommandPalette />);
		expect(commandPaletteOpen.value).toBe(true);
		fireEvent.keyDown(document, { key: "Escape" });
		expect(commandPaletteOpen.value).toBe(false);
	});
});
