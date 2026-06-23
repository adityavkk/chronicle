import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Connection } from "../lib/types";
import { activeConnectionId, connections, switcherOpen } from "../state/store";
import { ConnectionManager } from "./ConnectionManager";

const CONNS: Connection[] = [
	{
		id: "c1",
		name: "Local",
		baseUrl: "http://localhost:4437",
		streamRoot: "/v1/stream",
		createdAt: 0,
		lastUsedAt: null,
	},
	{
		id: "c2",
		name: "Staging",
		baseUrl: "https://staging.example.com",
		streamRoot: "/v1/stream",
		createdAt: 0,
		lastUsedAt: null,
	},
];

beforeEach(() => {
	connections.value = CONNS;
	activeConnectionId.value = "c1";
	switcherOpen.value = false;
});

afterEach(() => {
	cleanup();
	connections.value = [];
	activeConnectionId.value = null;
	switcherOpen.value = false;
});

/** Open the switcher popover and return its trigger button. */
function openSwitcher(): HTMLElement {
	const trigger = screen.getByTitle("Switch connection");
	fireEvent.click(trigger);
	return trigger;
}

/** Index into a queried element list with a non-undefined result (strict TS). */
function at(els: HTMLElement[], i: number): HTMLElement {
	const el = els[i];
	if (el === undefined) throw new Error(`expected an element at index ${i}`);
	return el;
}

describe("ConnectionManager switcher", () => {
	it("opens a menu listing the saved connections plus actions", () => {
		render(<ConnectionManager />);
		openSwitcher();
		expect(screen.getByRole("menu", { name: "Connections" })).toBeTruthy();
		expect(screen.getAllByRole("menuitemradio")).toHaveLength(2);
		expect(screen.getByRole("menuitem", { name: /New connection/ })).toBeTruthy();
		expect(screen.getByRole("menuitem", { name: /Disconnect/ })).toBeTruthy();
	});

	describe("keyboard operability (ARIA menu pattern)", () => {
		it("moves focus to the first item on open, with a roving tabindex", () => {
			render(<ConnectionManager />);
			openSwitcher();
			const rows = screen.getAllByRole("menuitemradio");
			expect(document.activeElement).toBe(at(rows, 0));
			expect(at(rows, 0).tabIndex).toBe(0);
			expect(at(rows, 1).tabIndex).toBe(-1);
		});

		it("cycles connection rows and actions with ArrowDown/ArrowUp/Home/End", () => {
			render(<ConnectionManager />);
			openSwitcher();
			const rows = screen.getAllByRole("menuitemradio"); // [c1, c2]
			const actions = screen.getAllByRole("menuitem"); // [New, Disconnect]
			const order = [at(rows, 0), at(rows, 1), at(actions, 0), at(actions, 1)];
			const last = order.length - 1;

			for (let i = 0; i < last; i++) {
				fireEvent.keyDown(at(order, i), { key: "ArrowDown" });
				expect(document.activeElement).toBe(at(order, i + 1));
			}
			// ArrowDown from the last item wraps to the first.
			fireEvent.keyDown(at(order, last), { key: "ArrowDown" });
			expect(document.activeElement).toBe(at(order, 0));
			// ArrowUp from the first wraps to the last.
			fireEvent.keyDown(at(order, 0), { key: "ArrowUp" });
			expect(document.activeElement).toBe(at(order, last));

			fireEvent.keyDown(document.activeElement as HTMLElement, { key: "Home" });
			expect(document.activeElement).toBe(at(order, 0));
			fireEvent.keyDown(at(order, 0), { key: "End" });
			expect(document.activeElement).toBe(at(order, last));
		});

		it("traps Tab so focus cannot escape the open menu", () => {
			render(<ConnectionManager />);
			openSwitcher();
			const menu = screen.getByRole("menu", { name: "Connections" });
			const actions = screen.getAllByRole("menuitem");
			const last = at(actions, actions.length - 1);
			last.focus();

			fireEvent.keyDown(last, { key: "Tab" });
			expect(menu.contains(document.activeElement)).toBe(true);
			fireEvent.keyDown(document.activeElement as HTMLElement, { key: "Tab", shiftKey: true });
			expect(menu.contains(document.activeElement)).toBe(true);
		});

		it("closes on Escape and restores focus to the trigger", () => {
			render(<ConnectionManager />);
			const trigger = openSwitcher();
			expect(screen.queryByRole("menu", { name: "Connections" })).not.toBeNull();

			fireEvent.keyDown(document.activeElement as HTMLElement, { key: "Escape" });
			expect(screen.queryByRole("menu", { name: "Connections" })).toBeNull();
			expect(document.activeElement).toBe(trigger);
		});
	});
});
