import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { StreamInfo } from "../lib/types";
import { activeConnectionId, connections } from "../state/store";
import { StreamActionsMenu } from "./StreamActionsMenu";

vi.mock("../state/store", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../state/store")>();
	return {
		...actual,
		openForkDialog: vi.fn(),
		refreshMeta: vi.fn(async () => {}),
		closeStream: vi.fn(async () => true),
		deleteStream: vi.fn(async () => true),
	};
});

import { closeStream, deleteStream, openForkDialog } from "../state/store";

const STREAM: StreamInfo = {
	path: "orders/created",
	contentType: "application/json",
	kind: "json",
	createdAt: null,
	manual: false,
};

/** Index into a queried element list with a non-undefined result (strict TS). */
function at(els: HTMLElement[], i: number): HTMLElement {
	const el = els[i];
	if (el === undefined) throw new Error(`expected an element at index ${i}`);
	return el;
}

beforeEach(() => {
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
});

afterEach(() => {
	cleanup();
	vi.clearAllMocks();
	connections.value = [];
	activeConnectionId.value = null;
});

describe("StreamActionsMenu", () => {
	it("opens a menu of lifecycle actions for the stream", () => {
		render(<StreamActionsMenu stream={STREAM} />);
		fireEvent.click(screen.getByRole("button", { name: "Stream actions" }));
		const menu = screen.getByRole("menu", { name: /Actions for orders\/created/ });
		expect(menu).toBeTruthy();
		expect(screen.getByRole("menuitem", { name: /Fork/ })).toBeTruthy();
		expect(screen.getByRole("menuitem", { name: /Refresh metadata/ })).toBeTruthy();
		expect(screen.getByRole("menuitem", { name: /Close stream/ })).toBeTruthy();
	});

	it("opens the fork dialog seeded from the stream + default offset", () => {
		render(<StreamActionsMenu stream={STREAM} />);
		fireEvent.click(screen.getByRole("button", { name: "Stream actions" }));
		fireEvent.click(screen.getByRole("menuitem", { name: /Fork/ }));
		expect(openForkDialog).toHaveBeenCalledWith("orders/created", "now");
	});

	it("dispatches closeStream from the menu", () => {
		render(<StreamActionsMenu stream={STREAM} />);
		fireEvent.click(screen.getByRole("button", { name: "Stream actions" }));
		fireEvent.click(screen.getByRole("menuitem", { name: /Close stream/ }));
		expect(closeStream).toHaveBeenCalledWith("orders/created");
	});

	it("requires a confirm before deleting", () => {
		render(<StreamActionsMenu stream={STREAM} />);
		fireEvent.click(screen.getByRole("button", { name: "Stream actions" }));
		// First the destructive entry only reveals a confirm, it does not delete.
		fireEvent.click(screen.getByRole("menuitem", { name: /Delete stream/ }));
		expect(deleteStream).not.toHaveBeenCalled();
		// The confirm button then commits the delete.
		fireEvent.click(screen.getByRole("button", { name: "Delete" }));
		expect(deleteStream).toHaveBeenCalledWith("orders/created");
	});

	describe("keyboard operability (ARIA menu pattern)", () => {
		it("moves focus to the first item on open, with a roving tabindex", () => {
			render(<StreamActionsMenu stream={STREAM} />);
			fireEvent.click(screen.getByRole("button", { name: "Stream actions" }));
			const items = screen.getAllByRole("menuitem");
			expect(document.activeElement).toBe(at(items, 0));
			expect(at(items, 0).tabIndex).toBe(0);
			expect(items.slice(1).every((el) => el.tabIndex === -1)).toBe(true);
		});

		it("cycles items with ArrowDown/ArrowUp (wrapping) and Home/End", () => {
			render(<StreamActionsMenu stream={STREAM} />);
			fireEvent.click(screen.getByRole("button", { name: "Stream actions" }));
			const items = screen.getAllByRole("menuitem");
			const last = items.length - 1;

			fireEvent.keyDown(at(items, 0), { key: "ArrowDown" });
			expect(document.activeElement).toBe(at(items, 1));
			expect(at(items, 1).tabIndex).toBe(0);
			expect(at(items, 0).tabIndex).toBe(-1);

			// ArrowUp from the first item wraps to the last.
			fireEvent.keyDown(at(items, 1), { key: "ArrowUp" });
			fireEvent.keyDown(at(items, 0), { key: "ArrowUp" });
			expect(document.activeElement).toBe(at(items, last));

			fireEvent.keyDown(at(items, last), { key: "Home" });
			expect(document.activeElement).toBe(at(items, 0));
			fireEvent.keyDown(at(items, 0), { key: "End" });
			expect(document.activeElement).toBe(at(items, last));
		});

		it("traps Tab so focus cannot escape the open menu", () => {
			render(<StreamActionsMenu stream={STREAM} />);
			fireEvent.click(screen.getByRole("button", { name: "Stream actions" }));
			const menu = screen.getByRole("menu", { name: /Actions for orders\/created/ });
			const items = screen.getAllByRole("menuitem");
			const last = at(items, items.length - 1);
			last.focus();

			fireEvent.keyDown(last, { key: "Tab" });
			expect(menu.contains(document.activeElement)).toBe(true);
			fireEvent.keyDown(document.activeElement as HTMLElement, { key: "Tab", shiftKey: true });
			expect(menu.contains(document.activeElement)).toBe(true);
		});

		it("keeps focus inside the menu through the delete-confirm sub-flow", () => {
			render(<StreamActionsMenu stream={STREAM} />);
			fireEvent.click(screen.getByRole("button", { name: "Stream actions" }));
			const menu = screen.getByRole("menu", { name: /Actions for orders\/created/ });
			fireEvent.click(screen.getByRole("menuitem", { name: /Delete stream/ }));

			// Opening the confirm moves focus to Cancel — still inside the menu,
			// never falling back to <body>.
			const cancel = screen.getByRole("button", { name: "Cancel" });
			expect(document.activeElement).toBe(cancel);
			expect(menu.contains(document.activeElement)).toBe(true);

			// Cancelling returns focus to the Delete menuitem.
			fireEvent.click(cancel);
			expect(document.activeElement).toBe(screen.getByRole("menuitem", { name: /Delete stream/ }));
		});

		it("closes on Escape and restores focus to the trigger", () => {
			render(<StreamActionsMenu stream={STREAM} />);
			const trigger = screen.getByRole("button", { name: "Stream actions" });
			fireEvent.click(trigger);
			expect(screen.queryByRole("menu", { name: /Actions for orders\/created/ })).not.toBeNull();

			fireEvent.keyDown(document.activeElement as HTMLElement, { key: "Escape" });
			expect(screen.queryByRole("menu", { name: /Actions for orders\/created/ })).toBeNull();
			expect(document.activeElement).toBe(trigger);
		});
	});
});
