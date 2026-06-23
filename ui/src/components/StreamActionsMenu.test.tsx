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
});
