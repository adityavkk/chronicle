import { cleanup, fireEvent, render, screen, within } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { StreamInfo } from "../lib/types";
import {
	activeConnectionId,
	connections,
	errorMessage,
	selectStream,
	selectedStreamPath,
	streams,
	streamsLoading,
} from "../state/store";
import { Navigator } from "./Navigator";

vi.mock("../state/store", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../state/store")>();
	// selectStream kicks off a network read via the active client; in a render
	// test we only care that it records the selection, so stub the read away
	// and just mark the path as selected.
	return {
		...actual,
		selectStream: vi.fn((path: string) => {
			actual.selectedStreamPath.value = path;
		}),
	};
});

function makeStream(path: string, kind: StreamInfo["kind"] = "json"): StreamInfo {
	return { path, contentType: null, kind, createdAt: null, manual: false };
}

/** Seed the store with an active connection and a fixed set of streams. */
function seedStore(streamList: StreamInfo[]): void {
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
	streams.value = streamList;
	selectedStreamPath.value = null;
	streamsLoading.value = false;
	errorMessage.value = null;
}

beforeEach(() => {
	seedStore([]);
});

afterEach(() => {
	cleanup();
	vi.clearAllMocks();
	// Reset signals so tests do not bleed into each other.
	connections.value = [];
	activeConnectionId.value = null;
	streams.value = [];
	selectedStreamPath.value = null;
	streamsLoading.value = false;
	errorMessage.value = null;
});

describe("Navigator", () => {
	it("renders one tree item per stream with its kind tag and a count", () => {
		seedStore([
			makeStream("logs/app", "text"),
			makeStream("orders/created", "json"),
			makeStream("blobs/raw", "binary"),
		]);
		render(<Navigator />);

		const tree = screen.getByRole("tree", { name: "Streams" });
		const items = within(tree).getAllByRole("treeitem");
		expect(items).toHaveLength(3);
		expect(screen.getByText("orders/created")).toBeTruthy();
		// The streams section header shows the total count.
		const header = screen.getByText("Streams").closest("header");
		expect(header?.textContent).toContain("3");
	});

	it("filters the tree instantly as the user types, never touching the store", () => {
		seedStore([makeStream("orders/created"), makeStream("orders/shipped"), makeStream("logs/app")]);
		render(<Navigator />);

		expect(screen.getAllByRole("treeitem")).toHaveLength(3);

		const filter = screen.getByRole("searchbox", { name: "Filter streams" });
		fireEvent.input(filter, { target: { value: "orders" } });

		const items = screen.getAllByRole("treeitem");
		expect(items).toHaveLength(2);
		expect(items.map((el) => el.textContent)).toEqual(
			expect.arrayContaining([expect.stringContaining("orders/created")]),
		);
		expect(screen.queryByText("logs/app")).toBeNull();
		// The unfiltered total (3) is still shown in the header count.
		expect(streams.value).toHaveLength(3);
	});

	it("matches the filter case-insensitively on a substring", () => {
		seedStore([makeStream("Orders/Created"), makeStream("logs/app")]);
		render(<Navigator />);

		fireEvent.input(screen.getByRole("searchbox", { name: "Filter streams" }), {
			target: { value: "ORDER" },
		});
		expect(screen.getAllByRole("treeitem")).toHaveLength(1);
		expect(screen.getByText("Orders/Created")).toBeTruthy();
	});

	it("shows the no-match placeholder when the filter excludes everything", () => {
		seedStore([makeStream("orders/created")]);
		render(<Navigator />);

		fireEvent.input(screen.getByRole("searchbox", { name: "Filter streams" }), {
			target: { value: "zzz-nothing" },
		});
		expect(screen.queryByRole("tree")).toBeNull();
		expect(screen.getByText("No matches")).toBeTruthy();
	});

	it("shows the empty-discovery placeholder pointing at the Playground when there are no streams", () => {
		seedStore([]);
		render(<Navigator />);
		expect(screen.getByText("No streams yet")).toBeTruthy();
		// The first-run hint points a newcomer at the Playground presets.
		expect(screen.getByRole("button", { name: "Start with the Playground" })).toBeTruthy();
	});

	it("surfaces a list error as an alert with a retry affordance", () => {
		seedStore([]);
		errorMessage.value = "registry unreachable";
		render(<Navigator />);

		const alert = screen.getByRole("alert");
		expect(alert.textContent).toContain("Could not list streams");
		expect(alert.textContent).toContain("registry unreachable");
		expect(within(alert).getByText("Try again")).toBeTruthy();
	});

	it("marks the selected stream with aria-selected and a roving tabindex", () => {
		seedStore([makeStream("orders/created"), makeStream("logs/app")]);
		selectedStreamPath.value = "orders/created";
		render(<Navigator />);

		const items = screen.getAllByRole("treeitem");
		const selected = items.find((el) => el.getAttribute("aria-selected") === "true");
		expect(selected?.textContent).toContain("orders/created");
		// Only the active item is tab-reachable (roving tabindex).
		const activeBtn = within(selected as HTMLElement).getByRole("button");
		expect(activeBtn.getAttribute("tabindex")).toBe("0");
	});

	it("keeps the tree keyboard-reachable when nothing is selected (one tab stop on the first item)", () => {
		seedStore([makeStream("orders/created"), makeStream("logs/app"), makeStream("blobs/raw")]);
		selectedStreamPath.value = null;
		render(<Navigator />);

		const buttons = screen
			.getAllByRole("treeitem")
			.map((el) => within(el as HTMLElement).getByRole("button"));
		// The WAI-ARIA tree pattern requires exactly one tab stop at all times.
		const tabbable = buttons.filter((b) => b.getAttribute("tabindex") === "0");
		expect(tabbable).toHaveLength(1);
		// With no selection it is the first visible item, so a keyboard user can Tab in.
		expect(tabbable[0]).toBe(buttons[0]);
	});

	it("keeps the single tree tab stop on the selected item once one exists", () => {
		seedStore([makeStream("orders/created"), makeStream("logs/app")]);
		selectedStreamPath.value = "logs/app";
		render(<Navigator />);

		const buttons = screen
			.getAllByRole("treeitem")
			.map((el) => within(el as HTMLElement).getByRole("button"));
		const tabbable = buttons.filter((b) => b.getAttribute("tabindex") === "0");
		expect(tabbable).toHaveLength(1);
		expect(tabbable[0]?.textContent).toContain("logs/app");
	});

	it("invokes the store's selectStream action when a tree item is clicked", () => {
		seedStore([makeStream("orders/created")]);
		render(<Navigator />);

		fireEvent.click(screen.getByText("orders/created"));
		expect(selectStream).toHaveBeenCalledWith("orders/created");
	});

	it("disables the filter and refresh controls when there is no active connection", () => {
		connections.value = [];
		activeConnectionId.value = null;
		streams.value = [];
		render(<Navigator />);

		expect(
			(screen.getByRole("searchbox", { name: "Filter streams" }) as HTMLInputElement).disabled,
		).toBe(true);
		expect(
			(screen.getByRole("button", { name: "Refresh streams" }) as HTMLButtonElement).disabled,
		).toBe(true);
	});
});
