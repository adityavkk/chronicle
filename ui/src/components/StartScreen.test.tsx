import { cleanup, fireEvent, render, screen, within } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Connection, ProbeStatus } from "../lib/types";
import {
	activeConnectionId,
	connections,
	probeStatuses,
	setActiveConnection,
} from "../state/store";
import { StartScreen } from "./StartScreen";

vi.mock("../state/store", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../state/store")>();
	// The real setActiveConnection fans out to a network probe + stream refresh,
	// and the screen re-probes everything on mount (which would overwrite the
	// probe statuses we seed). Stub the network-bound actions so the render is
	// deterministic; we only assert on rendering + the active-id flip.
	return {
		...actual,
		setActiveConnection: vi.fn((id: string | null) => {
			actual.activeConnectionId.value = id;
		}),
		probeAllConnections: vi.fn(async () => {}),
		probeConnection: vi.fn(async () => {}),
	};
});

function conn(id: string, name: string, baseUrl: string): Connection {
	return { id, name, baseUrl, streamRoot: "/v1/stream", createdAt: 0, lastUsedAt: null };
}

function setProbe(id: string, status: ProbeStatus): void {
	probeStatuses.value = { ...probeStatuses.value, [id]: status };
}

beforeEach(() => {
	// probeAllConnections fires on mount; keep it off the real network.
	vi.stubGlobal(
		"fetch",
		vi.fn(async () => new Response(null, { status: 200 })),
	);
	connections.value = [];
	activeConnectionId.value = null;
	probeStatuses.value = {};
});

afterEach(() => {
	cleanup();
	vi.clearAllMocks();
	vi.unstubAllGlobals();
	connections.value = [];
	activeConnectionId.value = null;
	probeStatuses.value = {};
});

describe("StartScreen connection cards", () => {
	it("opens straight to the new-connection form when nothing is saved", () => {
		connections.value = [];
		render(<StartScreen />);
		expect(screen.getByRole("region", { name: "New connection" })).toBeTruthy();
		expect(screen.queryByRole("region", { name: "Saved connections" })).toBeNull();
	});

	it("lists every saved connection as a card with its name, url, and probe text", () => {
		connections.value = [
			conn("c1", "Local dev", "http://localhost:4437"),
			conn("c2", "Staging", "https://streams.staging.example.com"),
		];
		setProbe("c1", { state: "done", probe: { ok: true, status: 200, latencyMs: 12 } });
		setProbe("c2", { state: "checking" });
		render(<StartScreen />);

		const list = screen.getByRole("list", { name: "Saved connections" });
		const cards = within(list).getAllByRole("listitem");
		expect(cards).toHaveLength(2);

		expect(screen.getByText("Local dev")).toBeTruthy();
		expect(screen.getByText("Staging")).toBeTruthy();
		// Compact url drops the scheme.
		expect(screen.getByText("localhost:4437")).toBeTruthy();
		// Probe text reflects each connection's live status.
		expect(screen.getByText(/Reachable · HTTP 200 · 12 ms/)).toBeTruthy();
		expect(screen.getByText("Checking…")).toBeTruthy();
	});

	it("activates a connection when its card is clicked", () => {
		connections.value = [conn("c1", "Local dev", "http://localhost:4437")];
		render(<StartScreen />);

		fireEvent.click(screen.getByRole("button", { name: /Connect to Local dev/ }));
		expect(setActiveConnection).toHaveBeenCalledWith("c1");
		expect(activeConnectionId.value).toBe("c1");
	});

	it("requires a second confirmation step before deleting a connection", () => {
		connections.value = [conn("c1", "Local dev", "http://localhost:4437")];
		render(<StartScreen />);

		// No destructive button is visible until the user arms it.
		expect(screen.queryByRole("button", { name: "Delete" })).toBeNull();
		fireEvent.click(screen.getByRole("button", { name: "Delete Local dev" }));
		// Now the confirm/cancel pair appears.
		expect(screen.getByRole("button", { name: "Delete" })).toBeTruthy();
		expect(screen.getByRole("button", { name: "Keep" })).toBeTruthy();
	});

	it("toggles between the saved list and the new-connection form", () => {
		connections.value = [conn("c1", "Local dev", "http://localhost:4437")];
		render(<StartScreen />);

		expect(screen.getByRole("region", { name: "Saved connections" })).toBeTruthy();
		fireEvent.click(screen.getByRole("button", { name: /New connection/ }));
		expect(screen.getByRole("region", { name: "New connection" })).toBeTruthy();
	});
});
