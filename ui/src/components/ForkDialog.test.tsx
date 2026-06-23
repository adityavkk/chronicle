import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { GridRow, ReadResult, StreamInfo } from "../lib/types";
import { activeConnectionId, connections, forkSeed, lastRead, streams } from "../state/store";
import { ForkDialog } from "./ForkDialog";

// forkStream performs a real network PUT via the active client; in a render test
// we only care that the dialog computes and dispatches the right (offset,
// subOffset), so stub the action and resolve it as a success.
vi.mock("../state/store", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../state/store")>();
	return {
		...actual,
		forkStream: vi.fn(async () => true),
	};
});

import { forkStream } from "../state/store";

function seedConnection(): void {
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
}

function streamInfo(path: string, kind: StreamInfo["kind"]): StreamInfo {
	return {
		path,
		contentType: kind === "json" ? "application/json" : "text/plain",
		kind,
		createdAt: null,
		manual: false,
	};
}

function jsonRead(path: string, count: number): ReadResult {
	const rows: GridRow[] = Array.from({ length: count }, (_, i) => ({
		index: i,
		byteSize: 2,
		preview: `{${i}}`,
		kind: "json",
		value: { i },
	}));
	return {
		path,
		kind: "json",
		requestedOffset: "-1",
		nextOffset: "42",
		closed: false,
		upToDate: true,
		rows,
		rawBytes: new TextEncoder().encode("[]"),
		exchange: {
			method: "GET",
			url: `http://localhost:4437/v1/stream/${path}`,
			requestHeaders: {},
			status: 200,
			statusText: "OK",
			responseHeaders: {},
			protocol: {
				streamNextOffset: "42",
				streamClosed: null,
				streamUpToDate: "true",
				etag: null,
				contentType: "application/json",
			},
			at: 0,
			durationMs: 1,
		},
	};
}

beforeEach(() => {
	seedConnection();
});

afterEach(() => {
	cleanup();
	vi.clearAllMocks();
	connections.value = [];
	activeConnectionId.value = null;
	streams.value = [];
	lastRead.value = null;
	forkSeed.value = null;
});

function fillPath(value: string): void {
	fireEvent.input(screen.getByLabelText(/New fork path/), { target: { value } });
}

describe("ForkDialog", () => {
	it("renders nothing without a fork seed", () => {
		const { container } = render(<ForkDialog />);
		expect(container.firstChild).toBeNull();
	});

	it('defaults to "Everything" and forks at the tail with no sub-offset', () => {
		streams.value = [streamInfo("orders", "text")];
		forkSeed.value = { fromPath: "orders", offset: "now" };
		render(<ForkDialog />);
		const everything = screen.getByRole("radio", { name: /Everything/ }) as HTMLInputElement;
		expect(everything.checked).toBe(true);
		fillPath("orders-fork");
		fireEvent.click(screen.getByRole("button", { name: "Create fork" }));
		expect(forkStream).toHaveBeenCalledWith("orders-fork", "orders", "now", undefined);
	});

	it('"Nothing" forks at the beginning with no sub-offset', () => {
		streams.value = [streamInfo("orders", "text")];
		forkSeed.value = { fromPath: "orders", offset: "now" };
		render(<ForkDialog />);
		fillPath("orders-fork");
		fireEvent.click(screen.getByRole("radio", { name: /Nothing/ }));
		fireEvent.click(screen.getByRole("button", { name: "Create fork" }));
		expect(forkStream).toHaveBeenCalledWith("orders-fork", "orders", "-1", undefined);
	});

	it('hides "First N messages" for a non-JSON source', () => {
		streams.value = [streamInfo("orders", "text")];
		forkSeed.value = { fromPath: "orders", offset: "now" };
		render(<ForkDialog />);
		expect(screen.queryByRole("radio", { name: /First N messages/ })).toBeNull();
	});

	it('shows "First N messages" for a JSON source and forks at beginning + N', () => {
		streams.value = [streamInfo("events", "json")];
		forkSeed.value = { fromPath: "events", offset: "now" };
		render(<ForkDialog />);
		fillPath("events-fork");
		fireEvent.click(screen.getByRole("radio", { name: /First N messages/ }));
		fireEvent.input(screen.getByLabelText(/Messages to keep/), { target: { value: "2" } });
		fireEvent.click(screen.getByRole("button", { name: "Create fork" }));
		expect(forkStream).toHaveBeenCalledWith("events-fork", "events", "-1", 2);
	});

	it("validates First N against the known message count and blocks an overshoot", () => {
		streams.value = [streamInfo("events", "json")];
		lastRead.value = jsonRead("events", 3);
		forkSeed.value = { fromPath: "events", offset: "now" };
		render(<ForkDialog />);
		fillPath("events-fork");
		fireEvent.click(screen.getByRole("radio", { name: /First N messages/ }));
		// The known-count hint is shown once "First N messages" is chosen.
		expect(screen.getByText(/Of 3 messages read/)).toBeTruthy();
		fireEvent.input(screen.getByLabelText(/Messages to keep/), { target: { value: "4" } });
		fireEvent.click(screen.getByRole("button", { name: "Create fork" }));
		expect(forkStream).not.toHaveBeenCalled();
		expect(screen.getByRole("alert").textContent).toContain("overshoots");
	});

	it("submits the raw offset + sub-offset when Advanced is open", () => {
		streams.value = [streamInfo("events", "json")];
		forkSeed.value = { fromPath: "events", offset: "now" };
		render(<ForkDialog />);
		fillPath("events-fork");
		// Open the Advanced disclosure (a controlled button, reliably clickable).
		fireEvent.click(screen.getByRole("button", { name: /Advanced — set the raw offset/ }));
		fireEvent.input(screen.getByLabelText(/Fork offset/), { target: { value: "12" } });
		fireEvent.input(screen.getByLabelText(/Sub-offset/), { target: { value: "1" } });
		fireEvent.click(screen.getByRole("button", { name: "Create fork" }));
		expect(forkStream).toHaveBeenCalledWith("events-fork", "events", "12", 1);
	});

	it("shows the equivalent curl reflecting the active selection", () => {
		streams.value = [streamInfo("orders", "text")];
		forkSeed.value = { fromPath: "orders", offset: "now" };
		render(<ForkDialog />);
		fillPath("orders-fork");
		const cmd = screen.getByText(/^curl/);
		expect(cmd.textContent).toContain("-X PUT");
		expect(cmd.textContent).toContain("Stream-Forked-From: orders");
		expect(cmd.textContent).toContain("Stream-Fork-Offset: now");
	});
});
