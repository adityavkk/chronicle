import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { activeConnectionId, connections } from "../state/store";
import { Playground } from "./Playground";

vi.mock("../state/store", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../state/store")>();
	return {
		...actual,
		createStream: vi.fn(async () => true),
		appendMessages: vi.fn(async () => true),
		forkStream: vi.fn(async () => true),
		closeStream: vi.fn(async () => true),
		deleteStream: vi.fn(async () => true),
		runDemoProducer: vi.fn(async () => {}),
		startTail: vi.fn(),
		setTailMode: vi.fn(),
		selectStream: vi.fn(),
	};
});

import { appendMessages, createStream } from "../state/store";

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

describe("Playground", () => {
	it("renders the seven bootstrap presets on the sample namespace", () => {
		render(<Playground />);
		expect(screen.getByRole("button", { name: /Create sample JSON stream/ })).toBeTruthy();
		expect(screen.getByRole("button", { name: /Publish a sample batch/ })).toBeTruthy();
		expect(screen.getByRole("button", { name: /Run a demo producer/ })).toBeTruthy();
		expect(screen.getByRole("button", { name: /Tail live \(SSE\)/ })).toBeTruthy();
		expect(screen.getByRole("button", { name: /Fork at latest/ })).toBeTruthy();
		expect(screen.getByRole("button", { name: /Close stream/ })).toBeTruthy();
		expect(screen.getByRole("button", { name: /Delete \/ reset playground/ })).toBeTruthy();
	});

	it("dispatches the real store action when a preset is clicked", () => {
		render(<Playground />);
		fireEvent.click(screen.getByRole("button", { name: /Create sample JSON stream/ }));
		expect(createStream).toHaveBeenCalledWith({
			path: "playground/demo",
			contentType: "application/json",
		});

		fireEvent.click(screen.getByRole("button", { name: /Publish a sample batch/ }));
		expect(appendMessages).toHaveBeenCalledWith(
			"playground/demo",
			expect.any(String),
			expect.objectContaining({ contentType: "application/json" }),
		);
	});

	it("discloses the exact equivalent curl for every preset", () => {
		render(<Playground />);
		// Each preset's curl-preview disclosure shows a copy-as-curl control.
		const copyButtons = screen.getAllByRole("button", {
			name: "Copy the equivalent curl command",
		});
		expect(copyButtons.length).toBe(7);

		// The create preset's curl is a PUT against the sample stream URL.
		const blocks = document.querySelectorAll(".dsui-playground__detail .dsui-curl__cmd");
		const commands = Array.from(blocks).map((el) => el.textContent ?? "");
		expect(commands.some((c) => c.includes("-X PUT") && c.includes("playground/demo"))).toBe(true);
		// The SSE tail preset carries curl's -N (no-buffering) flag for the stream.
		expect(commands.some((c) => c.includes("curl -N") && c.includes("live=sse"))).toBe(true);
	});
});
