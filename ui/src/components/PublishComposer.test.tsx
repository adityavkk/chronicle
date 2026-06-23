import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { StreamInfo } from "../lib/types";
import {
	activeConnectionId,
	composerOpen,
	connections,
	producerSeqHint,
	selectedStreamPath,
	setProducerSeqHint,
	streams,
} from "../state/store";
import { PublishComposer } from "./PublishComposer";

vi.mock("../state/store", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../state/store")>();
	return {
		...actual,
		appendMessages: vi.fn(async () => true),
	};
});

import { appendMessages } from "../state/store";

function seed(kind: StreamInfo["kind"], contentType: string | null): void {
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
	streams.value = [{ path: "demo", contentType, kind, createdAt: null, manual: false }];
	selectedStreamPath.value = "demo";
}

beforeEach(() => {
	seed("json", "application/json");
});

afterEach(() => {
	cleanup();
	vi.clearAllMocks();
	connections.value = [];
	activeConnectionId.value = null;
	streams.value = [];
	selectedStreamPath.value = null;
	composerOpen.value = false;
	producerSeqHint.value = null;
});

/** Open the composer's disclosure so its body renders. */
function openComposer(): void {
	const summary = screen.getByText("Publish to this stream");
	const details = summary.closest("details");
	if (details !== null) details.open = true;
}

describe("PublishComposer", () => {
	it("renders nothing when no stream is selected", () => {
		selectedStreamPath.value = null;
		const { container } = render(<PublishComposer />);
		expect(container.querySelector(".dsui-publish")).toBeNull();
	});

	it("shows a JSON batch editor for a JSON stream and rejects invalid JSON on send", () => {
		render(<PublishComposer />);
		openComposer();
		const body = screen.getByLabelText("JSON batch");
		fireEvent.input(body, { target: { value: "{not json" } });
		fireEvent.click(screen.getByRole("button", { name: /Publish/ }));
		expect(appendMessages).not.toHaveBeenCalled();
		expect(screen.getByRole("alert").textContent).toContain("Invalid JSON");
	});

	it("publishes a normalized JSON batch on a valid send", () => {
		render(<PublishComposer />);
		openComposer();
		fireEvent.input(screen.getByLabelText("JSON batch"), {
			target: { value: '[{"id":1},{"id":2}]' },
		});
		fireEvent.click(screen.getByRole("button", { name: /Publish/ }));
		expect(appendMessages).toHaveBeenCalledTimes(1);
		const call = (appendMessages as unknown as { mock: { calls: unknown[][] } }).mock.calls[0];
		expect(call?.[0]).toBe("demo");
		expect(call?.[1]).toBe('[{"id":1},{"id":2}]');
	});

	it("offers a base64 mode for a binary stream", () => {
		seed("binary", "application/octet-stream");
		render(<PublishComposer />);
		openComposer();
		expect(screen.getByRole("radio", { name: "Base64" })).toBeTruthy();
		expect(screen.getByRole("radio", { name: "UTF-8 text" })).toBeTruthy();
	});

	it("opens its disclosure when the composerOpen signal is set", () => {
		composerOpen.value = true;
		render(<PublishComposer />);
		const details = screen.getByText("Publish to this stream").closest("details");
		expect(details?.open).toBe(true);
	});

	it("adopts an expected-seq hint: reveals the producer block, prefills Seq, and clears the hint", async () => {
		render(<PublishComposer />);
		openComposer();
		// As the producer-conflict toast's "Use expected seq" action would do:
		setProducerSeqHint(7);
		const seq = (await screen.findByLabelText("Seq")) as HTMLInputElement;
		expect(seq.value).toBe("7");
		// The hint is one-shot — consumed and cleared by the composer.
		expect(producerSeqHint.value).toBeNull();
	});
});
