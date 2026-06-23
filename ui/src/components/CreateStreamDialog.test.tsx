import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { activeConnectionId, connections } from "../state/store";
import { CreateStreamDialog } from "./CreateStreamDialog";

// createStream performs a real network PUT via the active client; in a render
// test we only care that the dialog validates and dispatches the right options,
// so stub the action and resolve it as a success.
vi.mock("../state/store", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../state/store")>();
	return {
		...actual,
		createStream: vi.fn(async () => true),
	};
});

import { createStream } from "../state/store";

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

beforeEach(() => {
	seedConnection();
});

afterEach(() => {
	cleanup();
	vi.clearAllMocks();
	connections.value = [];
	activeConnectionId.value = null;
});

describe("CreateStreamDialog", () => {
	it("renders as a labelled modal dialog with a path field and content-type radios", () => {
		render(<CreateStreamDialog />);
		const dialog = screen.getByRole("dialog", { name: "New stream" });
		expect(dialog).toBeTruthy();
		expect(screen.getByText("Stream path")).toBeTruthy();
		// JSON is the default content-type selection.
		const json = screen.getByRole("radio", { name: /JSON/ }) as HTMLInputElement;
		expect(json.checked).toBe(true);
	});

	it("blocks submit and shows an inline error for an invalid path", () => {
		render(<CreateStreamDialog />);
		fireEvent.input(screen.getByLabelText(/Stream path/), { target: { value: "/bad/" } });
		fireEvent.click(screen.getByRole("button", { name: "Create stream" }));
		expect(createStream).not.toHaveBeenCalled();
		expect(screen.getByRole("alert").textContent).toContain("slash");
	});

	it("dispatches createStream with the path + chosen content type on a valid submit", () => {
		render(<CreateStreamDialog />);
		fireEvent.input(screen.getByLabelText(/Stream path/), { target: { value: "orders/created" } });
		fireEvent.click(screen.getByRole("radio", { name: /Text/ }));
		fireEvent.click(screen.getByRole("button", { name: "Create stream" }));
		expect(createStream).toHaveBeenCalledTimes(1);
		expect(createStream).toHaveBeenCalledWith(
			expect.objectContaining({ path: "orders/created", contentType: "text/plain" }),
		);
	});

	it("shows the equivalent curl once the form is valid", () => {
		render(<CreateStreamDialog />);
		fireEvent.input(screen.getByLabelText(/Stream path/), { target: { value: "demo" } });
		expect(screen.getByText("Equivalent curl")).toBeTruthy();
		const cmd = screen.getByText(/^curl/);
		expect(cmd.textContent).toContain("-X PUT");
		expect(cmd.textContent).toContain("/v1/stream/demo");
	});
});
