import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { activeConnectionId, connections } from "../state/store";
import { CreateSubscriptionDialog } from "./CreateSubscriptionDialog";

// createSubscription performs a real network PUT via the active client; in a
// render test we only care that the dialog validates and dispatches the right
// options, so stub the action and resolve it as a success.
vi.mock("../state/store", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../state/store")>();
	return {
		...actual,
		createSubscription: vi.fn(async () => true),
	};
});

import { createSubscription } from "../state/store";

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

describe("CreateSubscriptionDialog", () => {
	it("renders as a labelled modal with an id field and webhook as the default type", () => {
		render(<CreateSubscriptionDialog />);
		expect(screen.getByRole("dialog", { name: "New subscription" })).toBeTruthy();
		expect(screen.getByText("Subscription id")).toBeTruthy();
		const webhook = screen.getByRole("radio", { name: /Webhook/ }) as HTMLInputElement;
		expect(webhook.checked).toBe(true);
		// The webhook type shows the URL field, not the wake stream.
		expect(screen.getByLabelText(/Webhook URL/)).toBeTruthy();
		expect(screen.queryByLabelText(/Wake stream/)).toBeNull();
	});

	it("swaps the target field to a wake stream when pull-wake is chosen", () => {
		render(<CreateSubscriptionDialog />);
		fireEvent.click(screen.getByRole("radio", { name: /Pull-wake/ }));
		expect(screen.getByLabelText(/Wake stream/)).toBeTruthy();
		expect(screen.queryByLabelText(/Webhook URL/)).toBeNull();
	});

	it("blocks submit and shows inline errors when required fields are missing", () => {
		render(<CreateSubscriptionDialog />);
		fireEvent.click(screen.getByRole("button", { name: "Create subscription" }));
		expect(createSubscription).not.toHaveBeenCalled();
		// The id error is surfaced as an alert.
		const alerts = screen.getAllByRole("alert");
		expect(alerts.some((a) => /id is required/i.test(a.textContent ?? ""))).toBe(true);
	});

	it("dispatches createSubscription with the typed options on a valid webhook submit", () => {
		render(<CreateSubscriptionDialog />);
		fireEvent.input(screen.getByLabelText(/Subscription id/), {
			target: { value: "orders-fanout" },
		});
		fireEvent.input(screen.getByLabelText(/Glob pattern/), { target: { value: "orders/**" } });
		fireEvent.input(screen.getByLabelText(/Webhook URL/), {
			target: { value: "https://hooks.example.com/ds" },
		});
		fireEvent.click(screen.getByRole("button", { name: "Create subscription" }));
		expect(createSubscription).toHaveBeenCalledTimes(1);
		expect(createSubscription).toHaveBeenCalledWith(
			expect.objectContaining({
				id: "orders-fanout",
				type: "webhook",
				pattern: "orders/**",
				webhookUrl: "https://hooks.example.com/ds",
			}),
		);
	});

	it("shows the equivalent curl once the form is valid", () => {
		render(<CreateSubscriptionDialog />);
		fireEvent.input(screen.getByLabelText(/Subscription id/), { target: { value: "s1" } });
		fireEvent.input(screen.getByLabelText(/Glob pattern/), { target: { value: "x/**" } });
		fireEvent.input(screen.getByLabelText(/Webhook URL/), { target: { value: "https://x/ds" } });
		expect(screen.getByText("Equivalent curl")).toBeTruthy();
		const cmd = screen.getByText(/^curl/);
		expect(cmd.textContent).toContain("-X PUT");
		expect(cmd.textContent).toContain("/__ds/subscriptions/s1");
	});
});
