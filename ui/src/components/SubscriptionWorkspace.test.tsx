import { cleanup, fireEvent, render, screen, within } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Subscription } from "../lib/types";
import {
	activeClaim,
	activeConnectionId,
	connections,
	selectedSubscriptionId,
	subscriptionDetails,
} from "../state/store";
import { SubscriptionWorkspace } from "./SubscriptionWorkspace";

// The worker + mutation actions perform real network calls via the active
// client; in a render test we only care that the controls dispatch them, so
// stub them and resolve as success.
vi.mock("../state/store", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../state/store")>();
	return {
		...actual,
		claimWake: vi.fn(async () => true),
		ackWake: vi.fn(async () => true),
		releaseWake: vi.fn(async () => true),
		removeSubscriptionStream: vi.fn(async () => true),
		getSubscription: vi.fn(async () => {}),
		deleteSubscription: vi.fn(async () => true),
	};
});

import { claimWake, removeSubscriptionStream } from "../state/store";

function seed(sub: Subscription | null): void {
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
	if (sub === null) {
		selectedSubscriptionId.value = null;
		subscriptionDetails.value = {};
	} else {
		selectedSubscriptionId.value = sub.id;
		subscriptionDetails.value = { [sub.id]: sub };
	}
	activeClaim.value = null;
}

function webhookSub(): Subscription {
	return {
		id: "orders-fanout",
		type: "webhook",
		pattern: "orders/**",
		streams: [
			{ path: "orders/created", linkType: "glob", ackedOffset: "off-1" },
			{ path: "orders/extra", linkType: "explicit", ackedOffset: "off-2" },
		],
		webhook: {
			url: "https://hooks.example.com/ds",
			signing: { alg: "ed25519", kid: "ds_abc", jwksUrl: "https://x/__ds/jwks.json" },
		},
		wakeStream: null,
		leaseTtlMs: 30000,
		createdAt: "2026-05-09T00:00:00.000Z",
		status: "active",
		description: null,
	};
}

function pullWakeSub(): Subscription {
	return {
		id: "orders-pull",
		type: "pull-wake",
		pattern: null,
		streams: [{ path: "events/x", linkType: "explicit", ackedOffset: "off-1" }],
		webhook: null,
		wakeStream: "__ds/wakes/orders",
		leaseTtlMs: 30000,
		createdAt: null,
		status: "active",
		description: "Pull orders",
	};
}

afterEach(() => {
	cleanup();
	vi.clearAllMocks();
	connections.value = [];
	activeConnectionId.value = null;
	selectedSubscriptionId.value = null;
	subscriptionDetails.value = {};
	activeClaim.value = null;
});

describe("SubscriptionWorkspace", () => {
	it("shows the empty prompt when no subscription is selected", () => {
		seed(null);
		render(<SubscriptionWorkspace />);
		expect(screen.getByText("Select a subscription")).toBeTruthy();
	});

	it("renders a webhook subscription's detail, delivery URL, and JWKS link", () => {
		seed(webhookSub());
		render(<SubscriptionWorkspace />);
		// The id and a webhook chip.
		expect(screen.getByText("orders-fanout")).toBeTruthy();
		expect(screen.getByText("https://hooks.example.com/ds")).toBeTruthy();
		// The JWKS link points at the well-known path.
		const jwks = screen.getByRole("link", { name: /jwks/i });
		expect(jwks.getAttribute("href")).toContain("/__ds/jwks.json");
	});

	it("renders the links table with the link types and offsets", () => {
		seed(webhookSub());
		render(<SubscriptionWorkspace />);
		const table = screen.getByRole("table", { name: "Linked streams" });
		expect(within(table).getByText("orders/created")).toBeTruthy();
		expect(within(table).getByText("orders/extra")).toBeTruthy();
		// The explicit link offers an unlink button; the glob link does not.
		expect(within(table).getByRole("button", { name: "Unlink orders/extra" })).toBeTruthy();
		expect(within(table).queryByRole("button", { name: "Unlink orders/created" })).toBeNull();
	});

	it("dispatches removeSubscriptionStream when an explicit link is unlinked", () => {
		seed(webhookSub());
		render(<SubscriptionWorkspace />);
		fireEvent.click(screen.getByRole("button", { name: "Unlink orders/extra" }));
		expect(removeSubscriptionStream).toHaveBeenCalledWith("orders-fanout", "orders/extra");
	});

	it("shows the pull-wake worker plane with a claim control", () => {
		seed(pullWakeSub());
		render(<SubscriptionWorkspace />);
		expect(screen.getByText("Worker plane")).toBeTruthy();
		expect(screen.getByText("no lease")).toBeTruthy();
		const claimBtn = screen.getByRole("button", { name: /Claim lease/ });
		fireEvent.click(claimBtn);
		expect(claimWake).toHaveBeenCalledTimes(1);
		expect(claimWake).toHaveBeenCalledWith("orders-pull", "dsui-worker");
	});

	it("shows the ack + release controls once a lease is held", () => {
		seed(pullWakeSub());
		activeClaim.value = {
			wakeId: "w_abc",
			generation: 7,
			token: "tok",
			streams: [
				{
					path: "events/x",
					linkType: "explicit",
					ackedOffset: "off-1",
					tailOffset: "off-9",
					hasPending: true,
				},
			],
			leaseTtlMs: 30000,
		};
		render(<SubscriptionWorkspace />);
		expect(screen.getByText(/lease held/)).toBeTruthy();
		expect(screen.getByRole("button", { name: "Ack + release" })).toBeTruthy();
		expect(screen.getByRole("button", { name: "Heartbeat" })).toBeTruthy();
		expect(screen.getByRole("button", { name: "Release" })).toBeTruthy();
	});
});
