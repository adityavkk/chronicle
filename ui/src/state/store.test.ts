/**
 * Store tests for the actionable-failure-toast + auto-open-composer behavior
 * (issue #56). Like dsClient.test.ts these stub `globalThis.fetch` with a queued
 * set of real `Response`s, then drive the store's write/selection actions and
 * assert the toast that results (its tone + inline action) and the composer's
 * open state. The store is the only mutation seam, so this is where the wiring
 * is checked; the components only lay it out.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Connection, Toast } from "../lib/types";
import {
	activeConnectionId,
	appendMessages,
	closeStream,
	composerOpen,
	composerOpenPref,
	connections,
	createStream,
	deleteStream,
	forkStream,
	producerSeqHint,
	protocolOpen,
	selectStream,
	selectedStreamPath,
	setComposerOpen,
	streams,
	toasts,
} from "./store";

const CONN: Connection = {
	id: "c1",
	name: "Local",
	baseUrl: "http://localhost:4437",
	streamRoot: "/v1/stream",
	createdAt: 0,
	lastUsedAt: null,
};

interface StubResponse {
	readonly status?: number;
	readonly statusText?: string;
	readonly headers?: Record<string, string>;
	readonly body?: string | Uint8Array;
}

/** Build a real Response from a stub spec (mirrors dsClient.test.ts). */
function makeResponse(spec: StubResponse): Response {
	const body: BodyInit | null = spec.body === undefined ? null : (spec.body as BodyInit);
	return new Response(body, {
		status: spec.status ?? 200,
		statusText: spec.statusText ?? "",
		headers: spec.headers ?? {},
	});
}

/** Install a fetch stub that returns the given responses in call order. */
function stubFetch(...responses: StubResponse[]): ReturnType<typeof vi.fn> {
	const fn = vi.fn(async () => makeResponse(responses.shift() ?? { status: 200 }));
	vi.stubGlobal("fetch", fn);
	return fn;
}

/** Install a fetch stub that rejects (simulates a network/CORS failure). */
function stubFetchReject(error: unknown): ReturnType<typeof vi.fn> {
	const fn = vi.fn(async () => {
		throw error;
	});
	vi.stubGlobal("fetch", fn);
	return fn;
}

function lastToast(): Toast | undefined {
	return toasts.value[toasts.value.length - 1];
}

beforeEach(() => {
	connections.value = [CONN];
	activeConnectionId.value = "c1";
	streams.value = [];
	selectedStreamPath.value = null;
	toasts.value = [];
	protocolOpen.value = false;
	producerSeqHint.value = null;
	composerOpen.value = false;
	composerOpenPref.value = false;
});

afterEach(() => {
	vi.unstubAllGlobals();
	vi.restoreAllMocks();
	toasts.value = [];
});

describe("actionable failure toasts", () => {
	it("a server-rejected create toasts an error with a Show details action that opens the panel", async () => {
		stubFetch({ status: 409, statusText: "Conflict" });
		const ok = await createStream({ path: "demo", contentType: "application/json" });
		expect(ok).toBe(false);
		const t = lastToast();
		expect(t?.kind).toBe("error");
		expect(t?.action?.label).toBe("Show details");
		// The action expands the Under-the-hood protocol disclosure.
		expect(protocolOpen.value).toBe(false);
		t?.action?.onAction();
		expect(protocolOpen.value).toBe(true);
	});

	it("a network-failed create toasts an error with a Retry action that re-invokes the write", async () => {
		const fetchMock = stubFetchReject(new TypeError("Failed to fetch"));
		const ok = await createStream({ path: "demo", contentType: "application/json" });
		expect(ok).toBe(false);
		const t = lastToast();
		expect(t?.kind).toBe("error");
		expect(t?.action?.label).toBe("Retry");
		// The Retry action re-invokes the same store action (another request goes out).
		const before = fetchMock.mock.calls.length;
		t?.action?.onAction();
		await vi.waitFor(() => expect(fetchMock.mock.calls.length).toBeGreaterThan(before));
	});

	it("a producer-seq conflict warns with a Use expected seq action that hints the composer", async () => {
		stubFetch({
			status: 409,
			statusText: "Conflict",
			headers: { "Producer-Expected-Seq": "7", "Producer-Received-Seq": "3" },
		});
		const ok = await appendMessages("demo", "[]", { contentType: "application/json" });
		expect(ok).toBe(false);
		const t = lastToast();
		expect(t?.kind).toBe("warning");
		expect(t?.action?.label).toBe("Use expected seq");
		t?.action?.onAction();
		expect(producerSeqHint.value).toBe(7);
	});

	it("a producer-seq conflict with no expected seq falls back to a Show details action", async () => {
		// Only Producer-Received-Seq present (expectedSeq null): still a conflict, but
		// there is no number to adopt, so the action falls back to inspecting the exchange.
		stubFetch({ status: 409, statusText: "Conflict", headers: { "Producer-Received-Seq": "3" } });
		const ok = await appendMessages("demo", "[]", { contentType: "application/json" });
		expect(ok).toBe(false);
		const t = lastToast();
		expect(t?.kind).toBe("warning");
		expect(t?.action?.label).toBe("Show details");
	});

	it("a non-conflict publish failure toasts an error with a Show details action", async () => {
		stubFetch({ status: 400, statusText: "Bad Request" });
		const ok = await appendMessages("demo", "[]", { contentType: "application/json" });
		expect(ok).toBe(false);
		const t = lastToast();
		expect(t?.kind).toBe("error");
		expect(t?.action?.label).toBe("Show details");
	});

	it("a server-rejected close toasts an error with a Show details action", async () => {
		stubFetch({ status: 409, statusText: "Conflict" });
		const ok = await closeStream("demo");
		expect(ok).toBe(false);
		const t = lastToast();
		expect(t?.kind).toBe("error");
		expect(t?.action?.label).toBe("Show details");
	});

	it("a server-rejected delete toasts an error with a Show details action", async () => {
		stubFetch({ status: 409, statusText: "Conflict" });
		const ok = await deleteStream("demo");
		expect(ok).toBe(false);
		const t = lastToast();
		expect(t?.kind).toBe("error");
		expect(t?.action?.label).toBe("Show details");
	});

	it("a server-rejected fork toasts an error with a Show details action", async () => {
		// forkStream HEAD-probes the source, then PUTs the fork create (which 409s here).
		stubFetch({ status: 200 }, { status: 409, statusText: "Conflict" });
		const ok = await forkStream("demo2", "demo", "0");
		expect(ok).toBe(false);
		const t = lastToast();
		expect(t?.kind).toBe("error");
		expect(t?.action?.label).toBe("Show details");
	});
});

describe("auto-open the composer on empty / new streams", () => {
	it("opens the composer when a selected stream reads back empty", async () => {
		composerOpenPref.value = false;
		streams.value = [
			{
				path: "demo",
				contentType: "application/json",
				kind: "json",
				createdAt: null,
				manual: false,
			},
		];
		// A 200 with an empty body is a writable, empty stream (at the tail).
		stubFetch({
			status: 200,
			headers: { "Content-Type": "application/json", "Stream-Up-To-Date": "true" },
		});
		selectStream("demo");
		await vi.waitFor(() => expect(composerOpen.value).toBe(true));
	});

	it("respects a remembered manual collapse on a non-empty stream", async () => {
		// User previously collapsed the composer; a prior empty stream had forced it open.
		composerOpenPref.value = false;
		composerOpen.value = true;
		streams.value = [
			{
				path: "demo",
				contentType: "application/json",
				kind: "json",
				createdAt: null,
				manual: false,
			},
		];
		stubFetch({ status: 200, headers: { "Content-Type": "application/json" }, body: '[{"id":1}]' });
		selectStream("demo");
		// Selecting a non-empty stream reverts the composer to the manual preference.
		await vi.waitFor(() => expect(composerOpen.value).toBe(false));
	});

	it("opens the composer on a freshly created (empty) stream", async () => {
		composerOpenPref.value = false;
		// All calls return 200 empty: create ok, registry write ok, registry list
		// empty, and the post-create read of the new stream is empty -> auto-open.
		stubFetch();
		const ok = await createStream({ path: "demo", contentType: "application/json" });
		expect(ok).toBe(true);
		await vi.waitFor(() => expect(composerOpen.value).toBe(true));
	});

	it("does not auto-open the composer on a closed (empty) stream", async () => {
		composerOpenPref.value = false;
		composerOpen.value = true; // prove it reverts to the preference rather than staying open
		streams.value = [
			{
				path: "demo",
				contentType: "application/json",
				kind: "json",
				createdAt: null,
				manual: false,
			},
		];
		stubFetch({
			status: 200,
			headers: { "Content-Type": "application/json", "Stream-Closed": "true" },
		});
		selectStream("demo");
		await vi.waitFor(() => expect(composerOpen.value).toBe(false));
	});

	it("setComposerOpen records the manual preference (so it survives non-empty switches)", () => {
		setComposerOpen(true);
		expect(composerOpen.value).toBe(true);
		expect(composerOpenPref.value).toBe(true);
		setComposerOpen(false);
		expect(composerOpen.value).toBe(false);
		expect(composerOpenPref.value).toBe(false);
	});
});
