import { describe, expect, it } from "vitest";
import {
	describeTailMode,
	isLiveMode,
	isTerminalTailState,
	previewTailOperation,
	previewTailUrl,
	tailAnnouncePolite,
	tailStatusDetail,
	tailStatusLabel,
	tailToCurl,
	tailTone,
} from "./tail";
import type { TailStatus } from "./types";

describe("isLiveMode", () => {
	it("is true only for the two streaming modes", () => {
		expect(isLiveMode("long-poll")).toBe(true);
		expect(isLiveMode("sse")).toBe(true);
		expect(isLiveMode("catchup")).toBe(false);
	});
});

describe("describeTailMode", () => {
	it("labels each mode", () => {
		expect(describeTailMode("catchup")).toBe("Catch-up");
		expect(describeTailMode("long-poll")).toBe("Long-poll");
		expect(describeTailMode("sse")).toBe("SSE");
	});
});

describe("previewTailUrl", () => {
	it("carries offset + live on the query, keeping slashes as separators", () => {
		const url = previewTailUrl(
			"http://localhost:4437",
			"/v1/stream",
			"orders/created",
			"-1",
			"sse",
		);
		expect(url).toBe("http://localhost:4437/v1/stream/orders/created?offset=-1&live=sse");
	});

	it("encodes path segments but not the separators", () => {
		const url = previewTailUrl("http://h", "/v1/stream", "a b/c", "now", "long-poll");
		expect(url).toBe("http://h/v1/stream/a%20b/c?offset=now&live=long-poll");
	});

	it("strips a leading slash on the path", () => {
		const url = previewTailUrl("http://h", "/v1/stream", "/x", "-1", "sse");
		expect(url).toBe("http://h/v1/stream/x?offset=-1&live=sse");
	});
});

describe("previewTailOperation", () => {
	it("is a GET with the live URL; SSE asks for an event-stream", () => {
		const op = previewTailOperation("http://h", "/v1/stream", "orders", "now", "sse");
		expect(op.method).toBe("GET");
		expect(op.url).toContain("live=sse");
		expect(op.headers).toEqual({ Accept: "text/event-stream" });
		expect(op.body).toBeUndefined();
	});

	it("long-poll uses the plain Accept header", () => {
		const op = previewTailOperation("http://h", "/v1/stream", "orders", "-1", "long-poll");
		expect(op.headers).toEqual({ Accept: "*/*" });
	});
});

describe("tailToCurl", () => {
	it("adds -N for an SSE stream", () => {
		const op = previewTailOperation("http://h", "/v1/stream", "orders", "now", "sse");
		const cmd = tailToCurl(op, "sse");
		expect(cmd.startsWith("curl -N ")).toBe(true);
		expect(cmd).toContain("live=sse");
	});

	it("leaves long-poll as a plain curl GET", () => {
		const op = previewTailOperation("http://h", "/v1/stream", "orders", "-1", "long-poll");
		const cmd = tailToCurl(op, "long-poll");
		expect(cmd.startsWith("curl -N")).toBe(false);
		expect(cmd).toContain("live=long-poll");
	});
});

describe("tail status mapping", () => {
	const cases: readonly { status: TailStatus; tone: string; label: string }[] = [
		{ status: { state: "idle" }, tone: "idle", label: "Not tailing" },
		{ status: { state: "connecting" }, tone: "pending", label: "Connecting…" },
		{ status: { state: "live", atOffset: "42" }, tone: "ok", label: "Live" },
		{
			status: { state: "reconnecting", attempt: 2, reason: "dropped" },
			tone: "warn",
			label: "Reconnecting (attempt 2)",
		},
		{ status: { state: "closed" }, tone: "idle", label: "Stream closed" },
		{ status: { state: "error", message: "boom" }, tone: "err", label: "Error" },
	];

	for (const c of cases) {
		it(`maps ${c.status.state} → ${c.tone} / "${c.label}"`, () => {
			expect(tailTone(c.status)).toBe(c.tone);
			expect(tailStatusLabel(c.status)).toBe(c.label);
		});
	}

	it("details the live offset and surfaces the error/reason text", () => {
		expect(tailStatusDetail({ state: "live", atOffset: "42" })).toContain("offset 42");
		expect(tailStatusDetail({ state: "error", message: "boom" })).toBe("boom");
		expect(tailStatusDetail({ state: "reconnecting", attempt: 1, reason: "dropped" })).toBe(
			"dropped",
		);
	});

	it("announces errors + reconnects assertively, the rest politely", () => {
		expect(tailAnnouncePolite({ state: "error", message: "x" })).toBe(false);
		expect(tailAnnouncePolite({ state: "reconnecting", attempt: 1, reason: "x" })).toBe(false);
		expect(tailAnnouncePolite({ state: "live", atOffset: null })).toBe(true);
		expect(tailAnnouncePolite({ state: "connecting" })).toBe(true);
	});

	it("treats idle / closed / error as terminal (no open connection)", () => {
		expect(isTerminalTailState({ state: "idle" })).toBe(true);
		expect(isTerminalTailState({ state: "closed" })).toBe(true);
		expect(isTerminalTailState({ state: "error", message: "x" })).toBe(true);
		expect(isTerminalTailState({ state: "live", atOffset: null })).toBe(false);
		expect(isTerminalTailState({ state: "connecting" })).toBe(false);
	});
});
