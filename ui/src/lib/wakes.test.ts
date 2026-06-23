import { describe, expect, it } from "vitest";
import {
	WAKE_DEMO_STREAM,
	WAKE_DEMO_SUB_ID,
	captureStreamUrl,
	captureUrl,
	hasSignature,
	parseCaptureDelivery,
	parseCaptureDeliveryData,
	parseSignatureHeader,
	parseWakeEvent,
	parseWakeNotification,
	previewWakeDemoPublish,
	previewWakeDemoRegister,
	wakeDemoBody,
} from "./wakes";

const BASE = "http://localhost:4437";
const CAPTURE = "http://localhost:4438";

/* ----------------------------------------------------------------------------
 * capture URLs
 * ------------------------------------------------------------------------- */

describe("captureUrl / captureStreamUrl", () => {
	it("builds the capture + stream URLs for a bucket", () => {
		expect(captureUrl(CAPTURE, "sub-1")).toBe("http://localhost:4438/__hooks/sub-1");
		expect(captureStreamUrl(CAPTURE, "sub-1")).toBe("http://localhost:4438/__hooks/sub-1/stream");
	});

	it("trims a trailing slash on the base and encodes the bucket", () => {
		expect(captureUrl("http://localhost:4438/", "a/b")).toBe("http://localhost:4438/__hooks/a%2Fb");
	});
});

/* ----------------------------------------------------------------------------
 * parseCaptureDelivery
 * ------------------------------------------------------------------------- */

describe("parseCaptureDelivery", () => {
	it("parses a full delivery record", () => {
		const d = parseCaptureDelivery({
			seq: 3,
			receivedAt: 1700000000000,
			method: "POST",
			signature: "t=1,kid=k,ed25519=s",
			contentType: "application/json",
			headers: { "Content-Type": "application/json", "X-Other": "y" },
			body: '{"wake_id":"w1"}',
		});
		expect(d).not.toBeNull();
		expect(d?.seq).toBe(3);
		expect(d?.headers["X-Other"]).toBe("y");
		expect(d?.body).toBe('{"wake_id":"w1"}');
	});

	it("fills defaults for a partial record but keeps the exact body", () => {
		const d = parseCaptureDelivery({ body: "raw-bytes" });
		expect(d).not.toBeNull();
		expect(d?.seq).toBe(0);
		expect(d?.method).toBe("POST");
		expect(d?.signature).toBe("");
		expect(d?.body).toBe("raw-bytes");
	});

	it("drops non-string header values", () => {
		const d = parseCaptureDelivery({ headers: { ok: "v", bad: 3 } });
		expect(d?.headers).toEqual({ ok: "v" });
	});

	it("returns null for a non-object", () => {
		expect(parseCaptureDelivery(null)).toBeNull();
		expect(parseCaptureDelivery("nope")).toBeNull();
	});

	it("parses from the SSE data string, and returns null for bad JSON", () => {
		expect(parseCaptureDeliveryData('{"seq":1,"body":"x"}')?.seq).toBe(1);
		expect(parseCaptureDeliveryData("not json")).toBeNull();
	});
});

/* ----------------------------------------------------------------------------
 * parseSignatureHeader
 * ------------------------------------------------------------------------- */

describe("parseSignatureHeader", () => {
	it("parses t / kid / ed25519 in any order", () => {
		const p = parseSignatureHeader("kid=ds_abc, t=1699564800 , ed25519=AbC-_123");
		expect(p.timestamp).toBe(1699564800);
		expect(p.kid).toBe("ds_abc");
		expect(p.ed25519).toBe("AbC-_123");
		expect(hasSignature(p)).toBe(true);
	});

	it("yields all-null for an empty header but keeps the raw", () => {
		const p = parseSignatureHeader("");
		expect(p.timestamp).toBeNull();
		expect(p.kid).toBeNull();
		expect(p.ed25519).toBeNull();
		expect(p.raw).toBe("");
		expect(hasSignature(p)).toBe(false);
	});

	it("ignores a non-numeric timestamp and unknown keys", () => {
		const p = parseSignatureHeader("t=abc,foo=bar,ed25519=sig");
		expect(p.timestamp).toBeNull();
		expect(p.ed25519).toBe("sig");
	});
});

/* ----------------------------------------------------------------------------
 * parseWakeNotification
 * ------------------------------------------------------------------------- */

describe("parseWakeNotification", () => {
	it("parses a full wake notification body", () => {
		const body = JSON.stringify({
			subscription_id: "sub-1",
			wake_id: "wake-9",
			generation: 7,
			streams: [
				{
					path: "orders/created",
					link_type: "glob",
					acked_offset: "10",
					tail_offset: "20",
					has_pending: true,
				},
			],
			callback_url: "http://h/__ds/subscriptions/sub-1/callback",
			callback_token: "tok",
		});
		const n = parseWakeNotification(body);
		expect(n).not.toBeNull();
		expect(n?.wakeId).toBe("wake-9");
		expect(n?.generation).toBe(7);
		expect(n?.streams[0]?.tailOffset).toBe("20");
		expect(n?.streams[0]?.hasPending).toBe(true);
		expect(n?.callbackToken).toBe("tok");
	});

	it("returns null when wake_id is missing", () => {
		expect(parseWakeNotification(JSON.stringify({ subscription_id: "s" }))).toBeNull();
	});

	it("returns null for non-JSON or a non-object body", () => {
		expect(parseWakeNotification("not json")).toBeNull();
		expect(parseWakeNotification("[1,2,3]")).toBeNull();
	});

	it("degrades missing optional fields to defaults", () => {
		const n = parseWakeNotification(JSON.stringify({ wake_id: "w" }));
		expect(n?.generation).toBe(0);
		expect(n?.streams).toEqual([]);
		expect(n?.callbackUrl).toBeNull();
		expect(n?.callbackToken).toBeNull();
	});
});

/* ----------------------------------------------------------------------------
 * parseWakeEvent (pull-wake wake_stream rows)
 * ------------------------------------------------------------------------- */

describe("parseWakeEvent", () => {
	it("parses a wake event object", () => {
		const ev = parseWakeEvent({
			type: "wake",
			subscription_id: "sub-1",
			stream: "orders/created",
			generation: 4,
			ts: 1700000000000,
		});
		expect(ev).not.toBeNull();
		expect(ev?.stream).toBe("orders/created");
		expect(ev?.generation).toBe(4);
		expect(ev?.ts).toBe(1700000000000);
	});

	it("ignores non-wake rows and rows without a stream", () => {
		expect(parseWakeEvent({ type: "other", stream: "s" })).toBeNull();
		expect(parseWakeEvent({ type: "wake" })).toBeNull();
		expect(parseWakeEvent("x")).toBeNull();
	});
});

/* ----------------------------------------------------------------------------
 * Wake-demo previews
 * ------------------------------------------------------------------------- */

describe("wake-demo previews", () => {
	it("registers a webhook subscription pointed at the capture endpoint", () => {
		const op = previewWakeDemoRegister(BASE, CAPTURE);
		expect(op.method).toBe("PUT");
		expect(op.url).toBe(`${BASE}/__ds/subscriptions/${WAKE_DEMO_SUB_ID}`);
		const body = JSON.parse(String(op.body));
		expect(body.type).toBe("webhook");
		expect(body.streams).toEqual([WAKE_DEMO_STREAM]);
		expect(body.webhook.url).toBe(`${CAPTURE}/__hooks/${WAKE_DEMO_SUB_ID}`);
	});

	it("publishes the demo body to the sample stream", () => {
		const op = previewWakeDemoPublish(BASE, "/v1/stream");
		expect(op.method).toBe("POST");
		expect(op.url).toBe(`${BASE}/v1/stream/${WAKE_DEMO_STREAM}`);
		expect(op.body).toBe(wakeDemoBody());
	});
});
