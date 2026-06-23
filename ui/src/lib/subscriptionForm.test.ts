import { describe, expect, it } from "vitest";
import { toCurl } from "./curl";
import {
	EMPTY_SUBSCRIPTION_FORM,
	type SubscriptionFormValues,
	buildSubscriptionOptions,
	isInsecureWebhookUrl,
	isSubscriptionFormValid,
	parseStreamsInput,
	previewCallbackOperation,
	validateGlobPattern,
	validateLeaseTtl,
	validateSubscriptionForm,
	validateSubscriptionId,
	validateWakeStream,
	validateWebhookUrl,
} from "./subscriptionForm";

describe("validateSubscriptionId", () => {
	it("requires a non-empty id", () => {
		expect(validateSubscriptionId("")).not.toBeNull();
		expect(validateSubscriptionId("   ")).not.toBeNull();
	});
	it("rejects whitespace, slashes, and url-ish tokens", () => {
		expect(validateSubscriptionId("a b")).not.toBeNull();
		expect(validateSubscriptionId("a/b")).not.toBeNull();
		expect(validateSubscriptionId("http://x")).not.toBeNull();
	});
	it("accepts a plain id token", () => {
		expect(validateSubscriptionId("orders-fanout")).toBeNull();
		expect(validateSubscriptionId("sub_1")).toBeNull();
	});
});

describe("validateGlobPattern", () => {
	it("allows blank (explicit-only subscriptions)", () => {
		expect(validateGlobPattern("")).toBeNull();
	});
	it("accepts glob wildcards", () => {
		expect(validateGlobPattern("orders/**")).toBeNull();
		expect(validateGlobPattern("events/*/created")).toBeNull();
	});
	it("rejects whitespace, leading slash, and url-ish tokens", () => {
		expect(validateGlobPattern("a b")).not.toBeNull();
		expect(validateGlobPattern("/orders")).not.toBeNull();
		expect(validateGlobPattern("https://x")).not.toBeNull();
	});
});

describe("validateWebhookUrl", () => {
	it("requires a url and rejects non-http(s)", () => {
		expect(validateWebhookUrl("")).not.toBeNull();
		expect(validateWebhookUrl("ftp://x")).not.toBeNull();
		expect(validateWebhookUrl("not a url")).not.toBeNull();
	});
	it("accepts http (dev localhost) and https", () => {
		expect(validateWebhookUrl("http://localhost:9000/ds")).toBeNull();
		expect(validateWebhookUrl("https://hooks.example.com/ds")).toBeNull();
	});
});

describe("isInsecureWebhookUrl", () => {
	it("flags plain http but not https or blank", () => {
		expect(isInsecureWebhookUrl("http://localhost/ds")).toBe(true);
		expect(isInsecureWebhookUrl("https://x/ds")).toBe(false);
		expect(isInsecureWebhookUrl("")).toBe(false);
		expect(isInsecureWebhookUrl("garbage")).toBe(false);
	});
});

describe("validateWakeStream", () => {
	it("requires a stream-shaped path", () => {
		expect(validateWakeStream("")).not.toBeNull();
		expect(validateWakeStream("/a")).not.toBeNull();
		expect(validateWakeStream("a//b")).not.toBeNull();
		expect(validateWakeStream("__ds/wakes/orders")).toBeNull();
	});
});

describe("validateLeaseTtl", () => {
	it("allows blank (server default)", () => {
		expect(validateLeaseTtl("")).toBeNull();
	});
	it("requires an integer in the 1000–600000 ms window", () => {
		expect(validateLeaseTtl("abc")).not.toBeNull();
		expect(validateLeaseTtl("999")).not.toBeNull();
		expect(validateLeaseTtl("600001")).not.toBeNull();
		expect(validateLeaseTtl("30000")).toBeNull();
	});
});

describe("parseStreamsInput", () => {
	it("splits on newlines and commas, trims, and de-dupes order-preserving", () => {
		expect(parseStreamsInput("a\nb, c\n a")).toEqual(["a", "b", "c"]);
	});
	it("strips leading/trailing slashes and drops blanks", () => {
		expect(parseStreamsInput("/events/x/\n\n,  ")).toEqual(["events/x"]);
	});
	it("returns [] for an empty field", () => {
		expect(parseStreamsInput("")).toEqual([]);
	});
});

describe("validateSubscriptionForm", () => {
	const base: SubscriptionFormValues = {
		...EMPTY_SUBSCRIPTION_FORM,
		id: "sub-1",
		webhookUrl: "https://x/ds",
		pattern: "orders/**",
	};

	it("passes a well-formed webhook form", () => {
		const errors = validateSubscriptionForm(base);
		expect(isSubscriptionFormValid(errors)).toBe(true);
	});

	it("requires a pattern or at least one explicit stream", () => {
		const errors = validateSubscriptionForm({ ...base, pattern: "", streamsText: "" });
		expect(errors.streams).not.toBeUndefined();
		// An explicit stream satisfies it without a pattern.
		const ok = validateSubscriptionForm({ ...base, pattern: "", streamsText: "events/x" });
		expect(ok.streams).toBeUndefined();
	});

	it("requires a webhook url for the webhook type", () => {
		const errors = validateSubscriptionForm({ ...base, webhookUrl: "" });
		expect(errors.webhookUrl).not.toBeUndefined();
	});

	it("requires a wake stream for the pull-wake type", () => {
		const errors = validateSubscriptionForm({
			...base,
			type: "pull-wake",
			webhookUrl: "",
			wakeStream: "",
		});
		expect(errors.wakeStream).not.toBeUndefined();
		const ok = validateSubscriptionForm({
			...base,
			type: "pull-wake",
			webhookUrl: "",
			wakeStream: "__ds/wakes/x",
		});
		expect(isSubscriptionFormValid(ok)).toBe(true);
	});
});

describe("buildSubscriptionOptions", () => {
	it("omits blank optionals and normalizes explicit streams", () => {
		const opts = buildSubscriptionOptions({
			...EMPTY_SUBSCRIPTION_FORM,
			id: "  sub-1 ",
			type: "webhook",
			pattern: " orders/** ",
			streamsText: "/events/x/\nevents/x\nevents/y",
			webhookUrl: " https://x/ds ",
			leaseTtl: "30000",
			description: " label ",
		});
		expect(opts).toEqual({
			id: "sub-1",
			type: "webhook",
			pattern: "orders/**",
			streams: ["events/x", "events/y"],
			webhookUrl: "https://x/ds",
			leaseTtlMs: 30000,
			description: "label",
		});
		// No wakeStream key leaks onto a webhook subscription.
		expect("wakeStream" in opts).toBe(false);
	});

	it("carries wakeStream (not webhookUrl) for a pull-wake subscription", () => {
		const opts = buildSubscriptionOptions({
			...EMPTY_SUBSCRIPTION_FORM,
			id: "sub-2",
			type: "pull-wake",
			pattern: "",
			streamsText: "events/x",
			wakeStream: "__ds/wakes/x",
		});
		expect(opts.wakeStream).toBe("__ds/wakes/x");
		expect("webhookUrl" in opts).toBe(false);
		expect(opts.streams).toEqual(["events/x"]);
	});
});

describe("previewCallbackOperation", () => {
	it("builds the POST …/callback request with a Bearer token and ack body", () => {
		const op = previewCallbackOperation("http://localhost:4437", "sub-1", "tok123", {
			wakeId: "w_abc",
			generation: 7,
			acks: [{ stream: "events/x", offset: "off-9" }],
			done: true,
		});
		expect(op.method).toBe("POST");
		expect(op.url).toBe("http://localhost:4437/__ds/subscriptions/sub-1/callback");
		expect(op.headers.Authorization).toBe("Bearer tok123");
		expect(JSON.parse(op.body as string)).toEqual({
			wake_id: "w_abc",
			generation: 7,
			acks: [{ stream: "events/x", offset: "off-9" }],
			done: true,
		});
		// It reproduces as a runnable curl.
		const curl = toCurl(op);
		expect(curl).toContain("-X POST");
		expect(curl).toContain("'Authorization: Bearer tok123'");
		expect(curl).toContain("/__ds/subscriptions/sub-1/callback");
	});
});
