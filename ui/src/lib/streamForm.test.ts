import { describe, expect, it } from "vitest";
import { toCurl } from "./curl";
import {
	contentTypeForKind,
	forkSelection,
	isProducerValid,
	parseSubOffset,
	previewAppendOperation,
	previewCloseOperation,
	previewCreateOperation,
	previewDeleteOperation,
	previewStreamUrl,
	toProducerIdentity,
	validateExpiresAt,
	validateFirstN,
	validateJsonBatch,
	validateProducer,
	validateStreamPath,
	validateSubOffset,
	validateTtl,
} from "./streamForm";

describe("contentTypeForKind", () => {
	it("maps each radio kind to its wire Content-Type", () => {
		expect(contentTypeForKind("text")).toBe("text/plain");
		expect(contentTypeForKind("json")).toBe("application/json");
		expect(contentTypeForKind("binary")).toBe("application/octet-stream");
	});
});

describe("validateStreamPath", () => {
	it("accepts a normal segmented path", () => {
		expect(validateStreamPath("orders/created")).toBeNull();
		expect(validateStreamPath("a")).toBeNull();
	});

	it("rejects empties, slashes at the ends, double slashes, and whitespace", () => {
		expect(validateStreamPath("")).not.toBeNull();
		expect(validateStreamPath("   ")).not.toBeNull();
		expect(validateStreamPath("/leading")).not.toBeNull();
		expect(validateStreamPath("trailing/")).not.toBeNull();
		expect(validateStreamPath("a//b")).not.toBeNull();
		expect(validateStreamPath("a b")).not.toBeNull();
	});

	it("rejects scheme/query/fragment and the reserved registry path", () => {
		expect(validateStreamPath("http://x/y")).not.toBeNull();
		expect(validateStreamPath("a?b")).not.toBeNull();
		expect(validateStreamPath("a#b")).not.toBeNull();
		expect(validateStreamPath("__registry__")).not.toBeNull();
	});
});

describe("validateTtl + validateExpiresAt", () => {
	it("accepts blank and valid durations, rejects garbage", () => {
		expect(validateTtl("")).toBeNull();
		expect(validateTtl("1h")).toBeNull();
		expect(validateTtl("2h30m")).toBeNull();
		expect(validateTtl("90s")).toBeNull();
		expect(validateTtl("soon")).not.toBeNull();
		expect(validateTtl("1hour")).not.toBeNull();
	});

	it("accepts blank and RFC3339 timestamps, rejects garbage", () => {
		expect(validateExpiresAt("")).toBeNull();
		expect(validateExpiresAt("2030-01-01T00:00:00Z")).toBeNull();
		expect(validateExpiresAt("not-a-date")).not.toBeNull();
	});
});

describe("validateJsonBatch", () => {
	it("accepts an array and reports its element count, normalized", () => {
		const out = validateJsonBatch('[{"id":1}, {"id":2}]');
		expect(out.ok).toBe(true);
		if (out.ok) {
			expect(out.count).toBe(2);
			expect(out.normalized).toBe('[{"id":1},{"id":2}]');
		}
	});

	it("forgivingly wraps a lone object into a one-element batch", () => {
		const out = validateJsonBatch('{"hello":"world"}');
		expect(out.ok).toBe(true);
		if (out.ok) {
			expect(out.count).toBe(1);
			expect(out.normalized).toBe('[{"hello":"world"}]');
		}
	});

	it("rejects blank, invalid JSON, and an empty array", () => {
		expect(validateJsonBatch("").ok).toBe(false);
		expect(validateJsonBatch("{not json").ok).toBe(false);
		expect(validateJsonBatch("[]").ok).toBe(false);
	});
});

describe("producer validation", () => {
	it("requires an id and non-negative integer epoch + seq", () => {
		expect(isProducerValid(validateProducer({ id: "p1", epoch: "0", seq: "0" }))).toBe(true);
		expect(isProducerValid(validateProducer({ id: "", epoch: "0", seq: "0" }))).toBe(false);
		expect(isProducerValid(validateProducer({ id: "p1", epoch: "-1", seq: "0" }))).toBe(false);
		expect(isProducerValid(validateProducer({ id: "p1", epoch: "x", seq: "0" }))).toBe(false);
		expect(isProducerValid(validateProducer({ id: "p1", epoch: "1.5", seq: "0" }))).toBe(false);
	});

	it("builds a typed identity from valid values, null otherwise", () => {
		expect(toProducerIdentity({ id: "p1", epoch: "2", seq: "7" })).toEqual({
			id: "p1",
			epoch: 2,
			seq: 7,
		});
		expect(toProducerIdentity({ id: "", epoch: "2", seq: "7" })).toBeNull();
	});
});

describe("sub-offset", () => {
	it("validates and parses a blank or non-negative integer", () => {
		expect(validateSubOffset("")).toBeNull();
		expect(validateSubOffset("3")).toBeNull();
		expect(validateSubOffset("-1")).not.toBeNull();
		expect(parseSubOffset("")).toBeUndefined();
		expect(parseSubOffset("4")).toBe(4);
		expect(parseSubOffset("bad")).toBeUndefined();
	});
});

describe("forkSelection", () => {
	it('maps "everything" to the tail with no sub-offset', () => {
		expect(forkSelection("everything", 0)).toEqual({ offset: "now", subOffset: undefined });
	});

	it('maps "nothing" to the beginning with no sub-offset', () => {
		expect(forkSelection("nothing", 0)).toEqual({ offset: "-1", subOffset: undefined });
	});

	it('maps "first-n" to the beginning + N as the sub-offset', () => {
		expect(forkSelection("first-n", 3)).toEqual({ offset: "-1", subOffset: 3 });
		expect(forkSelection("first-n", 0)).toEqual({ offset: "-1", subOffset: 0 });
	});

	it("clamps an out-of-range N defensively", () => {
		expect(forkSelection("first-n", -5)).toEqual({ offset: "-1", subOffset: 0 });
		expect(forkSelection("first-n", Number.NaN)).toEqual({ offset: "-1", subOffset: 0 });
	});
});

describe("validateFirstN", () => {
	it("requires a value and rejects non-whole / negative input", () => {
		expect(validateFirstN("", null)).not.toBeNull();
		expect(validateFirstN("2.5", null)).not.toBeNull();
		expect(validateFirstN("-1", null)).not.toBeNull();
		expect(validateFirstN("abc", null)).not.toBeNull();
	});

	it("accepts any N ≥ 0 when the count is unknown", () => {
		expect(validateFirstN("0", null)).toBeNull();
		expect(validateFirstN("9999", null)).toBeNull();
	});

	it("rejects an N that overshoots a known count, accepts up to it", () => {
		expect(validateFirstN("3", 3)).toBeNull();
		expect(validateFirstN("0", 3)).toBeNull();
		const err = validateFirstN("4", 3);
		expect(err).not.toBeNull();
		expect(err).toContain("overshoots");
		expect(validateFirstN("2", 1)).toContain("1 message");
	});
});

describe("operation previews mirror the wire request", () => {
	const base = "http://localhost:4437";
	const root = "/v1/stream";

	it("builds an absolute, segment-encoded stream URL", () => {
		expect(previewStreamUrl(base, root, "a b/c")).toBe("http://localhost:4437/v1/stream/a%20b/c");
		expect(previewStreamUrl(base, root, "/leading")).toBe(
			"http://localhost:4437/v1/stream/leading",
		);
	});

	it("create: emits Content-Type, then TTL / Expires-At / closed in order", () => {
		const op = previewCreateOperation(base, root, {
			path: "demo",
			contentType: "application/json",
			ttl: "1h",
			expiresAt: "2030-01-01T00:00:00Z",
			closed: true,
		});
		expect(op.method).toBe("PUT");
		expect(op.headers["Content-Type"]).toBe("application/json");
		expect(op.headers["Stream-TTL"]).toBe("1h");
		expect(op.headers["Stream-Expires-At"]).toBe("2030-01-01T00:00:00Z");
		expect(op.headers["Stream-Closed"]).toBe("true");
		const cmd = toCurl(op);
		expect(cmd).toContain("-X PUT");
		expect(cmd.indexOf("Content-Type")).toBeLessThan(cmd.indexOf("Stream-TTL"));
	});

	it("create-fork: emits the fork headers", () => {
		const op = previewCreateOperation(base, root, {
			path: "demo-fork",
			contentType: "application/octet-stream",
			fork: { fromPath: "demo", offset: "42", subOffset: 1 },
		});
		expect(op.headers["Stream-Forked-From"]).toBe("demo");
		expect(op.headers["Stream-Fork-Offset"]).toBe("42");
		expect(op.headers["Stream-Fork-Sub-Offset"]).toBe("1");
	});

	it("append: carries the body and optional producer + close headers", () => {
		const op = previewAppendOperation(base, root, "demo", {
			body: '[{"id":1}]',
			contentType: "application/json",
			producer: { id: "p1", epoch: 0, seq: 3 },
			closeAfter: true,
		});
		expect(op.method).toBe("POST");
		expect(op.body).toBe('[{"id":1}]');
		expect(op.headers["Producer-Id"]).toBe("p1");
		expect(op.headers["Producer-Epoch"]).toBe("0");
		expect(op.headers["Producer-Seq"]).toBe("3");
		expect(op.headers["Stream-Closed"]).toBe("true");
		expect(toCurl(op)).toContain(`--data-raw '[{"id":1}]'`);
	});

	it("close + delete previews match the client's shapes", () => {
		const close = previewCloseOperation(base, root, "demo");
		expect(close.method).toBe("POST");
		expect(close.headers["Stream-Closed"]).toBe("true");
		expect(close.body).toBe("");
		const del = previewDeleteOperation(base, root, "demo");
		expect(del.method).toBe("DELETE");
		expect(toCurl(del)).toContain("-X DELETE");
	});
});
