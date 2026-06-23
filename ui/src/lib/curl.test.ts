import { describe, expect, it } from "vitest";
import { toCurl } from "./curl";
import type { Operation } from "./types";

function op(overrides: Partial<Operation> = {}): Operation {
	const base: Operation = {
		method: "GET",
		url: "http://localhost:4437/v1/stream/orders?offset=-1",
		headers: { Accept: "*/*" },
	};
	return { ...base, ...overrides };
}

describe("toCurl", () => {
	it("reproduces a GET with headers and a single-quoted url", () => {
		expect(toCurl(op())).toBe(
			"curl -H 'Accept: */*' 'http://localhost:4437/v1/stream/orders?offset=-1'",
		);
	});

	it("uses -I for HEAD and -X for other verbs", () => {
		expect(toCurl(op({ method: "HEAD", headers: {} }))).toBe(
			"curl -I 'http://localhost:4437/v1/stream/orders?offset=-1'",
		);
		expect(toCurl(op({ method: "DELETE", headers: {} }))).toContain("-X DELETE");
	});

	it("emits header flags in insertion order", () => {
		const cmd = toCurl(
			op({
				method: "PUT",
				headers: { "Content-Type": "application/json", "Stream-TTL": "1h" },
			}),
		);
		expect(cmd.indexOf("Content-Type")).toBeLessThan(cmd.indexOf("Stream-TTL"));
	});

	it("emits a string body as --data-raw, single-quoted", () => {
		const cmd = toCurl(op({ method: "POST", headers: {}, body: '[{"id":1}]' }));
		expect(cmd).toContain(`--data-raw '[{"id":1}]'`);
	});

	it("escapes single quotes inside a body and headers", () => {
		const cmd = toCurl(op({ method: "POST", headers: { "X-Note": "it's fine" }, body: "a'b" }));
		expect(cmd).toContain("'X-Note: it'\\''s fine'");
		expect(cmd).toContain("--data-raw 'a'\\''b'");
	});

	it("represents a binary body as --data-binary @- with a byte-count note", () => {
		const cmd = toCurl(op({ method: "POST", headers: {}, body: new Uint8Array([1, 2, 3, 4]) }));
		expect(cmd).toContain("--data-binary @-");
		expect(cmd).toContain("# 4 bytes piped on stdin");
		// The body is never inlined as a corrupted string.
		expect(cmd).not.toContain("--data-raw");
	});
});
