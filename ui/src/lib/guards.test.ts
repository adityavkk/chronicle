import { describe, expect, it } from "vitest";
import {
	jsonByteSize,
	kindFromContentType,
	parseJsonArray,
	parseRegistryBody,
	previewOf,
	reduceRegistry,
} from "./guards";

describe("kindFromContentType", () => {
	it("classifies JSON, text, and binary", () => {
		expect(kindFromContentType("application/json")).toBe("json");
		expect(kindFromContentType("application/vnd.x+json")).toBe("json");
		expect(kindFromContentType("text/plain; charset=utf-8")).toBe("text");
		expect(kindFromContentType("application/octet-stream")).toBe("binary");
		expect(kindFromContentType(null)).toBe("binary");
	});
});

describe("parseRegistryBody + reduceRegistry", () => {
	it("parses a JSON array of events and reduces to the live set", () => {
		const body = JSON.stringify([
			{
				type: "stream",
				key: "orders/created",
				value: { path: "orders/created", contentType: "application/json", createdAt: "2026-01-01" },
				headers: { operation: "upsert" },
			},
			{
				type: "stream",
				key: "orders/deleted",
				value: { path: "orders/deleted", contentType: "text/plain" },
				headers: { operation: "upsert" },
			},
			{
				type: "stream",
				key: "orders/deleted",
				value: { path: "orders/deleted" },
				headers: { operation: "deleted" },
			},
		]);
		const current = reduceRegistry(parseRegistryBody(body));
		expect([...current.keys()]).toEqual(["orders/created"]);
		expect(current.get("orders/created")?.contentType).toBe("application/json");
	});

	it("parses newline-delimited JSON and skips malformed lines", () => {
		const body = [
			'{"key":"a","value":{"path":"a"},"headers":{"operation":"upsert"}}',
			"not json",
			'{"key":"b","value":{"path":"b"},"headers":{"operation":"upsert"}}',
		].join("\n");
		const current = reduceRegistry(parseRegistryBody(body));
		expect([...current.keys()].sort()).toEqual(["a", "b"]);
	});

	it("returns empty for empty bodies", () => {
		expect(parseRegistryBody("")).toEqual([]);
		expect(parseRegistryBody("   ")).toEqual([]);
	});
});

describe("parseJsonArray", () => {
	it("parses an array body", () => {
		const out = parseJsonArray("[1,2,3]");
		expect(out.ok && out.value).toEqual([1, 2, 3]);
	});

	it("wraps a single object into a one-element array", () => {
		const out = parseJsonArray('{"a":1}');
		expect(out.ok && out.value).toEqual([{ a: 1 }]);
	});

	it("reports malformed JSON as a typed error", () => {
		const out = parseJsonArray("{ bad");
		expect(out.ok).toBe(false);
	});
});

describe("jsonByteSize + previewOf", () => {
	it("measures UTF-8 byte size", () => {
		expect(jsonByteSize("a")).toBe(3); // "a" -> quotes + char
		expect(jsonByteSize({ x: 1 })).toBe(7); // {"x":1}
	});

	it("collapses whitespace and truncates previews", () => {
		expect(previewOf({ a: "  x\ny" })).toBe('{"a":" x\\ny"}');
		expect(previewOf("a".repeat(300)).length).toBe(160);
	});
});
