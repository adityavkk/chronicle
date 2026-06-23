import { describe, expect, it } from "vitest";
import {
	exchangeOutcome,
	explainOffset,
	isSignificantHeader,
	partitionHeaders,
	protocolHeaderRows,
	splitUrl,
	statusLabel,
	toCurl,
} from "./protocol";
import type { HttpExchange } from "./types";

function exchange(overrides: Partial<HttpExchange> = {}): HttpExchange {
	const base: HttpExchange = {
		method: "GET",
		url: "http://localhost:4437/v1/stream/orders?offset=-1",
		requestHeaders: { Accept: "*/*" },
		status: 200,
		statusText: "OK",
		responseHeaders: {
			"content-type": "application/json",
			"stream-next-offset": "42",
			etag: 'W/"abc"',
			"x-trace-id": "t-1",
		},
		protocol: {
			streamNextOffset: "42",
			streamClosed: null,
			streamUpToDate: "true",
			etag: 'W/"abc"',
			contentType: "application/json",
		},
		at: 1_700_000_000_000,
		durationMs: 12,
	};
	return { ...base, ...overrides };
}

describe("protocolHeaderRows", () => {
	it("orders the resume cursor first and carries values + notes", () => {
		const rows = protocolHeaderRows(exchange());
		expect(rows.map((r) => r.name)).toEqual([
			"Stream-Next-Offset",
			"Stream-Up-To-Date",
			"Stream-Closed",
			"ETag",
			"Content-Type",
		]);
		expect(rows[0]?.value).toBe("42");
		expect(rows[2]?.value).toBeNull();
		for (const row of rows) {
			expect(row.note.length).toBeGreaterThan(0);
		}
	});
});

describe("isSignificantHeader / partitionHeaders", () => {
	it("recognizes the protocol headers case-insensitively", () => {
		expect(isSignificantHeader("Stream-Next-Offset")).toBe(true);
		expect(isSignificantHeader("content-type")).toBe(true);
		expect(isSignificantHeader("x-trace-id")).toBe(false);
	});

	it("partitions and sorts headers, significant first", () => {
		const { significant, other } = partitionHeaders(exchange().responseHeaders);
		expect(significant.map(([k]) => k)).toEqual(["content-type", "etag", "stream-next-offset"]);
		expect(other.map(([k]) => k)).toEqual(["x-trace-id"]);
	});
});

describe("exchangeOutcome / statusLabel", () => {
	it("classifies ok, err, and fail", () => {
		expect(exchangeOutcome(exchange({ status: 200 }))).toBe("ok");
		expect(exchangeOutcome(exchange({ status: 404 }))).toBe("err");
		expect(exchangeOutcome(exchange({ status: 0 }))).toBe("fail");
	});

	it("labels status, falling back to the error on a network failure", () => {
		expect(statusLabel(exchange({ status: 200, statusText: "OK" }))).toBe("200 OK");
		expect(statusLabel(exchange({ status: 204, statusText: "" }))).toBe("204");
		expect(statusLabel(exchange({ status: 0, error: "Failed to fetch" }))).toBe("Failed to fetch");
	});
});

describe("explainOffset", () => {
	it("special-cases the reserved cursors", () => {
		expect(explainOffset("-1")).toContain("beginning");
		expect(explainOffset("now")).toContain("tail");
		expect(explainOffset("opaque-123")).toContain("opaque cursor");
	});
});

describe("toCurl", () => {
	it("reproduces a GET with headers and a single-quoted url", () => {
		const cmd = toCurl(exchange());
		expect(cmd).toBe("curl -H 'Accept: */*' 'http://localhost:4437/v1/stream/orders?offset=-1'");
	});

	it("uses -I for HEAD and -X for other verbs", () => {
		expect(toCurl(exchange({ method: "HEAD", requestHeaders: {} }))).toBe(
			"curl -I 'http://localhost:4437/v1/stream/orders?offset=-1'",
		);
		expect(toCurl(exchange({ method: "DELETE", requestHeaders: {} }))).toContain("-X DELETE");
	});

	it("escapes single quotes inside header values", () => {
		const cmd = toCurl(exchange({ requestHeaders: { "X-Note": "it's fine" } }));
		expect(cmd).toContain("'X-Note: it'\\''s fine'");
	});
});

describe("splitUrl", () => {
	it("splits origin+path from an ordered query list", () => {
		const { base, query } = splitUrl("http://localhost:4437/v1/stream/orders?offset=-1&x=2");
		expect(base).toBe("http://localhost:4437/v1/stream/orders");
		expect(query).toEqual([
			["offset", "-1"],
			["x", "2"],
		]);
	});

	it("falls back to the whole url when unparseable", () => {
		const { base, query } = splitUrl("not a url");
		expect(base).toBe("not a url");
		expect(query).toEqual([]);
	});
});
