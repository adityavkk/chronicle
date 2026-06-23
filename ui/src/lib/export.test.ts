import { afterEach, describe, expect, it, vi } from "vitest";
import {
	CSV_MIME,
	NDJSON_MIME,
	buildExportFilename,
	bytesToBase64,
	downloadBlob,
	rawExtensionForKind,
	rawMimeForKind,
	rowsToCsv,
	rowsToNdjson,
} from "./export";
import type { GridRow } from "./types";

/** Build a JSON grid row with sensible defaults for the value under test. */
function jsonRow(index: number, value: unknown, byteSize = 0): GridRow {
	return { index, byteSize, preview: "", kind: "json", value };
}
function textRow(index: number, value: string, byteSize = value.length): GridRow {
	return { index, byteSize, preview: value, kind: "text", value };
}
function binaryRow(index: number, value: Uint8Array): GridRow {
	return { index, byteSize: value.byteLength, preview: "", kind: "binary", value };
}

describe("rowsToNdjson", () => {
	it("emits one JSON value per line with a trailing newline", () => {
		const out = rowsToNdjson([jsonRow(0, { a: 1 }), jsonRow(1, { b: 2 })]);
		expect(out).toBe('{"a":1}\n{"b":2}\n');
	});

	it("is empty for no rows", () => {
		expect(rowsToNdjson([])).toBe("");
	});

	it("encodes a text row as a JSON string and round-trips per line", () => {
		const out = rowsToNdjson([textRow(0, 'he said "hi"\nbye')]);
		expect(out).toBe('"he said \\"hi\\"\\nbye"\n');
		expect(JSON.parse(out.trimEnd())).toBe('he said "hi"\nbye');
	});

	it("encodes a binary row as a base64 JSON string", () => {
		const out = rowsToNdjson([binaryRow(0, new Uint8Array([102, 111, 111]))]);
		expect(out).toBe('"Zm9v"\n');
	});

	it("preserves null, numbers, and arrays as valid lines", () => {
		const out = rowsToNdjson([jsonRow(0, null), jsonRow(1, 42), jsonRow(2, [1, "x"])]);
		expect(out).toBe('null\n42\n[1,"x"]\n');
	});

	it("never throws on an unserializable value, falling back to a string", () => {
		const cyclic: Record<string, unknown> = {};
		cyclic.self = cyclic;
		const out = rowsToNdjson([jsonRow(0, cyclic)]);
		// Falls back to JSON.stringify(String(value)); the point is it does not throw.
		expect(() => JSON.parse(out.trimEnd())).not.toThrow();
	});
});

describe("rowsToCsv", () => {
	it("writes a header and RFC-4180 CRLF-separated records", () => {
		const out = rowsToCsv([jsonRow(0, { a: 1 }, 7)]);
		expect(out).toBe('index,byteSize,time,value\r\n0,7,,"{""a"":1}"\r\n');
	});

	it("has a header row even with no data rows", () => {
		expect(rowsToCsv([])).toBe("index,byteSize,time,value\r\n");
	});

	it("quote-escapes commas, quotes, and newlines per RFC-4180", () => {
		const out = rowsToCsv([textRow(0, 'a,b "c"\nd', 9)]);
		const dataLine = out.split("\r\n")[1];
		expect(dataLine).toBe('0,9,,"a,b ""c""\nd"');
	});

	it("emits the ISO event time for a JSON row carrying a timestamp", () => {
		const out = rowsToCsv([jsonRow(0, { timestamp: "2024-01-02T03:04:05.000Z", v: 1 }, 10)]);
		const fields = (out.split("\r\n")[1] ?? "").split(",");
		expect(fields[2]).toBe("2024-01-02T03:04:05.000Z");
	});

	it("flattens a binary row's value as base64 and leaves the time blank", () => {
		const out = rowsToCsv([binaryRow(0, new Uint8Array([102, 111, 111]))]);
		expect(out).toBe("index,byteSize,time,value\r\n0,3,,Zm9v\r\n");
	});
});

describe("bytesToBase64", () => {
	it("encodes with correct padding", () => {
		expect(bytesToBase64(new Uint8Array([]))).toBe("");
		expect(bytesToBase64(new Uint8Array([102]))).toBe("Zg==");
		expect(bytesToBase64(new Uint8Array([102, 111]))).toBe("Zm8=");
		expect(bytesToBase64(new Uint8Array([102, 111, 111]))).toBe("Zm9v");
		expect(bytesToBase64(new Uint8Array([0, 255, 16]))).toBe("AP8Q");
	});
});

describe("buildExportFilename", () => {
	it("joins a sanitized path + offset with the extension", () => {
		expect(buildExportFilename("orders/created", "-1", "ndjson")).toBe("orders-created@-1.ndjson");
		expect(buildExportFilename("a/b/c", "now", "csv")).toBe("a-b-c@now.csv");
	});

	it("sanitizes unsafe offset cursors and caps their length", () => {
		const name = buildExportFilename("s", "aa/bb+cc==dd", "json");
		expect(name).toBe("s@aa-bb-cc-dd.json");
		const long = buildExportFilename("s", "x".repeat(80), "bin");
		expect(long.length).toBeLessThanOrEqual("s@".length + 40 + ".bin".length);
	});

	it("falls back for a blank path or offset and strips a leading dot on the ext", () => {
		expect(buildExportFilename("", "", ".csv")).toBe("stream@0.csv");
		expect(buildExportFilename("///", "@@@", "txt")).toBe("stream@0.txt");
	});
});

describe("raw kind helpers", () => {
	it("maps stream kinds to a MIME type and extension", () => {
		expect(rawMimeForKind("json")).toBe("application/json");
		expect(rawMimeForKind("text")).toBe("text/plain;charset=utf-8");
		expect(rawMimeForKind("binary")).toBe("application/octet-stream");
		expect(rawExtensionForKind("json")).toBe("json");
		expect(rawExtensionForKind("text")).toBe("txt");
		expect(rawExtensionForKind("binary")).toBe("bin");
	});

	it("exposes stable MIME constants for the formatters", () => {
		expect(NDJSON_MIME).toBe("application/x-ndjson");
		expect(CSV_MIME).toBe("text/csv;charset=utf-8");
	});
});

describe("downloadBlob", () => {
	const realCreate = (globalThis.URL as { createObjectURL?: unknown }).createObjectURL;
	const realRevoke = (globalThis.URL as { revokeObjectURL?: unknown }).revokeObjectURL;

	afterEach(() => {
		vi.restoreAllMocks();
		vi.unstubAllGlobals();
		(globalThis.URL as { createObjectURL?: unknown }).createObjectURL = realCreate;
		(globalThis.URL as { revokeObjectURL?: unknown }).revokeObjectURL = realRevoke;
	});

	it("returns false when the object-URL API is unavailable", () => {
		(globalThis.URL as { createObjectURL?: unknown }).createObjectURL = undefined;
		expect(downloadBlob("x.ndjson", NDJSON_MIME, "{}\n")).toBe(false);
	});

	it("returns false when there is no document", () => {
		vi.stubGlobal("document", undefined);
		expect(downloadBlob("x.ndjson", NDJSON_MIME, "{}\n")).toBe(false);
	});

	it("creates and revokes an object URL and clicks an <a download> with the filename", () => {
		const createSpy = vi.fn((_blob: Blob) => "blob:test-url");
		const revokeSpy = vi.fn();
		(globalThis.URL as { createObjectURL?: unknown }).createObjectURL = createSpy;
		(globalThis.URL as { revokeObjectURL?: unknown }).revokeObjectURL = revokeSpy;

		let clickedName = "";
		const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (
			this: HTMLAnchorElement,
		) {
			clickedName = this.download;
		});

		const ok = downloadBlob("orders@-1.ndjson", NDJSON_MIME, '{"a":1}\n');
		expect(ok).toBe(true);
		expect(createSpy).toHaveBeenCalledTimes(1);
		const blobArg = createSpy.mock.calls[0]?.[0];
		expect(blobArg).toBeInstanceOf(Blob);
		expect(blobArg?.type).toBe(NDJSON_MIME);
		expect(clickSpy).toHaveBeenCalledTimes(1);
		expect(clickedName).toBe("orders@-1.ndjson");
		expect(revokeSpy).toHaveBeenCalledWith("blob:test-url");
	});
});
