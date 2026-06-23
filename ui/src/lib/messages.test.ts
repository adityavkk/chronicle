import { describe, expect, it } from "vitest";
import {
	DEFAULT_ROW_CAP,
	OFFSET_EARLIEST,
	OFFSET_LATEST,
	type RowMatchOptions,
	batchHasTimes,
	clampRowCap,
	compileQuery,
	describeStartMode,
	extractTimestamp,
	formatBytes,
	formatTime,
	formatTimeFull,
	republishOffset,
	matchCompiled,
	resolveOffset,
	rowHaystack,
	rowMatches,
} from "./messages";
import type { GridRow } from "./types";

describe("resolveOffset", () => {
	it("maps modes to protocol offsets", () => {
		expect(resolveOffset("earliest", "")).toBe(OFFSET_EARLIEST);
		expect(resolveOffset("latest", "")).toBe(OFFSET_LATEST);
		expect(resolveOffset("at", "abc123")).toBe("abc123");
	});

	it("trims a custom offset and falls back to the beginning when blank", () => {
		expect(resolveOffset("at", "  cursor  ")).toBe("cursor");
		expect(resolveOffset("at", "   ")).toBe(OFFSET_EARLIEST);
	});
});

describe("republishOffset", () => {
	it("re-reads from the pre-append tail cursor so the new rows are shown", () => {
		expect(republishOffset("cursor-42", OFFSET_EARLIEST)).toBe("cursor-42");
	});

	it("never re-reads from the empty tail (the prior cursor wins over the toolbar)", () => {
		expect(republishOffset("cursor-42", OFFSET_LATEST)).toBe("cursor-42");
	});

	it("falls back to the toolbar offset when there is no prior read cursor", () => {
		expect(republishOffset(null, OFFSET_EARLIEST)).toBe(OFFSET_EARLIEST);
		expect(republishOffset(undefined, "cursor-7")).toBe("cursor-7");
		expect(republishOffset("", OFFSET_EARLIEST)).toBe(OFFSET_EARLIEST);
	});
});

describe("clampRowCap", () => {
	it("keeps positive integers and floors fractions", () => {
		expect(clampRowCap(100)).toBe(100);
		expect(clampRowCap(50.9)).toBe(50);
	});

	it("falls back on non-positive or non-finite input", () => {
		expect(clampRowCap(0)).toBe(DEFAULT_ROW_CAP);
		expect(clampRowCap(-5)).toBe(DEFAULT_ROW_CAP);
		expect(clampRowCap(Number.NaN)).toBe(DEFAULT_ROW_CAP);
		expect(clampRowCap(Number.NaN, 25)).toBe(25);
	});
});

describe("describeStartMode", () => {
	it("describes each mode honestly", () => {
		expect(describeStartMode("earliest", "")).toBe("Earliest (offset -1)");
		expect(describeStartMode("latest", "")).toBe("Latest (offset now)");
		expect(describeStartMode("at", "xyz")).toBe("At offset xyz");
		expect(describeStartMode("at", "")).toBe("At offset -1");
	});
});

describe("extractTimestamp", () => {
	it("reads recognized ISO string fields", () => {
		const iso = "2026-01-02T03:04:05.000Z";
		expect(extractTimestamp({ timestamp: iso })).toBe(Date.parse(iso));
		expect(extractTimestamp({ createdAt: iso })).toBe(Date.parse(iso));
		expect(extractTimestamp({ "@timestamp": iso })).toBe(Date.parse(iso));
	});

	it("treats small numbers as seconds and large numbers as ms", () => {
		expect(extractTimestamp({ ts: 1_700_000_000 })).toBe(1_700_000_000_000);
		expect(extractTimestamp({ ts: 1_700_000_000_000 })).toBe(1_700_000_000_000);
	});

	it("returns null for non-objects and missing/unparseable fields", () => {
		expect(extractTimestamp("not an object")).toBeNull();
		expect(extractTimestamp(42)).toBeNull();
		expect(extractTimestamp({ name: "x" })).toBeNull();
		expect(extractTimestamp({ time: "not a date" })).toBeNull();
		expect(extractTimestamp({ time: "" })).toBeNull();
	});
});

describe("formatTime / formatTimeFull", () => {
	it("renders empty for null and a stable ISO for a value", () => {
		expect(formatTime(null)).toBe("");
		expect(formatTimeFull(null)).toBe("");
		expect(formatTimeFull(Date.parse("2026-01-02T03:04:05.000Z"))).toBe("2026-01-02T03:04:05.000Z");
	});

	it("renders a non-empty local time for a valid epoch", () => {
		expect(formatTime(Date.parse("2026-01-02T03:04:05.000Z")).length).toBeGreaterThan(0);
	});
});

describe("formatBytes", () => {
	it("scales across units", () => {
		expect(formatBytes(512)).toBe("512 B");
		expect(formatBytes(2048)).toBe("2.0 KB");
		expect(formatBytes(5 * 1024 * 1024)).toBe("5.00 MB");
	});
});

describe("batchHasTimes", () => {
	const jsonRow = (value: unknown): GridRow => ({
		index: 0,
		byteSize: 1,
		preview: "",
		kind: "json",
		value,
	});

	it("is true only when a JSON row carries a timestamp", () => {
		expect(batchHasTimes([jsonRow({ ts: 1_700_000_000 })])).toBe(true);
		expect(batchHasTimes([jsonRow({ name: "x" })])).toBe(false);
		expect(batchHasTimes([{ index: 0, byteSize: 1, preview: "", kind: "text", value: "hi" }])).toBe(
			false,
		);
	});
});

/* ---------------------------------------------------------------------------
 * Row filtering (issue #53)
 * ------------------------------------------------------------------------ */

const jsonRow = (value: unknown, preview = ""): GridRow => ({
	index: 0,
	byteSize: 1,
	preview,
	kind: "json",
	value,
});
const textRow = (value: string, preview = value): GridRow => ({
	index: 0,
	byteSize: 1,
	preview,
	kind: "text",
	value,
});

describe("compileQuery", () => {
	it("classifies the empty query as inactive", () => {
		const q = compileQuery("   ");
		expect(q.kind).toBe("empty");
		expect(q.active).toBe(false);
	});

	it("classifies a plain query as a lowercased substring", () => {
		const q = compileQuery("  Hello ");
		expect(q.kind).toBe("substring");
		expect(q.active).toBe(true);
		expect(q.needle).toBe("hello");
	});

	it("compiles a /regex/ form and strips stateful g/y flags", () => {
		const q = compileQuery("/ab.c/gi");
		expect(q.kind).toBe("regex");
		expect(q.active).toBe(true);
		expect(q.regex?.flags).toBe("i");
	});

	it("flags an invalid regex without throwing, staying inactive", () => {
		const q = compileQuery("/(unclosed/");
		expect(q.kind).toBe("invalid");
		expect(q.active).toBe(false);
		expect(q.error).not.toBeNull();
	});

	it("treats a leading identifier + colon as a field query", () => {
		const q = compileQuery("user.id:42");
		expect(q.kind).toBe("field");
		expect(q.field).toBe("user.id");
		expect(q.needle).toBe("42");
	});

	it("does NOT treat a URL as a field query (colon followed by //)", () => {
		const q = compileQuery("http://example.com");
		expect(q.kind).toBe("substring");
	});
});

describe("rowMatches — substring mode (the default)", () => {
	it("matches case-insensitively against the preview", () => {
		const row = textRow("Order #4242 shipped");
		expect(rowMatches(row, "shipped")).toBe(true);
		expect(rowMatches(row, "SHIPPED")).toBe(true);
		expect(rowMatches(row, "cancelled")).toBe(false);
	});

	it("matches against the full stringified value beyond the truncated preview", () => {
		// The preview omits the deep field, but the value carries it.
		const row = jsonRow({ id: 1, note: "needle-in-haystack" }, "{id:1}");
		expect(rowMatches(row, "needle-in-haystack")).toBe(true);
	});

	it("matches against the formatted time of a JSON row", () => {
		const ms = Date.parse("2026-01-02T03:04:05.000Z");
		const row = jsonRow({ timestamp: ms });
		const time = formatTime(ms);
		expect(time).not.toBe("");
		expect(rowMatches(row, time)).toBe(true);
	});

	it("an empty query matches every row (no narrowing)", () => {
		expect(rowMatches(textRow("anything"), "")).toBe(true);
		expect(rowMatches(textRow("anything"), "   ")).toBe(true);
	});

	it("respects includeTime:false by dropping the time from the haystack", () => {
		const ms = Date.parse("2026-01-02T03:04:05.000Z");
		const row = jsonRow({ timestamp: ms });
		const time = formatTime(ms);
		const opts: RowMatchOptions = { includeTime: false };
		expect(rowMatches(row, time, opts)).toBe(false);
	});
});

describe("rowMatches — regex mode (/pattern/)", () => {
	it("matches a regex against the haystack", () => {
		const row = textRow("status=200 ok");
		expect(rowMatches(row, "/status=\\d+/")).toBe(true);
		expect(rowMatches(row, "/status=[a-z]+/")).toBe(false);
	});

	it("honors the case-insensitive flag", () => {
		const row = textRow("ERROR boom");
		expect(rowMatches(row, "/error/")).toBe(false);
		expect(rowMatches(row, "/error/i")).toBe(true);
	});

	it("is stateless across rows even with a global flag", () => {
		const q = compileQuery("/a/g");
		const a = textRow("aaa");
		// Re-running the same compiled regex must not skip due to lastIndex.
		expect(matchCompiled(a, q)).toBe(true);
		expect(matchCompiled(a, q)).toBe(true);
		expect(matchCompiled(a, q)).toBe(true);
	});

	it("an invalid regex narrows nothing", () => {
		expect(rowMatches(textRow("x"), "/(/")).toBe(true);
	});
});

describe("rowMatches — field mode (field:value)", () => {
	it("matches a dotted field path on JSON rows", () => {
		const row = jsonRow({ user: { id: 42, name: "ada" } });
		expect(rowMatches(row, "user.id:42")).toBe(true);
		expect(rowMatches(row, "user.name:ad")).toBe(true);
		expect(rowMatches(row, "user.name:zzz")).toBe(false);
	});

	it("an empty value means the field must exist", () => {
		const row = jsonRow({ level: "warn" });
		expect(rowMatches(row, "level:")).toBe(true);
		expect(rowMatches(row, "missing:")).toBe(false);
	});

	it("never matches a non-JSON row", () => {
		expect(rowMatches(textRow("level:warn"), "level:warn")).toBe(false);
	});
});

describe("rowHaystack", () => {
	it("includes preview + stringified value for JSON, preview only for binary", () => {
		const json = rowHaystack(jsonRow({ a: 1 }, "preview-a"));
		expect(json).toContain("preview-a");
		expect(json).toContain('"a":1');

		const binary: GridRow = {
			index: 0,
			byteSize: 4,
			preview: "<binary 4 bytes>",
			kind: "binary",
			value: new Uint8Array([1, 2, 3, 4]),
		};
		expect(rowHaystack(binary)).toBe("<binary 4 bytes>");
	});
});
