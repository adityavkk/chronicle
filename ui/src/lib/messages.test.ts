import { describe, expect, it } from "vitest";
import {
	DEFAULT_ROW_CAP,
	OFFSET_EARLIEST,
	OFFSET_LATEST,
	batchHasTimes,
	clampRowCap,
	describeStartMode,
	extractTimestamp,
	formatBytes,
	formatTime,
	formatTimeFull,
	republishOffset,
	resolveOffset,
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
