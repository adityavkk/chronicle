import { describe, expect, it } from "vitest";
import { findMetric, parseMetrics, parseSampleLine, sumSamples } from "./metrics";

/* ----------------------------------------------------------------------------
 * parseSampleLine — the line-level grammar
 * ------------------------------------------------------------------------- */

describe("parseSampleLine", () => {
	it("parses an unlabelled sample", () => {
		const s = parseSampleLine("chronicle_sweep_wakes_total 42");
		expect(s).not.toBeNull();
		expect(s?.name).toBe("chronicle_sweep_wakes_total");
		expect(s?.series).toBe("chronicle_sweep_wakes_total");
		expect(s?.labels).toEqual({});
		expect(s?.value).toBe(42);
	});

	it("parses a labelled sample and keeps label order-independent", () => {
		const s = parseSampleLine('chronicle_wake_delivery_seconds_count{outcome="ok"} 7');
		expect(s?.name).toBe("chronicle_wake_delivery_seconds");
		expect(s?.series).toBe("chronicle_wake_delivery_seconds_count");
		expect(s?.labels).toEqual({ outcome: "ok" });
		expect(s?.value).toBe(7);
	});

	it("parses multiple labels including a histogram le bucket", () => {
		const s = parseSampleLine('chronicle_fanout_seconds_bucket{le="0.005"} 3');
		expect(s?.name).toBe("chronicle_fanout_seconds");
		expect(s?.series).toBe("chronicle_fanout_seconds_bucket");
		expect(s?.labels).toEqual({ le: "0.005" });
	});

	it("preserves NaN / +Inf / -Inf values", () => {
		expect(parseSampleLine("m NaN")?.value).toBeNaN();
		expect(parseSampleLine('m{le="+Inf"} +Inf')?.value).toBe(Number.POSITIVE_INFINITY);
		expect(parseSampleLine("m -Inf")?.value).toBe(Number.NEGATIVE_INFINITY);
	});

	it("ignores an optional trailing scrape timestamp", () => {
		const s = parseSampleLine("m 5 1700000000000");
		expect(s?.value).toBe(5);
	});

	it("unescapes quoted label values", () => {
		const s = parseSampleLine('m{path="a\\"b",note="x\\ny"} 1');
		expect(s?.labels).toEqual({ path: 'a"b', note: "x\ny" });
	});

	it("returns null for a line with no value", () => {
		expect(parseSampleLine("just_a_name")).toBeNull();
		expect(parseSampleLine("")).toBeNull();
	});
});

/* ----------------------------------------------------------------------------
 * parseMetrics — families, HELP/TYPE, grouping by base name
 * ------------------------------------------------------------------------- */

const DOC = `
# HELP chronicle_sweep_wakes_total total wakes issued by recovery sweep
# TYPE chronicle_sweep_wakes_total counter
chronicle_sweep_wakes_total 17

# HELP chronicle_wake_delivery_seconds webhook POST round-trip duration
# TYPE chronicle_wake_delivery_seconds histogram
chronicle_wake_delivery_seconds_bucket{outcome="ok",le="0.005"} 2
chronicle_wake_delivery_seconds_bucket{outcome="ok",le="0.01"} 5
chronicle_wake_delivery_seconds_bucket{outcome="ok",le="+Inf"} 9
chronicle_wake_delivery_seconds_sum{outcome="ok"} 0.42
chronicle_wake_delivery_seconds_count{outcome="ok"} 9
chronicle_wake_delivery_seconds_count{outcome="failed"} 1

# a bare comment line that should be ignored
go_goroutines 31
`;

describe("parseMetrics", () => {
	it("groups the bucket/sum/count series of a histogram under one base family", () => {
		const snap = parseMetrics(DOC);
		const hist = findMetric(snap, "chronicle_wake_delivery_seconds");
		expect(hist).not.toBeNull();
		expect(hist?.type).toBe("histogram");
		expect(hist?.help).toBe("webhook POST round-trip duration");
		// 3 buckets + 1 sum + 2 counts = 6 samples for the family.
		expect(hist?.samples).toHaveLength(6);
		const series = new Set(hist?.samples.map((s) => s.series));
		expect(series).toEqual(
			new Set([
				"chronicle_wake_delivery_seconds_bucket",
				"chronicle_wake_delivery_seconds_sum",
				"chronicle_wake_delivery_seconds_count",
			]),
		);
	});

	it("reads HELP + TYPE for a counter and keeps families in first-seen order", () => {
		const snap = parseMetrics(DOC);
		const names = snap.metrics.map((m) => m.name);
		expect(names).toEqual([
			"chronicle_sweep_wakes_total",
			"chronicle_wake_delivery_seconds",
			"go_goroutines",
		]);
		const counter = findMetric(snap, "chronicle_sweep_wakes_total");
		expect(counter?.type).toBe("counter");
		expect(counter?.samples[0]?.value).toBe(17);
	});

	it("treats a sample with no declared TYPE as untyped", () => {
		const snap = parseMetrics(DOC);
		expect(findMetric(snap, "go_goroutines")?.type).toBe("untyped");
	});

	it("stamps a parsedAt and is tolerant of an empty document", () => {
		const snap = parseMetrics("");
		expect(snap.metrics).toEqual([]);
		expect(snap.parsedAt).toBeGreaterThan(0);
	});

	it("skips unparseable lines without throwing", () => {
		const snap = parseMetrics("garbage line with no value field nope\nm 1\n{bad} 2");
		// Only `m 1` is a valid sample.
		expect(snap.metrics.map((x) => x.name)).toEqual(["m"]);
	});
});

/* ----------------------------------------------------------------------------
 * sumSamples — convenience accessor
 * ------------------------------------------------------------------------- */

describe("sumSamples", () => {
	it("sums the _count series across label sets for a histogram", () => {
		const snap = parseMetrics(DOC);
		// counts: ok=9, failed=1 -> 10. Buckets + sum are excluded.
		expect(sumSamples(snap, "chronicle_wake_delivery_seconds")).toBe(10);
	});

	it("sums the bare series for a counter", () => {
		const snap = parseMetrics(DOC);
		expect(sumSamples(snap, "chronicle_sweep_wakes_total")).toBe(17);
	});

	it("returns 0 for an absent family", () => {
		expect(sumSamples(parseMetrics(DOC), "nope")).toBe(0);
	});
});
