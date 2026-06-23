import { describe, expect, it } from "vitest";
import { coerceConfig } from "./config";

describe("coerceConfig", () => {
	it("reads defaultServer + captureBase from the binary's config", () => {
		expect(
			coerceConfig({
				defaultServer: "http://localhost:4437",
				captureBase: "http://localhost:4438",
			}),
		).toEqual({ defaultServer: "http://localhost:4437", captureBase: "http://localhost:4438" });
	});

	it("maps empty / missing fields to null (the 'nothing to prefill' sentinel)", () => {
		expect(coerceConfig({ defaultServer: "", captureBase: "  " })).toEqual({
			defaultServer: null,
			captureBase: null,
		});
		expect(coerceConfig({})).toEqual({ defaultServer: null, captureBase: null });
	});

	it("trims surrounding whitespace", () => {
		expect(coerceConfig({ captureBase: "  http://h:4438  " }).captureBase).toBe("http://h:4438");
	});

	it("falls back to all-null for a non-object body", () => {
		expect(coerceConfig(null)).toEqual({ defaultServer: null, captureBase: null });
		expect(coerceConfig("nope")).toEqual({ defaultServer: null, captureBase: null });
	});
});
