import { describe, expect, it } from "vitest";
import { parseSchema, skeletonBatchText, skeletonFromSchema, summarizeSchema } from "./schema";

describe("parseSchema", () => {
	it("rejects empty, non-JSON, and non-object input", () => {
		expect(parseSchema("")).toMatchObject({ ok: false });
		expect(parseSchema("   ")).toMatchObject({ ok: false });
		expect(parseSchema("not json")).toMatchObject({ ok: false });
		expect(parseSchema("[1,2,3]")).toMatchObject({ ok: false });
		expect(parseSchema("42")).toMatchObject({ ok: false });
	});

	it("accepts a JSON object and summarizes it", () => {
		const out = parseSchema('{"type":"object","properties":{"a":{"type":"string"}}}');
		expect(out.ok).toBe(true);
		if (out.ok) expect(out.summary).toBe("object · 1 field");
	});
});

describe("skeletonFromSchema — scalars", () => {
	it("maps primitive types to empty-ish values", () => {
		expect(skeletonFromSchema({ type: "string" })).toBe("");
		expect(skeletonFromSchema({ type: "number" })).toBe(0);
		expect(skeletonFromSchema({ type: "integer" })).toBe(0);
		expect(skeletonFromSchema({ type: "boolean" })).toBe(false);
		expect(skeletonFromSchema({ type: "null" })).toBe(null);
	});

	it("returns null for an unknown/absent/unsupported type", () => {
		expect(skeletonFromSchema({})).toBe(null);
		expect(skeletonFromSchema({ $ref: "#/defs/X" })).toBe(null);
		expect(skeletonFromSchema("nonsense")).toBe(null);
	});

	it("uses the first non-null type of a type[] union", () => {
		expect(skeletonFromSchema({ type: ["null", "string"] })).toBe("");
		expect(skeletonFromSchema({ type: ["integer", "null"] })).toBe(0);
	});
});

describe("skeletonFromSchema — value precedence", () => {
	it("prefers const, then default, then example(s), then enum", () => {
		expect(skeletonFromSchema({ type: "string", const: "C" })).toBe("C");
		expect(skeletonFromSchema({ type: "string", default: "D" })).toBe("D");
		expect(skeletonFromSchema({ type: "string", examples: ["E1", "E2"] })).toBe("E1");
		expect(skeletonFromSchema({ type: "string", example: "EX" })).toBe("EX");
		expect(skeletonFromSchema({ type: "string", enum: ["red", "green"] })).toBe("red");
	});

	it("const wins over default and an empty examples array is ignored", () => {
		expect(skeletonFromSchema({ const: 1, default: 2 })).toBe(1);
		expect(skeletonFromSchema({ type: "string", examples: [] })).toBe("");
	});
});

describe("skeletonFromSchema — objects and arrays", () => {
	it("includes every declared property, recursing nested shapes", () => {
		const schema = {
			type: "object",
			properties: {
				id: { type: "integer" },
				name: { type: "string" },
				active: { type: "boolean" },
				tags: { type: "array", items: { type: "string" } },
				meta: {
					type: "object",
					properties: { source: { type: "string" }, score: { type: "number" } },
				},
			},
		};
		expect(skeletonFromSchema(schema)).toEqual({
			id: 0,
			name: "",
			active: false,
			tags: [""],
			meta: { source: "", score: 0 },
		});
	});

	it("infers object from properties and array from items when type is absent", () => {
		expect(skeletonFromSchema({ properties: { a: { type: "string" } } })).toEqual({ a: "" });
		expect(skeletonFromSchema({ items: { type: "integer" } })).toEqual([0]);
	});

	it("gives an empty array when items is missing", () => {
		expect(skeletonFromSchema({ type: "array" })).toEqual([]);
	});

	it("uses the first entry of a tuple items[] schema", () => {
		expect(skeletonFromSchema({ type: "array", items: [{ type: "boolean" }] })).toEqual([false]);
	});
});

describe("skeletonBatchText", () => {
	it("wraps one skeleton message in an array, pretty-printed", () => {
		const text = skeletonBatchText('{"type":"object","properties":{"event":{"type":"string"}}}');
		expect(text).not.toBeNull();
		expect(JSON.parse(text as string)).toEqual([{ event: "" }]);
		expect(text).toContain("\n"); // pretty-printed
	});

	it("returns null when the schema text does not parse", () => {
		expect(skeletonBatchText("{ bad json")).toBeNull();
		expect(skeletonBatchText("")).toBeNull();
	});
});

describe("summarizeSchema", () => {
	it("describes objects, arrays, and scalars", () => {
		expect(summarizeSchema({ type: "object", properties: { a: {}, b: {} } })).toBe(
			"object · 2 fields",
		);
		expect(summarizeSchema({ type: "array", items: { type: "object" } })).toBe("array of object");
		expect(summarizeSchema({ type: "string" })).toBe("string");
		expect(summarizeSchema({})).toBe("any");
	});
});
