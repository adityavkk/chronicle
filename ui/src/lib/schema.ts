/**
 * Pure helpers for the optional per-stream message schema — a CLIENT-SIDE
 * authoring aid. The Durable Streams protocol stores no schema (the API is
 * frozen), so dsui keeps it in the browser, per connection, exactly like the
 * known-subscription-ids and metrics-URL it already remembers. A schema
 * describes ONE message; {@link skeletonFromSchema} turns it into an empty
 * instance the publish composer pre-fills, so you edit values instead of typing
 * JSON from scratch.
 *
 * It understands a pragmatic subset of JSON Schema (draft-07-ish): `type`
 * (string or array), `properties`, `items`, `enum`, `const`, `default`,
 * `example`/`examples`. Unknown keywords (`$ref`, `oneOf`, `allOf`, …) are
 * ignored — the worst case is a `null` placeholder, never a throw. Pure +
 * dependency-free, like the rest of lib/.
 */

import { isRecord } from "./guards";

/** The outcome of parsing schema text. */
export type SchemaParse =
	| { readonly ok: true; readonly schema: Record<string, unknown>; readonly summary: string }
	| { readonly ok: false; readonly error: string };

/** Parse + lightly validate schema text into a plain object (never throws). */
export function parseSchema(text: string): SchemaParse {
	const trimmed = text.trim();
	if (trimmed === "") return { ok: false, error: "Paste a JSON Schema to use." };
	let parsed: unknown;
	try {
		parsed = JSON.parse(trimmed);
	} catch (e) {
		return { ok: false, error: `Not valid JSON: ${(e as Error).message}` };
	}
	if (!isRecord(parsed)) return { ok: false, error: "A JSON Schema must be a JSON object." };
	return { ok: true, schema: parsed, summary: summarizeSchema(parsed) };
}

/** A one-line human summary, e.g. "object · 4 fields" or "array of object". */
export function summarizeSchema(schema: Record<string, unknown>): string {
	const t = primaryType(schema);
	if (t === "object") {
		const props = schema.properties;
		const n = isRecord(props) ? Object.keys(props).length : 0;
		return `object · ${n} field${n === 1 ? "" : "s"}`;
	}
	if (t === "array") {
		const item = itemSchema(schema);
		return item === null ? "array" : `array of ${primaryType(item) ?? "any"}`;
	}
	return t ?? "any";
}

/**
 * Build an empty/example instance from a schema. Precedence: const → default →
 * first example → first enum → by type. Objects include EVERY declared property
 * (so the full shape is visible to edit), arrays get one sample element, and
 * scalars get an empty-ish value (`""` / `0` / `false` / `null`). Anything it
 * cannot resolve becomes `null`. Depth-capped so a cyclic/huge schema is safe.
 */
export function skeletonFromSchema(schema: unknown): unknown {
	return build(schema, 0);
}

const MAX_DEPTH = 24;

function build(schema: unknown, depth: number): unknown {
	if (depth > MAX_DEPTH || !isRecord(schema)) return null;

	// Explicit values win, in spec-precedence order.
	if ("const" in schema) return schema.const;
	if ("default" in schema) return schema.default;
	const examples = schema.examples;
	if (Array.isArray(examples) && examples.length > 0) return examples[0];
	if ("example" in schema) return schema.example;
	const en = schema.enum;
	if (Array.isArray(en) && en.length > 0) return en[0];

	switch (primaryType(schema)) {
		case "object": {
			const out: Record<string, unknown> = {};
			const props = schema.properties;
			if (isRecord(props)) {
				for (const [key, propSchema] of Object.entries(props)) {
					out[key] = build(propSchema, depth + 1);
				}
			}
			return out;
		}
		case "array": {
			const item = itemSchema(schema);
			return item === null ? [] : [build(item, depth + 1)];
		}
		case "string":
			return "";
		case "number":
		case "integer":
			return 0;
		case "boolean":
			return false;
		default:
			// "null", an unknown/absent type, or an unsupported keyword.
			return null;
	}
}

/** The effective primary type: a string `type`, the first non-null of a `type`
 * array, or inferred from `properties`/`items`. Undefined when unknowable. */
function primaryType(schema: Record<string, unknown>): string | undefined {
	const t = schema.type;
	if (typeof t === "string") return t;
	if (Array.isArray(t)) {
		const nonNull = t.find((x): x is string => typeof x === "string" && x !== "null");
		if (nonNull !== undefined) return nonNull;
		const first = t[0];
		return typeof first === "string" ? first : undefined;
	}
	if (isRecord(schema.properties)) return "object";
	if (schema.items !== undefined) return "array";
	return undefined;
}

/** The schema for an array's elements (`items`, or its first entry if a tuple). */
function itemSchema(schema: Record<string, unknown>): Record<string, unknown> | null {
	const items = schema.items;
	if (Array.isArray(items)) {
		const first = items[0];
		return isRecord(first) ? first : null;
	}
	return isRecord(items) ? items : null;
}

/**
 * Build the JSON-batch text the publish composer pre-fills: one skeleton message
 * wrapped in an array (the append body is a batch). Returns null when the schema
 * text does not parse, so the caller can leave the editor untouched.
 */
export function skeletonBatchText(schemaText: string): string | null {
	const parsed = parseSchema(schemaText);
	if (!parsed.ok) return null;
	return JSON.stringify([skeletonFromSchema(parsed.schema)], null, 2);
}
