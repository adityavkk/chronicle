import { describe, expect, it } from "vitest";
import {
	type ConnectionFormValues,
	isFormValid,
	validateBaseUrl,
	validateConnectionForm,
	validateName,
	validateStreamRoot,
} from "./validation";

describe("validateBaseUrl", () => {
	it("accepts absolute http(s) URLs with a host and port", () => {
		expect(validateBaseUrl("http://localhost:4437")).toBeNull();
		expect(validateBaseUrl("https://streams.example.com")).toBeNull();
		expect(validateBaseUrl("  http://10.0.0.5:8080  ")).toBeNull();
	});

	it("rejects blank, relative, and non-http inputs", () => {
		expect(validateBaseUrl("")).not.toBeNull();
		expect(validateBaseUrl("localhost:4437")).not.toBeNull();
		expect(validateBaseUrl("ftp://host")).not.toBeNull();
		expect(validateBaseUrl("not a url")).not.toBeNull();
	});
});

describe("validateStreamRoot", () => {
	it("allows blank (falls back to default) and a plain path", () => {
		expect(validateStreamRoot("")).toBeNull();
		expect(validateStreamRoot("/v1/stream")).toBeNull();
		expect(validateStreamRoot("custom/root")).toBeNull();
	});

	it("rejects spaces, schemes, and query strings", () => {
		expect(validateStreamRoot("/v1 stream")).not.toBeNull();
		expect(validateStreamRoot("http://x/v1")).not.toBeNull();
		expect(validateStreamRoot("/v1?x=1")).not.toBeNull();
	});
});

describe("validateName", () => {
	it("allows empty and reasonable names, rejects very long ones", () => {
		expect(validateName("")).toBeNull();
		expect(validateName("Local dev")).toBeNull();
		expect(validateName("x".repeat(61))).not.toBeNull();
	});
});

describe("validateConnectionForm + isFormValid", () => {
	it("reports a valid form when only the base URL is filled", () => {
		const values: ConnectionFormValues = {
			name: "",
			baseUrl: "http://localhost:4437",
			streamRoot: "",
		};
		const errors = validateConnectionForm(values);
		expect(isFormValid(errors)).toBe(true);
	});

	it("collects per-field errors and is not valid", () => {
		const values: ConnectionFormValues = {
			name: "x".repeat(61),
			baseUrl: "bad",
			streamRoot: "a b",
		};
		const errors = validateConnectionForm(values);
		expect(errors.name).toBeDefined();
		expect(errors.baseUrl).toBeDefined();
		expect(errors.streamRoot).toBeDefined();
		expect(isFormValid(errors)).toBe(false);
	});
});
