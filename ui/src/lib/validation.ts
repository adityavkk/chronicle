/**
 * Tiny, hand-written validation for the connection form. No schema library
 * (per the lightweight stack rules) — these are small, stable shapes that are
 * cheap to check by hand. Pure functions: no DOM, no store, no I/O, so they are
 * trivially unit-testable and reusable by any form (new connection, future edit).
 */

/** A connection form's raw, user-entered fields. */
export interface ConnectionFormValues {
	readonly name: string;
	readonly baseUrl: string;
	readonly streamRoot: string;
}

/** Per-field validation messages; a field is absent when it is valid. */
export interface ConnectionFormErrors {
	readonly name?: string;
	readonly baseUrl?: string;
	readonly streamRoot?: string;
}

/** Empty starting values for a fresh form. */
export const EMPTY_FORM: ConnectionFormValues = {
	name: "",
	baseUrl: "",
	streamRoot: "",
};

/** The default stream root applied when the user leaves the field blank. */
export const DEFAULT_STREAM_ROOT = "/v1/stream";

/**
 * Validate a base URL. Must parse as an absolute http(s) URL with a host. We
 * use the platform URL parser rather than a regex so odd-but-valid inputs
 * (IPv6, explicit ports, userinfo) are accepted and genuinely broken ones are
 * rejected. Returns an error string, or null when valid.
 */
export function validateBaseUrl(raw: string): string | null {
	const value = raw.trim();
	if (value === "") return "Base URL is required.";
	let url: URL;
	try {
		url = new URL(value);
	} catch {
		return "Enter a full URL, e.g. http://localhost:4437";
	}
	if (url.protocol !== "http:" && url.protocol !== "https:") {
		return "Use an http:// or https:// URL.";
	}
	if (url.hostname === "") return "The URL is missing a host.";
	return null;
}

/**
 * Validate an optional stream root. Blank is allowed (it falls back to the
 * default). When provided it must be a single path segment chain with no
 * scheme, query, or whitespace. Returns an error string, or null when valid.
 */
export function validateStreamRoot(raw: string): string | null {
	const value = raw.trim();
	if (value === "") return null;
	if (/\s/.test(value)) return "No spaces allowed in the stream root.";
	if (value.includes("://") || value.includes("?") || value.includes("#")) {
		return "Use a path only, e.g. /v1/stream";
	}
	return null;
}

/** Name is optional (defaults to the base URL); only length is constrained. */
export function validateName(raw: string): string | null {
	if (raw.trim().length > 60) return "Keep the name under 60 characters.";
	return null;
}

/** Validate every field, returning only the fields that have problems. */
export function validateConnectionForm(values: ConnectionFormValues): ConnectionFormErrors {
	const errors: {
		name?: string;
		baseUrl?: string;
		streamRoot?: string;
	} = {};
	const name = validateName(values.name);
	if (name !== null) errors.name = name;
	const baseUrl = validateBaseUrl(values.baseUrl);
	if (baseUrl !== null) errors.baseUrl = baseUrl;
	const streamRoot = validateStreamRoot(values.streamRoot);
	if (streamRoot !== null) errors.streamRoot = streamRoot;
	return errors;
}

/** True when a {@link ConnectionFormErrors} has no field errors. */
export function isFormValid(errors: ConnectionFormErrors): boolean {
	return (
		errors.name === undefined && errors.baseUrl === undefined && errors.streamRoot === undefined
	);
}
