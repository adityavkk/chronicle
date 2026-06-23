/**
 * Pure validation + curl-preview helpers for the write-operation UI (create,
 * publish/append, fork). No schema library (per the lightweight stack rules):
 * these are small, stable shapes that are cheap to check by hand, mirroring the
 * pattern in lib/validation.ts. Pure functions only — no DOM, no store, no I/O —
 * so they are trivially unit-testable and reusable by every write form.
 *
 * The companion to validation here is *operation preview*: each write form can
 * build the {@link Operation} descriptor it WILL send (without running it) so
 * the equivalent curl can be shown next to the control before the user commits.
 * That keeps the dsClient the single seam that actually performs the request,
 * while the form still gets an honest, identical preview from the same shapes
 * the client builds its headers from.
 */

import { tryJson } from "./guards";
import type {
	AppendOptions,
	CreateStreamOptions,
	Operation,
	ProducerIdentity,
	StreamContentType,
} from "./types";

/* ----------------------------------------------------------------------------
 * Content-type vocabulary (the three first-class kinds, plus free-form)
 * ------------------------------------------------------------------------- */

/** The content-type a CREATE form offers as first-class radio choices. */
export type CreateKind = "text" | "json" | "binary";

/** Map a {@link CreateKind} radio choice to its wire Content-Type. */
export function contentTypeForKind(kind: CreateKind): StreamContentType {
	switch (kind) {
		case "text":
			return "text/plain";
		case "json":
			return "application/json";
		case "binary":
			return "application/octet-stream";
	}
}

/* ----------------------------------------------------------------------------
 * Path validation (shared by create + fork "new path")
 * ------------------------------------------------------------------------- */

/** The reserved registry path the UI must never let a user create over. */
export const RESERVED_REGISTRY_PATH = "__registry__";

/**
 * Validate a stream path. A path is one-or-more non-empty segments joined by
 * single slashes, no leading/trailing slash, no scheme/query/fragment, and no
 * whitespace — the same shape the server addresses under streamRoot. Returns an
 * error string, or null when valid.
 */
export function validateStreamPath(raw: string): string | null {
	const value = raw.trim();
	if (value === "") return "A stream path is required.";
	if (/\s/.test(value)) return "No spaces allowed in a stream path.";
	if (value.includes("://") || value.includes("?") || value.includes("#")) {
		return "Use a path only, e.g. orders/created";
	}
	if (value.startsWith("/")) return "Do not start the path with a slash.";
	if (value.endsWith("/")) return "Do not end the path with a slash.";
	if (value.includes("//")) return "Path segments cannot be empty (no //).";
	if (value === RESERVED_REGISTRY_PATH) {
		return "__registry__ is reserved for stream discovery.";
	}
	return null;
}

/**
 * Validate an optional RFC3339 expiry timestamp. Blank is allowed (no expiry).
 * Uses the platform Date parser rather than a regex so valid offsets/zones are
 * accepted. Returns an error string, or null when valid.
 */
export function validateExpiresAt(raw: string): string | null {
	const value = raw.trim();
	if (value === "") return null;
	const parsed = Date.parse(value);
	if (Number.isNaN(parsed)) return "Enter an RFC3339 time, e.g. 2030-01-01T00:00:00Z";
	return null;
}

/**
 * Validate an optional TTL like "1h", "30m", "45s", "2h30m". Blank is allowed.
 * Accepts a sequence of integer+unit (h/m/s) groups. Returns an error string,
 * or null when valid.
 */
export function validateTtl(raw: string): string | null {
	const value = raw.trim();
	if (value === "") return null;
	if (!/^(\d+[hms])+$/.test(value)) {
		return "Use a duration like 1h, 30m, or 90s.";
	}
	return null;
}

/* ----------------------------------------------------------------------------
 * JSON batch validation (the publish composer for application/json streams)
 * ------------------------------------------------------------------------- */

/** A typed result of validating a JSON-batch editor's text. */
export type JsonBatchOutcome =
	| { readonly ok: true; readonly count: number; readonly normalized: string }
	| { readonly ok: false; readonly error: string };

/**
 * Validate the text in a JSON-batch editor. The protocol publishes a JSON array
 * where each element is one message, so the editor's text must parse to a
 * non-empty array (a lone object is forgivingly wrapped into a one-element
 * batch, matching how reads coerce a single value). Returns the element count
 * and a normalized (re-serialized) array on success, or a human error.
 */
export function validateJsonBatch(raw: string): JsonBatchOutcome {
	const trimmed = raw.trim();
	if (trimmed === "") return { ok: false, error: "Enter at least one JSON message." };
	const parsed = tryJson(trimmed);
	if (!parsed.ok) return { ok: false, error: `Invalid JSON: ${parsed.message}` };
	const value = parsed.value;
	const arr = Array.isArray(value) ? value : [value];
	if (arr.length === 0) {
		return { ok: false, error: "The batch is empty — add at least one message." };
	}
	let normalized: string;
	try {
		normalized = JSON.stringify(arr);
	} catch {
		return { ok: false, error: "The batch could not be serialized." };
	}
	return { ok: true, count: arr.length, normalized };
}

/* ----------------------------------------------------------------------------
 * Producer-identity validation (idempotent appends)
 * ------------------------------------------------------------------------- */

/** Raw, user-entered idempotent-producer fields from the composer. */
export interface ProducerFormValues {
	readonly id: string;
	readonly epoch: string;
	readonly seq: string;
}

/** Per-field producer-form errors; a field is absent when valid. */
export interface ProducerFormErrors {
	readonly id?: string;
	readonly epoch?: string;
	readonly seq?: string;
}

/** Parse a non-negative integer field, or null when blank/invalid. */
function parseNonNegInt(raw: string): number | null {
	const t = raw.trim();
	if (t === "") return null;
	if (!/^\d+$/.test(t)) return null;
	const n = Number.parseInt(t, 10);
	return Number.isSafeInteger(n) && n >= 0 ? n : null;
}

/**
 * Validate the idempotent-producer fields. All three are required together when
 * the producer disclosure is enabled: an id, and non-negative integer epoch +
 * seq. Returns only the fields that have problems.
 */
export function validateProducer(values: ProducerFormValues): ProducerFormErrors {
	const errors: { id?: string; epoch?: string; seq?: string } = {};
	if (values.id.trim() === "") errors.id = "A producer id is required.";
	if (parseNonNegInt(values.epoch) === null) errors.epoch = "Epoch must be a whole number ≥ 0.";
	if (parseNonNegInt(values.seq) === null) errors.seq = "Seq must be a whole number ≥ 0.";
	return errors;
}

/** True when {@link ProducerFormErrors} has no field errors. */
export function isProducerValid(errors: ProducerFormErrors): boolean {
	return errors.id === undefined && errors.epoch === undefined && errors.seq === undefined;
}

/**
 * Build a {@link ProducerIdentity} from valid producer-form values, or null if
 * the values do not validate. Callers guard on {@link validateProducer} first;
 * this re-parses defensively so it can never produce a NaN.
 */
export function toProducerIdentity(values: ProducerFormValues): ProducerIdentity | null {
	if (!isProducerValid(validateProducer(values))) return null;
	const epoch = parseNonNegInt(values.epoch);
	const seq = parseNonNegInt(values.seq);
	if (epoch === null || seq === null) return null;
	return { id: values.id.trim(), epoch, seq };
}

/* ----------------------------------------------------------------------------
 * Fork-offset validation
 * ------------------------------------------------------------------------- */

/** Validate an optional fork sub-offset (a batch element index ≥ 0). Blank ok. */
export function validateSubOffset(raw: string): string | null {
	const t = raw.trim();
	if (t === "") return null;
	if (parseNonNegInt(t) === null) return "Sub-offset must be a whole number ≥ 0.";
	return null;
}

/** Parse a sub-offset string to a number, or undefined when blank/invalid. */
export function parseSubOffset(raw: string): number | undefined {
	const n = parseNonNegInt(raw);
	return n === null ? undefined : n;
}

/* ----------------------------------------------------------------------------
 * Message-centric fork selection
 *
 * The raw (Stream-Fork-Offset, Stream-Fork-Sub-Offset) pair is coupled in a way
 * that confuses users: the offset is a batch boundary in [-1 (beginning), tail],
 * and the sub-offset (for a JSON source) is the number of messages PAST the fork
 * offset to ALSO inherit — so it must not overshoot what is available past that
 * point. Pairing the TAIL offset with a sub-offset always overshoots (zero
 * messages past the tail) and the server returns 400.
 *
 * To spare users that coupling, the dialog offers a friendly "Fork point" choice
 * and {@link forkSelection} maps it to the correct (offset, subOffset):
 *   - "everything" → offset "now"  (tail), no sub-offset → inherit all messages.
 *   - "nothing"    → offset "-1"   (beginning), no sub-offset → an empty fork.
 *   - "first-n"    → offset "-1"   (beginning) + sub-offset N → keep the first N.
 * Only "first-n" uses a sub-offset, and it pins the offset to the beginning, so a
 * sub-offset can never be paired with the tail from the friendly control.
 * ------------------------------------------------------------------------- */

/** The friendly fork-point choices offered in place of raw offset + sub-offset. */
export type ForkPoint = "everything" | "nothing" | "first-n";

/** The resolved wire pair a fork CREATE sends, derived from a {@link ForkPoint}. */
export interface ForkSelection {
	readonly offset: string;
	readonly subOffset: number | undefined;
}

/**
 * Map a friendly {@link ForkPoint} (and, for "first-n", the message count N) to
 * the correct (Stream-Fork-Offset, Stream-Fork-Sub-Offset) pair. See the block
 * comment above for the semantics. For "first-n" an out-of-range N is clamped to
 * 0 defensively; callers should gate submit on {@link validateFirstN} first.
 */
export function forkSelection(point: ForkPoint, n: number): ForkSelection {
	switch (point) {
		case "everything":
			return { offset: "now", subOffset: undefined };
		case "nothing":
			return { offset: "-1", subOffset: undefined };
		case "first-n": {
			const count = Number.isSafeInteger(n) && n >= 0 ? n : 0;
			return { offset: "-1", subOffset: count };
		}
	}
}

/**
 * Validate the "First N messages" number input. N is the sub-offset counted from
 * the start of the source, so it must be a whole number ≥ 0. When the source's
 * message count `max` is known (the read in hand is for this same JSON source),
 * N may not exceed it — the server rejects an overshoot with a 400. When `max`
 * is null the count is unknown, so any N ≥ 0 is allowed and the server is the
 * final arbiter. Returns an error string, or null when valid.
 */
export function validateFirstN(raw: string, max: number | null): string | null {
	const t = raw.trim();
	if (t === "") return "Enter how many messages to keep (0 or more).";
	const n = parseNonNegInt(t);
	if (n === null) return "Keep a whole number of messages ≥ 0 (counted from the start).";
	if (max !== null && n > max) {
		return `Only ${max} message${max === 1 ? "" : "s"} are available — that overshoots and is rejected.`;
	}
	return null;
}

/* ----------------------------------------------------------------------------
 * Operation preview (the equivalent curl, before the request runs)
 *
 * These mirror the header builders in dsClient so a form can show the EXACT
 * Operation it will send. They are intentionally duplicated as pure functions
 * here (the client's builders are private) and unit-tested against the same
 * expectations, so a drift between preview and reality is caught by tests.
 * ------------------------------------------------------------------------- */

/** The request header the client always sends (CORS is open server-side). */
const ACCEPT_HEADER: Readonly<Record<string, string>> = { Accept: "*/*" };

/** Join an absolute stream URL the same way dsClient.streamUrl does (preview). */
export function previewStreamUrl(baseUrl: string, streamRoot: string, path: string): string {
	const cleanPath = path.trim().replace(/^\/+/, "");
	const segs = cleanPath
		.split("/")
		.map((s) => encodeURIComponent(s))
		.join("/");
	return `${baseUrl}${streamRoot}/${segs}`;
}

/** Build the CREATE/FORK PUT operation a form will send, for the curl preview. */
export function previewCreateOperation(
	baseUrl: string,
	streamRoot: string,
	opts: CreateStreamOptions,
): Operation {
	const headers: Record<string, string> = { ...ACCEPT_HEADER, "Content-Type": opts.contentType };
	if (opts.ttl !== undefined) headers["Stream-TTL"] = opts.ttl;
	if (opts.expiresAt !== undefined) headers["Stream-Expires-At"] = opts.expiresAt;
	if (opts.closed === true) headers["Stream-Closed"] = "true";
	if (opts.fork !== undefined) {
		headers["Stream-Forked-From"] = opts.fork.fromPath;
		headers["Stream-Fork-Offset"] = opts.fork.offset;
		if (opts.fork.subOffset !== undefined) {
			headers["Stream-Fork-Sub-Offset"] = String(opts.fork.subOffset);
		}
	}
	return { method: "PUT", url: previewStreamUrl(baseUrl, streamRoot, opts.path), headers };
}

/** Build the APPEND POST operation a composer will send, for the curl preview. */
export function previewAppendOperation(
	baseUrl: string,
	streamRoot: string,
	path: string,
	opts: AppendOptions,
): Operation {
	const headers: Record<string, string> = { ...ACCEPT_HEADER };
	if (opts.contentType !== undefined) headers["Content-Type"] = opts.contentType;
	if (opts.producer !== undefined) {
		headers["Producer-Id"] = opts.producer.id;
		headers["Producer-Epoch"] = String(opts.producer.epoch);
		headers["Producer-Seq"] = String(opts.producer.seq);
	}
	if (opts.closeAfter === true) headers["Stream-Closed"] = "true";
	return {
		method: "POST",
		url: previewStreamUrl(baseUrl, streamRoot, path),
		headers,
		body: opts.body,
	};
}

/** Build the CLOSE POST operation (Stream-Closed: true, empty body), for preview. */
export function previewCloseOperation(
	baseUrl: string,
	streamRoot: string,
	path: string,
): Operation {
	return {
		method: "POST",
		url: previewStreamUrl(baseUrl, streamRoot, path),
		headers: { ...ACCEPT_HEADER, "Stream-Closed": "true" },
		body: "",
	};
}

/** Build the DELETE operation, for the curl preview. */
export function previewDeleteOperation(
	baseUrl: string,
	streamRoot: string,
	path: string,
): Operation {
	return {
		method: "DELETE",
		url: previewStreamUrl(baseUrl, streamRoot, path),
		headers: { ...ACCEPT_HEADER },
	};
}
