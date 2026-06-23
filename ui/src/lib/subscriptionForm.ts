/**
 * Pure validation + form helpers for the subscription create UI (the reserved
 * /__ds/* control plane). No schema library (per the lightweight stack rules):
 * these are small, stable shapes that are cheap to check by hand, mirroring
 * lib/streamForm.ts and lib/validation.ts. Pure functions only — no DOM, no
 * store, no I/O — so they are trivially unit-testable.
 *
 * The form lets a user pick a delivery type (webhook | pull-wake), match streams
 * by a glob pattern AND/OR an explicit newline/comma list, set a lease TTL, and
 * give the type-specific target (a webhook URL or a wake stream). This module
 * validates those raw fields, parses the explicit-streams text into a normalized
 * list, and builds the typed {@link CreateSubscriptionOptions} the store + the
 * dsClient consume. The operation previews (the equivalent curl) live in
 * lib/subscriptions.ts; this module only validates and shapes the inputs, plus a
 * tiny webhook ack-callback preview the control-plane preview set does not cover.
 */

import type { CreateSubscriptionOptions, OffsetAck, Operation, SubscriptionType } from "./types";

/** Lease TTL bounds the server clamps to (1s–10m); 0 defaults to 30000. */
export const LEASE_TTL_MIN_MS = 1000;
export const LEASE_TTL_MAX_MS = 600000;
export const LEASE_TTL_DEFAULT_MS = 30000;

/** The reserved control-plane prefix used by the callback preview. */
const SUBSCRIPTIONS_PREFIX = "/__ds/subscriptions";
const ACCEPT_HEADER: Readonly<Record<string, string>> = { Accept: "*/*" };

/* ----------------------------------------------------------------------------
 * Field validation
 * ------------------------------------------------------------------------- */

/**
 * Validate a subscription id. Client-provided, unique within the __ds namespace.
 * Keep it to a safe URL-path token: no whitespace, no slashes, no scheme/query/
 * fragment. Returns an error string, or null when valid.
 */
export function validateSubscriptionId(raw: string): string | null {
	const value = raw.trim();
	if (value === "") return "A subscription id is required.";
	if (/\s/.test(value)) return "No spaces allowed in a subscription id.";
	if (value.includes("/")) return "No slashes allowed in a subscription id.";
	if (value.includes("://") || value.includes("?") || value.includes("#")) {
		return "Use a plain id token, e.g. orders-fanout";
	}
	if (value.length > 128) return "Keep the id under 128 characters.";
	return null;
}

/**
 * Validate a glob pattern (optional). The server's glob syntax is `*` = one
 * segment, `**` = zero or more segments. Blank is allowed (the subscription may
 * link only explicit streams). When given it must be a path-shaped token with no
 * whitespace / scheme. Returns an error string, or null when valid.
 */
export function validateGlobPattern(raw: string): string | null {
	const value = raw.trim();
	if (value === "") return null;
	if (/\s/.test(value)) return "No spaces allowed in a glob pattern.";
	if (value.includes("://") || value.includes("?") || value.includes("#")) {
		return "Use a path-shaped pattern, e.g. orders/**";
	}
	if (value.startsWith("/")) return "Do not start the pattern with a slash.";
	return null;
}

/**
 * Validate a webhook delivery URL (required for type "webhook"). Must parse as an
 * absolute http(s) URL with a host. The server enforces HTTPS in production and
 * allows localhost in dev, so we accept http here (a dev localhost target) and
 * leave the prod-only SSRF policy to the server. Returns an error, or null.
 */
export function validateWebhookUrl(raw: string): string | null {
	const value = raw.trim();
	if (value === "") return "A webhook URL is required for a webhook subscription.";
	let url: URL;
	try {
		url = new URL(value);
	} catch {
		return "Enter a full URL, e.g. https://hooks.example.com/ds";
	}
	if (url.protocol !== "http:" && url.protocol !== "https:") {
		return "Use an http:// or https:// URL.";
	}
	if (url.hostname === "") return "The URL is missing a host.";
	return null;
}

/** True when a (valid) webhook URL is plain http — surfaced as a soft warning. */
export function isInsecureWebhookUrl(raw: string): boolean {
	const value = raw.trim();
	if (value === "") return false;
	try {
		return new URL(value).protocol === "http:";
	} catch {
		return false;
	}
}

/**
 * Validate a wake stream path (required for type "pull-wake"). Same shape as a
 * stream path: one-or-more slash-joined segments, no leading/trailing slash, no
 * whitespace / scheme. Returns an error string, or null when valid.
 */
export function validateWakeStream(raw: string): string | null {
	const value = raw.trim();
	if (value === "") return "A wake stream is required for a pull-wake subscription.";
	if (/\s/.test(value)) return "No spaces allowed in a stream path.";
	if (value.includes("://") || value.includes("?") || value.includes("#")) {
		return "Use a path only, e.g. __ds/wakes/orders";
	}
	if (value.startsWith("/")) return "Do not start the path with a slash.";
	if (value.endsWith("/")) return "Do not end the path with a slash.";
	if (value.includes("//")) return "Path segments cannot be empty (no //).";
	return null;
}

/**
 * Validate a lease TTL in milliseconds (optional; blank uses the server default
 * of 30000). When given it must be an integer the server accepts: 1000–600000.
 * Returns an error string, or null when valid.
 */
export function validateLeaseTtl(raw: string): string | null {
	const value = raw.trim();
	if (value === "") return null;
	if (!/^\d+$/.test(value)) return "Enter a whole number of milliseconds.";
	const n = Number.parseInt(value, 10);
	if (n < LEASE_TTL_MIN_MS || n > LEASE_TTL_MAX_MS) {
		return `Lease TTL must be ${LEASE_TTL_MIN_MS}–${LEASE_TTL_MAX_MS} ms (1s–10m).`;
	}
	return null;
}

/* ----------------------------------------------------------------------------
 * Explicit-streams text parsing
 * ------------------------------------------------------------------------- */

/**
 * Parse a free-form explicit-streams field (newline- and/or comma-separated)
 * into a normalized, de-duplicated, order-preserving list. Leading slashes are
 * trimmed and blank entries dropped, so the list matches the server's
 * normalization for the config hash. Returns [] when nothing usable is present.
 */
export function parseStreamsInput(raw: string): readonly string[] {
	const seen = new Set<string>();
	const out: string[] = [];
	for (const token of raw.split(/[\n,]/)) {
		const clean = token.trim().replace(/^\/+/, "").replace(/\/+$/, "");
		if (clean === "" || seen.has(clean)) continue;
		seen.add(clean);
		out.push(clean);
	}
	return out;
}

/* ----------------------------------------------------------------------------
 * Whole-form validation + options builder
 * ------------------------------------------------------------------------- */

/** A subscription create form's raw, user-entered fields. */
export interface SubscriptionFormValues {
	readonly id: string;
	readonly type: SubscriptionType;
	readonly pattern: string;
	readonly streamsText: string;
	readonly webhookUrl: string;
	readonly wakeStream: string;
	readonly leaseTtl: string;
	readonly description: string;
}

/** Per-field validation messages; a field is absent when it is valid. */
export interface SubscriptionFormErrors {
	id?: string;
	pattern?: string;
	streams?: string;
	webhookUrl?: string;
	wakeStream?: string;
	leaseTtl?: string;
}

/** Empty starting values for a fresh subscription form. */
export const EMPTY_SUBSCRIPTION_FORM: SubscriptionFormValues = {
	id: "",
	type: "webhook",
	pattern: "",
	streamsText: "",
	webhookUrl: "",
	wakeStream: "",
	leaseTtl: "",
	description: "",
};

/**
 * Validate every field of a subscription create form, returning only the fields
 * that have problems. Enforces the protocol rule that at least one of a glob
 * pattern or an explicit stream list must be present, and the type-specific
 * target (webhook URL for "webhook", wake stream for "pull-wake").
 */
export function validateSubscriptionForm(v: SubscriptionFormValues): SubscriptionFormErrors {
	const errors: SubscriptionFormErrors = {};

	const id = validateSubscriptionId(v.id);
	if (id !== null) errors.id = id;

	const pattern = validateGlobPattern(v.pattern);
	if (pattern !== null) errors.pattern = pattern;

	const streams = parseStreamsInput(v.streamsText);
	if (v.pattern.trim() === "" && streams.length === 0) {
		errors.streams = "Give a glob pattern or at least one explicit stream.";
	}

	if (v.type === "webhook") {
		const url = validateWebhookUrl(v.webhookUrl);
		if (url !== null) errors.webhookUrl = url;
	} else {
		const wake = validateWakeStream(v.wakeStream);
		if (wake !== null) errors.wakeStream = wake;
	}

	const ttl = validateLeaseTtl(v.leaseTtl);
	if (ttl !== null) errors.leaseTtl = ttl;

	return errors;
}

/** True when a {@link SubscriptionFormErrors} has no field errors. */
export function isSubscriptionFormValid(errors: SubscriptionFormErrors): boolean {
	return (
		errors.id === undefined &&
		errors.pattern === undefined &&
		errors.streams === undefined &&
		errors.webhookUrl === undefined &&
		errors.wakeStream === undefined &&
		errors.leaseTtl === undefined
	);
}

/**
 * Build the typed {@link CreateSubscriptionOptions} from raw form values,
 * omitting blank optionals so they satisfy exactOptionalPropertyTypes (the
 * pattern used by the stream create form) and so the server's config-hash
 * idempotency matches. Callers should validate first; this trusts its input.
 */
export function buildSubscriptionOptions(v: SubscriptionFormValues): CreateSubscriptionOptions {
	const streams = parseStreamsInput(v.streamsText);
	const opts: {
		id: string;
		type: SubscriptionType;
		pattern?: string;
		streams?: readonly string[];
		webhookUrl?: string;
		wakeStream?: string;
		leaseTtlMs?: number;
		description?: string;
	} = {
		id: v.id.trim(),
		type: v.type,
	};
	if (v.pattern.trim() !== "") opts.pattern = v.pattern.trim();
	if (streams.length > 0) opts.streams = streams;
	if (v.type === "webhook" && v.webhookUrl.trim() !== "") opts.webhookUrl = v.webhookUrl.trim();
	if (v.type === "pull-wake" && v.wakeStream.trim() !== "") opts.wakeStream = v.wakeStream.trim();
	if (v.leaseTtl.trim() !== "") opts.leaseTtlMs = Number.parseInt(v.leaseTtl.trim(), 10);
	if (v.description.trim() !== "") opts.description = v.description.trim();
	return opts;
}

/* ----------------------------------------------------------------------------
 * Webhook ack-callback curl preview
 *
 * The control-plane previews in lib/subscriptions.ts cover create/get/delete/
 * streams/claim/ack/release, but a *webhook* handler acks on the …/callback path
 * (not …/ack) with the callback_token from the wake notification. This pure
 * preview builds that exact request so a webhook subscription's detail view can
 * show "the curl your handler would run to ack asynchronously".
 * ------------------------------------------------------------------------- */

/**
 * Build the webhook ack-callback (POST …/callback) operation for the curl
 * preview, with a Bearer {@link callbackToken} placeholder. Mirrors the ack body
 * builder ({wake_id, generation, acks, done}).
 */
export function previewCallbackOperation(
	baseUrl: string,
	streamRoot: string,
	id: string,
	callbackToken: string,
	req: {
		readonly wakeId: string;
		readonly generation: number;
		readonly acks: readonly OffsetAck[];
		readonly done?: boolean;
	},
): Operation {
	const body: Record<string, unknown> = {
		wake_id: req.wakeId,
		generation: req.generation,
		acks: req.acks.map((a) => ({ stream: a.stream, offset: a.offset })),
	};
	if (req.done !== undefined) body.done = req.done;
	return {
		method: "POST",
		url: `${baseUrl}${streamRoot}${SUBSCRIPTIONS_PREFIX}/${encodeURIComponent(id)}/callback`,
		headers: {
			...ACCEPT_HEADER,
			"Content-Type": "application/json",
			Authorization: `Bearer ${callbackToken}`,
		},
		body: JSON.stringify(body),
	};
}
