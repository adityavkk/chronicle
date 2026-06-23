/**
 * Pure helpers for the Wake Monitor — the dual split-screen that makes the
 * publish → wake → hook → ack loop visible.
 *
 * Two wake planes are modeled:
 *  - WEBHOOK: chronicle POSTs a signed wake to a URL it can reach. The browser
 *    cannot host that URL, so the dsui binary captures the delivery and relays
 *    it over SSE as a {@link CaptureDelivery}; this module parses that record,
 *    its raw body into a {@link WakeNotification}, and its `Webhook-Signature`
 *    header into {@link WebhookSignatureParts} (display only — verification is
 *    asymmetric Ed25519 against the JWKS, which the UI links to).
 *  - PULL-WAKE: wakes are durable JSON events on the subscription's wake_stream;
 *    this module turns a tailed grid row's decoded value into a {@link WakeEvent}.
 *
 * It also builds the exact {@link Operation}s the one-click "Wake demo" issues
 * (create the sample stream, register the webhook subscription pointed at the
 * capture endpoint, publish a message) so the copy-as-curl is honest before the
 * request runs — mirroring lib/streamForm + lib/subscriptions previews.
 *
 * No DOM, no store, no I/O. Tolerant of missing/extra fields, like lib/guards
 * and lib/subscriptions: a malformed delivery degrades to a "could not decode"
 * outcome rather than throwing.
 */

import { isNonEmptyString, isRecord } from "./guards";
import { previewAppendOperation, previewCreateOperation } from "./streamForm";
import { parseWakeStreamSnapshot } from "./subscriptions";
import type {
	CaptureDelivery,
	Operation,
	WakeEvent,
	WakeNotification,
	WakeStreamSnapshot,
	WebhookSignatureParts,
} from "./types";

/* ----------------------------------------------------------------------------
 * The capture endpoint (the dsui binary's webhook receiver)
 * ------------------------------------------------------------------------- */

/** The capture endpoint path prefix the dsui binary serves. */
const HOOKS_PREFIX = "/__hooks";

/**
 * Build the absolute capture URL a webhook subscription's webhook_url points at:
 * `${captureBase}/__hooks/{bucket}`. The bucket is an arbitrary name the UI
 * chooses (the subscription id by convention). Both the capture-base and the
 * bucket are taken verbatim; the bucket is URL-encoded as a single path segment.
 */
export function captureUrl(captureBase: string, bucket: string): string {
	const base = captureBase.replace(/\/+$/, "");
	return `${base}${HOOKS_PREFIX}/${encodeURIComponent(bucket)}`;
}

/** Build the SSE stream URL for a capture bucket: the capture URL + "/stream". */
export function captureStreamUrl(captureBase: string, bucket: string): string {
	return `${captureUrl(captureBase, bucket)}/stream`;
}

/* ----------------------------------------------------------------------------
 * Capture delivery (the SSE record relayed from the binary)
 * ------------------------------------------------------------------------- */

/** Read a finite number field, or a fallback when absent / not a number. */
function asNumber(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

/** Read a string field, or "" when absent / not a string. */
function asString(value: unknown): string {
	return typeof value === "string" ? value : "";
}

/** Coerce an unknown record into a flat string header map (skips non-strings). */
function asHeaders(value: unknown): Record<string, string> {
	if (!isRecord(value)) return {};
	const out: Record<string, string> = {};
	for (const [k, v] of Object.entries(value)) {
		if (typeof v === "string") out[k] = v;
	}
	return out;
}

/**
 * Parse one captured delivery from the SSE `delivery` event's JSON data. Returns
 * null only when the payload is not an object at all; otherwise it fills missing
 * fields with safe defaults (an empty body, seq 0) so a partial record still
 * renders rather than disappearing.
 */
export function parseCaptureDelivery(raw: unknown): CaptureDelivery | null {
	if (!isRecord(raw)) return null;
	return {
		seq: asNumber(raw.seq, 0),
		receivedAt: asNumber(raw.receivedAt, 0),
		method: asString(raw.method) || "POST",
		signature: asString(raw.signature),
		contentType: asString(raw.contentType),
		headers: asHeaders(raw.headers),
		body: asString(raw.body),
	};
}

/** Parse the SSE `delivery` event's `data` string into a delivery, or null. */
export function parseCaptureDeliveryData(data: string): CaptureDelivery | null {
	let parsed: unknown;
	try {
		parsed = JSON.parse(data);
	} catch {
		return null;
	}
	return parseCaptureDelivery(parsed);
}

/* ----------------------------------------------------------------------------
 * Webhook-Signature header
 * ------------------------------------------------------------------------- */

/**
 * Parse a `Webhook-Signature` header value of the form
 * `t=<unix>,kid=<id>,ed25519=<base64url>` into its parts, tolerant of ordering,
 * extra whitespace, and missing components. An empty/whitespace input yields all
 * nulls with the raw preserved. This does NOT verify the signature: verification
 * is Ed25519 over `"<t>.<rawBody>"` against the JWKS key for `kid`, which the
 * monitor surfaces a link to rather than performing here.
 */
export function parseSignatureHeader(raw: string): WebhookSignatureParts {
	const trimmed = raw.trim();
	let timestamp: number | null = null;
	let kid: string | null = null;
	let ed25519: string | null = null;
	if (trimmed !== "") {
		for (const part of trimmed.split(",")) {
			const eq = part.indexOf("=");
			if (eq < 0) continue;
			const key = part.slice(0, eq).trim();
			const value = part.slice(eq + 1).trim();
			if (value === "") continue;
			if (key === "t") {
				const n = Number.parseInt(value, 10);
				if (Number.isFinite(n)) timestamp = n;
			} else if (key === "kid") {
				kid = value;
			} else if (key === "ed25519") {
				ed25519 = value;
			}
		}
	}
	return { timestamp, kid, ed25519, raw };
}

/** True when a parsed signature carries an actual signature value. */
export function hasSignature(parts: WebhookSignatureParts): boolean {
	return parts.ed25519 !== null;
}

/* ----------------------------------------------------------------------------
 * Wake notification (the JSON body of a webhook delivery)
 * ------------------------------------------------------------------------- */

/** Parse the `streams` array of a wake notification, skipping malformed items. */
function parseSnapshots(raw: unknown): WakeStreamSnapshot[] {
	if (!Array.isArray(raw)) return [];
	const out: WakeStreamSnapshot[] = [];
	for (const item of raw) {
		const snap = parseWakeStreamSnapshot(item);
		if (snap !== null) out.push(snap);
	}
	return out;
}

/**
 * Parse a wake notification from a delivery's raw body text. Returns null when
 * the body is not a JSON object carrying a wake_id (the one field the UI needs
 * to treat it as a wake); other fields degrade to defaults so a partial
 * notification still renders.
 */
export function parseWakeNotification(body: string): WakeNotification | null {
	let parsed: unknown;
	try {
		parsed = JSON.parse(body);
	} catch {
		return null;
	}
	if (!isRecord(parsed)) return null;
	if (!isNonEmptyString(parsed.wake_id)) return null;
	return {
		subscriptionId: asString(parsed.subscription_id),
		wakeId: parsed.wake_id,
		generation: asNumber(parsed.generation, 0),
		streams: parseSnapshots(parsed.streams),
		callbackUrl: isNonEmptyString(parsed.callback_url) ? parsed.callback_url : null,
		callbackToken: isNonEmptyString(parsed.callback_token) ? parsed.callback_token : null,
	};
}

/* ----------------------------------------------------------------------------
 * Pull-wake wake events (decoded from a tailed wake_stream row)
 * ------------------------------------------------------------------------- */

/**
 * Parse a pull-wake wake event from a decoded JSON value (a tailed grid row's
 * `value`). Returns null unless it is an object whose `type` is "wake" and which
 * names a stream — so non-wake rows on the stream are ignored rather than shown.
 */
export function parseWakeEvent(value: unknown): WakeEvent | null {
	if (!isRecord(value)) return null;
	if (value.type !== "wake") return null;
	if (!isNonEmptyString(value.stream)) return null;
	return {
		subscriptionId: asString(value.subscription_id),
		stream: value.stream,
		generation: asNumber(value.generation, 0),
		ts: asNumber(value.ts, 0),
	};
}

/* ----------------------------------------------------------------------------
 * "Wake demo" one-click setup — the previewed operations
 *
 * The demo creates a sample stream, registers a webhook subscription whose
 * webhook_url is the built-in capture endpoint for that stream, and publishes a
 * message. These pure builders mirror the operations the store actions issue so
 * the copy-as-curl is exact. The subscription PUT body is built inline (the same
 * shape lib/subscriptions.buildCreateBody produces) so this stays a thin preview.
 * ------------------------------------------------------------------------- */

/** The reserved control-plane prefix, mirrored for the registration preview. */
const SUBSCRIPTIONS_PREFIX = "/__ds/subscriptions";

/** The implicit Accept header the client always sends. */
const ACCEPT_HEADER: Readonly<Record<string, string>> = { Accept: "*/*" };

/** A fixed, obvious sample namespace for the wake demo. */
export const WAKE_DEMO_STREAM = "playground/wakes";
/** The wake-demo subscription id (also the capture bucket name). */
export const WAKE_DEMO_SUB_ID = "dsui-wake-demo";
/** The wire content type of the wake-demo stream. */
export const WAKE_DEMO_CONTENT_TYPE = "application/json";

/** A sample one-message JSON batch the demo publishes to trigger a wake. */
export function wakeDemoBody(): string {
	return JSON.stringify([{ event: "wake-demo", note: "this append should fire a wake", at: 0 }]);
}

/** Build the create-stream Operation the wake demo issues (for the curl preview). */
export function previewWakeDemoCreateStream(baseUrl: string, streamRoot: string): Operation {
	return previewCreateOperation(baseUrl, streamRoot, {
		path: WAKE_DEMO_STREAM,
		contentType: WAKE_DEMO_CONTENT_TYPE,
	});
}

/**
 * Build the subscription-registration (PUT …/subscriptions/{id}) Operation the
 * wake demo issues: a webhook subscription on the sample stream whose
 * webhook_url is the binary's capture endpoint for the demo bucket.
 */
export function previewWakeDemoRegister(baseUrl: string, captureBase: string): Operation {
	const body = {
		type: "webhook",
		streams: [WAKE_DEMO_STREAM],
		webhook: { url: captureUrl(captureBase, WAKE_DEMO_SUB_ID) },
	};
	return {
		method: "PUT",
		url: `${baseUrl}${SUBSCRIPTIONS_PREFIX}/${encodeURIComponent(WAKE_DEMO_SUB_ID)}`,
		headers: { ...ACCEPT_HEADER, "Content-Type": "application/json" },
		body: JSON.stringify(body),
	};
}

/** Build the publish Operation the wake demo issues (for the curl preview). */
export function previewWakeDemoPublish(baseUrl: string, streamRoot: string): Operation {
	return previewAppendOperation(baseUrl, streamRoot, WAKE_DEMO_STREAM, {
		body: wakeDemoBody(),
		contentType: WAKE_DEMO_CONTENT_TYPE,
	});
}
