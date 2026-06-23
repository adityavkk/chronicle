/**
 * Hand-written runtime guards + parsers for the subscription control-plane JSON
 * shapes (the reserved /__ds/* surface). No schema library — these are small,
 * stable wire shapes (verified against webhook/wire.go + webhook/types.go), so a
 * few defensive functions keep the runtime dependency surface at preact +
 * signals, mirroring lib/guards.ts.
 *
 * Everything here is tolerant of missing/extra fields: the server may omit
 * optional blocks (no webhook for a pull-wake sub, no signing until a key is
 * minted, no created_at on some paths), and the UI must degrade rather than
 * throw. Pure functions only — no DOM, no store, no I/O.
 */

import { isNonEmptyString, isRecord } from "./guards";
import type {
	AckResult,
	CreateSubscriptionOptions,
	LinkType,
	OffsetAck,
	Operation,
	StreamLink,
	Subscription,
	SubscriptionPhase,
	SubscriptionStatus,
	SubscriptionType,
	WakeClaim,
	WakeStreamSnapshot,
	WebhookConfig,
	WebhookSigning,
} from "./types";

/** The reserved control-plane prefix, mirrored from dsClient for the preview. */
const SUBSCRIPTIONS_PREFIX = "/__ds/subscriptions";

/** The request header the client always sends (CORS is open server-side). */
const ACCEPT_HEADER: Readonly<Record<string, string>> = { Accept: "*/*" };

/* ----------------------------------------------------------------------------
 * Small field coercions
 * ------------------------------------------------------------------------- */

/** Coerce a dispatch type, defaulting to "webhook" for an unknown value. */
function asSubscriptionType(value: unknown): SubscriptionType {
	return value === "pull-wake" ? "pull-wake" : "webhook";
}

/** Coerce a link type, defaulting to "glob" for an unknown value. */
function asLinkType(value: unknown): LinkType {
	return value === "explicit" ? "explicit" : "glob";
}

/** Coerce a status, defaulting to "active" for an unknown value. */
function asStatus(value: unknown): SubscriptionStatus {
	return value === "failed" ? "failed" : "active";
}

/** Coerce a runtime phase to one of the three known values, or undefined. */
function asPhase(value: unknown): SubscriptionPhase | undefined {
	if (value === "idle" || value === "waking" || value === "live") return value;
	return undefined;
}

/** Read a string field, or null when absent / not a string. */
function asStringOrNull(value: unknown): string | null {
	return typeof value === "string" ? value : null;
}

/** Read a string field, or "" when absent / not a string (for required cursors). */
function asString(value: unknown): string {
	return typeof value === "string" ? value : "";
}

/** Read a finite number field, or null when absent / not a number. */
function asNumberOrNull(value: unknown): number | null {
	return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/* ----------------------------------------------------------------------------
 * Stream links + snapshots
 * ------------------------------------------------------------------------- */

/** Parse one serialized stream link (path / link_type / acked_offset). */
export function parseStreamLink(raw: unknown): StreamLink | null {
	if (!isRecord(raw)) return null;
	if (!isNonEmptyString(raw.path)) return null;
	return {
		path: raw.path,
		linkType: asLinkType(raw.link_type),
		ackedOffset: asString(raw.acked_offset),
	};
}

/** Parse the `streams` array of a subscription view, skipping malformed items. */
function parseStreamLinks(raw: unknown): StreamLink[] {
	if (!Array.isArray(raw)) return [];
	const out: StreamLink[] = [];
	for (const item of raw) {
		const link = parseStreamLink(item);
		if (link !== null) out.push(link);
	}
	return out;
}

/**
 * Parse one wake-plane stream snapshot (the richer shape in a claim response and
 * webhook wake notification: link + tail_offset + has_pending).
 */
export function parseWakeStreamSnapshot(raw: unknown): WakeStreamSnapshot | null {
	if (!isRecord(raw)) return null;
	if (!isNonEmptyString(raw.path)) return null;
	return {
		path: raw.path,
		linkType: asLinkType(raw.link_type),
		ackedOffset: asString(raw.acked_offset),
		tailOffset: asString(raw.tail_offset),
		hasPending: raw.has_pending === true,
	};
}

/** Parse a `streams` array of wake snapshots, skipping malformed items. */
function parseWakeStreamSnapshots(raw: unknown): WakeStreamSnapshot[] {
	if (!Array.isArray(raw)) return [];
	const out: WakeStreamSnapshot[] = [];
	for (const item of raw) {
		const snap = parseWakeStreamSnapshot(item);
		if (snap !== null) out.push(snap);
	}
	return out;
}

/* ----------------------------------------------------------------------------
 * Webhook block + signing
 * ------------------------------------------------------------------------- */

/** Parse the optional webhook signing metadata block. */
function parseSigning(raw: unknown): WebhookSigning | null {
	if (!isRecord(raw)) return null;
	if (!isNonEmptyString(raw.kid)) return null;
	return {
		alg: isNonEmptyString(raw.alg) ? raw.alg : "ed25519",
		kid: raw.kid,
		jwksUrl: asString(raw.jwks_url),
	};
}

/** Parse the optional webhook block (url + signing). */
function parseWebhook(raw: unknown): WebhookConfig | null {
	if (!isRecord(raw)) return null;
	if (!isNonEmptyString(raw.url)) return null;
	return { url: raw.url, signing: parseSigning(raw.signing) };
}

/* ----------------------------------------------------------------------------
 * Subscription view (GET / create response)
 * ------------------------------------------------------------------------- */

/**
 * Parse a subscription view (the GET / create 201|200 body) into a typed
 * {@link Subscription}, tolerant of omitted optional blocks. Returns null only
 * when the shape carries no usable id at all.
 *
 * The server echoes both `id` and `subscription_id`; either is accepted as the
 * id. `pattern` is omitted when empty; `wake_stream` is null for webhook subs;
 * `webhook` is absent for pull-wake subs; `phase`/`generation` are not in the
 * serialized response (they are read loosely here for forward compatibility).
 */
export function parseSubscription(raw: unknown): Subscription | null {
	if (!isRecord(raw)) return null;
	const idCandidate = isNonEmptyString(raw.id)
		? raw.id
		: isNonEmptyString(raw.subscription_id)
			? raw.subscription_id
			: null;
	if (idCandidate === null) return null;

	const pattern = isNonEmptyString(raw.pattern) ? raw.pattern : null;
	const leaseTtlMs = asNumberOrNull(raw.lease_ttl_ms) ?? 0;
	const description = isNonEmptyString(raw.description) ? raw.description : null;
	const phase = asPhase(raw.phase);
	const generation = asNumberOrNull(raw.generation);

	const sub: Subscription = {
		id: idCandidate,
		type: asSubscriptionType(raw.type),
		pattern,
		streams: parseStreamLinks(raw.streams),
		webhook: parseWebhook(raw.webhook),
		wakeStream: asStringOrNull(raw.wake_stream),
		leaseTtlMs,
		createdAt: isNonEmptyString(raw.created_at) ? raw.created_at : null,
		status: asStatus(raw.status),
		description,
		...(phase !== undefined ? { phase } : {}),
		...(generation !== null ? { generation } : {}),
	};
	return sub;
}

/* ----------------------------------------------------------------------------
 * Claim / ack response shapes
 * ------------------------------------------------------------------------- */

/**
 * Parse a successful pull-wake claim response. Returns null when it lacks the
 * fields a worker needs to ack/release (wake_id + token), so a caller never
 * tries to drive a half-formed claim.
 */
export function parseWakeClaim(raw: unknown): WakeClaim | null {
	if (!isRecord(raw)) return null;
	if (!isNonEmptyString(raw.wake_id)) return null;
	if (!isNonEmptyString(raw.token)) return null;
	return {
		wakeId: raw.wake_id,
		generation: asNumberOrNull(raw.generation) ?? 0,
		token: raw.token,
		streams: parseWakeStreamSnapshots(raw.streams),
		leaseTtlMs: asNumberOrNull(raw.lease_ttl_ms) ?? 0,
	};
}

/**
 * Parse an ack / callback success body ({"ok":true,"next_wake":bool}). Tolerant:
 * a missing `ok` is treated as true on a 2xx (the caller only calls this on ok),
 * and a missing `next_wake` defaults to false.
 */
export function parseAckResult(raw: unknown): AckResult {
	if (!isRecord(raw)) return { ok: true, nextWake: false };
	return {
		ok: raw.ok !== false,
		nextWake: raw.next_wake === true,
	};
}

/* ----------------------------------------------------------------------------
 * Error envelope
 * ------------------------------------------------------------------------- */

/**
 * Extract the wire error code from an `{"error":{"code":…}}` envelope body, or
 * null when the body is not that shape. Used to surface FENCED / ALREADY_CLAIMED
 * / CONFIG_CONFLICT as typed outcomes rather than opaque 409s.
 */
export function parseErrorCode(raw: unknown): string | null {
	if (!isRecord(raw)) return null;
	const err = raw.error;
	if (!isRecord(err)) return null;
	return isNonEmptyString(err.code) ? err.code : null;
}

/* ----------------------------------------------------------------------------
 * Outbound body builders (request shapes the client + curl preview send)
 * ------------------------------------------------------------------------- */

/**
 * Build the PUT create body from typed options, omitting empty fields so the
 * server's config-hash idempotency matches. At least one of pattern/streams must
 * be set by the caller; this does not enforce it (validation lives in the form).
 */
export function buildCreateBody(opts: {
	readonly type: SubscriptionType;
	readonly pattern?: string;
	readonly streams?: readonly string[];
	readonly webhookUrl?: string;
	readonly wakeStream?: string;
	readonly leaseTtlMs?: number;
	readonly description?: string;
}): Record<string, unknown> {
	const body: Record<string, unknown> = { type: opts.type };
	if (opts.pattern !== undefined && opts.pattern !== "") body.pattern = opts.pattern;
	if (opts.streams !== undefined && opts.streams.length > 0) body.streams = opts.streams;
	if (opts.webhookUrl !== undefined && opts.webhookUrl !== "") {
		body.webhook = { url: opts.webhookUrl };
	}
	if (opts.wakeStream !== undefined && opts.wakeStream !== "") body.wake_stream = opts.wakeStream;
	if (opts.leaseTtlMs !== undefined) body.lease_ttl_ms = opts.leaseTtlMs;
	if (opts.description !== undefined && opts.description !== "")
		body.description = opts.description;
	return body;
}

/** Build the ack / callback body ({wake_id, generation, acks, done?}). */
export function buildAckBody(req: {
	readonly wakeId: string;
	readonly generation: number;
	readonly acks: readonly OffsetAck[];
	readonly done?: boolean;
}): Record<string, unknown> {
	const body: Record<string, unknown> = {
		wake_id: req.wakeId,
		generation: req.generation,
		acks: req.acks.map((a) => ({ stream: a.stream, offset: a.offset })),
	};
	if (req.done !== undefined) body.done = req.done;
	return body;
}

/* ----------------------------------------------------------------------------
 * Operation previews (the equivalent curl, before the request runs)
 *
 * These mirror the request builders in dsClient so a subscription form can show
 * the EXACT {@link Operation} it will send. Like the stream previews in
 * lib/streamForm, they are pure and unit-tested against the client, so a drift
 * between preview and reality is caught by tests. Per the spec the /__ds/*
 * surface is STREAM-ROOT-RELATIVE, so the previews take streamRoot too (matching
 * the stream previews in lib/streamForm).
 * ------------------------------------------------------------------------- */

/** Build the absolute /__ds subscription URL the same way dsClient does. */
export function previewSubscriptionUrl(
	baseUrl: string,
	streamRoot: string,
	id: string,
	suffix = "",
): string {
	return `${baseUrl}${streamRoot}${SUBSCRIPTIONS_PREFIX}/${encodeURIComponent(id)}${suffix}`;
}

/** Build the create (PUT) operation a subscription form will send, for the preview. */
export function previewCreateSubscriptionOperation(
	baseUrl: string,
	streamRoot: string,
	opts: CreateSubscriptionOptions,
): Operation {
	return {
		method: "PUT",
		url: previewSubscriptionUrl(baseUrl, streamRoot, opts.id),
		headers: { ...ACCEPT_HEADER, "Content-Type": "application/json" },
		body: JSON.stringify(
			buildCreateBody({
				type: opts.type,
				...(opts.pattern !== undefined ? { pattern: opts.pattern } : {}),
				...(opts.streams !== undefined ? { streams: opts.streams } : {}),
				...(opts.webhookUrl !== undefined ? { webhookUrl: opts.webhookUrl } : {}),
				...(opts.wakeStream !== undefined ? { wakeStream: opts.wakeStream } : {}),
				...(opts.leaseTtlMs !== undefined ? { leaseTtlMs: opts.leaseTtlMs } : {}),
				...(opts.description !== undefined ? { description: opts.description } : {}),
			}),
		),
	};
}

/** Build the GET subscription operation, for the curl preview. */
export function previewGetSubscriptionOperation(
	baseUrl: string,
	streamRoot: string,
	id: string,
): Operation {
	return {
		method: "GET",
		url: previewSubscriptionUrl(baseUrl, streamRoot, id),
		headers: { ...ACCEPT_HEADER },
	};
}

/** Build the DELETE subscription operation, for the curl preview. */
export function previewDeleteSubscriptionOperation(
	baseUrl: string,
	streamRoot: string,
	id: string,
): Operation {
	return {
		method: "DELETE",
		url: previewSubscriptionUrl(baseUrl, streamRoot, id),
		headers: { ...ACCEPT_HEADER },
	};
}

/** Build the add-streams (POST …/streams) operation, for the curl preview. */
export function previewAddStreamsOperation(
	baseUrl: string,
	streamRoot: string,
	id: string,
	streams: readonly string[],
): Operation {
	return {
		method: "POST",
		url: previewSubscriptionUrl(baseUrl, streamRoot, id, "/streams"),
		headers: { ...ACCEPT_HEADER, "Content-Type": "application/json" },
		body: JSON.stringify({ streams: [...streams] }),
	};
}

/** Build the remove-stream (DELETE …/streams/{path}) operation, for the preview. */
export function previewRemoveStreamOperation(
	baseUrl: string,
	streamRoot: string,
	id: string,
	path: string,
): Operation {
	const clean = path.trim().replace(/^\/+/, "");
	return {
		method: "DELETE",
		url: previewSubscriptionUrl(baseUrl, streamRoot, id, `/streams/${encodeURIComponent(clean)}`),
		headers: { ...ACCEPT_HEADER },
	};
}

/** Build the claim (POST …/claim) operation, for the curl preview. */
export function previewClaimOperation(
	baseUrl: string,
	streamRoot: string,
	id: string,
	worker: string,
): Operation {
	return {
		method: "POST",
		url: previewSubscriptionUrl(baseUrl, streamRoot, id, "/claim"),
		headers: { ...ACCEPT_HEADER, "Content-Type": "application/json" },
		body: JSON.stringify({ worker }),
	};
}

/** Build the ack (POST …/ack) operation with a Bearer token, for the preview. */
export function previewAckOperation(
	baseUrl: string,
	streamRoot: string,
	id: string,
	token: string,
	req: {
		readonly wakeId: string;
		readonly generation: number;
		readonly acks: readonly OffsetAck[];
		readonly done?: boolean;
	},
): Operation {
	return {
		method: "POST",
		url: previewSubscriptionUrl(baseUrl, streamRoot, id, "/ack"),
		headers: {
			...ACCEPT_HEADER,
			"Content-Type": "application/json",
			Authorization: `Bearer ${token}`,
		},
		body: JSON.stringify(buildAckBody(req)),
	};
}

/** Build the release (POST …/release) operation with a Bearer token, for preview. */
export function previewReleaseOperation(
	baseUrl: string,
	streamRoot: string,
	id: string,
	token: string,
	req: { readonly wakeId: string; readonly generation: number },
): Operation {
	return {
		method: "POST",
		url: previewSubscriptionUrl(baseUrl, streamRoot, id, "/release"),
		headers: {
			...ACCEPT_HEADER,
			"Content-Type": "application/json",
			Authorization: `Bearer ${token}`,
		},
		body: JSON.stringify({ wake_id: req.wakeId, generation: req.generation }),
	};
}
