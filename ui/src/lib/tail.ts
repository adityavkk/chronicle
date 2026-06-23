/**
 * Pure helpers for the live-tail UI (long-poll + SSE). No DOM, no store, no I/O
 * — so they are trivially unit-testable and shared by the toolbar, the live
 * tail panel, and the protocol disclosure.
 *
 * This module is to live tailing what lib/streamForm is to writes: it builds the
 * exact {@link Operation} the live read WILL send (so the equivalent curl can be
 * shown before/while it runs), and turns a {@link TailStatus} into the small
 * pieces a status affordance needs (a label, a tone, an aria-live politeness).
 *
 * The tail URL it builds mirrors dsClient's private `tailUrl` (…?offset=X&live=
 * long-poll | sse); it is duplicated here as a pure function and unit-tested
 * against the same expectations, so a drift between preview and reality is
 * caught (the same pattern as the streamForm previews mirroring dsClient's
 * private header builders).
 */

import { toCurl } from "./curl";
import type { Operation, TailMode, TailStatus } from "./types";

/** The request header dsClient always sends (CORS is open server-side). */
const ACCEPT_HEADER: Readonly<Record<string, string>> = { Accept: "*/*" };

/** The two live-tail modes that open a streaming connection (not catch-up). */
export type LiveTailMode = Exclude<TailMode, "catchup">;

/** True when a read mode opens a live (streaming) connection. */
export function isLiveMode(mode: TailMode): mode is LiveTailMode {
	return mode === "long-poll" || mode === "sse";
}

/** A short human label for a read/tail mode, for the toolbar + disclosure. */
export function describeTailMode(mode: TailMode): string {
	switch (mode) {
		case "catchup":
			return "Catch-up";
		case "long-poll":
			return "Long-poll";
		case "sse":
			return "SSE";
	}
}

/**
 * Build the live-read URL the same way dsClient does (preview): the stream URL
 * with ?offset=X and &live=long-poll | sse. Path segments are encoded but
 * slashes are kept as separators, matching dsClient.streamUrl / encodeStreamPath.
 */
export function previewTailUrl(
	baseUrl: string,
	streamRoot: string,
	path: string,
	offset: string,
	live: LiveTailMode,
): string {
	const cleanPath = path.trim().replace(/^\/+/, "");
	const segs = cleanPath
		.split("/")
		.map((s) => encodeURIComponent(s))
		.join("/");
	const base = `${baseUrl}${streamRoot}/${segs}`;
	try {
		const u = new URL(base);
		u.searchParams.set("offset", offset);
		u.searchParams.set("live", live);
		return u.toString();
	} catch {
		// Fall back to a hand-joined query when base is not absolute-parseable
		// (e.g. a relative streamRoot under test); honest rather than throwing.
		const q = `offset=${encodeURIComponent(offset)}&live=${live}`;
		return `${base}?${q}`;
	}
}

/**
 * Build the {@link Operation} a live tail WILL send, so the equivalent curl can
 * be shown next to the live status. Both live modes are a plain GET that carries
 * the cursor + the live mode on the URL — SSE needs no custom request headers in
 * the browser (the EventSource URL carries everything), so only the implicit
 * Accept is included, matching the wire.
 */
export function previewTailOperation(
	baseUrl: string,
	streamRoot: string,
	path: string,
	offset: string,
	live: LiveTailMode,
): Operation {
	const headers =
		live === "sse" ? ({ Accept: "text/event-stream" } as const) : { ...ACCEPT_HEADER };
	return { method: "GET", url: previewTailUrl(baseUrl, streamRoot, path, offset, live), headers };
}

/**
 * Reproduce a live-tail {@link Operation} as curl. SSE is a long-lived response,
 * so it gets curl's `-N` (no buffering) flag — the honest way to follow an
 * event stream from the command line. Long-poll is a plain GET (each call
 * returns one batch), so it reuses the standard curl reproduction.
 */
export function tailToCurl(op: Operation, live: LiveTailMode): string {
	const base = toCurl(op);
	if (live !== "sse") return base;
	// Insert -N right after `curl` so the stream is not buffered.
	return base.startsWith("curl ") ? `curl -N ${base.slice("curl ".length)}` : `${base} -N`;
}

/** The tone (for color) a tail status maps to, reusing the proto status tones. */
export type TailTone = "idle" | "pending" | "ok" | "warn" | "err";

/** Map a {@link TailStatus} to a tone for the status badge. */
export function tailTone(status: TailStatus): TailTone {
	switch (status.state) {
		case "idle":
			return "idle";
		case "connecting":
			return "pending";
		case "live":
			return "ok";
		case "reconnecting":
			return "warn";
		case "closed":
			return "idle";
		case "error":
			return "err";
	}
}

/** A short human label for a {@link TailStatus} (for the badge text). */
export function tailStatusLabel(status: TailStatus): string {
	switch (status.state) {
		case "idle":
			return "Not tailing";
		case "connecting":
			return "Connecting…";
		case "live":
			return "Live";
		case "reconnecting":
			return `Reconnecting (attempt ${status.attempt})`;
		case "closed":
			return "Stream closed";
		case "error":
			return "Error";
	}
}

/**
 * A longer, plain-language detail line for a {@link TailStatus}, for the
 * aria-live announcement + a subtitle under the badge. Empty when there is
 * nothing extra to say beyond the label.
 */
export function tailStatusDetail(status: TailStatus): string {
	switch (status.state) {
		case "idle":
			return "";
		case "connecting":
			return "Opening the live connection…";
		case "live":
			return status.atOffset === null
				? "Connected — new messages appear as they arrive."
				: `Connected at offset ${status.atOffset} — new messages appear as they arrive.`;
		case "reconnecting":
			return status.reason;
		case "closed":
			return "The stream is closed; no more data will arrive.";
		case "error":
			return status.message;
	}
}

/**
 * Whether a status should be announced assertively (it interrupts) or politely.
 * Errors + reconnects are assertive; everything else is polite, matching how the
 * Toaster splits its live regions by urgency.
 */
export function tailAnnouncePolite(status: TailStatus): boolean {
	return status.state !== "error" && status.state !== "reconnecting";
}

/** True when a status is a settled, terminal state (no live connection open). */
export function isTerminalTailState(status: TailStatus): boolean {
	return status.state === "idle" || status.state === "closed" || status.state === "error";
}
