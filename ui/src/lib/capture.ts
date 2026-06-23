/**
 * The capture-stream opener: a thin EventSource client for the dsui binary's
 * built-in webhook-capture endpoint (cmd/dsui/capture.go).
 *
 * This is deliberately separate from dsClient: dsClient is bound to a chronicle
 * {@link Connection} and only ever talks to the Durable Streams protocol surface.
 * The capture endpoint is the TOOL's own origin (captureBase, served by the same
 * dsui binary that serves this UI), not chronicle — so its single EventSource
 * lives here, mirroring how dsClient.openSse owns the stream EventSource. The
 * browser cannot host an inbound webhook, so chronicle POSTs signed wakes to the
 * binary and the binary relays each one here over SSE as a `delivery` event.
 *
 * Parsing the `delivery` JSON is delegated to lib/wakes (pure); this module is
 * only the connection + lifecycle (open / error / reconnect / stop), with the
 * same status vocabulary as the stream tail so the monitor can reuse lib/tail's
 * status helpers.
 */

import type { CaptureDelivery, TailStatus } from "./types";
import { captureStreamUrl, parseCaptureDeliveryData } from "./wakes";

/** A handle returned by the opener; call it to close the EventSource. */
export type CaptureStopper = () => void;

/**
 * Open an EventSource on `${captureBase}/__hooks/{bucket}/stream` and deliver
 * each captured webhook (named SSE `delivery` event) to {@link onDelivery},
 * reporting connection lifecycle via {@link onState} using the shared
 * {@link TailStatus} vocabulary. The binary replays its buffered backlog first,
 * then streams live deliveries; the caller dedupes on the monotonic `seq`.
 *
 * Never throws: a missing EventSource (non-browser) or a malformed event resolves
 * to an error status / a skipped record rather than an exception. Returns a
 * stopper that closes the connection and sets the status to idle.
 */
export function openCaptureStream(
	captureBase: string,
	bucket: string,
	onDelivery: (delivery: CaptureDelivery) => void,
	onState: (status: TailStatus) => void,
): CaptureStopper {
	const url = captureStreamUrl(captureBase, bucket);
	let stopped = false;
	let opened = false;

	const EventSourceCtor = globalThis.EventSource;
	if (EventSourceCtor === undefined) {
		onState({ state: "error", message: "EventSource is not available in this environment" });
		return () => {};
	}

	onState({ state: "connecting" });
	const es = new EventSourceCtor(url);

	es.onopen = (): void => {
		if (stopped) return;
		opened = true;
		onState({ state: "live", atOffset: null });
	};

	// The binary emits NAMED `delivery` events; listen by name. Keep onmessage as
	// a fallback for any unnamed event a proxy might rewrite.
	const onDeliveryEvent = (ev: Event): void => {
		if (stopped) return;
		const delivery = parseCaptureDeliveryData((ev as MessageEvent<string>).data);
		if (delivery !== null) onDelivery(delivery);
	};
	es.addEventListener("delivery", onDeliveryEvent);
	es.onmessage = (ev: MessageEvent<string>): void => {
		if (stopped) return;
		const delivery = parseCaptureDeliveryData(ev.data);
		if (delivery !== null) onDelivery(delivery);
	};

	es.onerror = (): void => {
		if (stopped) return;
		if (es.readyState === EventSourceCtor.CLOSED) {
			onState({ state: "error", message: "the capture connection closed" });
		} else {
			onState({
				state: "reconnecting",
				attempt: 1,
				reason: opened ? "capture connection dropped" : "could not reach the capture endpoint",
			});
		}
	};

	return (): void => {
		if (stopped) return;
		stopped = true;
		es.close();
		onState({ state: "idle" });
	};
}
