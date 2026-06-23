/**
 * WakeMonitorWorkspace — the center region that makes the Durable Streams wake
 * loop visible. A dual split-screen for ONE subscription:
 *
 *   ┌───────────── source stream ──────────┬─────────── wake timeline ─────────┐
 *   │ pick a linked stream                  │ phase · generation · lease        │
 *   │ ┌ compact publish composer ────────┐  │ ┌ webhook: captured deliveries ─┐ │
 *   │ │ body · [Publish] · curl          │  │ │  ts · wake_id/gen · stream/off │ │
 *   │ └──────────────────────────────────┘  │ │  signature (kid/JWKS) · [Ack]  │ │
 *   │ ┌ live tail of the source stream ──┐  │ └────────────────────────────────┘ │
 *   │ │  messages appear as they arrive  │  │ ┌ pull-wake: wake events ────────┐ │
 *   │ └──────────────────────────────────┘  │ │  tail wake_stream · claim/ack  │ │
 *   └───────────────────────────────────────┴─────────────────────────────────┘
 *
 * Causal link: publishing on the LEFT bumps store.wakePulse; the RIGHT pane
 * flashes a "a wake should be arriving" cue and the next captured delivery /
 * wake event animates in (message → wake → hook invoked → acked). The whole loop
 * is one glance. Motion honors prefers-reduced-motion (handled in CSS).
 *
 * Everything is layout + thin wiring over the store: the feed (capture SSE or
 * wake_stream tail), the buffers, and every mutation live in the store; the
 * publish reuses the shared publish seam; the curl previews come from pure libs.
 * The browser cannot host a webhook, so the webhook pane relies on the dsui
 * binary's capture endpoint — noted in-UI, and webhooks only fire for real
 * against a redis-backed chronicle with subscriptions enabled.
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useEffect, useId } from "preact/hooks";
import { previewOf } from "../lib/guards";
import { formatTime, formatTimeFull } from "../lib/messages";
import { previewAppendOperation } from "../lib/streamForm";
import { previewCallbackOperation } from "../lib/subscriptionForm";
import { previewClaimOperation } from "../lib/subscriptions";
import { tailAnnouncePolite, tailStatusDetail, tailStatusLabel, tailTone } from "../lib/tail";
import type {
	CaptureDelivery,
	OffsetAck,
	Subscription,
	TailStatus,
	WakeNotification,
} from "../lib/types";
import { hasSignature, parseSignatureHeader, parseWakeNotification } from "../lib/wakes";
import {
	activeClaim,
	activeConnection,
	callbackAck,
	captureBase,
	claimInFlight,
	claimWake,
	closeWakeMonitor,
	getSubscription,
	lastExchange,
	publishAndPulse,
	restartWakeFeed,
	selectStream,
	selectedStream,
	selectedStreamPath,
	setTailMode,
	startTail,
	stopTail,
	streams,
	subscriptionLoading,
	tailRows,
	tailStatus,
	wakeBucket,
	wakeDeliveries,
	wakeEvents,
	wakeFeedStatus,
	wakePulse,
	wakeSubId,
	wakeSubscription,
} from "../state/store";
import { CurlPreview } from "./CurlPreview";
import { ProtocolPanel } from "./ProtocolPanel";
import {
	IconBell,
	IconBroadcast,
	IconCheck,
	IconClose,
	IconCornerDownRight,
	IconKey,
	IconLoader,
	IconPlay,
	IconRefresh,
	IconSend,
	IconWebhook,
	IconZap,
} from "./icons";

/* ---------------------------------------------------------------------------
 * Shared bits
 * ------------------------------------------------------------------------ */

/** A small connection-status dot + label for a feed, announced via aria-live. */
function FeedStatus(props: { status: TailStatus; label: string }): JSX.Element {
	const { status, label } = props;
	const tone = tailTone(status);
	const text = tailStatusLabel(status);
	const detail = tailStatusDetail(status);
	const polite = tailAnnouncePolite(status);
	const pulse = status.state === "connecting" || status.state === "reconnecting";
	return (
		<div
			class={`dsui-wm__feedstatus dsui-tail__status--${tone}`}
			// biome-ignore lint/a11y/useSemanticElements: role="status" is the correct ARIA live-region for a connection-status readout.
			role="status"
			aria-live={polite ? "polite" : "assertive"}
			aria-label={`${label}: ${text}`}
		>
			<span class={`dsui-tail__dot${pulse ? " is-pulsing" : ""}`} aria-hidden="true" />
			<span class="dsui-tail__statuslabel">{text}</span>
			{detail !== "" ? <span class="dsui-tail__statusdetail">{detail}</span> : null}
		</div>
	);
}

/* ---------------------------------------------------------------------------
 * LEFT pane — source stream picker + publish composer + live tail
 * ------------------------------------------------------------------------ */

/** A compact publish composer for the watched source stream (JSON/text/binary). */
function WakeComposer(props: { sub: Subscription }): JSX.Element {
	const conn = activeConnection.value;
	const stream = selectedStream.value;
	const path = selectedStreamPath.value;
	const idBase = useId();
	const text = useSignal("");

	const kind = stream?.kind ?? "json";
	const placeholder =
		kind === "json" ? '[{ "event": "ping" }]' : kind === "text" ? "a message…" : "bytes as text…";

	// The body is sent verbatim (a JSON batch / raw text); the full validating
	// composer lives in the Messages workspace — this is the in-loop "fire one
	// message" affordance, kept minimal.
	const contentType = stream?.contentType ?? undefined;

	const previewOp = useComputed(() => {
		if (conn === null || path === null || text.value.trim() === "") return null;
		return previewAppendOperation(conn.baseUrl, conn.streamRoot, path, {
			body: text.value,
			...(contentType !== undefined ? { contentType } : {}),
		});
	});

	if (path === null) {
		return (
			<div class="dsui-empty dsui-empty--inline">
				<IconSend size={22} class="dsui-empty__icon" />
				<p class="dsui-empty__title">No source stream</p>
				<p class="dsui-empty__hint">
					Pick a linked stream above to publish into it and watch the wake fire.
				</p>
			</div>
		);
	}

	function onSubmit(e: Event): void {
		e.preventDefault();
		if (path === null || text.value.trim() === "") return;
		void publishAndPulse(path, text.value, {
			...(contentType !== undefined ? { contentType } : {}),
		}).then((ok) => {
			if (ok) text.value = "";
		});
	}

	return (
		<form class="dsui-wm__composer" onSubmit={onSubmit}>
			<label class="dsui-field__label" for={`${idBase}-body`}>
				Publish to <code>{path}</code> <span class={`dsui-kind dsui-kind--${kind}`}>{kind}</span>
			</label>
			<textarea
				id={`${idBase}-body`}
				class="dsui-textarea dsui-textarea--mono"
				rows={3}
				placeholder={placeholder}
				spellcheck={false}
				value={text.value}
				onInput={(e) => {
					text.value = e.currentTarget.value;
				}}
			/>
			<div class="dsui-wm__composerfoot">
				<button
					type="submit"
					class="dsui-btn dsui-btn--primary dsui-btn--sm"
					disabled={text.value.trim() === ""}
				>
					<IconSend size={14} />
					<span>Publish &amp; watch</span>
				</button>
				<span class="dsui-wm__composerhint">
					{props.sub.type === "webhook"
						? "fires a signed webhook to the capture endpoint"
						: "appends a wake event to the wake_stream"}
				</span>
			</div>
			<CurlPreview operation={previewOp.value} copyKey="wm-publish-curl" label="Equivalent curl" />
		</form>
	);
}

/** The left pane: source picker, composer, and a live tail of the source stream. */
function SourcePane(props: { sub: Subscription }): JSX.Element {
	const { sub } = props;
	const all = streams.value;
	const path = selectedStreamPath.value;
	const status = tailStatus.value;
	const rows = tailRows.value;
	const live = status.state === "live" || status.state === "connecting";
	const linkedPaths = sub.streams.map((s) => s.path);

	// Auto-start an SSE tail of the source stream when one is chosen and not yet
	// tailing, so the left pane shows messages landing without an extra click.
	// The store tail actions are module-level (not reactive deps), so the chosen
	// path is the only dependency.
	useEffect(() => {
		if (path === null) return;
		setTailMode("sse");
		startTail("now");
		return () => stopTail();
	}, [path]);

	return (
		<section class="dsui-wm__pane dsui-wm__pane--source" aria-label="Source stream">
			<header class="dsui-wm__panehead">
				<IconBroadcast size={14} />
				<span class="dsui-wm__panetitle">Source stream</span>
				<FeedStatus status={status} label="Source tail" />
			</header>

			<div class="dsui-wm__panebody">
				<div class="dsui-wm__sourcepick">
					<label class="dsui-field__label" for="wm-source-select">
						Watched stream
					</label>
					<select
						id="wm-source-select"
						class="dsui-input"
						value={path ?? ""}
						onChange={(e) => {
							const v = e.currentTarget.value;
							if (v !== "") selectStream(v);
						}}
					>
						<option value="" disabled>
							Pick a stream…
						</option>
						{linkedPaths.length > 0 ? (
							<optgroup label="Linked to this subscription">
								{linkedPaths.map((p) => (
									<option key={p} value={p}>
										{p}
									</option>
								))}
							</optgroup>
						) : null}
						<optgroup label="All streams">
							{all.map((s) => (
								<option key={s.path} value={s.path}>
									{s.path}
								</option>
							))}
						</optgroup>
					</select>
				</div>

				<WakeComposer sub={sub} />

				<div class="dsui-wm__tail" aria-label="Source stream live tail">
					<div class="dsui-wm__tailhead">
						<span class="dsui-wm__tailtitle">Live messages</span>
						<span class="dsui-wm__count">
							{rows.length} {rows.length === 1 ? "message" : "messages"}
						</span>
					</div>
					<div class="dsui-wm__taillist">
						{rows.length === 0 ? (
							<p class="dsui-wm__tailempty">
								{path === null
									? "Pick a stream to follow its tail."
									: live
										? "Connected — publish above to see a message land here, then a wake on the right."
										: "Not tailing."}
							</p>
						) : (
							<ul class="dsui-wm__rows">
								{rows.slice(-12).map((row, i) => (
									<li key={`${i}-${row.preview}`} class="dsui-wm__row">
										<IconCornerDownRight size={11} class="dsui-wm__rowmark" />
										<span class="dsui-wm__rowpreview">{previewOf(row.value, 90)}</span>
									</li>
								))}
							</ul>
						)}
					</div>
				</div>
			</div>
		</section>
	);
}

/* ---------------------------------------------------------------------------
 * RIGHT pane — webhook capture timeline
 * ------------------------------------------------------------------------ */

/** One captured webhook delivery card: decoded wake + signature + ack control. */
function DeliveryCard(props: {
	sub: Subscription;
	delivery: CaptureDelivery;
	fresh: boolean;
}): JSX.Element {
	const { sub, delivery, fresh } = props;
	const conn = activeConnection.value;
	const busy = claimInFlight.value;
	const note: WakeNotification | null = parseWakeNotification(delivery.body);
	const sig = parseSignatureHeader(delivery.signature);
	const signed = hasSignature(sig);

	// The acks the callback would send: each notified stream to its current tail.
	const acks: readonly OffsetAck[] =
		note === null
			? []
			: note.streams
					.filter((s) => s.tailOffset !== "")
					.map((s) => ({ stream: s.path, offset: s.tailOffset }));

	const callbackOp = useComputed(() =>
		conn === null || note === null
			? null
			: previewCallbackOperation(conn.baseUrl, sub.id, note.callbackToken ?? "<callback_token>", {
					wakeId: note.wakeId,
					generation: note.generation,
					acks,
					done: true,
				}),
	);

	const canAck = note !== null && note.callbackToken !== null;

	return (
		<li class={`dsui-wm__delivery${fresh ? " is-fresh" : ""}`}>
			<div class="dsui-wm__deliveryhead">
				<span class="dsui-wm__deliveryseq" title="capture sequence">
					#{delivery.seq}
				</span>
				<span class="dsui-wm__deliverytime" title={formatTimeFull(delivery.receivedAt)}>
					{formatTime(delivery.receivedAt)}
				</span>
				<span class="dsui-wm__deliverymethod">{delivery.method}</span>
				{signed ? (
					<span class="dsui-pill dsui-pill--ok dsui-wm__sigpill" title="Webhook-Signature present">
						<IconKey size={11} /> signed
					</span>
				) : (
					<span class="dsui-pill dsui-wm__sigpill" title="no Webhook-Signature header">
						unsigned
					</span>
				)}
			</div>

			{note === null ? (
				<p class="dsui-wm__rawnote">
					Body did not decode as a wake notification. Raw body:
					<code class="dsui-wm__rawbody">{delivery.body.slice(0, 200) || "—"}</code>
				</p>
			) : (
				<dl class="dsui-wm__wakefields">
					<div>
						<dt>wake_id</dt>
						<dd>
							<code>{note.wakeId}</code>
						</dd>
					</div>
					<div>
						<dt>generation</dt>
						<dd>
							<code>{note.generation}</code>
						</dd>
					</div>
					{note.streams.map((s) => (
						<div key={s.path} class="dsui-wm__wakestream">
							<dt>{s.path}</dt>
							<dd>
								<code title="acked → tail">
									{`${s.ackedOffset || "—"} → ${s.tailOffset || "—"}`}
								</code>
								{s.hasPending ? (
									<span class="dsui-pill dsui-pill--warn">pending</span>
								) : (
									<span class="dsui-pill dsui-pill--ok">caught up</span>
								)}
							</dd>
						</div>
					))}
				</dl>
			)}

			<details class="dsui-wm__sigdetail">
				<summary>Signature{signed ? "" : " (none)"}</summary>
				<dl class="dsui-meta">
					<div class="dsui-meta__row">
						<span class="dsui-meta__label">kid</span>
						<span class="dsui-meta__value">
							{sig.kid !== null ? <code>{sig.kid}</code> : <span class="dsui-meta__muted">—</span>}
						</span>
					</div>
					<div class="dsui-meta__row">
						<span class="dsui-meta__label">t</span>
						<span class="dsui-meta__value">
							{sig.timestamp !== null ? (
								<code>{sig.timestamp}</code>
							) : (
								<span class="dsui-meta__muted">—</span>
							)}
						</span>
					</div>
					<div class="dsui-meta__row">
						<span class="dsui-meta__label">ed25519</span>
						<span class="dsui-meta__value">
							{sig.ed25519 !== null ? (
								<code class="dsui-wm__sigval" title={sig.ed25519}>
									{sig.ed25519}
								</code>
							) : (
								<span class="dsui-meta__muted">—</span>
							)}
						</span>
					</div>
				</dl>
				<p class="dsui-wm__sighint">
					Verification is Ed25519 over <code>"&lt;t&gt;.&lt;raw_body&gt;"</code> against the JWKS
					key for <code>kid</code>. The exact raw body bytes are kept above so the check stays
					honest.
				</p>
			</details>

			<div class="dsui-wm__deliveryfoot">
				<button
					type="button"
					class="dsui-btn dsui-btn--sm dsui-btn--primary"
					disabled={!canAck || busy}
					title={
						canAck
							? "POST the callback with done=true to ack + release the lease"
							: "No callback_token in this delivery — nothing to ack"
					}
					onClick={() => {
						if (note === null || note.callbackToken === null) return;
						void callbackAck(sub.id, note.callbackToken, {
							wakeId: note.wakeId,
							generation: note.generation,
							acks,
							done: true,
						});
					}}
				>
					{busy ? <IconLoader size={13} class="dsui-spin" /> : <IconCheck size={13} />}
					<span>Ack callback</span>
				</button>
				<CurlPreview
					operation={callbackOp.value}
					copyKey={`wm-callback-${delivery.seq}`}
					label="Equivalent curl — ack callback"
				/>
			</div>
		</li>
	);
}

/** The webhook wake pane: captured deliveries relayed over SSE, newest on top. */
function WebhookPane(props: { sub: Subscription }): JSX.Element {
	const { sub } = props;
	const deliveries = wakeDeliveries.value;
	const status = wakeFeedStatus.value;
	const pulse = wakePulse.value;
	const base = captureBase.value;
	const bucket = wakeBucket.value;
	// The newest delivery is "fresh" right after a publish pulse, for the animation.
	const newestSeq = deliveries.length > 0 ? deliveries[deliveries.length - 1]?.seq : undefined;
	const freshSignal = useSignal<number | undefined>(undefined);
	// biome-ignore lint/correctness/useExhaustiveDependencies: react to a new delivery arriving (its seq) and to the publish pulse; freshSignal is a stable handle.
	useEffect(() => {
		freshSignal.value = newestSeq;
		const id = globalThis.setTimeout(() => {
			freshSignal.value = undefined;
		}, 1500);
		return () => globalThis.clearTimeout(id);
	}, [newestSeq, pulse]);

	return (
		<section class="dsui-wm__pane dsui-wm__pane--hooks" aria-label="Webhook deliveries">
			<header class="dsui-wm__panehead">
				<IconWebhook size={14} />
				<span class="dsui-wm__panetitle">Captured webhooks</span>
				<FeedStatus status={status} label="Capture feed" />
			</header>

			<div class="dsui-wm__panebody">
				{base !== null && bucket !== null ? (
					<p class="dsui-wm__capturenote">
						Capturing at <code>{`${base}/__hooks/${bucket}`}</code>
						{status.state === "error" ? null : status.state === "idle" ? (
							<button type="button" class="dsui-btn dsui-btn--xs" onClick={() => restartWakeFeed()}>
								<IconPlay size={12} />
								<span>Reconnect</span>
							</button>
						) : null}
					</p>
				) : (
					<p class="dsui-wm__capturenote dsui-wm__capturenote--warn" role="note">
						No capture endpoint — run the <code>dsui</code> binary (not <code>vite dev</code>) so it
						can receive webhooks.
					</p>
				)}

				<div
					class="dsui-wm__deliveries"
					role="log"
					aria-label="Captured webhook deliveries"
					aria-live="polite"
				>
					{deliveries.length === 0 ? (
						<div class="dsui-empty dsui-empty--inline">
							<IconWebhook size={22} class="dsui-empty__icon" />
							<p class="dsui-empty__title">No webhooks captured yet</p>
							<p class="dsui-empty__hint">
								Publish on the left. If chronicle is redis-backed with subscriptions enabled, it
								POSTs a signed wake to the capture endpoint and it appears here.
							</p>
						</div>
					) : (
						<ul class="dsui-wm__deliverylist">
							{[...deliveries].reverse().map((d) => (
								<DeliveryCard
									key={d.seq}
									sub={sub}
									delivery={d}
									fresh={d.seq === freshSignal.value}
								/>
							))}
						</ul>
					)}
				</div>
			</div>
		</section>
	);
}

/* ---------------------------------------------------------------------------
 * RIGHT pane — pull-wake wake_stream timeline
 * ------------------------------------------------------------------------ */

function PullWakePane(props: { sub: Subscription }): JSX.Element {
	const { sub } = props;
	const events = wakeEvents.value;
	const status = wakeFeedStatus.value;
	const conn = activeConnection.value;
	const claim = activeClaim.value;
	const busy = claimInFlight.value;
	const pulse = wakePulse.value;
	const worker = useSignal("dsui-worker");
	const newestTs = events.length > 0 ? events[events.length - 1]?.ts : undefined;
	const freshSignal = useSignal<number | undefined>(undefined);
	// biome-ignore lint/correctness/useExhaustiveDependencies: react to a new event and to the publish pulse; freshSignal is stable.
	useEffect(() => {
		freshSignal.value = newestTs;
		const id = globalThis.setTimeout(() => {
			freshSignal.value = undefined;
		}, 1500);
		return () => globalThis.clearTimeout(id);
	}, [newestTs, pulse]);

	const claimOp = useComputed(() =>
		conn === null ? null : previewClaimOperation(conn.baseUrl, sub.id, worker.value.trim()),
	);

	return (
		<section class="dsui-wm__pane dsui-wm__pane--hooks" aria-label="Wake events">
			<header class="dsui-wm__panehead">
				<IconZap size={14} />
				<span class="dsui-wm__panetitle">Wake events</span>
				<FeedStatus status={status} label="Wake stream" />
			</header>

			<div class="dsui-wm__panebody">
				<p class="dsui-wm__capturenote">
					Tailing <code>{sub.wakeStream ?? `${sub.id}/wake_stream`}</code> — a worker claims, then
					acks/releases the lease.
				</p>

				<div class="dsui-wm__claimrow">
					{claim === null ? (
						<>
							<input
								class="dsui-input dsui-input--mono dsui-input--sm"
								type="text"
								aria-label="Worker name"
								value={worker.value}
								onInput={(e) => {
									worker.value = e.currentTarget.value;
								}}
							/>
							<button
								type="button"
								class="dsui-btn dsui-btn--sm dsui-btn--primary"
								disabled={busy || worker.value.trim() === ""}
								onClick={() => void claimWake(sub.id, worker.value)}
							>
								{busy ? <IconLoader size={13} class="dsui-spin" /> : <IconZap size={13} />}
								<span>Claim</span>
							</button>
						</>
					) : (
						<span class="dsui-pill dsui-pill--ok">
							lease held · gen {claim.generation} · wake {claim.wakeId}
						</span>
					)}
				</div>
				<CurlPreview
					operation={claimOp.value}
					copyKey="wm-claim-curl"
					label="Equivalent curl — claim"
				/>

				<div class="dsui-wm__deliveries" role="log" aria-label="Wake events" aria-live="polite">
					{events.length === 0 ? (
						<div class="dsui-empty dsui-empty--inline">
							<IconZap size={22} class="dsui-empty__icon" />
							<p class="dsui-empty__title">No wake events yet</p>
							<p class="dsui-empty__hint">
								Publish on the left. If chronicle is redis-backed with subscriptions enabled, a wake
								event is appended to the wake_stream and appears here.
							</p>
						</div>
					) : (
						<ul class="dsui-wm__deliverylist">
							{[...events].reverse().map((ev, i) => (
								<li
									key={`${ev.ts}-${i}`}
									class={`dsui-wm__delivery${ev.ts === freshSignal.value ? " is-fresh" : ""}`}
								>
									<div class="dsui-wm__deliveryhead">
										<span class="dsui-wm__deliverytime" title={formatTimeFull(ev.ts)}>
											{formatTime(ev.ts)}
										</span>
										<span class="dsui-pill">gen {ev.generation}</span>
									</div>
									<dl class="dsui-wm__wakefields">
										<div class="dsui-wm__wakestream">
											<dt>stream</dt>
											<dd>
												<code>{ev.stream}</code>
											</dd>
										</div>
									</dl>
								</li>
							))}
						</ul>
					)}
				</div>
				<p class="dsui-wm__sighint">
					Once claimed, ack/heartbeat/release the lease from the subscription's Worker plane in its
					detail view.
				</p>
			</div>
		</section>
	);
}

/* ---------------------------------------------------------------------------
 * Causal-link banner (message → wake → hook → ack)
 * ------------------------------------------------------------------------ */

function CausalStrip(props: { type: Subscription["type"] }): JSX.Element {
	const pulse = wakePulse.value;
	const armed = useSignal(false);
	// biome-ignore lint/correctness/useExhaustiveDependencies: keyed on the publish pulse tick; armed is a stable handle.
	useEffect(() => {
		if (pulse === 0) return;
		armed.value = true;
		const id = globalThis.setTimeout(() => {
			armed.value = false;
		}, 2000);
		return () => globalThis.clearTimeout(id);
	}, [pulse]);
	const hookLabel = props.type === "webhook" ? "webhook captured" : "wake event";
	return (
		<div class={`dsui-wm__causal${armed.value ? " is-armed" : ""}`} aria-hidden="true">
			<span class="dsui-wm__causalstep">publish</span>
			<span class="dsui-wm__causalarrow">→</span>
			<span class="dsui-wm__causalstep">wake</span>
			<span class="dsui-wm__causalarrow">→</span>
			<span class="dsui-wm__causalstep">{hookLabel}</span>
			<span class="dsui-wm__causalarrow">→</span>
			<span class="dsui-wm__causalstep">ack</span>
		</div>
	);
}

/* ---------------------------------------------------------------------------
 * Workspace
 * ------------------------------------------------------------------------ */

export function WakeMonitorWorkspace(): JSX.Element {
	const id = wakeSubId.value;
	const sub = wakeSubscription.value;
	const loading = subscriptionLoading.value;

	if (id === null) {
		return (
			<div class="dsui-ws dsui-ws--empty">
				<div class="dsui-empty">
					<IconBell size={26} class="dsui-empty__icon" />
					<p class="dsui-empty__title">No wake to watch</p>
					<p class="dsui-empty__hint">
						Open the Wake Monitor from a subscription's "Watch wakes" action to see the publish →
						wake → ack loop.
					</p>
				</div>
			</div>
		);
	}

	return (
		<div class="dsui-ws dsui-wm">
			<header class="dsui-ws__head">
				<div class="dsui-ws__title">
					<IconBroadcast size={15} />
					<span class="dsui-ws__name">Wake monitor</span>
					<code class="dsui-wm__subid">{id}</code>
					{sub !== null ? (
						<span
							class={`dsui-subchip dsui-subchip--${sub.type === "webhook" ? "webhook" : "pull"}`}
						>
							{sub.type === "webhook" ? <IconWebhook size={12} /> : <IconZap size={12} />}
							{sub.type}
						</span>
					) : null}
				</div>
				<div class="dsui-ws__headend">
					{sub !== null ? <CausalStrip type={sub.type} /> : null}
					<button
						type="button"
						class="dsui-iconbtn dsui-iconbtn--sm"
						title="Refresh subscription"
						aria-label="Refresh subscription"
						disabled={loading}
						onClick={() => void getSubscription(id)}
					>
						<IconRefresh size={14} class={loading ? "dsui-spin" : undefined} />
					</button>
					<button
						type="button"
						class="dsui-iconbtn dsui-iconbtn--sm"
						title="Close the wake monitor"
						aria-label="Close the wake monitor"
						onClick={() => closeWakeMonitor()}
					>
						<IconClose size={14} />
					</button>
				</div>
			</header>

			{sub === null ? (
				<div class="dsui-ws__scroll">
					<div class="dsui-empty">
						{loading ? (
							<>
								<IconLoader size={24} class="dsui-empty__icon dsui-spin" />
								<p class="dsui-empty__title">Loading subscription…</p>
							</>
						) : (
							<>
								<IconBell size={24} class="dsui-empty__icon" />
								<p class="dsui-empty__title">No view for {id}</p>
								<p class="dsui-empty__hint">
									Could not load it. Subscriptions need a redis-backed chronicle. Refresh to retry.
								</p>
								<button
									type="button"
									class="dsui-btn dsui-btn--xs"
									onClick={() => void getSubscription(id)}
								>
									<IconRefresh size={13} />
									<span>Try again</span>
								</button>
							</>
						)}
					</div>
				</div>
			) : (
				<div class="dsui-wm__split">
					<SourcePane sub={sub} />
					{sub.type === "webhook" ? <WebhookPane sub={sub} /> : <PullWakePane sub={sub} />}
				</div>
			)}

			<ProtocolPanel exchange={lastExchange.value} tail={null} />
		</div>
	);
}
