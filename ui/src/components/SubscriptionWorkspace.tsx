/**
 * SubscriptionWorkspace — the center region when a subscription is selected.
 * Parallel to MessagesWorkspace, it lays out the detail view of one subscription
 * on the reserved /__ds/* control plane and is a vertical stack of <section>s:
 *
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │ head: id · type chip · phase/status pills · Refresh · Delete   │
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ meta: type · phase · generation · lease TTL · created · target │
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ webhook: url + jwks link + ack-callback curl   (webhook type)  │
 *   │ pull-wake: Claim / Ack / Release controls      (pull-wake)     │
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ links table: path · link_type · acked → tail · pending         │
 *   │   add explicit streams · remove an explicit link               │
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ "Under the hood" protocol disclosure (the last /__ds exchange) │
 *   └──────────────────────────────────────────────────────────────┘
 *
 * It reads the cached subscription view from the store (store.selectedSubscription,
 * hydrated by getSubscription) and calls store actions for every mutation; each
 * action records the captured exchange + the Operation so the protocol panel and
 * copy-as-curl stay honest. The previews next to each control come from
 * lib/subscriptions (pure), so the curl is exact before the request runs.
 *
 * Extensibility seam: a new control is a section here + a store action; keep the
 * fetch in dsClient and the mutation in the store.
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { previewCallbackOperation } from "../lib/subscriptionForm";
import {
	previewAckOperation,
	previewAddStreamsOperation,
	previewClaimOperation,
	previewReleaseOperation,
} from "../lib/subscriptions";
import type { OffsetAck, StreamLink, Subscription, WakeStreamSnapshot } from "../lib/types";
import {
	ackWake,
	activeClaim,
	activeConnection,
	addSubscriptionStreams,
	claimInFlight,
	claimWake,
	deleteSubscription,
	getSubscription,
	lastExchange,
	releaseWake,
	removeSubscriptionStream,
	selectedSubscription,
	selectedSubscriptionId,
	subscriptionInFlight,
	subscriptionLoading,
	untrackSubscriptionId,
} from "../state/store";
import { CurlPreview } from "./CurlPreview";
import { ProtocolPanel } from "./ProtocolPanel";
import {
	IconArrowUpRight,
	IconBell,
	IconCheck,
	IconKey,
	IconLink,
	IconLoader,
	IconPlus,
	IconRefresh,
	IconTrash,
	IconWebhook,
	IconZap,
} from "./icons";

/* ---------------------------------------------------------------------------
 * Small building blocks
 * ------------------------------------------------------------------------ */

/** A type chip (webhook / pull-wake) with the matching icon. */
function TypeChip(props: { type: Subscription["type"] }): JSX.Element {
	const webhook = props.type === "webhook";
	return (
		<span class={`dsui-subchip dsui-subchip--${webhook ? "webhook" : "pull"}`}>
			{webhook ? <IconWebhook size={12} /> : <IconZap size={12} />}
			{webhook ? "webhook" : "pull-wake"}
		</span>
	);
}

/** A phase pill (idle / waking / live) tinted by liveness; omitted when unknown. */
function PhasePill(props: { phase: Subscription["phase"] }): JSX.Element | null {
	const phase = props.phase;
	if (phase === undefined) return null;
	const tone = phase === "live" ? "ok" : phase === "waking" ? "warn" : "muted";
	return <span class={`dsui-phase dsui-phase--${tone}`}>{phase}</span>;
}

/** One key/value row in the metadata grid, reusing the inspector's meta styles. */
function MetaRow(props: { label: string; children: JSX.Element | string | null }): JSX.Element {
	return (
		<div class="dsui-meta__row">
			<span class="dsui-meta__label">{props.label}</span>
			<span class="dsui-meta__value">{props.children}</span>
		</div>
	);
}

/* ---------------------------------------------------------------------------
 * Links table
 * ------------------------------------------------------------------------ */

/**
 * A linked stream as the table renders it: the serialized link plus, when a
 * claim is held, the matching wake snapshot (tail_offset + has_pending). The
 * GET view only carries acked_offset, so tail/pending are shown only when a
 * claim has surfaced them (otherwise "—").
 */
interface LinkRow {
	readonly link: StreamLink;
	readonly snapshot: WakeStreamSnapshot | null;
}

function LinkTable(props: {
	rows: readonly LinkRow[];
	canRemove: boolean;
	onRemove: (path: string) => void;
}): JSX.Element {
	const { rows, canRemove, onRemove } = props;
	if (rows.length === 0) {
		return (
			<div class="dsui-empty dsui-empty--inline">
				<IconLink size={22} class="dsui-empty__icon" />
				<p class="dsui-empty__title">No linked streams</p>
				<p class="dsui-empty__hint">
					No stream matches the pattern yet, and none are explicitly linked. Add one below.
				</p>
			</div>
		);
	}
	return (
		<table class="dsui-linktable" aria-label="Linked streams">
			<thead class="dsui-linktable__head">
				<tr>
					<th scope="col">Path</th>
					<th scope="col">Link</th>
					<th scope="col">Acked → Tail</th>
					<th scope="col">Pending</th>
					{canRemove ? <th scope="col" class="dsui-linktable__rmcol" /> : null}
				</tr>
			</thead>
			<tbody>
				{rows.map(({ link, snapshot }) => (
					<tr class="dsui-linktable__row" key={link.path}>
						<td class="dsui-linktable__path" title={link.path}>
							{link.path}
						</td>
						<td>
							<span class={`dsui-linktype dsui-linktype--${link.linkType}`}>{link.linkType}</span>
						</td>
						<td class="dsui-linktable__offsets">
							<code title={`acked: ${link.ackedOffset || "—"}`}>{link.ackedOffset || "—"}</code>
							<span class="dsui-linktable__arrow" aria-hidden="true">
								→
							</span>
							<code title={snapshot === null ? "tail unknown until a claim" : snapshot.tailOffset}>
								{snapshot?.tailOffset ?? "—"}
							</code>
						</td>
						<td>
							{snapshot === null ? (
								<span class="dsui-pill">unknown</span>
							) : snapshot.hasPending ? (
								<span class="dsui-pill dsui-pill--warn">pending</span>
							) : (
								<span class="dsui-pill dsui-pill--ok">caught up</span>
							)}
						</td>
						{canRemove ? (
							<td class="dsui-linktable__rmcol">
								{link.linkType === "explicit" ? (
									<button
										type="button"
										class="dsui-iconbtn dsui-iconbtn--sm"
										title={`Unlink ${link.path}`}
										aria-label={`Unlink ${link.path}`}
										onClick={() => onRemove(link.path)}
									>
										<IconTrash size={13} />
									</button>
								) : (
									<span
										class="dsui-linktable__globnote"
										title="A glob link cannot be removed directly; it stays while the pattern matches."
									>
										glob
									</span>
								)}
							</td>
						) : null}
					</tr>
				))}
			</tbody>
		</table>
	);
}

/* ---------------------------------------------------------------------------
 * Pull-wake worker controls (claim → ack/heartbeat → release)
 * ------------------------------------------------------------------------ */

function PullWakeControls(props: { sub: Subscription }): JSX.Element {
	const { sub } = props;
	const conn = activeConnection.value;
	const claim = activeClaim.value;
	const busy = claimInFlight.value;
	const worker = useSignal("dsui-worker");

	// When a claim is held, ack to each stream's tail (the offsets a worker would
	// have processed up to). Build the OffsetAck[] from the claim snapshots.
	const acks = useComputed<readonly OffsetAck[]>(() =>
		claim === null
			? []
			: claim.streams
					.filter((s) => s.tailOffset !== "")
					.map((s) => ({ stream: s.path, offset: s.tailOffset })),
	);

	const claimOp = useComputed(() =>
		conn === null ? null : previewClaimOperation(conn.baseUrl, sub.id, worker.value.trim()),
	);
	const ackOp = useComputed(() =>
		conn === null || claim === null
			? null
			: previewAckOperation(conn.baseUrl, sub.id, claim.token, {
					wakeId: claim.wakeId,
					generation: claim.generation,
					acks: acks.value,
					done: true,
				}),
	);
	const releaseOp = useComputed(() =>
		conn === null || claim === null
			? null
			: previewReleaseOperation(conn.baseUrl, sub.id, claim.token, {
					wakeId: claim.wakeId,
					generation: claim.generation,
				}),
	);

	return (
		<section class="dsui-subsection" aria-label="Pull-wake worker">
			<header class="dsui-subsection__head">
				<IconZap size={14} />
				<span class="dsui-subsection__title">Worker plane</span>
				{claim !== null ? (
					<span class="dsui-pill dsui-pill--ok">lease held · gen {claim.generation}</span>
				) : (
					<span class="dsui-pill">no lease</span>
				)}
			</header>

			{claim === null ? (
				<>
					<p class="dsui-subsection__lede">
						Race other workers to claim the lease. On success you hold a Bearer token scoped to one
						generation; ack offsets (with done to release) or release without acking.
					</p>
					<div class="dsui-inlineform">
						<label class="dsui-field__label" for={`${sub.id}-worker`}>
							Worker name
						</label>
						<input
							id={`${sub.id}-worker`}
							class="dsui-input dsui-input--mono"
							type="text"
							value={worker.value}
							autocomplete="off"
							spellcheck={false}
							onInput={(e) => {
								worker.value = e.currentTarget.value;
							}}
						/>
						<button
							type="button"
							class="dsui-btn dsui-btn--primary"
							disabled={busy || worker.value.trim() === ""}
							onClick={() => void claimWake(sub.id, worker.value)}
						>
							{busy ? <IconLoader size={14} class="dsui-spin" /> : <IconZap size={14} />}
							<span>Claim lease</span>
						</button>
					</div>
					<CurlPreview
						operation={claimOp.value}
						copyKey="sub-claim-curl"
						label="Equivalent curl — claim"
					/>
				</>
			) : (
				<>
					<dl class="dsui-claiminfo">
						<div>
							<dt>wake_id</dt>
							<dd>
								<code>{claim.wakeId}</code>
							</dd>
						</div>
						<div>
							<dt>generation</dt>
							<dd>
								<code>{claim.generation}</code>
							</dd>
						</div>
						<div>
							<dt>lease TTL</dt>
							<dd>
								<code>{claim.leaseTtlMs} ms</code>
							</dd>
						</div>
					</dl>
					<p class="dsui-subsection__lede">
						Ack advances each stream to its claimed tail. <strong>Ack + done</strong> applies the
						acks and releases the lease; <strong>heartbeat</strong> extends it without acking;{" "}
						<strong>release</strong> drops the lease without applying. A stale generation/wake
						returns <code>409 FENCED</code>.
					</p>
					<div class="dsui-btnrow">
						<button
							type="button"
							class="dsui-btn dsui-btn--primary"
							disabled={busy}
							onClick={() => void ackWake(sub.id, acks.value, true)}
						>
							{busy ? <IconLoader size={14} class="dsui-spin" /> : <IconCheck size={14} />}
							<span>Ack + release</span>
						</button>
						<button
							type="button"
							class="dsui-btn"
							disabled={busy}
							title="Ack with done=false — extends the lease as a heartbeat"
							onClick={() => void ackWake(sub.id, [], false)}
						>
							<IconRefresh size={14} />
							<span>Heartbeat</span>
						</button>
						<button
							type="button"
							class="dsui-btn dsui-btn--ghost"
							disabled={busy}
							onClick={() => void releaseWake(sub.id)}
						>
							<span>Release</span>
						</button>
					</div>
					<CurlPreview
						operation={ackOp.value}
						copyKey="sub-ack-curl"
						label="Equivalent curl — ack"
					/>
					<CurlPreview
						operation={releaseOp.value}
						copyKey="sub-release-curl"
						label="Equivalent curl — release"
					/>
				</>
			)}
		</section>
	);
}

/* ---------------------------------------------------------------------------
 * Webhook controls (delivery url + jwks link + ack-callback curl)
 * ------------------------------------------------------------------------ */

function WebhookPanel(props: { sub: Subscription }): JSX.Element {
	const { sub } = props;
	const conn = activeConnection.value;
	const url = sub.webhook?.url ?? null;
	const signing = sub.webhook?.signing ?? null;
	const jwksUrl =
		signing?.jwksUrl !== undefined && signing.jwksUrl !== ""
			? signing.jwksUrl
			: conn !== null
				? `${conn.baseUrl}/__ds/jwks.json`
				: null;

	// A representative ack-callback curl: a webhook handler POSTs to …/callback
	// with the callback_token + the fencing fields from the wake notification.
	// Use placeholders the user fills in from the signed wake they received.
	const callbackOp = useComputed(() =>
		conn === null
			? null
			: previewCallbackOperation(conn.baseUrl, sub.id, "<callback_token>", {
					wakeId: "<wake_id>",
					generation: sub.generation ?? 0,
					acks: [{ stream: "<stream>", offset: "<offset>" }],
					done: true,
				}),
	);

	return (
		<section class="dsui-subsection" aria-label="Webhook delivery">
			<header class="dsui-subsection__head">
				<IconWebhook size={14} />
				<span class="dsui-subsection__title">Webhook delivery</span>
				{sub.status === "failed" ? (
					<span class="dsui-pill dsui-pill--warn">retry scheduled</span>
				) : (
					<span class="dsui-pill dsui-pill--ok">active</span>
				)}
			</header>
			<dl class="dsui-meta">
				<MetaRow label="Delivery URL">
					{url === null ? <span class="dsui-meta__muted">—</span> : <code>{url}</code>}
				</MetaRow>
				<MetaRow label="Signature">
					{signing === null ? (
						<span class="dsui-meta__muted">no key minted yet</span>
					) : (
						<span class="dsui-keyline">
							<IconKey size={12} />
							<code>{signing.alg}</code>
							<code title="stable key id">{signing.kid}</code>
						</span>
					)}
				</MetaRow>
				<MetaRow label="JWKS">
					{jwksUrl === null ? (
						<span class="dsui-meta__muted">—</span>
					) : (
						<a class="dsui-extlink" href={jwksUrl} target="_blank" rel="noreferrer noopener">
							<code>{jwksUrl}</code>
							<IconArrowUpRight size={12} />
						</a>
					)}
				</MetaRow>
			</dl>
			<p class="dsui-subsection__lede">
				The server POSTs a signed wake to your URL. Your handler returns{" "}
				<code>{`{"done":true}`}</code> to auto-ack, or processes asynchronously and acks later on
				the callback path:
			</p>
			<CurlPreview
				operation={callbackOp.value}
				copyKey="sub-callback-curl"
				label="Equivalent curl — ack callback"
			/>
		</section>
	);
}

/* ---------------------------------------------------------------------------
 * Add-streams inline form
 * ------------------------------------------------------------------------ */

function AddStreamsForm(props: { sub: Subscription }): JSX.Element {
	const { sub } = props;
	const conn = activeConnection.value;
	const busy = subscriptionInFlight.value;
	const draft = useSignal("");

	const parsed = useComputed(() =>
		draft.value
			.split(/[\n,]/)
			.map((s) => s.trim().replace(/^\/+/, "").replace(/\/+$/, ""))
			.filter((s) => s !== ""),
	);
	const addOp = useComputed(() =>
		conn === null || parsed.value.length === 0
			? null
			: previewAddStreamsOperation(conn.baseUrl, sub.id, parsed.value),
	);

	function submit(e: Event): void {
		e.preventDefault();
		if (parsed.value.length === 0) return;
		void addSubscriptionStreams(sub.id, parsed.value).then((ok) => {
			if (ok) draft.value = "";
		});
	}

	return (
		<form class="dsui-addstreams" onSubmit={submit}>
			<input
				type="text"
				class="dsui-input dsui-input--mono"
				placeholder="events/x, events/y"
				aria-label="Explicit stream paths to link"
				value={draft.value}
				autocomplete="off"
				spellcheck={false}
				onInput={(e) => {
					draft.value = e.currentTarget.value;
				}}
			/>
			<button
				type="submit"
				class="dsui-btn dsui-btn--sm"
				disabled={busy || parsed.value.length === 0}
			>
				<IconPlus size={14} />
				<span>Link {parsed.value.length > 0 ? parsed.value.length : ""}</span>
			</button>
			<CurlPreview operation={addOp.value} copyKey="sub-addstreams-curl" />
		</form>
	);
}

/* ---------------------------------------------------------------------------
 * Workspace
 * ------------------------------------------------------------------------ */

export function SubscriptionWorkspace(): JSX.Element {
	const id = selectedSubscriptionId.value;
	const sub = selectedSubscription.value;
	const loading = subscriptionLoading.value;
	const inFlight = subscriptionInFlight.value;
	const conn = activeConnection.value;
	const claim = activeClaim.value;
	const confirmingDelete = useSignal(false);

	if (id === null) {
		return (
			<div class="dsui-ws dsui-ws--empty">
				<div class="dsui-empty">
					<IconBell size={26} class="dsui-empty__icon" />
					<p class="dsui-empty__title">Select a subscription</p>
					<p class="dsui-empty__hint">
						Pick a subscription from the Navigator, or create one, to manage it here.
					</p>
				</div>
			</div>
		);
	}

	// Loading the first view (no cached detail yet).
	if (sub === null) {
		return (
			<div class="dsui-ws">
				<header class="dsui-ws__head">
					<div class="dsui-ws__title">
						<span class="dsui-ws__name">{id}</span>
					</div>
					<div class="dsui-ws__headend">
						<button
							type="button"
							class="dsui-iconbtn dsui-iconbtn--sm"
							title="Refresh"
							aria-label="Refresh subscription"
							disabled={loading}
							onClick={() => void getSubscription(id)}
						>
							<IconRefresh size={14} class={loading ? "dsui-spin" : undefined} />
						</button>
					</div>
				</header>
				<div class="dsui-ws__scroll">
					<div class="dsui-empty">
						{loading ? (
							<>
								<IconLoader size={24} class="dsui-empty__icon dsui-spin" />
								<p class="dsui-empty__title">Loading subscription…</p>
								<p class="dsui-empty__hint">Fetching the current view of {id}.</p>
							</>
						) : (
							<>
								<IconBell size={24} class="dsui-empty__icon" />
								<p class="dsui-empty__title">No view yet</p>
								<p class="dsui-empty__hint">
									Could not load {id}. It may not exist on this server, or the server may lack the
									Redis backend subscriptions require. Refresh to try again, or stop tracking it.
								</p>
								<div class="dsui-empty__actions">
									<button
										type="button"
										class="dsui-btn dsui-btn--xs"
										onClick={() => void getSubscription(id)}
									>
										<IconRefresh size={13} />
										<span>Try again</span>
									</button>
									<button
										type="button"
										class="dsui-btn dsui-btn--xs"
										onClick={() => untrackSubscriptionId(id)}
									>
										<span>Stop tracking</span>
									</button>
								</div>
							</>
						)}
					</div>
				</div>
				<ProtocolPanel exchange={lastExchange.value} tail={null} />
			</div>
		);
	}

	// Build the link rows, joining the claim snapshots when a claim is held.
	const snapshotByPath = new Map<string, WakeStreamSnapshot>();
	if (claim !== null) {
		for (const s of claim.streams) snapshotByPath.set(s.path, s);
	}
	const linkRows: LinkRow[] = sub.streams.map((link) => ({
		link,
		snapshot: snapshotByPath.get(link.path) ?? null,
	}));

	const leaseSeconds =
		sub.leaseTtlMs > 0 ? `${sub.leaseTtlMs} ms (${sub.leaseTtlMs / 1000}s)` : "—";

	return (
		<div class="dsui-ws">
			<header class="dsui-ws__head">
				<div class="dsui-ws__title">
					<IconBell size={15} />
					<span class="dsui-ws__name">{sub.id}</span>
					<TypeChip type={sub.type} />
					<PhasePill phase={sub.phase} />
					{sub.status === "failed" ? <span class="dsui-pill dsui-pill--warn">failed</span> : null}
				</div>
				<div class="dsui-ws__headend">
					<button
						type="button"
						class="dsui-iconbtn dsui-iconbtn--sm"
						title="Refresh subscription"
						aria-label="Refresh subscription"
						disabled={loading}
						onClick={() => void getSubscription(sub.id)}
					>
						<IconRefresh size={14} class={loading ? "dsui-spin" : undefined} />
					</button>
					{confirmingDelete.value ? (
						<div class="dsui-confirm">
							<span class="dsui-confirm__text">Delete?</span>
							<button
								type="button"
								class="dsui-btn dsui-btn--xs dsui-btn--danger"
								disabled={inFlight}
								onClick={() => {
									void deleteSubscription(sub.id).then(() => {
										confirmingDelete.value = false;
									});
								}}
							>
								{inFlight ? <IconLoader size={13} class="dsui-spin" /> : <IconTrash size={13} />}
								<span>Delete</span>
							</button>
							<button
								type="button"
								class="dsui-btn dsui-btn--xs dsui-btn--ghost"
								onClick={() => {
									confirmingDelete.value = false;
								}}
							>
								Cancel
							</button>
						</div>
					) : (
						<button
							type="button"
							class="dsui-iconbtn dsui-iconbtn--sm dsui-iconbtn--danger"
							title="Delete subscription"
							aria-label="Delete subscription"
							onClick={() => {
								confirmingDelete.value = true;
							}}
						>
							<IconTrash size={14} />
						</button>
					)}
				</div>
			</header>

			<div class="dsui-ws__scroll">
				<section class="dsui-subsection" aria-label="Subscription details">
					<dl class="dsui-meta">
						<MetaRow label="Type">
							<TypeChip type={sub.type} />
						</MetaRow>
						<MetaRow label="Phase">
							{sub.phase !== undefined ? (
								<PhasePill phase={sub.phase} />
							) : (
								<span class="dsui-meta__muted">not reported by the server</span>
							)}
						</MetaRow>
						<MetaRow label="Generation">
							{sub.generation !== undefined ? (
								<code>{sub.generation}</code>
							) : (
								<span class="dsui-meta__muted">—</span>
							)}
						</MetaRow>
						<MetaRow label="Lease TTL">
							<code>{leaseSeconds}</code>
						</MetaRow>
						<MetaRow label="Pattern">
							{sub.pattern !== null ? (
								<code>{sub.pattern}</code>
							) : (
								<span class="dsui-meta__muted">explicit streams only</span>
							)}
						</MetaRow>
						{sub.type === "pull-wake" ? (
							<MetaRow label="Wake stream">
								{sub.wakeStream !== null ? (
									<code>{sub.wakeStream}</code>
								) : (
									<span class="dsui-meta__muted">—</span>
								)}
							</MetaRow>
						) : null}
						<MetaRow label="Created">
							{sub.createdAt !== null ? (
								<span title={sub.createdAt}>{sub.createdAt}</span>
							) : (
								<span class="dsui-meta__muted">—</span>
							)}
						</MetaRow>
						{sub.description !== null ? (
							<MetaRow label="Description">{sub.description}</MetaRow>
						) : null}
					</dl>
				</section>

				{sub.type === "webhook" ? <WebhookPanel sub={sub} /> : <PullWakeControls sub={sub} />}

				<section class="dsui-subsection" aria-label="Linked streams">
					<header class="dsui-subsection__head">
						<IconLink size={14} />
						<span class="dsui-subsection__title">Linked streams</span>
						<span class="dsui-nav__count">{sub.streams.length}</span>
					</header>
					<LinkTable
						rows={linkRows}
						canRemove={conn !== null}
						onRemove={(path) => void removeSubscriptionStream(sub.id, path)}
					/>
					<AddStreamsForm sub={sub} />
				</section>
			</div>

			<ProtocolPanel exchange={lastExchange.value} tail={null} />
		</div>
	);
}
