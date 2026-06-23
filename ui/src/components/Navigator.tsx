/**
 * Navigator — the left rail. For the active connection it shows:
 *
 *   ┌──────────────────────────────┐
 *   │ connected-server header       │  name · url · live reachability dot
 *   ├──────────────────────────────┤
 *   │ Streams  (count)      ↻       │  section head + refresh
 *   │ [ filter… ]                   │  instant client-side filter
 *   │ [ add stream path…    ] +     │  manual add for streams the registry
 *   │ ┌──────────────────────────┐  │  did not surface
 *   │ │ tree (role=tree)         │  │
 *   │ └──────────────────────────┘  │
 *   ├──────────────────────────────┤
 *   │ Playground                    │  one-click API bootstrap presets
 *   ├──────────────────────────────┤
 *   │ Subscriptions  (count)   +    │  the client-side known-ids list, a
 *   │ [ track existing id…  ] +     │  "track existing id" box, create button
 *   ├──────────────────────────────┤
 *   │ Metrics                  ›    │  a rail entry that switches the center
 *   └──────────────────────────────┘  pane to the metrics workspace
 *
 * Discovery comes from the store (`refreshStreams` reads __registry__ via
 * dsClient.listStreams; `addManualStream` covers streams the registry did not
 * surface). Selecting a stream calls `selectStream`, which updates
 * store.selectedStreamPath and kicks off the first read.
 *
 * The filter is component-local (a useSignal) so typing is instant and never
 * touches global state. Tree semantics are real: a single role="tree" with
 * role="treeitem" children, roving focus via ArrowUp/ArrowDown/Home/End, and
 * aria-selected on the active item.
 *
 * The Subscriptions section lists store.subscriptionIds (the per-connection
 * known-ids set; there is no list-all endpoint) and selects one via
 * selectSubscription, which flips centerView to "subscription". The Metrics
 * entry calls setCenterView("metrics"). Both are the same center-pane routing
 * seam the rest of the app uses.
 *
 * Extensibility seam: each rail block is a <section class="dsui-nav__section">.
 * Add a new side-panel section by adding a section here, reading store signals
 * and calling store actions (keep the fetch in dsClient and mutation in the
 * store).
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useRef } from "preact/hooks";
import { compactUrl, describeProbe, dotStatusOf } from "../lib/format";
import type { StreamInfo, Subscription } from "../lib/types";
import {
	activeConnection,
	addManualStream,
	centerView,
	errorMessage,
	highlightPlayground,
	openCreateDialog,
	openCreateSubscriptionDialog,
	probeStatuses,
	refreshStreams,
	selectStream,
	selectSubscription,
	selectedStreamPath,
	selectedSubscriptionId,
	setCenterView,
	streams,
	streamsLoading,
	subscriptionDetails,
	subscriptionIds,
	trackSubscriptionId,
} from "../state/store";
import { Playground } from "./Playground";
import { StatusDot } from "./StatusDot";
import {
	IconBell,
	IconChart,
	IconChevronRight,
	IconFilePlus,
	IconPlus,
	IconRefresh,
	IconSearch,
	IconServer,
	IconSparkles,
	IconStream,
	IconWebhook,
	IconZap,
} from "./icons";

/* ---------------------------------------------------------------------------
 * Connected-server header
 * ------------------------------------------------------------------------ */

/** The active server, with a live reachability dot mirrored from probeStatuses. */
function ConnectedServer(): JSX.Element | null {
	const conn = activeConnection.value;
	const status = useComputed(() =>
		dotStatusOf(conn === null ? undefined : probeStatuses.value[conn.id]),
	);
	const title = useComputed(() => {
		const s = conn === null ? undefined : probeStatuses.value[conn.id];
		return describeProbe(s === undefined || s.state !== "done" ? null : s.probe);
	});
	if (conn === null) return null;
	return (
		<header class="dsui-nav__server">
			<IconServer size={15} class="dsui-nav__servericon" />
			<span class="dsui-nav__servertext">
				<span class="dsui-nav__servername">{conn.name}</span>
				<span class="dsui-nav__serverurl">{compactUrl(conn.baseUrl)}</span>
			</span>
			<span class="dsui-nav__serverdot" title={title.value}>
				<StatusDot status={status.value} />
			</span>
		</header>
	);
}

/* ---------------------------------------------------------------------------
 * Streams tree
 * ------------------------------------------------------------------------ */

function Skeleton(): JSX.Element {
	return (
		<ul class="dsui-nav__list" aria-hidden="true">
			{[0, 1, 2, 3, 4].map((i) => (
				<li key={i} class="dsui-skel-row">
					<span class="dsui-skel" style={{ inlineSize: `${50 + ((i * 13) % 40)}%` }} />
				</li>
			))}
		</ul>
	);
}

/** Empty / no-match / error states for the stream list region. */
function StreamsPlaceholder(props: {
	readonly error: string | null;
	readonly hasStreams: boolean;
	readonly filtered: boolean;
}): JSX.Element {
	const { error, hasStreams, filtered } = props;
	if (error !== null && !hasStreams) {
		return (
			<div class="dsui-empty dsui-empty--inline" role="alert">
				<IconStream size={26} class="dsui-empty__icon" />
				<p class="dsui-empty__title">Could not list streams</p>
				<p class="dsui-empty__hint">{error}</p>
				<button type="button" class="dsui-btn dsui-btn--xs" onClick={() => void refreshStreams()}>
					<IconRefresh size={13} />
					<span>Try again</span>
				</button>
			</div>
		);
	}
	if (filtered) {
		return (
			<div class="dsui-empty dsui-empty--inline">
				<IconSearch size={24} class="dsui-empty__icon" />
				<p class="dsui-empty__title">No matches</p>
				<p class="dsui-empty__hint">No stream path matches your filter.</p>
			</div>
		);
	}
	return (
		<div class="dsui-empty dsui-empty--inline dsui-empty--firstrun">
			<IconStream size={26} class="dsui-empty__icon" />
			<p class="dsui-empty__title">No streams yet</p>
			<p class="dsui-empty__hint">
				The <code>__registry__</code> stream is empty or absent. New here? The Playground below
				bootstraps the whole API in one click each.
			</p>
			<div class="dsui-empty__actions">
				<button
					type="button"
					class="dsui-btn dsui-btn--xs dsui-btn--primary"
					onClick={() => highlightPlayground()}
				>
					<IconSparkles size={13} />
					<span>Start with the Playground</span>
				</button>
				<button type="button" class="dsui-btn dsui-btn--xs" onClick={() => openCreateDialog()}>
					<IconFilePlus size={13} />
					<span>New stream</span>
				</button>
			</div>
		</div>
	);
}

/** A single tree row. The button carries focus; the <li> carries tree roles. */
function StreamItem(props: {
	readonly stream: StreamInfo;
	readonly active: boolean;
	readonly tabbable: boolean;
	readonly onKeyDown: (e: KeyboardEvent) => void;
}): JSX.Element {
	const { stream, active, tabbable, onKeyDown } = props;
	return (
		<li role="treeitem" aria-selected={active}>
			<button
				type="button"
				class={`dsui-treeitem${active ? " is-active" : ""}`}
				// Roving tabindex: exactly one item is tab-reachable at all times.
				// The selected item owns the tab stop; when nothing is selected the
				// first visible item does, so a keyboard user can always Tab in.
				tabIndex={tabbable ? 0 : -1}
				data-streamitem="true"
				title={stream.path}
				onClick={() => selectStream(stream.path)}
				onKeyDown={onKeyDown}
			>
				<IconStream size={14} class="dsui-treeitem__icon" />
				<span class="dsui-treeitem__label">{stream.path}</span>
				{stream.manual ? (
					<span class="dsui-treeitem__tag" title="Added manually — not from the registry">
						manual
					</span>
				) : null}
				<span class={`dsui-kind dsui-kind--${stream.kind}`}>{stream.kind}</span>
			</button>
		</li>
	);
}

/* ---------------------------------------------------------------------------
 * Subscriptions section (the reserved /__ds/* control plane)
 *
 * There is no list-all endpoint, so the rail lists the client-side known-ids set
 * (store.subscriptionIds, persisted per connection). Each row selects the
 * subscription (which flips the center pane to its detail view); a small form
 * tracks an id created elsewhere, and the header "+" opens the create dialog.
 * ------------------------------------------------------------------------ */

/** A single subscription row: id + a type chip when the view is cached. */
function SubscriptionItem(props: {
	readonly id: string;
	readonly detail: Subscription | undefined;
	readonly active: boolean;
}): JSX.Element {
	const { id, detail, active } = props;
	const type = detail?.type;
	return (
		<li role="treeitem" aria-selected={active}>
			<button
				type="button"
				class={`dsui-treeitem${active ? " is-active" : ""}`}
				title={id}
				onClick={() => selectSubscription(id)}
			>
				<IconBell size={14} class="dsui-treeitem__icon" />
				<span class="dsui-treeitem__label">{id}</span>
				{type === "webhook" ? (
					<span class="dsui-subdot" title="webhook">
						<IconWebhook size={12} />
					</span>
				) : type === "pull-wake" ? (
					<span class="dsui-subdot" title="pull-wake">
						<IconZap size={12} />
					</span>
				) : null}
			</button>
		</li>
	);
}

function SubscriptionsSection(): JSX.Element {
	const conn = activeConnection.value;
	const ids = subscriptionIds.value;
	const details = subscriptionDetails.value;
	const selectedSub = selectedSubscriptionId.value;
	const onSubView = centerView.value === "subscription";
	const draft = useSignal("");

	function commitTrack(): void {
		const id = draft.value.trim();
		if (id === "") return;
		draft.value = "";
		trackSubscriptionId(id);
	}

	return (
		<section class="dsui-nav__section dsui-nav__section--subs" aria-label="Subscriptions">
			<header class="dsui-nav__head">
				<span class="dsui-nav__title">
					Subscriptions
					{ids.length > 0 ? <span class="dsui-nav__count">{ids.length}</span> : null}
				</span>
				<div class="dsui-nav__headactions">
					<button
						type="button"
						class="dsui-iconbtn dsui-iconbtn--sm"
						title="New subscription"
						aria-label="New subscription"
						disabled={conn === null}
						onClick={() => openCreateSubscriptionDialog()}
					>
						<IconPlus size={14} />
					</button>
				</div>
			</header>

			<div class="dsui-nav__body">
				{ids.length === 0 ? (
					<div class="dsui-empty dsui-empty--inline">
						<IconBell size={24} class="dsui-empty__icon" />
						<p class="dsui-empty__title">No subscriptions tracked</p>
						<p class="dsui-empty__hint">
							The control plane has no list-all endpoint, so dsui remembers the ids you create or
							track here (per connection). Create one, or track an existing id below.
						</p>
					</div>
				) : (
					<ul class="dsui-nav__list" role="tree" aria-label="Subscriptions">
						{ids.map((id) => (
							<SubscriptionItem
								key={id}
								id={id}
								detail={details[id]}
								active={onSubView && id === selectedSub}
							/>
						))}
					</ul>
				)}
			</div>

			<form
				class="dsui-nav__add"
				onSubmit={(e) => {
					e.preventDefault();
					commitTrack();
				}}
			>
				<input
					type="text"
					class="dsui-nav__addinput"
					placeholder="Track existing id…"
					aria-label="Track an existing subscription id"
					value={draft.value}
					disabled={conn === null}
					autocomplete="off"
					spellcheck={false}
					onInput={(e) => {
						draft.value = e.currentTarget.value;
					}}
				/>
				<button
					type="submit"
					class="dsui-iconbtn dsui-iconbtn--sm"
					title="Track subscription id"
					aria-label="Track subscription id"
					disabled={conn === null || draft.value.trim() === ""}
				>
					<IconPlus size={14} />
				</button>
			</form>
		</section>
	);
}

/* ---------------------------------------------------------------------------
 * Metrics entry (switches the center pane to the metrics workspace)
 * ------------------------------------------------------------------------ */

function MetricsEntry(): JSX.Element {
	const active = centerView.value === "metrics";
	return (
		<section class="dsui-nav__section dsui-nav__section--metrics" aria-label="Metrics">
			<button
				type="button"
				class={`dsui-nav__entry${active ? " is-active" : ""}`}
				aria-current={active ? "page" : undefined}
				onClick={() => setCenterView("metrics")}
			>
				<IconChart size={15} class="dsui-nav__entryicon" />
				<span class="dsui-nav__entrylabel">Metrics</span>
				<IconChevronRight size={14} class="dsui-nav__entrycaret" />
			</button>
		</section>
	);
}

/* ---------------------------------------------------------------------------
 * Navigator
 * ------------------------------------------------------------------------ */

export function Navigator(): JSX.Element {
	const conn = activeConnection.value;
	const loading = streamsLoading.value;
	const all = streams.value;
	const selected = selectedStreamPath.value;
	const error = errorMessage.value;

	// Component-local, instant filter — never touches the store.
	const query = useSignal("");
	const draft = useSignal("");
	const listRef = useRef<HTMLUListElement>(null);

	const q = query.value.trim().toLowerCase();
	const visible = q === "" ? all : all.filter((s) => s.path.toLowerCase().includes(q));
	const isFiltering = q !== "";

	/** Move roving focus to the n-th visible tree item (clamped). */
	function focusItem(index: number): void {
		const items = listRef.current?.querySelectorAll<HTMLButtonElement>("[data-streamitem]");
		if (items === undefined || items.length === 0) return;
		const clamped = Math.max(0, Math.min(index, items.length - 1));
		items.item(clamped)?.focus();
	}

	function onItemKeyDown(currentPath: string): (e: KeyboardEvent) => void {
		return (e) => {
			const idx = visible.findIndex((s) => s.path === currentPath);
			if (idx < 0) return;
			switch (e.key) {
				case "ArrowDown":
					e.preventDefault();
					focusItem(idx + 1);
					break;
				case "ArrowUp":
					e.preventDefault();
					focusItem(idx - 1);
					break;
				case "Home":
					e.preventDefault();
					focusItem(0);
					break;
				case "End":
					e.preventDefault();
					focusItem(visible.length - 1);
					break;
				default:
					break;
			}
		};
	}

	function commitAdd(): void {
		const path = draft.value;
		draft.value = "";
		addManualStream(path);
	}

	return (
		<nav class="dsui-nav" aria-label="Streams navigator">
			<ConnectedServer />

			<section class="dsui-nav__section dsui-nav__section--streams" aria-label="Streams">
				<header class="dsui-nav__head">
					<span class="dsui-nav__title">
						Streams
						{all.length > 0 ? <span class="dsui-nav__count">{all.length}</span> : null}
					</span>
					<div class="dsui-nav__headactions">
						<button
							type="button"
							class="dsui-iconbtn dsui-iconbtn--sm"
							title="New stream"
							aria-label="New stream"
							disabled={conn === null}
							onClick={() => openCreateDialog()}
						>
							<IconFilePlus size={14} />
						</button>
						<button
							type="button"
							class="dsui-iconbtn dsui-iconbtn--sm"
							title="Refresh streams"
							aria-label="Refresh streams"
							disabled={conn === null || loading}
							onClick={() => void refreshStreams()}
						>
							<IconRefresh size={14} class={loading ? "dsui-spin" : undefined} />
						</button>
					</div>
				</header>

				<div class="dsui-nav__filter">
					<IconSearch size={14} class="dsui-nav__filtericon" />
					<input
						type="search"
						class="dsui-nav__filterinput"
						placeholder="Filter streams…"
						aria-label="Filter streams"
						value={query.value}
						disabled={conn === null}
						onInput={(e) => {
							query.value = e.currentTarget.value;
						}}
					/>
				</div>

				<div class="dsui-nav__body">
					{loading && all.length === 0 ? (
						<Skeleton />
					) : visible.length === 0 ? (
						<StreamsPlaceholder error={error} hasStreams={all.length > 0} filtered={isFiltering} />
					) : (
						<ul class="dsui-nav__list" role="tree" aria-label="Streams" ref={listRef}>
							{visible.map((s, index) => (
								<StreamItem
									key={s.path}
									stream={s}
									active={s.path === selected}
									// The WAI-ARIA tree pattern requires exactly one tab stop.
									// Normally the selected item owns it; when nothing is
									// selected (first load, after a connection switch) the
									// first visible item owns it so the tree is reachable.
									tabbable={s.path === selected || (selected === null && index === 0)}
									onKeyDown={onItemKeyDown(s.path)}
								/>
							))}
						</ul>
					)}
				</div>

				<form
					class="dsui-nav__add"
					onSubmit={(e) => {
						e.preventDefault();
						commitAdd();
					}}
				>
					<input
						type="text"
						class="dsui-nav__addinput"
						placeholder="Add stream path…"
						aria-label="Add a stream path to view"
						value={draft.value}
						disabled={conn === null}
						autocomplete="off"
						spellcheck={false}
						onInput={(e) => {
							draft.value = e.currentTarget.value;
						}}
					/>
					<button
						type="submit"
						class="dsui-iconbtn dsui-iconbtn--sm"
						title="Add stream path"
						aria-label="Add stream path"
						disabled={conn === null || draft.value.trim() === ""}
					>
						<IconPlus size={14} />
					</button>
				</form>
			</section>

			<Playground />

			<SubscriptionsSection />

			<MetricsEntry />
		</nav>
	);
}
