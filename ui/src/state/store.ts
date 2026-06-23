/**
 * Application state, built on @preact/signals. State is global and lives here;
 * components read signals and call the typed actions below. This is the single
 * seam through which the UI mutates anything — keep mutations out of components.
 *
 * Persistence: connections and the active connection id and the theme are
 * mirrored to localStorage so a reload restores the session. Everything else
 * (streams, selection, last exchange) is ephemeral and re-derived on connect.
 */

import { computed, effect, signal } from "@preact/signals";
import { type CaptureStopper, openCaptureStream } from "../lib/capture";
import { loadConfig } from "../lib/config";
import { type DsClient, createClient } from "../lib/dsClient";
import { isRecord, kindFromContentType } from "../lib/guards";
import { DEFAULT_ROW_CAP, type StartMode, clampRowCap, resolveOffset } from "../lib/messages";
import { isLiveMode, previewTailOperation } from "../lib/tail";
import { appendCapped } from "../lib/tailBuffer";
import type {
	CaptureDelivery,
	Connection,
	ConnectionProbe,
	CreateSubscriptionOptions,
	GridRow,
	HttpExchange,
	MetricsSnapshot,
	OffsetAck,
	Operation,
	ProbeStatus,
	ProducerIdentity,
	ReadResult,
	StreamContentType,
	StreamInfo,
	Subscription,
	TailBatch,
	TailMode,
	TailStatus,
	TailStopper,
	Theme,
	Toast,
	ToastAction,
	ToastKind,
	WakeClaim,
	WakeEvent,
} from "../lib/types";
import {
	WAKE_DEMO_CONTENT_TYPE,
	WAKE_DEMO_STREAM,
	WAKE_DEMO_SUB_ID,
	captureUrl,
	parseWakeEvent,
	wakeDemoBody,
} from "../lib/wakes";

// Re-exported for back-compat: ProbeStatus now lives in lib/types (a shared
// contract), but existing imports of it from the store keep working.
export type { ProbeStatus } from "../lib/types";

const LS_CONNECTIONS = "dsui.connections";
const LS_ACTIVE = "dsui.activeConnection";
const LS_THEME = "dsui.theme";
const LS_INSPECTOR_COLLAPSED = "dsui.inspector-collapsed";
const LS_PLAYGROUND_OPEN = "dsui.playground-open";
/** Known subscription ids are tracked per connection (no list-all endpoint). */
const LS_SUBS_PREFIX = "dsui.subs.";
/** The metrics URL is remembered per connection (a separate --metrics-listen). */
const LS_METRICS_PREFIX = "dsui.metricsUrl.";

/* ----------------------------------------------------------------------------
 * Signals (the reactive state atoms)
 * ------------------------------------------------------------------------- */

/** All saved connections. */
export const connections = signal<readonly Connection[]>(loadConnections());

/** The id of the active connection, or null. */
export const activeConnectionId = signal<string | null>(loadActiveId());

/** Streams in the active connection (from the registry + manual adds). */
export const streams = signal<readonly StreamInfo[]>([]);

/** Path of the currently selected stream, or null. */
export const selectedStreamPath = signal<string | null>(null);

/** The most recent read result for the selected stream, or null. */
export const lastRead = signal<ReadResult | null>(null);

/** The last HTTP exchange of any kind, for the protocol disclosure. */
export const lastExchange = signal<HttpExchange | null>(null);

/** The selected grid row (drives the inspector), or null. */
export const selectedRow = signal<GridRow | null>(null);

/**
 * Which view occupies the center workspace region. The shell renders one of:
 *  - "messages":     the stream messages workspace (the default / on stream select)
 *  - "subscription": the subscription detail workspace (on subscription select)
 *  - "metrics":      the Prometheus metrics workspace (from the Metrics nav entry)
 *  - "wakes":        the Wake Monitor split-screen (from a "Watch wakes" action)
 *
 * This is the single routing seam for the center pane; selecting a stream, a
 * subscription, or the Metrics entry flips it through the matching action.
 */
export type CenterView = "messages" | "subscription" | "metrics" | "wakes";

/** The active center-workspace view. Defaults to the messages workspace. */
export const centerView = signal<CenterView>("messages");

/** Switch the center workspace view directly (used by the Metrics nav entry). */
export function setCenterView(view: CenterView): void {
	centerView.value = view;
}

/* ----------------------------------------------------------------------------
 * Messages workspace toolbar state
 *
 * The toolbar picks a starting position (Earliest / Latest / At offset…) and a
 * row cap, then a Read/Refresh resolves those into a concrete protocol offset
 * via lib/messages.resolveOffset. State lives here (not in the component) so a
 * stream switch can read with the current toolbar settings and the protocol
 * disclosure can describe exactly what was sent.
 * ------------------------------------------------------------------------- */

/** Starting-position mode for the next read. */
export const startMode = signal<StartMode>("earliest");

/** Explicit offset cursor used when {@link startMode} is "at". */
export const customOffset = signal<string>("");

/** Maximum number of grid rows kept from a read batch. */
export const rowCap = signal<number>(DEFAULT_ROW_CAP);

/**
 * True when the last read's batch was larger than the row cap and was
 * truncated for display. The full byte body is still available in the Raw view.
 */
export const rowsTruncated = signal<boolean>(false);

/** Current theme preference. */
export const theme = signal<Theme>(loadTheme());

/**
 * Whether the right-hand Inspector panel is collapsed. Default false (expanded).
 * Persisted to localStorage, mirroring the theme preference, so a user's
 * layout choice survives a reload.
 */
export const inspectorCollapsed = signal<boolean>(loadBool(LS_INSPECTOR_COLLAPSED, false));

/**
 * Whether the left sidebar's foldable Playground section is open. Default true
 * (open). Persisted to localStorage like {@link inspectorCollapsed}.
 */
export const playgroundOpen = signal<boolean>(loadBool(LS_PLAYGROUND_OPEN, true));

/** Coarse async status for the stream list and reads. */
export const streamsLoading = signal<boolean>(false);
export const readLoading = signal<boolean>(false);

/** Last connection probe result for the active connection. */
export const connectionProbe = signal<ConnectionProbe | null>(null);

/**
 * Probe status per connection id, for the status dots on the start screen and
 * the header switcher. A new object identity is published on every change so
 * signal subscribers re-render.
 */
export const probeStatuses = signal<Readonly<Record<string, ProbeStatus>>>({});

/** Whether the header connection switcher popover is open. */
export const switcherOpen = signal<boolean>(false);

/** A surfaced, dismissible error message, or null. */
export const errorMessage = signal<string | null>(null);

/* ----------------------------------------------------------------------------
 * Write / operation state
 *
 * Every write the UI performs (create, append, close, delete, fork) goes
 * through a single in-flight flag plus the last {@link Operation} descriptor,
 * so a control can disable itself while running and the protocol panel + the
 * "Copy as curl" affordance can show exactly what was (or will be) sent.
 * ------------------------------------------------------------------------- */

/** True while any write/operation is in flight (drives optimistic disabling). */
export const operationInFlight = signal<boolean>(false);

/**
 * The descriptor of the most recently issued (or previewed) operation, for the
 * curl helper and the under-the-hood disclosure. Null until one has been built.
 */
export const lastOperation = signal<Operation | null>(null);

/**
 * The active producer identity used for idempotent appends, or null when the
 * user is publishing without one. The seq is advanced after a successful
 * append via {@link bumpProducerSeq} so consecutive publishes dedupe correctly.
 */
export const producerIdentity = signal<ProducerIdentity | null>(null);

/* ----------------------------------------------------------------------------
 * Write-operation dialog + metadata state
 *
 * The create / fork forms are modal dialogs; their open-state lives here so any
 * affordance (the navigator's "New stream", a workspace "Fork" button) can open
 * them and the shell can render exactly one at a time. A fork dialog carries the
 * source path + the offset to prefill. The latest HEAD/metadata exchange is
 * surfaced too, so the workspace can show a "refresh metadata" affordance whose
 * result also flows into the protocol disclosure.
 * ------------------------------------------------------------------------- */

/** Which write dialog is open, or null when none is. */
export const activeDialog = signal<"create" | "fork" | "subscription" | null>(null);

/**
 * A one-shot highlight pulse on the Playground, set by the first-run empty-state
 * hint so the navigator's Playground section can briefly draw the eye and scroll
 * into view. A monotonic counter (rather than a boolean) so re-triggering it
 * always re-fires the effect even if the section is already highlighted.
 */
export const playgroundHighlight = signal<number>(0);

/** Pulse the Playground highlight, pointing a first-run user at the presets. */
export function highlightPlayground(): void {
	playgroundHighlight.value = playgroundHighlight.value + 1;
}

/** When the fork dialog is open, the source it forks from + a default offset. */
export const forkSeed = signal<{ readonly fromPath: string; readonly offset: string } | null>(null);

/** The most recent HEAD/metadata exchange for the selected stream, or null. */
export const streamMeta = signal<HttpExchange | null>(null);

/** True while a HEAD/metadata refresh is in flight. */
export const metaLoading = signal<boolean>(false);

/* ----------------------------------------------------------------------------
 * Live-tail state
 *
 * A live tail (long-poll or SSE) streams rows into a bounded buffer. The buffer
 * is capped so a fast stream cannot grow memory without bound; `tailDropped`
 * records how many rows aged out. `tailPaused` stops appending to the buffer
 * without tearing down the connection. The stopper handle lives here (not in a
 * component) so a stream switch / unmount can always clean it up.
 * ------------------------------------------------------------------------- */

/**
 * The chosen read mode — the toolbar's Catch-up | Long-poll | SSE selector.
 * "catchup" is the existing paged-read path; the two live modes open a tail.
 * Defaults to "catchup" so the workspace opens in the familiar paged view.
 */
export const tailMode = signal<TailMode>("catchup");

/** The live-tail connection lifecycle status. */
export const tailStatus = signal<TailStatus>({ state: "idle" });

/**
 * The {@link Operation} descriptor of the live connection currently being
 * followed (the GET …?offset=X&live=…), or null when not tailing. SSE captures
 * no per-request {@link HttpExchange}, so this is what lets the protocol
 * disclosure show the live request and the copy-as-curl work for both modes.
 */
export const tailOperation = signal<Operation | null>(null);

/** The offset a live tail started from, for the protocol disclosure copy. */
export const tailStartOffset = signal<string | null>(null);

/** The rolling buffer of rows received by the active tail (most recent last). */
export const tailRows = signal<readonly GridRow[]>([]);

/** True when the tail is connected but the user has paused buffering. */
export const tailPaused = signal<boolean>(false);

/** How many rows have aged out of the capped tail buffer. */
export const tailDropped = signal<number>(0);

/** Max rows kept in the tail buffer before the oldest age out. */
export const TAIL_BUFFER_CAP = 1000;

/**
 * How many rows age out at once when the buffer overflows (~10% of the cap).
 * Evicting in a block, rather than trimming to exactly the cap on every append,
 * lets the steady-state appends fall through the cheap no-eviction branch in
 * {@link appendCapped} — eviction work is paid once per block, not per message.
 */
const TAIL_EVICT_BLOCK = TAIL_BUFFER_CAP / 10;

/** The stopper for the active tail, or null when not tailing. Not reactive UI. */
let tailStopper: TailStopper | null = null;

/* ----------------------------------------------------------------------------
 * Tail coalescer
 *
 * A fast producer delivers one onMessage per event; writing the `tailRows`
 * signal on each one means a full render per message — more updates than a
 * human can read. Instead, received rows accumulate in a module-level scratch
 * array and flush to the signal once per animation frame, so a burst within one
 * frame becomes a single signal write (one render). This is store-internal and
 * invisible to components, which still just read `tailRows`.
 * ------------------------------------------------------------------------- */

/** Rows received since the last flush, awaiting the next animation frame. */
let tailScratch: GridRow[] = [];

/** The most recent batch exchange seen since the last flush (long-poll only). */
let tailPendingExchange: HttpExchange | null = null;

/** The pending requestAnimationFrame handle, or null when none is scheduled. */
let tailFrame: number | null = null;

/** Schedule a flush of {@link tailScratch} on the next frame (idempotent). */
function scheduleTailFlush(): void {
	if (tailFrame !== null) return;
	if (typeof requestAnimationFrame !== "function") {
		// No rAF (non-browser / test without a stub): flush synchronously so the
		// buffer never silently strands rows.
		flushTailRows();
		return;
	}
	tailFrame = requestAnimationFrame(flushTailRows);
}

/** Cancel any scheduled flush and drop the un-flushed scratch rows. */
function cancelTailFlush(): void {
	if (tailFrame !== null) {
		if (typeof cancelAnimationFrame === "function") cancelAnimationFrame(tailFrame);
		tailFrame = null;
	}
	tailScratch = [];
	tailPendingExchange = null;
}

/**
 * Flush the coalesced scratch rows into the bounded `tailRows` buffer in a
 * single signal write. This is the scheduled animation-frame callback (and the
 * no-rAF synchronous fallback) — nothing else calls it.
 */
function flushTailRows(): void {
	tailFrame = null;
	const incoming = tailScratch;
	const exchange = tailPendingExchange;
	tailScratch = [];
	tailPendingExchange = null;
	if (incoming.length > 0) {
		const { rows, dropped } = appendCapped(
			tailRows.value,
			incoming,
			TAIL_BUFFER_CAP,
			TAIL_EVICT_BLOCK,
		);
		tailRows.value = rows;
		if (dropped > 0) tailDropped.value += dropped;
	}
	// Mirror the latest long-poll exchange (SSE carries none) so the protocol
	// disclosure stays current without a write per message.
	if (exchange !== null) lastExchange.value = exchange;
}

/* ----------------------------------------------------------------------------
 * Subscription state (the reserved /__ds/* control plane)
 *
 * There is NO list-all endpoint, so the UI keeps the set of known subscription
 * ids client-side, persisted per connection to localStorage exactly like the
 * connection list. `subscriptionDetails` caches the last fetched view per id.
 * The selected id drives the detail panel; in-flight flags disable controls.
 * ------------------------------------------------------------------------- */

/** Known subscription ids for the active connection (persisted per connection). */
export const subscriptionIds = signal<readonly string[]>([]);

/** Cached subscription views by id (the last GET / create response). */
export const subscriptionDetails = signal<Readonly<Record<string, Subscription>>>({});

/** The selected subscription id (drives the detail panel), or null. */
export const selectedSubscriptionId = signal<string | null>(null);

/** True while any subscription create/get/delete/streams op is in flight. */
export const subscriptionInFlight = signal<boolean>(false);

/** True while the selected subscription's detail is being (re)fetched. */
export const subscriptionLoading = signal<boolean>(false);

/**
 * The active pull-wake claim for the selected subscription, or null when no
 * lease is held in this session. A claim carries the Bearer token + fencing
 * (generation, wake_id) the ack/release controls need; it is ephemeral (a
 * worker identity lives only as long as the lease) so it is never persisted and
 * is cleared on a connection switch or a successful release / done-ack.
 */
export const activeClaim = signal<WakeClaim | null>(null);

/** True while a claim / ack / release request is in flight. */
export const claimInFlight = signal<boolean>(false);

/* ----------------------------------------------------------------------------
 * Metrics state (Prometheus on the separate --metrics-listen address)
 * ------------------------------------------------------------------------- */

/** The metrics endpoint URL for the active connection, or "" when unset. */
export const metricsUrl = signal<string>("");

/** The last parsed metrics snapshot, or null when never fetched / failed. */
export const metrics = signal<MetricsSnapshot | null>(null);

/** True while a metrics scrape is in flight. */
export const metricsLoading = signal<boolean>(false);

/** A surfaced metrics error (unreachable / non-2xx), or null. */
export const metricsError = signal<string | null>(null);

/* ----------------------------------------------------------------------------
 * Wake monitor state (the publish → wake → hook → ack loop, made visible)
 *
 * The monitor watches ONE subscription at a time. The left pane publishes to /
 * tails a chosen source stream (reusing the existing publish + tail seams via
 * selectedStreamPath); the right pane shows the wake plane: for a webhook
 * subscription, captured deliveries relayed over SSE from the dsui binary; for a
 * pull-wake subscription, wake events tailed from the wake_stream. A short-lived
 * `wakePulse` tick fires when a publish lands so the right pane can animate the
 * causal link (respecting prefers-reduced-motion in CSS).
 * ------------------------------------------------------------------------- */

/**
 * The dsui binary's capture-endpoint base URL (from /dsui-config.json), or null
 * under `vite dev` / when the binary did not supply one. The webhook wake plane
 * needs this to build the capture URL + open the relay SSE.
 */
export const captureBase = signal<string | null>(null);

/** The subscription id the Wake Monitor is watching, or null when not open. */
export const wakeSubId = signal<string | null>(null);

/** The capture bucket the monitor streams from (the subscription id by convention). */
export const wakeBucket = signal<string | null>(null);

/** Captured webhook deliveries for the watched bucket, newest last (capped). */
export const wakeDeliveries = signal<readonly CaptureDelivery[]>([]);

/** Decoded pull-wake wake events tailed from the wake_stream, newest last (capped). */
export const wakeEvents = signal<readonly WakeEvent[]>([]);

/** Connection lifecycle of the right-pane wake feed (capture SSE or wake_stream). */
export const wakeFeedStatus = signal<TailStatus>({ state: "idle" });

/** Max wake records (deliveries or events) kept before the oldest age out. */
export const WAKE_BUFFER_CAP = 200;

/**
 * A monotonically-incrementing tick bumped each time a publish to the watched
 * stream lands, so the right pane can flash the causal "a wake should be coming"
 * cue. Not persisted; purely a visual pulse.
 */
export const wakePulse = signal<number>(0);

/** True while the one-click "Wake demo" setup is running its steps. */
export const wakeDemoInFlight = signal<boolean>(false);

/** The stopper for the active wake feed (capture SSE or wake_stream tail). */
let wakeFeedStopper: CaptureStopper | TailStopper | null = null;

/* ----------------------------------------------------------------------------
 * Toasts (transient notifications)
 * ------------------------------------------------------------------------- */

/** The live toast stack, newest last. Rendered by the Toaster. */
export const toasts = signal<readonly Toast[]>([]);

/** Per-toast auto-dismiss timers, so dismissing early can clear them. */
const toastTimers = new Map<string, ReturnType<typeof setTimeout>>();

/* ----------------------------------------------------------------------------
 * Derived (computed) state
 * ------------------------------------------------------------------------- */

/** The active Connection object, or null. */
export const activeConnection = computed<Connection | null>(() => {
	const id = activeConnectionId.value;
	if (id === null) return null;
	return connections.value.find((c) => c.id === id) ?? null;
});

/** The selected StreamInfo, or null. */
export const selectedStream = computed<StreamInfo | null>(() => {
	const path = selectedStreamPath.value;
	if (path === null) return null;
	return streams.value.find((s) => s.path === path) ?? null;
});

/** A client bound to the active connection, recreated when it changes. */
export const activeClient = computed<DsClient | null>(() => {
	const conn = activeConnection.value;
	return conn === null ? null : createClient(conn);
});

/** The selected subscription's cached view, or null. */
export const selectedSubscription = computed<Subscription | null>(() => {
	const id = selectedSubscriptionId.value;
	if (id === null) return null;
	return subscriptionDetails.value[id] ?? null;
});

/** The cached view of the subscription the Wake Monitor is watching, or null. */
export const wakeSubscription = computed<Subscription | null>(() => {
	const id = wakeSubId.value;
	if (id === null) return null;
	return subscriptionDetails.value[id] ?? null;
});

/* ----------------------------------------------------------------------------
 * Persistence effects
 * ------------------------------------------------------------------------- */

effect(() => {
	writeLs(LS_CONNECTIONS, JSON.stringify(connections.value));
});

effect(() => {
	const id = activeConnectionId.value;
	if (id === null) removeLs(LS_ACTIVE);
	else writeLs(LS_ACTIVE, id);
});

effect(() => {
	const t = theme.value;
	writeLs(LS_THEME, t);
	applyTheme(t);
});

effect(() => {
	writeLs(LS_INSPECTOR_COLLAPSED, inspectorCollapsed.value ? "true" : "false");
});

effect(() => {
	writeLs(LS_PLAYGROUND_OPEN, playgroundOpen.value ? "true" : "false");
});

/* ----------------------------------------------------------------------------
 * Wake-feed lifecycle effect
 *
 * The wake feed (capture SSE / wake_stream EventSource) is owned by the
 * module-level wakeFeedStopper, but WakeMonitorWorkspace only mounts while
 * centerView === "wakes". The always-visible Navigator can flip centerView away
 * (selectStream → "messages", selectSubscription → "subscription", the Metrics
 * entry → "metrics") and unmount the workspace WITHOUT going through
 * closeWakeMonitor(). This effect is the single seam that tears the live feed
 * down whenever the center pane leaves the monitor, so a hidden view never keeps
 * an SSE connection open or mutates the wake buffers. Re-opening the monitor
 * re-establishes the feed via openWakeMonitor → startWakeFeed.
 */
effect(() => {
	if (centerView.value !== "wakes") stopWakeFeed();
});

/* ----------------------------------------------------------------------------
 * Actions (the only sanctioned mutations)
 * ------------------------------------------------------------------------- */

/** Generate a reasonably-unique id without a dependency. */
function newId(): string {
	const c = globalThis.crypto;
	if (c !== undefined && typeof c.randomUUID === "function") return c.randomUUID();
	return `conn-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

/** Normalize user input into a Connection. */
export function makeConnection(input: {
	name: string;
	baseUrl: string;
	streamRoot?: string;
}): Connection {
	const baseUrl = input.baseUrl.trim().replace(/\/+$/, "");
	const rawRoot = (input.streamRoot ?? "/v1/stream").trim();
	const streamRoot = `/${rawRoot.replace(/^\/+/, "").replace(/\/+$/, "")}`;
	const name = input.name.trim() === "" ? baseUrl : input.name.trim();
	return { id: newId(), name, baseUrl, streamRoot, createdAt: Date.now(), lastUsedAt: null };
}

/** Add a connection and make it active. Returns the created connection. */
export function addConnection(input: {
	name: string;
	baseUrl: string;
	streamRoot?: string;
}): Connection {
	const conn = makeConnection(input);
	connections.value = [...connections.value, conn];
	setActiveConnection(conn.id);
	return conn;
}

/** Apply a partial update to a saved connection (name / baseUrl / streamRoot). */
export function updateConnection(
	id: string,
	patch: { name?: string; baseUrl?: string; streamRoot?: string },
): void {
	connections.value = connections.value.map((c) => {
		if (c.id !== id) return c;
		const baseUrl =
			patch.baseUrl !== undefined ? patch.baseUrl.trim().replace(/\/+$/, "") : c.baseUrl;
		const streamRoot =
			patch.streamRoot !== undefined
				? `/${patch.streamRoot.trim().replace(/^\/+/, "").replace(/\/+$/, "")}`
				: c.streamRoot;
		const name =
			patch.name !== undefined ? (patch.name.trim() === "" ? baseUrl : patch.name.trim()) : c.name;
		return { ...c, name, baseUrl, streamRoot };
	});
}

/** Remove a connection by id; clears active selection if it was active. */
export function removeConnection(id: string): void {
	connections.value = connections.value.filter((c) => c.id !== id);
	clearProbeStatus(id);
	if (activeConnectionId.value === id) {
		// Return to the start screen rather than silently jumping connections.
		setActiveConnection(null);
	}
}

/**
 * Switch the active connection and reset per-connection state. Stamps the
 * connection's lastUsedAt, kicks off a reachability probe and a stream refresh,
 * and closes the switcher popover. Passing null returns to the start screen.
 */
export function setActiveConnection(id: string | null): void {
	stopTail();
	closeWakeMonitor();
	activeConnectionId.value = id;
	streams.value = [];
	selectedStreamPath.value = null;
	lastRead.value = null;
	selectedRow.value = null;
	rowsTruncated.value = false;
	startMode.value = "earliest";
	customOffset.value = "";
	connectionProbe.value = null;
	errorMessage.value = null;
	switcherOpen.value = false;
	// Reset + rehydrate the per-connection subscription set and metrics URL. The
	// known-ids set has no server-side discovery, so it is restored from storage.
	subscriptionDetails.value = {};
	selectedSubscriptionId.value = null;
	activeClaim.value = null;
	centerView.value = "messages";
	metrics.value = null;
	metricsError.value = null;
	subscriptionIds.value = id === null ? [] : loadSubscriptionIds(id);
	metricsUrl.value = id === null ? "" : loadMetricsUrl(id);
	if (id !== null) {
		const now = Date.now();
		connections.value = connections.value.map((c) => (c.id === id ? { ...c, lastUsedAt: now } : c));
		void refreshStreams();
		void probeConnection(id);
	}
}

/** Probe the active connection's reachability (kept for back-compat callers). */
export async function probeActiveConnection(): Promise<void> {
	const id = activeConnectionId.value;
	if (id === null) return;
	await probeConnection(id);
}

/* ----------------------------------------------------------------------------
 * Connection probing (per-connection status dots)
 * ------------------------------------------------------------------------- */

/** Publish a probe status for one connection id (immutably). */
function setProbeStatus(id: string, status: ProbeStatus): void {
	probeStatuses.value = { ...probeStatuses.value, [id]: status };
}

/** Drop a connection's probe status (on removal). */
function clearProbeStatus(id: string): void {
	const next = { ...probeStatuses.value };
	delete next[id];
	probeStatuses.value = next;
}

/**
 * Probe a saved connection by id and record its status. Marks it "checking"
 * first so the dot can pulse, then "done" with the probe. Mirrors the result
 * into connectionProbe when the probed connection is the active one.
 */
export async function probeConnection(id: string): Promise<void> {
	const conn = connections.value.find((c) => c.id === id);
	if (conn === undefined) return;
	setProbeStatus(id, { state: "checking" });
	const probe = await createClient(conn).testConnection();
	setProbeStatus(id, { state: "done", probe });
	if (activeConnectionId.value === id) connectionProbe.value = probe;
}

/** Probe every saved connection (used to light up the start screen). */
export async function probeAllConnections(): Promise<void> {
	await Promise.all(connections.value.map((c) => probeConnection(c.id)));
}

/**
 * Probe a not-yet-saved candidate connection, for the "Test connection" step in
 * the new-connection form. Pure: records nothing in the store, just returns the
 * probe so the form can show inline feedback before the user commits to saving.
 */
export async function testCandidate(
	input: { name: string; baseUrl: string; streamRoot?: string },
	signal?: AbortSignal,
): Promise<ConnectionProbe> {
	const conn = makeConnection(input);
	return createClient(conn).testConnection(signal);
}

/** Reload the stream list from the registry for the active connection. */
export async function refreshStreams(): Promise<void> {
	const client = activeClient.value;
	if (client === null) return;
	streamsLoading.value = true;
	errorMessage.value = null;
	try {
		const { streams: discovered, exchange } = await client.listStreams();
		lastExchange.value = exchange;
		// Preserve any manually-added streams not present in the registry.
		const manual = streams.value.filter(
			(s) => s.manual && !discovered.some((d) => d.path === s.path),
		);
		streams.value = [...discovered, ...manual].sort((a, b) => a.path.localeCompare(b.path));
	} catch (err) {
		errorMessage.value = err instanceof Error ? err.message : "failed to list streams";
	} finally {
		streamsLoading.value = false;
	}
}

/** Add a stream path the registry did not surface, so the user can view it. */
export function addManualStream(path: string): void {
	const clean = path.trim().replace(/^\/+/, "").replace(/\/+$/, "");
	if (clean === "") return;
	if (streams.value.some((s) => s.path === clean)) {
		selectStream(clean);
		return;
	}
	const info: StreamInfo = {
		path: clean,
		contentType: null,
		kind: "binary",
		createdAt: null,
		manual: true,
	};
	streams.value = [...streams.value, info].sort((a, b) => a.path.localeCompare(b.path));
	selectStream(clean);
}

/**
 * Select a stream and read it using the current toolbar settings. Resets the
 * toolbar to "Earliest" so opening a new stream starts at a predictable place;
 * the user can then change the starting position and Read again.
 */
export function selectStream(path: string): void {
	stopTail();
	tailRows.value = [];
	tailDropped.value = 0;
	selectedStreamPath.value = path;
	selectedRow.value = null;
	lastRead.value = null;
	startMode.value = "earliest";
	customOffset.value = "";
	// Selecting a stream brings the messages workspace back to the center pane.
	centerView.value = "messages";
	void readSelected(OFFSET_EARLIEST_VALUE);
}

/**
 * Read the selected stream at the given offset and update grid state. The
 * decoded rows are capped to {@link rowCap} for display; rowsTruncated records
 * whether anything was dropped (the full bytes remain in the Raw view).
 */
export async function readSelected(offset: string): Promise<void> {
	const client = activeClient.value;
	const path = selectedStreamPath.value;
	if (client === null || path === null) return;
	readLoading.value = true;
	errorMessage.value = null;
	try {
		const result = await client.readStream(path, offset);
		const cap = clampRowCap(rowCap.value);
		const truncated = result.rows.length > cap;
		const capped: ReadResult = truncated ? { ...result, rows: result.rows.slice(0, cap) } : result;
		rowsTruncated.value = truncated;
		lastRead.value = capped;
		lastExchange.value = result.exchange;
		selectedRow.value = capped.rows[0] ?? null;
		// If the read revealed the real Content-Type, upgrade the StreamInfo.
		const ct = result.exchange.protocol.contentType;
		if (ct !== null) {
			streams.value = streams.value.map((s) =>
				s.path === path ? { ...s, contentType: ct, kind: result.kind } : s,
			);
		}
	} catch (err) {
		errorMessage.value = err instanceof Error ? err.message : "failed to read stream";
	} finally {
		readLoading.value = false;
	}
}

/** Resolve the toolbar settings to an offset, then read the selected stream. */
export async function readFromToolbar(): Promise<void> {
	const offset = resolveOffset(startMode.value, customOffset.value);
	await readSelected(offset);
}

/** Read the next batch using the last Stream-Next-Offset, if any. */
export async function readNext(): Promise<void> {
	const next = lastRead.value?.nextOffset;
	if (next === null || next === undefined) return;
	await readSelected(next);
}

/** Set the toolbar starting-position mode. */
export function setStartMode(mode: StartMode): void {
	startMode.value = mode;
}

/** Set the explicit "at offset" cursor used when startMode is "at". */
export function setCustomOffset(value: string): void {
	customOffset.value = value;
}

/** Set the grid row cap (clamped to a positive integer). */
export function setRowCap(value: number): void {
	rowCap.value = clampRowCap(value);
}

/** The protocol offset for the beginning, shared with lib/messages. */
const OFFSET_EARLIEST_VALUE = "-1";

/** Select a grid row to drive the inspector. */
export function selectRow(row: GridRow | null): void {
	selectedRow.value = row;
}

/** Set the theme preference. */
export function setTheme(next: Theme): void {
	theme.value = next;
}

/** Cycle system -> light -> dark -> system. */
export function cycleTheme(): void {
	const order: readonly Theme[] = ["system", "light", "dark"];
	const idx = order.indexOf(theme.value);
	const next = order[(idx + 1) % order.length] ?? "system";
	theme.value = next;
}

/** Toggle the right-hand Inspector panel between collapsed and expanded. */
export function toggleInspector(): void {
	inspectorCollapsed.value = !inspectorCollapsed.value;
}

/** Toggle the left sidebar's foldable Playground section open/closed. */
export function togglePlayground(): void {
	playgroundOpen.value = !playgroundOpen.value;
}

/** Clear the surfaced error. */
export function dismissError(): void {
	errorMessage.value = null;
}

/* ----------------------------------------------------------------------------
 * Toast actions
 * ------------------------------------------------------------------------- */

/** Default auto-dismiss delays per tone (errors linger; successes are brief). */
const TOAST_DEFAULT_MS: Readonly<Record<ToastKind, number>> = {
	info: 4000,
	success: 3000,
	warning: 6000,
	error: 8000,
};

/**
 * Push a toast and schedule its auto-dismiss. Returns the toast id so a caller
 * can dismiss it early. A `durationMs` of 0 makes it sticky (no auto-dismiss).
 */
export function addToast(input: {
	kind: ToastKind;
	title: string;
	message?: string;
	durationMs?: number;
	action?: ToastAction;
}): string {
	const id = newId();
	const durationMs = input.durationMs ?? TOAST_DEFAULT_MS[input.kind];
	const toast: Toast = {
		id,
		kind: input.kind,
		title: input.title,
		durationMs,
		createdAt: Date.now(),
		...(input.message !== undefined ? { message: input.message } : {}),
		...(input.action !== undefined ? { action: input.action } : {}),
	};
	toasts.value = [...toasts.value, toast];
	if (durationMs > 0) {
		const timer = globalThis.setTimeout(() => dismissToast(id), durationMs);
		toastTimers.set(id, timer);
	}
	return id;
}

/** Dismiss a toast by id and clear its timer. */
export function dismissToast(id: string): void {
	const timer = toastTimers.get(id);
	if (timer !== undefined) {
		globalThis.clearTimeout(timer);
		toastTimers.delete(id);
	}
	toasts.value = toasts.value.filter((t) => t.id !== id);
}

/* ----------------------------------------------------------------------------
 * Producer-identity actions (idempotent appends)
 * ------------------------------------------------------------------------- */

/** Set (or clear) the active producer identity used for appends. */
export function setProducerIdentity(identity: ProducerIdentity | null): void {
	producerIdentity.value = identity;
}

/** Advance the active producer's seq after a successful append. */
export function bumpProducerSeq(): void {
	const p = producerIdentity.value;
	if (p === null) return;
	producerIdentity.value = { ...p, seq: p.seq + 1 };
}

/* ----------------------------------------------------------------------------
 * Dialog actions (open / close the create + fork modals)
 * ------------------------------------------------------------------------- */

/** Open the create-stream dialog. */
export function openCreateDialog(): void {
	forkSeed.value = null;
	activeDialog.value = "create";
}

/** Open the create-subscription dialog (the reserved /__ds/* control plane). */
export function openCreateSubscriptionDialog(): void {
	forkSeed.value = null;
	activeDialog.value = "subscription";
}

/**
 * Open the fork dialog seeded from a source path and a default offset (usually
 * the current read's Stream-Next-Offset, the selected row's batch, or "now").
 */
export function openForkDialog(fromPath: string, offset: string): void {
	forkSeed.value = { fromPath, offset };
	activeDialog.value = "fork";
}

/** Close whichever write dialog is open. */
export function closeDialog(): void {
	activeDialog.value = null;
	forkSeed.value = null;
}

/* ----------------------------------------------------------------------------
 * Write actions (create / append / close / delete / fork)
 *
 * Each mirrors the captured exchange into lastExchange (so the protocol panel
 * updates), records the Operation for the curl helper, surfaces a toast, and
 * refreshes the stream list so the navigator reflects the change. They never
 * throw: a failed write resolves to ok:false and a toast.
 * ------------------------------------------------------------------------- */

/** Create a stream from typed options; refreshes the list and toasts the result. */
export async function createStream(opts: {
	path: string;
	contentType: StreamContentType;
	ttl?: string;
	expiresAt?: string;
	closed?: boolean;
}): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	operationInFlight.value = true;
	try {
		const result = await client.createStream(opts);
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			// Register the stream so the navigator (which discovers via __registry__)
			// surfaces it; the server has no stream-listing API.
			await client.writeRegistryEvent(opts.path, opts.contentType, "upsert");
			addToast({ kind: "success", title: "Stream created", message: opts.path });
			await refreshStreams();
			selectStream(opts.path);
		} else {
			addToast({ kind: "error", title: "Create failed", message: result.error ?? opts.path });
		}
		return result.ok;
	} finally {
		operationInFlight.value = false;
	}
}

/**
 * Append/publish to a stream. Uses the active producer identity when set, and
 * advances its seq on success. Surfaces a producer-conflict toast when the
 * server reports a sequence mismatch.
 */
export async function appendMessages(
	path: string,
	body: string | Uint8Array,
	opts?: { closeAfter?: boolean; contentType?: StreamContentType },
): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	operationInFlight.value = true;
	try {
		const producer = producerIdentity.value;
		const result = await client.appendMessages(path, {
			body,
			...(producer !== null ? { producer } : {}),
			...(opts?.closeAfter !== undefined ? { closeAfter: opts.closeAfter } : {}),
			...(opts?.contentType !== undefined ? { contentType: opts.contentType } : {}),
		});
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			if (producer !== null) bumpProducerSeq();
			addToast({ kind: "success", title: "Published", message: path });
			// Reflect the new tail if we are viewing this stream.
			if (selectedStreamPath.value === path) await readSelected("now");
		} else if (result.conflict !== null) {
			const { expectedSeq, receivedSeq } = result.conflict;
			addToast({
				kind: "warning",
				title: "Producer sequence conflict",
				message: `server expected seq ${expectedSeq ?? "?"}, received ${receivedSeq ?? "?"}`,
			});
		} else {
			addToast({ kind: "error", title: "Publish failed", message: result.error ?? path });
		}
		return result.ok;
	} finally {
		operationInFlight.value = false;
	}
}

/** Close a stream; toasts and refreshes. */
export async function closeStream(path: string): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	operationInFlight.value = true;
	try {
		const result = await client.closeStream(path);
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			addToast({ kind: "success", title: "Stream closed", message: path });
			await refreshStreams();
		} else {
			addToast({ kind: "error", title: "Close failed", message: result.error ?? path });
		}
		return result.ok;
	} finally {
		operationInFlight.value = false;
	}
}

/** Delete a stream; clears selection if it was the active stream. */
export async function deleteStream(path: string): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	operationInFlight.value = true;
	try {
		const result = await client.deleteStream(path);
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			// Drop it from the discovery registry so it leaves the navigator.
			await client.writeRegistryEvent(path, null, "deleted");
			addToast({ kind: "success", title: "Stream deleted", message: path });
			if (selectedStreamPath.value === path) {
				selectedStreamPath.value = null;
				lastRead.value = null;
				selectedRow.value = null;
			}
			await refreshStreams();
		} else {
			addToast({ kind: "error", title: "Delete failed", message: result.error ?? path });
		}
		return result.ok;
	} finally {
		operationInFlight.value = false;
	}
}

/** Fork a stream into newPath at a source offset; refreshes and selects it. */
export async function forkStream(
	newPath: string,
	fromPath: string,
	offset: string,
	subOffset?: number,
): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	operationInFlight.value = true;
	try {
		const result = await client.forkStream(newPath, fromPath, offset, subOffset);
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			// Register the new fork so it appears in the navigator.
			await client.writeRegistryEvent(newPath, null, "upsert");
			addToast({ kind: "success", title: "Stream forked", message: `${fromPath} → ${newPath}` });
			await refreshStreams();
			selectStream(newPath);
		} else {
			addToast({ kind: "error", title: "Fork failed", message: result.error ?? newPath });
		}
		return result.ok;
	} finally {
		operationInFlight.value = false;
	}
}

/**
 * Refresh the selected stream's metadata via HEAD. Records the exchange in both
 * {@link streamMeta} (for the metadata affordance) and lastExchange (so the
 * protocol disclosure shows it), and upgrades the StreamInfo's Content-Type/kind
 * when the server reports one. Toasts the closed/open + tail summary on success.
 */
export async function refreshMeta(path?: string): Promise<void> {
	const client = activeClient.value;
	const target = path ?? selectedStreamPath.value;
	if (client === null || target === null) return;
	metaLoading.value = true;
	try {
		const exchange = await client.headStream(target);
		streamMeta.value = exchange;
		lastExchange.value = exchange;
		const ct = exchange.protocol.contentType;
		if (ct !== null) {
			const kind = kindFromContentType(ct);
			streams.value = streams.value.map((s) =>
				s.path === target ? { ...s, contentType: ct, kind } : s,
			);
		}
		if (exchange.status === 0) {
			addToast({ kind: "error", title: "Metadata unavailable", message: exchange.error ?? target });
		} else {
			const closed = headerLooseTrue(exchange.protocol.streamClosed);
			const next = exchange.protocol.streamNextOffset;
			addToast({
				kind: "info",
				title: closed ? "Stream is closed" : "Metadata refreshed",
				message: next === null ? target : `${target} · next offset ${next}`,
			});
		}
	} finally {
		metaLoading.value = false;
	}
}

/**
 * Run a small demo producer against a stream: publish `total` JSON messages one
 * at a time with a delay between them, so a live tail visibly updates. Each
 * message is a single-element JSON batch. Resolves when the run finishes (or is
 * cut short by a failure). Intended for the Playground's "Run a demo producer"
 * preset on a JSON stream; never throws.
 */
export async function runDemoProducer(
	path: string,
	options?: { total?: number; delayMs?: number },
): Promise<void> {
	const client = activeClient.value;
	if (client === null) return;
	const total = options?.total ?? 5;
	const delayMs = options?.delayMs ?? 700;
	operationInFlight.value = true;
	addToast({
		kind: "info",
		title: "Demo producer started",
		message: `${total} messages → ${path}`,
	});
	try {
		for (let i = 1; i <= total; i++) {
			const body = JSON.stringify([
				{ seq: i, of: total, note: `demo message ${i}`, at: new Date().toISOString() },
			]);
			const result = await client.appendMessages(path, { body });
			lastOperation.value = result.operation;
			lastExchange.value = result.exchange;
			if (!result.ok) {
				addToast({ kind: "error", title: "Demo producer stopped", message: result.error ?? path });
				return;
			}
			if (selectedStreamPath.value === path && tailStatus.value.state === "idle") {
				await readSelected("now");
			}
			if (i < total) await sleep(delayMs);
		}
		addToast({ kind: "success", title: "Demo producer finished", message: path });
	} finally {
		operationInFlight.value = false;
	}
}

/** A plain delay used by the demo producer. */
function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => globalThis.setTimeout(resolve, ms));
}

/** Interpret a protocol boolean-ish header loosely (present/true/1/yes). */
function headerLooseTrue(value: string | null): boolean {
	if (value === null) return false;
	const v = value.trim().toLowerCase();
	return v === "" || v === "true" || v === "1" || v === "yes";
}

/* ----------------------------------------------------------------------------
 * Live-tail actions
 * ------------------------------------------------------------------------- */

/**
 * Set the read mode (the toolbar's Catch-up | Long-poll | SSE selector).
 * Changing the mode tears down any active tail, since a tail belongs to the
 * mode that started it; the user then starts the new mode explicitly. A no-op
 * when the mode is unchanged so re-selecting the active mode never kills a live
 * connection.
 */
export function setTailMode(mode: TailMode): void {
	if (tailMode.value === mode) return;
	stopTail();
	tailMode.value = mode;
}

/** Pause/resume appending received rows to the tail buffer. */
export function setTailPaused(paused: boolean): void {
	tailPaused.value = paused;
}

/** Clear the tail buffer (and the dropped counter) without stopping the tail. */
export function clearTailBuffer(): void {
	// Drop any rows still waiting in the coalescer so a pending flush cannot
	// re-populate what the user just cleared; the connection stays open.
	tailScratch = [];
	tailPendingExchange = null;
	tailRows.value = [];
	tailDropped.value = 0;
}

/**
 * Accept a received tail batch (unless paused). Rows are buffered into the
 * coalescer scratch and flushed to the `tailRows` signal once per frame rather
 * than written per message — see the coalescer block above. Exported only so a
 * store test can feed batches; the tail openers are the real callers.
 */
export function pushTailBatch(batch: TailBatch): void {
	if (tailPaused.value) return;
	if (batch.rows.length === 0) return;
	for (const row of batch.rows) tailScratch.push(row);
	if (batch.exchange !== null) tailPendingExchange = batch.exchange;
	scheduleTailFlush();
}

/**
 * Start following the selected stream from the given offset using the current
 * {@link tailMode}. Stops any existing tail first. A no-op when no stream is
 * selected. The opener handles errors internally; this only wires the buffer.
 */
export function startTail(fromOffset: string): void {
	const client = activeClient.value;
	const conn = activeConnection.value;
	const path = selectedStreamPath.value;
	const mode = tailMode.value;
	// A live tail only opens for the two streaming modes; "catchup" is the paged
	// read path and never starts a connection here.
	if (client === null || conn === null || path === null || !isLiveMode(mode)) return;
	stopTail();
	tailRows.value = [];
	tailDropped.value = 0;
	tailPaused.value = false;
	tailStatus.value = { state: "connecting" };
	// Record the live request so the protocol disclosure can show it (SSE has no
	// per-request exchange, so this is the only honest source for its curl).
	tailStartOffset.value = fromOffset;
	const op = previewTailOperation(conn.baseUrl, conn.streamRoot, path, fromOffset, mode);
	tailOperation.value = op;
	lastOperation.value = op;
	const onState = (status: TailStatus): void => {
		tailStatus.value = status;
		if (status.state === "closed")
			addToast({ kind: "info", title: "Stream closed", message: path });
		if (status.state === "error")
			addToast({ kind: "error", title: "Live tail error", message: status.message });
	};
	tailStopper =
		mode === "sse"
			? client.openSse(path, fromOffset, pushTailBatch, onState)
			: client.openLongPoll(path, fromOffset, pushTailBatch, onState);
}

/** Stop the active tail and reset its status to idle. Safe to call when idle. */
export function stopTail(): void {
	if (tailStopper !== null) {
		tailStopper();
		tailStopper = null;
	}
	// Drop any scheduled flush so a frame queued by the last batch cannot fire
	// after the connection is gone (also clears the un-flushed scratch rows).
	cancelTailFlush();
	tailStatus.value = { state: "idle" };
	tailOperation.value = null;
	tailStartOffset.value = null;
}

/* ----------------------------------------------------------------------------
 * Subscription actions (the reserved /__ds/* control plane)
 *
 * Each mirrors the captured exchange into lastExchange + records the Operation
 * for the curl helper (matching the write actions), toasts the result, and keeps
 * the client-side known-ids set in sync (since there is no list-all endpoint).
 * They never throw: a failed op resolves and toasts. The FENCED / ALREADY_CLAIMED
 * 409 cases surface as a distinct warning toast.
 * ------------------------------------------------------------------------- */

/** Add an id to the per-connection known set and persist it. */
function rememberSubscriptionId(id: string): void {
	if (subscriptionIds.value.includes(id)) return;
	const next = [...subscriptionIds.value, id].sort((a, b) => a.localeCompare(b));
	subscriptionIds.value = next;
	persistSubscriptionIds();
}

/** Drop an id from the per-connection known set, its cache, and persist. */
function forgetSubscriptionId(id: string): void {
	subscriptionIds.value = subscriptionIds.value.filter((x) => x !== id);
	const next = { ...subscriptionDetails.value };
	delete next[id];
	subscriptionDetails.value = next;
	if (selectedSubscriptionId.value === id) selectedSubscriptionId.value = null;
	persistSubscriptionIds();
}

/** Cache a fetched subscription view by id (immutably). */
function cacheSubscription(sub: Subscription): void {
	subscriptionDetails.value = { ...subscriptionDetails.value, [sub.id]: sub };
}

/**
 * Add a subscription id to the tracked set WITHOUT contacting the server (the
 * user already knows the id of a subscription created elsewhere). Persists it
 * and selects it so the detail panel can fetch its view.
 */
export function trackSubscriptionId(id: string): void {
	const clean = id.trim();
	if (clean === "") return;
	rememberSubscriptionId(clean);
	selectSubscription(clean);
}

/** Forget a subscription id locally (does not delete it on the server). */
export function untrackSubscriptionId(id: string): void {
	forgetSubscriptionId(id);
}

/**
 * Select a subscription, bring its detail view to the center pane, and fetch its
 * current view. Switching to a different subscription drops any claim held for
 * the previous one (a Bearer token is scoped to a single subscription).
 */
export function selectSubscription(id: string | null): void {
	if (id !== selectedSubscriptionId.value) activeClaim.value = null;
	selectedSubscriptionId.value = id;
	if (id !== null) {
		centerView.value = "subscription";
		void getSubscription(id);
	}
}

/**
 * Create (or re-confirm) a subscription. On success, remembers + selects its id,
 * caches the returned view, and toasts (distinguishing a 200 idempotent match
 * from a 201 create is not surfaced separately — both are "ready"). A 409
 * CONFIG_CONFLICT toasts a warning. Returns ok.
 */
export async function createSubscription(opts: CreateSubscriptionOptions): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	subscriptionInFlight.value = true;
	try {
		const result = await client.createSubscription(opts);
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			if (result.value !== null) cacheSubscription(result.value);
			rememberSubscriptionId(opts.id);
			selectedSubscriptionId.value = opts.id;
			addToast({ kind: "success", title: "Subscription ready", message: opts.id });
		} else if (result.errorCode === "CONFIG_CONFLICT") {
			addToast({
				kind: "warning",
				title: "Subscription config conflict",
				message: `${opts.id} exists with a different config`,
			});
		} else {
			addToast({ kind: "error", title: "Create failed", message: result.error ?? opts.id });
		}
		return result.ok;
	} finally {
		subscriptionInFlight.value = false;
	}
}

/**
 * Fetch a subscription's current view and cache it. A 404 forgets the id from
 * the tracked set (the server tombstoned it). Surfaces the exchange.
 */
export async function getSubscription(id: string): Promise<void> {
	const client = activeClient.value;
	if (client === null) return;
	subscriptionLoading.value = true;
	try {
		const result = await client.getSubscription(id);
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok && result.value !== null) {
			cacheSubscription(result.value);
		} else if (result.exchange.status === 404) {
			addToast({ kind: "warning", title: "Subscription not found", message: id });
			forgetSubscriptionId(id);
		} else if (!result.ok) {
			addToast({
				kind: "error",
				title: "Could not load subscription",
				message: result.error ?? id,
			});
		}
	} finally {
		subscriptionLoading.value = false;
	}
}

/** Delete a subscription (DELETE), then forget it locally. */
export async function deleteSubscription(id: string): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	subscriptionInFlight.value = true;
	try {
		const result = await client.deleteSubscription(id);
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			forgetSubscriptionId(id);
			addToast({ kind: "success", title: "Subscription deleted", message: id });
		} else {
			addToast({ kind: "error", title: "Delete failed", message: result.error ?? id });
		}
		return result.ok;
	} finally {
		subscriptionInFlight.value = false;
	}
}

/** Add explicit stream links to a subscription, then refresh its view. */
export async function addSubscriptionStreams(
	id: string,
	streams: readonly string[],
): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	subscriptionInFlight.value = true;
	try {
		const result = await client.addSubscriptionStreams(id, streams);
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			addToast({ kind: "success", title: "Streams linked", message: `${streams.length} → ${id}` });
			await getSubscription(id);
		} else {
			addToast({ kind: "error", title: "Link failed", message: result.error ?? id });
		}
		return result.ok;
	} finally {
		subscriptionInFlight.value = false;
	}
}

/** Remove one explicit stream link from a subscription, then refresh its view. */
export async function removeSubscriptionStream(id: string, path: string): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	subscriptionInFlight.value = true;
	try {
		const result = await client.removeSubscriptionStream(id, path);
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			addToast({ kind: "success", title: "Stream unlinked", message: `${path} ← ${id}` });
			await getSubscription(id);
		} else {
			addToast({ kind: "error", title: "Unlink failed", message: result.error ?? path });
		}
		return result.ok;
	} finally {
		subscriptionInFlight.value = false;
	}
}

/* ----------------------------------------------------------------------------
 * Pull-wake worker actions (claim → ack/heartbeat → release)
 *
 * These drive a pull-wake subscription's lease lifecycle from the console so a
 * user can exercise the worker plane by hand: claim a lease (racing other
 * workers), ack offsets (with done to release + apply, or as a heartbeat that
 * extends the lease), or release without acking. The claim carries the Bearer
 * token + fencing (generation, wake_id) the ack/release need; it lives in
 * {@link activeClaim} for the session. A 409 (ALREADY_CLAIMED on claim, FENCED
 * on ack/release) surfaces as a distinct warning toast and clears the stale
 * claim so the controls reset. Like the other ops they never throw.
 * ------------------------------------------------------------------------- */

/** Claim a pull-wake lease (POST …/claim) as the given worker. */
export async function claimWake(id: string, worker: string): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	claimInFlight.value = true;
	try {
		const result = await client.claimWake(id, worker.trim());
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok && result.value !== null) {
			activeClaim.value = result.value;
			addToast({
				kind: "success",
				title: "Lease claimed",
				message: `${id} · gen ${result.value.generation}`,
			});
		} else if (result.fenced || result.errorCode === "ALREADY_CLAIMED") {
			addToast({
				kind: "warning",
				title: "Already claimed",
				message: "Another worker holds the lease.",
			});
		} else {
			addToast({ kind: "error", title: "Claim failed", message: result.error ?? id });
		}
		return result.ok;
	} finally {
		claimInFlight.value = false;
	}
}

/**
 * Ack a pull-wake claim (POST …/ack) with the active claim's Bearer token. With
 * `done` true the lease is released and the acks applied (clearing the claim);
 * with `done` false it heartbeat-extends the lease. Refreshes the subscription
 * view afterwards so the links table reflects the advanced cursors.
 */
export async function ackWake(
	id: string,
	acks: readonly OffsetAck[],
	done: boolean,
): Promise<boolean> {
	const client = activeClient.value;
	const claim = activeClaim.value;
	if (client === null || claim === null) return false;
	claimInFlight.value = true;
	try {
		const result = await client.ackWake(id, claim.token, {
			wakeId: claim.wakeId,
			generation: claim.generation,
			acks,
			done,
		});
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			if (done) {
				activeClaim.value = null;
				addToast({
					kind: "success",
					title: "Acked + released",
					message: result.value?.nextWake === true ? "Another wake is due." : id,
				});
			} else {
				addToast({ kind: "info", title: "Lease extended", message: "Heartbeat acked." });
			}
			await getSubscription(id);
		} else if (result.fenced) {
			activeClaim.value = null;
			addToast({
				kind: "warning",
				title: "Fenced (409)",
				message: "Stale generation, wake, or token — the claim was superseded.",
			});
		} else {
			addToast({ kind: "error", title: "Ack failed", message: result.error ?? id });
		}
		return result.ok;
	} finally {
		claimInFlight.value = false;
	}
}

/** Release a pull-wake lease without acking (POST …/release), then clear it. */
export async function releaseWake(id: string): Promise<boolean> {
	const client = activeClient.value;
	const claim = activeClaim.value;
	if (client === null || claim === null) return false;
	claimInFlight.value = true;
	try {
		const result = await client.releaseWake(id, claim.token, {
			wakeId: claim.wakeId,
			generation: claim.generation,
		});
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			activeClaim.value = null;
			addToast({ kind: "success", title: "Lease released", message: id });
			await getSubscription(id);
		} else if (result.fenced) {
			activeClaim.value = null;
			addToast({
				kind: "warning",
				title: "Fenced (409)",
				message: "The lease was already superseded.",
			});
		} else {
			addToast({ kind: "error", title: "Release failed", message: result.error ?? id });
		}
		return result.ok;
	} finally {
		claimInFlight.value = false;
	}
}

/* ----------------------------------------------------------------------------
 * Wake monitor actions (the publish → wake → hook → ack loop)
 *
 * openWakeMonitor flips the center pane to the split-screen, points the left pane
 * at a source stream (reusing selectedStreamPath + the existing publish/tail
 * seams), and opens the right-pane wake feed: a capture-endpoint SSE for a
 * webhook subscription, or a wake_stream tail for a pull-wake one. The feed
 * stopper + the deliveries/events buffers are owned here; the workspace only
 * reads signals and calls these actions. closeWakeMonitor tears the feed down.
 * ------------------------------------------------------------------------- */

/** Stop the active wake feed (capture SSE or wake_stream tail). Safe when idle. */
function stopWakeFeed(): void {
	if (wakeFeedStopper !== null) {
		wakeFeedStopper();
		wakeFeedStopper = null;
	}
	wakeFeedStatus.value = { state: "idle" };
}

/** Append a captured webhook delivery into the capped buffer, deduped on seq. */
function pushDelivery(delivery: CaptureDelivery): void {
	const existing = wakeDeliveries.value;
	if (existing.some((d) => d.seq === delivery.seq)) return;
	const combined = [...existing, delivery];
	wakeDeliveries.value =
		combined.length > WAKE_BUFFER_CAP
			? combined.slice(combined.length - WAKE_BUFFER_CAP)
			: combined;
}

/** Append decoded pull-wake events from a tailed batch into the capped buffer. */
function pushWakeEvents(batch: TailBatch): void {
	const decoded: WakeEvent[] = [];
	for (const row of batch.rows) {
		const ev = parseWakeEvent(row.value);
		if (ev !== null) decoded.push(ev);
	}
	if (batch.exchange !== null) lastExchange.value = batch.exchange;
	if (decoded.length === 0) return;
	const combined = [...wakeEvents.value, ...decoded];
	wakeEvents.value =
		combined.length > WAKE_BUFFER_CAP
			? combined.slice(combined.length - WAKE_BUFFER_CAP)
			: combined;
}

/**
 * Open the right-pane wake feed for the watched subscription. A webhook
 * subscription opens the capture-endpoint SSE (needs captureBase); a pull-wake
 * subscription tails its wake_stream over SSE. A no-op precondition (no client,
 * no capture base for a webhook sub) leaves the feed idle with a clear status.
 */
function startWakeFeed(sub: Subscription): void {
	stopWakeFeed();
	const client = activeClient.value;
	if (client === null) return;
	const onState = (status: TailStatus): void => {
		wakeFeedStatus.value = status;
	};
	if (sub.type === "webhook") {
		const base = captureBase.value;
		const bucket = wakeBucket.value;
		if (base === null || bucket === null) {
			wakeFeedStatus.value = {
				state: "error",
				message:
					"No capture endpoint — run the dsui binary (not vite dev) so it can receive webhooks.",
			};
			return;
		}
		wakeFeedStopper = openCaptureStream(base, bucket, pushDelivery, onState);
	} else {
		// Pull-wake: tail the wake_stream from its current tail over SSE.
		wakeFeedStopper = client.openWakeStreamSse(sub.id, "now", pushWakeEvents, onState);
	}
}

/**
 * Open the Wake Monitor for a subscription: flip the center pane to the
 * split-screen, default the left source stream to the subscription's first
 * linked stream (when none is already chosen), fetch the subscription view, and
 * open the right-pane wake feed. Reuses the existing publish + tail seams for the
 * left pane via selectedStreamPath.
 */
export function openWakeMonitor(id: string): void {
	stopWakeFeed();
	wakeDeliveries.value = [];
	wakeEvents.value = [];
	wakePulse.value = 0;
	wakeSubId.value = id;
	wakeBucket.value = id;
	centerView.value = "wakes";
	const cached = subscriptionDetails.value[id];
	// Default the left pane to the first linked stream, if the user has not already
	// chosen one (selecting a stream elsewhere keeps that choice).
	if (cached !== undefined && selectedStreamPath.value === null) {
		const first = cached.streams[0];
		if (first !== undefined) addManualStream(first.path);
	}
	// Fetch the view, then open the matching feed once it is known.
	void getSubscription(id).then(() => {
		if (wakeSubId.value !== id) return;
		const sub = subscriptionDetails.value[id];
		if (sub !== undefined) {
			if (selectedStreamPath.value === null) {
				const first = sub.streams[0];
				if (first !== undefined) addManualStream(first.path);
			}
			startWakeFeed(sub);
		}
	});
}

/** Re-open the wake feed (e.g. after a connection blip), reusing the cached view. */
export function restartWakeFeed(): void {
	const sub = wakeSubscription.value;
	if (sub !== null) startWakeFeed(sub);
}

/** Close the Wake Monitor: tear down the feed and clear its buffers. */
export function closeWakeMonitor(): void {
	stopWakeFeed();
	wakeSubId.value = null;
	wakeBucket.value = null;
	wakeDeliveries.value = [];
	wakeEvents.value = [];
	wakePulse.value = 0;
}

/**
 * Publish a single text/JSON body to the watched source stream from the monitor's
 * left pane, then pulse the causal cue so the right pane animates the incoming
 * wake. Reuses the shared appendMessages action (which records the exchange +
 * advances the producer seq); on success it bumps {@link wakePulse}.
 */
export async function publishAndPulse(
	path: string,
	body: string | Uint8Array,
	opts?: { contentType?: StreamContentType },
): Promise<boolean> {
	const ok = await appendMessages(path, body, opts ?? {});
	if (ok) wakePulse.value += 1;
	return ok;
}

/**
 * Ack a captured WEBHOOK wake on the callback path, using the callback_token +
 * fencing fields decoded from the delivery. `done` true releases the lease and
 * applies the acks; false heartbeat-extends. Refreshes the subscription view so
 * the links table reflects advanced cursors. Surfaces FENCED as a warning.
 */
export async function callbackAck(
	id: string,
	token: string,
	req: { wakeId: string; generation: number; acks: readonly OffsetAck[]; done: boolean },
): Promise<boolean> {
	const client = activeClient.value;
	if (client === null) return false;
	claimInFlight.value = true;
	try {
		const result = await client.callbackWake(id, token, req);
		lastOperation.value = result.operation;
		lastExchange.value = result.exchange;
		if (result.ok) {
			addToast({
				kind: "success",
				title: req.done ? "Callback acked + released" : "Callback heartbeat",
				message: result.value?.nextWake === true ? "Another wake is due." : id,
			});
			await getSubscription(id);
		} else if (result.fenced) {
			addToast({
				kind: "warning",
				title: "Fenced (409)",
				message: "Stale wake, generation, or token — re-claim or wait for the next wake.",
			});
		} else {
			addToast({ kind: "error", title: "Callback failed", message: result.error ?? id });
		}
		return result.ok;
	} finally {
		claimInFlight.value = false;
	}
}

/**
 * One-click "Wake demo": create the sample stream, register a webhook
 * subscription whose webhook_url is the binary's capture endpoint, publish a
 * message to fire a wake, and open the Wake Monitor on it. Runs the SAME store
 * actions the rest of the UI uses, so toasts + the protocol disclosure + the
 * copy-as-curl all stay honest. Needs an active connection + a capture base
 * (the dsui binary, not vite dev); without the latter it explains why and stops.
 */
export async function runWakeDemo(): Promise<void> {
	const conn = activeConnection.value;
	const client = activeClient.value;
	if (conn === null || client === null) return;
	const base = captureBase.value;
	if (base === null) {
		addToast({
			kind: "warning",
			title: "No capture endpoint",
			message: "Run the dsui binary (not vite dev) so webhooks can be received.",
		});
		return;
	}
	wakeDemoInFlight.value = true;
	try {
		// 1) Create the sample JSON stream (idempotent-ish; a 409 just means it
		//    already exists, which the toast surfaces but does not block on).
		await createStream({ path: WAKE_DEMO_STREAM, contentType: WAKE_DEMO_CONTENT_TYPE });
		// 2) Register a webhook subscription pointed at the capture endpoint.
		const ok = await createSubscription({
			id: WAKE_DEMO_SUB_ID,
			type: "webhook",
			streams: [WAKE_DEMO_STREAM],
			webhookUrl: captureUrl(base, WAKE_DEMO_SUB_ID),
			description: "dsui wake demo — fires captured webhooks on playground/wakes",
		});
		if (!ok) return;
		// 3) Select the source stream + open the monitor (which opens the feed).
		selectStream(WAKE_DEMO_STREAM);
		openWakeMonitor(WAKE_DEMO_SUB_ID);
		// 4) Publish a message so a wake should arrive (visible on the right pane,
		//    if the server is redis-backed with subscriptions enabled).
		await publishAndPulse(WAKE_DEMO_STREAM, wakeDemoBody(), {
			contentType: WAKE_DEMO_CONTENT_TYPE,
		});
	} finally {
		wakeDemoInFlight.value = false;
	}
}

/* ----------------------------------------------------------------------------
 * Metrics actions (Prometheus scrape of the separate --metrics-listen address)
 * ------------------------------------------------------------------------- */

/** Set + persist the metrics endpoint URL for the active connection. */
export function setMetricsUrl(url: string): void {
	metricsUrl.value = url.trim();
	const id = activeConnectionId.value;
	if (id === null) return;
	if (metricsUrl.value === "") removeLs(`${LS_METRICS_PREFIX}${id}`);
	else writeLs(`${LS_METRICS_PREFIX}${id}`, metricsUrl.value);
}

/**
 * Scrape + parse the metrics endpoint. Uses the explicit {@link metricsUrl} (the
 * separate --metrics-listen address); a blank URL surfaces a hint rather than a
 * request. Never throws: a failure sets {@link metricsError} and leaves the last
 * snapshot in place. Surfaces the exchange so the protocol panel can show it.
 */
export async function refreshMetrics(): Promise<void> {
	const client = activeClient.value;
	const url = metricsUrl.value.trim();
	if (client === null) return;
	if (url === "") {
		metricsError.value = "Set the metrics endpoint URL (the --metrics-listen address) first.";
		return;
	}
	metricsLoading.value = true;
	metricsError.value = null;
	try {
		const { snapshot, exchange } = await client.fetchMetrics(url);
		lastExchange.value = exchange;
		if (snapshot !== null) {
			metrics.value = snapshot;
		} else {
			metricsError.value =
				exchange.status === 0
					? (exchange.error ?? "metrics endpoint unreachable")
					: `metrics request failed (${exchange.status})`;
		}
	} finally {
		metricsLoading.value = false;
	}
}

/* ----------------------------------------------------------------------------
 * localStorage helpers + loaders (defensive: never throw on bad storage)
 * ------------------------------------------------------------------------- */

function readLs(key: string): string | null {
	try {
		return globalThis.localStorage?.getItem(key) ?? null;
	} catch {
		return null;
	}
}

function writeLs(key: string, value: string): void {
	try {
		globalThis.localStorage?.setItem(key, value);
	} catch {
		/* storage unavailable (private mode, etc.) — ignore */
	}
}

function removeLs(key: string): void {
	try {
		globalThis.localStorage?.removeItem(key);
	} catch {
		/* ignore */
	}
}

function loadConnections(): readonly Connection[] {
	const raw = readLs(LS_CONNECTIONS);
	if (raw === null) return [];
	try {
		const parsed: unknown = JSON.parse(raw);
		if (!Array.isArray(parsed)) return [];
		const out: Connection[] = [];
		for (const item of parsed) {
			const conn = coerceConnection(item);
			if (conn !== null) out.push(conn);
		}
		return out;
	} catch {
		return [];
	}
}

function coerceConnection(raw: unknown): Connection | null {
	if (!isRecord(raw)) return null;
	const { id, name, baseUrl, streamRoot, createdAt, lastUsedAt } = raw;
	if (typeof id !== "string" || typeof baseUrl !== "string") return null;
	return {
		id,
		name: typeof name === "string" ? name : baseUrl,
		baseUrl,
		streamRoot: typeof streamRoot === "string" ? streamRoot : "/v1/stream",
		createdAt: typeof createdAt === "number" ? createdAt : Date.now(),
		lastUsedAt: typeof lastUsedAt === "number" ? lastUsedAt : null,
	};
}

function loadActiveId(): string | null {
	return readLs(LS_ACTIVE);
}

/** Load the persisted known subscription ids for a connection (defensive). */
function loadSubscriptionIds(connId: string): readonly string[] {
	const raw = readLs(`${LS_SUBS_PREFIX}${connId}`);
	if (raw === null) return [];
	try {
		const parsed: unknown = JSON.parse(raw);
		if (!Array.isArray(parsed)) return [];
		const out = parsed.filter((x): x is string => typeof x === "string" && x !== "");
		return [...new Set(out)].sort((a, b) => a.localeCompare(b));
	} catch {
		return [];
	}
}

/** Persist the current known subscription ids for the active connection. */
function persistSubscriptionIds(): void {
	const id = activeConnectionId.value;
	if (id === null) return;
	writeLs(`${LS_SUBS_PREFIX}${id}`, JSON.stringify(subscriptionIds.value));
}

/** Load the persisted metrics URL for a connection. */
function loadMetricsUrl(connId: string): string {
	return readLs(`${LS_METRICS_PREFIX}${connId}`) ?? "";
}

function loadTheme(): Theme {
	const raw = readLs(LS_THEME);
	if (raw === "light" || raw === "dark" || raw === "system") return raw;
	return "system";
}

/** Read a persisted boolean ("true"/"false"), falling back on anything else. */
function loadBool(key: string, fallback: boolean): boolean {
	const raw = readLs(key);
	if (raw === "true") return true;
	if (raw === "false") return false;
	return fallback;
}

/** Reflect the theme onto <html data-theme>. "system" removes the attribute. */
function applyTheme(t: Theme): void {
	const root = globalThis.document?.documentElement;
	if (root === undefined) return;
	if (t === "system") root.removeAttribute("data-theme");
	else root.setAttribute("data-theme", t);
}

/**
 * Add a connection without making it active (so the user stays on the start
 * screen and picks one). Skips creation if a saved connection already targets
 * the same base URL. Returns the new or existing connection.
 */
export function ensureConnection(input: {
	name: string;
	baseUrl: string;
	streamRoot?: string;
}): Connection {
	const candidate = makeConnection(input);
	const existing = connections.value.find((c) => c.baseUrl === candidate.baseUrl);
	if (existing !== undefined) return existing;
	connections.value = [...connections.value, candidate];
	return candidate;
}

/**
 * Prefill a connection from the binary's /dsui-config.json defaultServer, if
 * present and not already saved. Probes whatever it ends up with so the start
 * screen has a fresh status dot. Never throws.
 */
export async function prefillFromConfig(): Promise<void> {
	const cfg = await loadConfig();
	// Remember the binary's capture-endpoint base so the webhook wake plane can
	// build the capture URL + open the relay SSE (null under pure `vite dev`).
	captureBase.value = cfg.captureBase;
	if (cfg.defaultServer !== null) {
		const conn = ensureConnection({ name: "Default server", baseUrl: cfg.defaultServer });
		void probeConnection(conn.id);
	}
}

/**
 * Initialize the store once at startup: apply theme; if a connection was
 * restored, refresh its streams + probe it; probe all saved connections so the
 * start-screen dots light up; and prefill from the runtime config. Safe to call
 * multiple times.
 */
export function initStore(): void {
	applyTheme(theme.value);
	const restored = activeConnectionId.value;
	if (restored !== null) {
		subscriptionIds.value = loadSubscriptionIds(restored);
		metricsUrl.value = loadMetricsUrl(restored);
		void refreshStreams();
	}
	void probeAllConnections();
	void prefillFromConfig();
}
