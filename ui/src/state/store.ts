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
import { loadConfig } from "../lib/config";
import { type DsClient, createClient } from "../lib/dsClient";
import { isRecord, kindFromContentType } from "../lib/guards";
import { DEFAULT_ROW_CAP, type StartMode, clampRowCap, resolveOffset } from "../lib/messages";
import { isLiveMode, previewTailOperation } from "../lib/tail";
import type {
	Connection,
	ConnectionProbe,
	GridRow,
	HttpExchange,
	Operation,
	ProbeStatus,
	ProducerIdentity,
	ReadResult,
	StreamContentType,
	StreamInfo,
	TailBatch,
	TailMode,
	TailStatus,
	TailStopper,
	Theme,
	Toast,
	ToastAction,
	ToastKind,
} from "../lib/types";

// Re-exported for back-compat: ProbeStatus now lives in lib/types (a shared
// contract), but existing imports of it from the store keep working.
export type { ProbeStatus } from "../lib/types";

const LS_CONNECTIONS = "dsui.connections";
const LS_ACTIVE = "dsui.activeConnection";
const LS_THEME = "dsui.theme";
const LS_INSPECTOR_COLLAPSED = "dsui.inspector-collapsed";
const LS_PLAYGROUND_OPEN = "dsui.playground-open";

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
export const activeDialog = signal<"create" | "fork" | null>(null);

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

/** The stopper for the active tail, or null when not tailing. Not reactive UI. */
let tailStopper: TailStopper | null = null;

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
	tailRows.value = [];
	tailDropped.value = 0;
}

/** Append a received tail batch into the capped buffer (unless paused). */
function pushTailBatch(batch: TailBatch): void {
	if (tailPaused.value) return;
	if (batch.rows.length === 0) return;
	const combined = [...tailRows.value, ...batch.rows];
	if (combined.length > TAIL_BUFFER_CAP) {
		const overflow = combined.length - TAIL_BUFFER_CAP;
		tailDropped.value += overflow;
		tailRows.value = combined.slice(overflow);
	} else {
		tailRows.value = combined;
	}
	if (batch.exchange !== null) lastExchange.value = batch.exchange;
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
	tailStatus.value = { state: "idle" };
	tailOperation.value = null;
	tailStartOffset.value = null;
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
	if (activeConnectionId.value !== null) {
		void refreshStreams();
	}
	void probeAllConnections();
	void prefillFromConfig();
}
