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
import { isRecord } from "../lib/guards";
import { DEFAULT_ROW_CAP, type StartMode, clampRowCap, resolveOffset } from "../lib/messages";
import type {
	Connection,
	ConnectionProbe,
	GridRow,
	HttpExchange,
	ProbeStatus,
	ReadResult,
	StreamInfo,
	Theme,
} from "../lib/types";

// Re-exported for back-compat: ProbeStatus now lives in lib/types (a shared
// contract), but existing imports of it from the store keep working.
export type { ProbeStatus } from "../lib/types";

const LS_CONNECTIONS = "dsui.connections";
const LS_ACTIVE = "dsui.activeConnection";
const LS_THEME = "dsui.theme";

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

/** Clear the surfaced error. */
export function dismissError(): void {
	errorMessage.value = null;
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
