/**
 * Inspector — the right panel: the decoded detail view for the selected grid
 * row, plus a per-stream "Raw" view of the exact response bytes and headers.
 *
 *   ┌──────────────────────────────────────┐
 *   │ tabs: Value · Raw · Headers            │
 *   ├──────────────────────────────────────┤
 *   │ Value: metadata block (index / size / │
 *   │   kind / time / batch offset range)    │
 *   │        decoded body: pretty JSON, text │
 *   │        verbatim, or a hex dump          │
 *   │ Raw:   exact bytes (text or hex) + size │
 *   │ Headers: every captured response header │
 *   └──────────────────────────────────────┘
 *
 * Content-type aware, mirroring the grid: JSON elements pretty-print, text
 * chunks render verbatim, binary renders as a hex dump. The metadata block is
 * honest about the protocol — there is no per-element offset, only the batch's
 * [requested → Stream-Next-Offset] range, so it shows the element index within
 * the batch and that range side by side.
 *
 * Extensibility seam: the inspector is a small tab strip + panel; add a tab by
 * extending the `Tab` union and the panel switch in `Inspector`.
 */

import { signal } from "@preact/signals";
import type { JSX } from "preact";
import { useId, useRef } from "preact/hooks";
import { extractTimestamp, formatBytes, formatTimeFull } from "../lib/messages";
import { isSignificantHeader, partitionHeaders } from "../lib/protocol";
import type { GridRow, ReadResult } from "../lib/types";
import { lastRead, selectedRow, toggleInspector } from "../state/store";
import { CopyButton } from "./CopyButton";
import { IconChevronRight } from "./icons";

type Tab = "value" | "raw" | "headers";

const activeTab = signal<Tab>("value");

/** The tab strip, in DOM/navigation order. */
const TABS: readonly { id: Tab; label: string }[] = [
	{ id: "value", label: "Value" },
	{ id: "raw", label: "Raw" },
	{ id: "headers", label: "Headers" },
];

/* ---------------------------------------------------------------------------
 * Hex dump
 * ------------------------------------------------------------------------ */

function hexDump(bytes: Uint8Array, max = 512): string {
	const slice = bytes.subarray(0, max);
	const lines: string[] = [];
	for (let i = 0; i < slice.length; i += 16) {
		const chunk = slice.subarray(i, i + 16);
		const hex: string[] = [];
		let ascii = "";
		for (let j = 0; j < 16; j++) {
			const b = chunk[j];
			if (b === undefined) {
				hex.push("  ");
			} else {
				hex.push(b.toString(16).padStart(2, "0"));
				ascii += b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : ".";
			}
		}
		const offset = i.toString(16).padStart(8, "0");
		const left = hex.slice(0, 8).join(" ");
		const right = hex.slice(8).join(" ");
		lines.push(`${offset}  ${left}  ${right}  |${ascii}|`);
	}
	if (bytes.length > max) lines.push(`… (${bytes.length - max} more bytes)`);
	return lines.join("\n");
}

/** Pretty-print a JSON value, falling back to String() on a cyclic value. */
function prettyJson(value: unknown): string {
	try {
		return JSON.stringify(value, null, 2);
	} catch {
		return String(value);
	}
}

/* ---------------------------------------------------------------------------
 * Value panel: metadata block + decoded body
 * ------------------------------------------------------------------------ */

/** One label/value pair in the metadata grid. */
function MetaRow(props: { label: string; children: JSX.Element | string }): JSX.Element {
	return (
		<div class="dsui-meta__row">
			<dt class="dsui-meta__label">{props.label}</dt>
			<dd class="dsui-meta__value">{props.children}</dd>
		</div>
	);
}

/** Honest, content-aware metadata for the selected row. */
function MetaBlock(props: { row: GridRow; read: ReadResult | null }): JSX.Element {
	const { row, read } = props;
	const ts = row.kind === "json" ? extractTimestamp(row.value) : null;
	const fullTime = formatTimeFull(ts);
	return (
		<dl class="dsui-meta" aria-label="Message metadata">
			<MetaRow label="Element index">
				<code>{String(row.index)}</code>
			</MetaRow>
			<MetaRow label="Size">{formatBytes(row.byteSize)}</MetaRow>
			<MetaRow label="Kind">
				<span class={`dsui-kind dsui-kind--${row.kind}`}>{row.kind}</span>
			</MetaRow>
			{ts !== null ? (
				<MetaRow label="Time">
					<code>{fullTime}</code>
				</MetaRow>
			) : null}
			{read !== null ? (
				<MetaRow label="Batch offset">
					<span class="dsui-meta__range">
						<code>{read.requestedOffset}</code>
						<span class="dsui-meta__arrow">→</span>
						<code>{read.nextOffset ?? "—"}</code>
					</span>
				</MetaRow>
			) : null}
			<p class="dsui-meta__note">
				The protocol returns a batch plus a single <code>Stream-Next-Offset</code>, not a
				per-element offset — so this element is identified by its index within the batch above.
			</p>
		</dl>
	);
}

function ValuePanel(props: { row: GridRow; read: ReadResult | null }): JSX.Element {
	const { row, read } = props;
	let body: string;
	let hex = false;
	if (row.kind === "json") {
		body = prettyJson(row.value);
	} else if (row.kind === "text") {
		body = String(row.value);
	} else {
		const bytes = row.value instanceof Uint8Array ? row.value : new Uint8Array(0);
		body = hexDump(bytes);
		hex = true;
	}
	return (
		<div class="dsui-value">
			<MetaBlock row={row} read={read} />
			<div class="dsui-value__bodyhead">
				<span class="dsui-value__bodylabel">{hex ? "Bytes" : "Decoded body"}</span>
				{hex ? null : <CopyButton text={body} label="Copy value" copyKey="value" />}
			</div>
			<pre class={`dsui-pre${hex ? " dsui-pre--hex" : ""}`}>{body}</pre>
		</div>
	);
}

/* ---------------------------------------------------------------------------
 * Raw + Headers panels
 * ------------------------------------------------------------------------ */

function RawPanel(props: { read: ReadResult }): JSX.Element {
	const { read } = props;
	const bytes = read.rawBytes;
	const isText = read.kind === "json" || read.kind === "text";
	const text = isText ? new TextDecoder().decode(bytes) : hexDump(bytes);
	return (
		<div class="dsui-raw">
			<div class="dsui-raw__head">
				<span class="dsui-raw__meta">
					{bytes.byteLength} bytes · {read.kind} · exact response body
				</span>
				{isText && text !== "" ? (
					<CopyButton text={text} label="Copy raw body" copyKey="raw" />
				) : null}
			</div>
			{bytes.byteLength === 0 ? (
				<p class="dsui-empty__hint">The response body was empty.</p>
			) : (
				<pre class={`dsui-pre${isText ? "" : " dsui-pre--hex"}`}>{text}</pre>
			)}
		</div>
	);
}

/** One response-header row. Protocol-significant headers get a marker + accent. */
function HeaderEntry(props: { name: string; value: string }): JSX.Element {
	const significant = isSignificantHeader(props.name);
	return (
		<div class={`dsui-headers__row${significant ? " is-significant" : ""}`}>
			<dt>
				{significant ? (
					<span
						class="dsui-headers__star"
						title="Durable Streams protocol header"
						aria-hidden="true"
					>
						●
					</span>
				) : null}
				{props.name}
			</dt>
			<dd>
				<code>{props.value}</code>
			</dd>
		</div>
	);
}

function HeadersPanel(props: { read: ReadResult }): JSX.Element {
	const { significant, other } = partitionHeaders(props.read.exchange.responseHeaders);
	if (significant.length === 0 && other.length === 0) {
		return <p class="dsui-empty__hint">No response headers captured.</p>;
	}
	return (
		<div class="dsui-headers">
			{significant.length > 0 ? (
				<p class="dsui-headers__legend">
					<span class="dsui-headers__star" aria-hidden="true">
						●
					</span>
					Durable Streams protocol headers — explained under the hood in the workspace.
				</p>
			) : null}
			<dl class="dsui-headers__list">
				{significant.map(([k, v]) => (
					<HeaderEntry key={k} name={k} value={v} />
				))}
				{other.map(([k, v]) => (
					<HeaderEntry key={k} name={k} value={v} />
				))}
			</dl>
		</div>
	);
}

/* ---------------------------------------------------------------------------
 * Tabs + Inspector
 * ------------------------------------------------------------------------ */

/** Ids for one tab and its panel, derived from a stable per-instance base. */
function tabIds(base: string, tab: Tab): { tabId: string; panelId: string } {
	return { tabId: `${base}-tab-${tab}`, panelId: `${base}-panel-${tab}` };
}

function TabButton(props: {
	tab: Tab;
	label: string;
	base: string;
	onKeyDown: (e: KeyboardEvent) => void;
}): JSX.Element {
	const active = activeTab.value === props.tab;
	const { tabId, panelId } = tabIds(props.base, props.tab);
	return (
		<button
			type="button"
			role="tab"
			id={tabId}
			aria-selected={active}
			aria-controls={panelId}
			// Roving tabindex: only the selected tab is in the Tab sequence; the
			// rest are reached with ArrowLeft/ArrowRight/Home/End.
			tabIndex={active ? 0 : -1}
			data-inspectortab="true"
			class={`dsui-tab${active ? " is-active" : ""}`}
			onClick={() => {
				activeTab.value = props.tab;
			}}
			onKeyDown={props.onKeyDown}
		>
			{props.label}
		</button>
	);
}

export function Inspector(): JSX.Element {
	const row = selectedRow.value;
	const read = lastRead.value;
	const tab = activeTab.value;
	const base = useId();
	const tablistRef = useRef<HTMLDivElement>(null);

	/** Move both selection and focus to the tab at index (wrapping). */
	function activateAt(index: number): void {
		const count = TABS.length;
		const next = TABS[((index % count) + count) % count];
		if (next === undefined) return;
		activeTab.value = next.id;
		const buttons = tablistRef.current?.querySelectorAll<HTMLButtonElement>("[data-inspectortab]");
		buttons?.item(((index % count) + count) % count)?.focus();
	}

	function onTabKeyDown(e: KeyboardEvent): void {
		const current = TABS.findIndex((t) => t.id === activeTab.value);
		if (current < 0) return;
		switch (e.key) {
			case "ArrowRight":
			case "ArrowDown":
				e.preventDefault();
				activateAt(current + 1);
				break;
			case "ArrowLeft":
			case "ArrowUp":
				e.preventDefault();
				activateAt(current - 1);
				break;
			case "Home":
				e.preventDefault();
				activateAt(0);
				break;
			case "End":
				e.preventDefault();
				activateAt(TABS.length - 1);
				break;
			default:
				break;
		}
	}

	const { tabId, panelId } = tabIds(base, tab);

	return (
		<aside class="dsui-inspector" aria-label="Inspector">
			<header class="dsui-inspector__head">
				<div class="dsui-tabs" role="tablist" aria-label="Inspector views" ref={tablistRef}>
					{TABS.map((t) => (
						<TabButton key={t.id} tab={t.id} label={t.label} base={base} onKeyDown={onTabKeyDown} />
					))}
				</div>
				<button
					type="button"
					class="dsui-iconbtn dsui-iconbtn--sm dsui-inspector__collapse"
					aria-label="Collapse inspector panel"
					title="Collapse inspector panel"
					onClick={() => toggleInspector()}
				>
					<IconChevronRight size={15} />
				</button>
			</header>
			<div class="dsui-inspector__body" role="tabpanel" id={panelId} aria-labelledby={tabId}>
				{tab === "value" ? (
					row === null ? (
						<div class="dsui-empty">
							<p class="dsui-empty__title">No row selected</p>
							<p class="dsui-empty__hint">Select a message row to inspect it.</p>
						</div>
					) : (
						<ValuePanel row={row} read={read} />
					)
				) : tab === "raw" ? (
					read === null ? (
						<p class="dsui-empty__hint">Read a stream to see its raw bytes.</p>
					) : (
						<RawPanel read={read} />
					)
				) : read === null ? (
					<p class="dsui-empty__hint">Read a stream to see its response headers.</p>
				) : (
					<HeadersPanel read={read} />
				)}
			</div>
		</aside>
	);
}
