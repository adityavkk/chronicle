/**
 * ProtocolPanel — the "Under the hood" disclosure (Feature 4).
 *
 * Progressive disclosure of the Durable Streams HTTP protocol: collapsed by
 * default so a beginner is not overwhelmed, it expands to show the exact
 * exchange dsClient captured for the current action, structured as a real HTTP
 * transcript rather than a debug dump:
 *
 *   ┌──────────────────────────────────────────────────────────┐
 *   │ ▸ Under the hood            GET · 200 OK · 12 ms           │  summary
 *   ├──────────────────────────────────────────────────────────┤
 *   │ REQUEST                                       [Copy curl]  │
 *   │   GET  {origin}{path}                                      │
 *   │   query   offset = -1   (← the cursor; -1 = beginning)     │
 *   │   headers Accept (the request headers sent)               │
 *   ├──────────────────────────────────────────────────────────┤
 *   │ RESPONSE                                     200 OK · 12 ms │
 *   │   Stream-Next-Offset  42     "send this as ?offset next…"  │
 *   │   Stream-Up-To-Date   true   "you reached the tail"        │
 *   │   … (each protocol header, value or honest "not present")  │
 *   ├──────────────────────────────────────────────────────────┤
 *   │ What's an offset?  short primer tailored to this read      │
 *   └──────────────────────────────────────────────────────────┘
 *
 * It renders purely from the captured {@link HttpExchange} — no re-fetching —
 * so it is honest about exactly what went over the wire, including which
 * headers the server did NOT send. All the explanatory copy and classification
 * lives in lib/protocol (pure + tested); this component only lays it out.
 *
 * Extensibility seam: this is a self-contained `<details>` driven by one
 * exchange. Drop it anywhere a captured exchange is in scope (the workspace
 * uses store.lastExchange). Add a section by adding a `<ProtoSection>`.
 */

import type { ComponentChildren, JSX } from "preact";
import {
	type ProtocolHeaderRow,
	exchangeOutcome,
	explainOffset,
	protocolHeaderRows,
	splitUrl,
	statusLabel,
	toCurl,
} from "../lib/protocol";
import {
	type LiveTailMode,
	describeTailMode,
	tailStatusDetail,
	tailStatusLabel,
	tailToCurl,
	tailTone,
} from "../lib/tail";
import type { HttpExchange, Operation, TailStatus } from "../lib/types";
import { CopyButton } from "./CopyButton";
import { IconBroadcast, IconChevronRight, IconCode, IconCornerDownRight } from "./icons";

/** The live-tail connection the disclosure is currently following, if any. */
export interface TailDisclosure {
	/** The GET …?offset=X&live=… operation being followed. */
	readonly operation: Operation;
	/** Its lifecycle status (drives the status pill + announcement copy). */
	readonly status: TailStatus;
	/** Which transport (long-poll | sse) — selects curl -N for SSE. */
	readonly mode: LiveTailMode;
	/** The offset the tail started from, for the resume/primer copy. */
	readonly fromOffset: string;
}

/* ---------------------------------------------------------------------------
 * Small building blocks
 * ------------------------------------------------------------------------ */

/** A coloured method chip (GET/HEAD/PUT/POST/DELETE). */
function MethodChip(props: { method: string }): JSX.Element {
	const m = props.method.toLowerCase();
	return <span class={`dsui-method dsui-method--${m}`}>{props.method.toUpperCase()}</span>;
}

/** A status pill coloured by outcome (ok / err / network failure). */
function StatusPill(props: { exchange: HttpExchange }): JSX.Element {
	const outcome = exchangeOutcome(props.exchange);
	return (
		<span class={`dsui-proto__status dsui-proto__status--${outcome}`}>
			{statusLabel(props.exchange)}
		</span>
	);
}

/** A labelled section heading inside the disclosure body. */
function ProtoSection(props: {
	label: string;
	action?: JSX.Element | undefined;
	children: ComponentChildren;
}): JSX.Element {
	return (
		<section class="dsui-proto__section">
			<div class="dsui-proto__sectionhead">
				<span class="dsui-proto__sectionlabel">{props.label}</span>
				{props.action !== undefined ? props.action : null}
			</div>
			{props.children}
		</section>
	);
}

/* ---------------------------------------------------------------------------
 * Request transcript
 * ------------------------------------------------------------------------ */

function RequestBlock(props: { exchange: HttpExchange }): JSX.Element {
	const { exchange } = props;
	const { base, query } = splitUrl(exchange.url);
	const reqHeaders = Object.entries(exchange.requestHeaders);
	return (
		<ProtoSection
			label="Request"
			action={
				<CopyButton
					text={toCurl(exchange)}
					label="Copy this request as a curl command"
					copyKey="proto-curl"
					variant="pill"
					pillLabel="Copy as curl"
				/>
			}
		>
			<div class="dsui-proto__reqline">
				<MethodChip method={exchange.method} />
				<code class="dsui-proto__url" title={exchange.url}>
					{base}
				</code>
			</div>

			{query.length > 0 ? (
				<dl class="dsui-proto__kv" aria-label="Query parameters">
					{query.map(([key, value]) => (
						<div key={key} class="dsui-proto__kvrow">
							<dt class="dsui-proto__kvkey">{key}</dt>
							<dd class="dsui-proto__kvval">
								<code>{value}</code>
								{key === "offset" ? (
									<span class="dsui-proto__kvnote">
										the cursor sent over the wire — <code>{value}</code> is exactly what this read
										requested
									</span>
								) : null}
							</dd>
						</div>
					))}
				</dl>
			) : (
				<p class="dsui-proto__muted">No query parameters.</p>
			)}

			{reqHeaders.length > 0 ? (
				<dl class="dsui-proto__kv" aria-label="Request headers">
					{reqHeaders.map(([name, value]) => (
						<div key={name} class="dsui-proto__kvrow">
							<dt class="dsui-proto__kvkey dsui-proto__kvkey--mono">{name}</dt>
							<dd class="dsui-proto__kvval">
								<code>{value}</code>
							</dd>
						</div>
					))}
				</dl>
			) : null}
		</ProtoSection>
	);
}

/* ---------------------------------------------------------------------------
 * Response transcript
 * ------------------------------------------------------------------------ */

/** One protocol header row: name, value-or-absent, plain-language note. */
function HeaderRow(props: { row: ProtocolHeaderRow }): JSX.Element {
	const { row } = props;
	const present = row.value !== null;
	return (
		<div class={`dsui-proto__hrow${present ? " is-present" : ""}`}>
			<dt class="dsui-proto__hname">{row.name}</dt>
			<dd class="dsui-proto__hval">
				{present ? (
					<code class="dsui-proto__hcode">{row.value}</code>
				) : (
					<span class="dsui-proto__absent">not sent on this response</span>
				)}
				<span class="dsui-proto__note">{row.note}</span>
			</dd>
		</div>
	);
}

function ResponseBlock(props: { exchange: HttpExchange }): JSX.Element {
	const { exchange } = props;
	const rows = protocolHeaderRows(exchange);
	const failed = exchange.status === 0;
	return (
		<ProtoSection
			label="Response"
			action={
				<span class="dsui-proto__timing">
					<StatusPill exchange={exchange} />
					<span class="dsui-proto__ms">{exchange.durationMs} ms</span>
				</span>
			}
		>
			{failed ? (
				<p class="dsui-proto__fail">
					The request never produced a response — usually a network error or a server that is not
					reachable. {exchange.error ?? ""}
				</p>
			) : (
				<dl class="dsui-proto__headers" aria-label="Protocol response headers">
					{rows.map((row) => (
						<HeaderRow key={row.name} row={row} />
					))}
				</dl>
			)}
		</ProtoSection>
	);
}

/* ---------------------------------------------------------------------------
 * Offset primer + the panel
 * ------------------------------------------------------------------------ */

function OffsetPrimer(props: { exchange: HttpExchange }): JSX.Element {
	const { exchange } = props;
	const { query } = splitUrl(exchange.url);
	const requested = query.find(([k]) => k === "offset")?.[1] ?? "";
	const next = exchange.protocol.streamNextOffset;
	return (
		<section class="dsui-proto__primer">
			<p class="dsui-proto__primerlead">What's an offset?</p>
			<p class="dsui-proto__primerbody">{explainOffset(requested)}</p>
			{next !== null ? (
				<p class="dsui-proto__resume">
					<IconCornerDownRight size={14} class="dsui-proto__resumeicon" />
					<span>
						To read the next batch, send <code>?offset={next}</code> — the value the server just
						returned in <code>Stream-Next-Offset</code>.
					</span>
				</p>
			) : null}
		</section>
	);
}

/* ---------------------------------------------------------------------------
 * Live connection block (shown while a long-poll / SSE tail is open)
 * ------------------------------------------------------------------------ */

/** A pill coloured by the live-tail status tone (reuses the proto status hues). */
function LiveStatusPill(props: { status: TailStatus }): JSX.Element {
	const tone = tailTone(props.status);
	// Map the tail tone onto the proto status palette: ok→ok, warn/pending→fail,
	// err→err, idle→err-less neutral via fail (idle never renders here).
	const cls = tone === "ok" ? "ok" : tone === "err" ? "err" : "fail";
	return (
		<span class={`dsui-proto__status dsui-proto__status--${cls}`}>
			{tailStatusLabel(props.status)}
		</span>
	);
}

/**
 * The live-connection transcript: the long-lived GET that is being followed,
 * its current status, and the equivalent curl (curl -N for SSE). It is honest
 * about SSE having no per-request captured exchange — this Operation is the live
 * request itself, not a completed round-trip.
 */
function LiveBlock(props: { tail: TailDisclosure }): JSX.Element {
	const { operation, status, mode, fromOffset } = props.tail;
	const { base, query } = splitUrl(operation.url);
	const command = tailToCurl(operation, mode);
	const detail = tailStatusDetail(status);
	return (
		<ProtoSection
			label={`Live connection · ${describeTailMode(mode)}`}
			action={
				<CopyButton
					text={command}
					label="Copy the live tail request as a curl command"
					copyKey="proto-tail-curl"
					variant="pill"
					pillLabel="Copy as curl"
				/>
			}
		>
			<div class="dsui-proto__reqline">
				<MethodChip method={operation.method} />
				<code class="dsui-proto__url" title={operation.url}>
					{base}
				</code>
				<LiveStatusPill status={status} />
			</div>
			{query.length > 0 ? (
				<dl class="dsui-proto__kv" aria-label="Live query parameters">
					{query.map(([key, value]) => (
						<div key={key} class="dsui-proto__kvrow">
							<dt class="dsui-proto__kvkey">{key}</dt>
							<dd class="dsui-proto__kvval">
								<code>{value}</code>
								{key === "live" ? (
									<span class="dsui-proto__kvnote">
										the live mode — the server holds the connection open and streams new messages
										instead of returning a single batch
									</span>
								) : key === "offset" ? (
									<span class="dsui-proto__kvnote">
										the cursor the tail started from — <code>{fromOffset}</code>
									</span>
								) : null}
							</dd>
						</div>
					))}
				</dl>
			) : null}
			{detail !== "" ? <p class="dsui-proto__muted">{detail}</p> : null}
		</ProtoSection>
	);
}

export interface ProtocolPanelProps {
	/** The captured exchange to disclose, or null when nothing has run yet. */
	readonly exchange: HttpExchange | null;
	/** The live tail being followed, if any — shown as a Live connection block. */
	readonly tail?: TailDisclosure | null | undefined;
}

/**
 * The collapsed-by-default "Under the hood" disclosure. Renders nothing until
 * there is something to show — a captured exchange OR an open live tail — so it
 * never occupies space on an empty view. When a tail is open it adds a Live
 * connection block (the long-lived GET …&live=… and its status) above the last
 * captured exchange, and the summary reflects the live status.
 */
export function ProtocolPanel(props: ProtocolPanelProps): JSX.Element | null {
	const { exchange } = props;
	const tail = props.tail ?? null;
	if (exchange === null && tail === null) return null;
	return (
		<details class="dsui-proto">
			<summary class="dsui-proto__summary">
				<IconChevronRight size={13} class="dsui-proto__summarycaret" />
				{tail !== null ? (
					<IconBroadcast size={14} class="dsui-proto__summaryicon" />
				) : (
					<IconCode size={14} class="dsui-proto__summaryicon" />
				)}
				<span class="dsui-proto__summarytitle">Under the hood</span>
				<span class="dsui-proto__summaryhint">
					{tail !== null ? "the live connection" : "the real HTTP exchange"}
				</span>
				<span class="dsui-proto__summarymeta">
					{tail !== null ? (
						<LiveStatusPill status={tail.status} />
					) : exchange !== null ? (
						<>
							<MethodChip method={exchange.method} />
							<StatusPill exchange={exchange} />
						</>
					) : null}
				</span>
			</summary>
			<div class="dsui-proto__body">
				{tail !== null ? <LiveBlock tail={tail} /> : null}
				{exchange !== null ? (
					<>
						<RequestBlock exchange={exchange} />
						<ResponseBlock exchange={exchange} />
						<OffsetPrimer exchange={exchange} />
					</>
				) : null}
			</div>
		</details>
	);
}
