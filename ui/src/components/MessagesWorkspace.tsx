/**
 * MessagesWorkspace — the center region. It is a vertical stack of <section>s:
 *
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │ head: stream path · kind · honest batch offset range + pills   │
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ toolbar: [Catch-up|Long-poll|SSE] · [Earliest|Latest|At…] ·    │
 *   │          rows cap (catch-up) · Read/Refresh OR Start/Stop tail │
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ catch-up: grid (#·size·time?·preview) + Read-next-batch pager  │
 *   │ live:     <TailPanel> (status badge · pause · stop · live grid)│
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ "Under the hood" protocol disclosure (collapsed by default)    │
 *   └──────────────────────────────────────────────────────────────┘
 *
 * The Mode segmented control (tailMode) chooses between the paged read path and
 * a live tail. In catch-up mode the toolbar mutates startMode / customOffset /
 * rowCap and calls readFromToolbar(), which resolves the choice into a concrete
 * protocol offset (lib/messages.resolveOffset) via dsClient.readStream; the grid
 * is content-type aware (one row per JSON element; a Time column only when rows
 * carry a timestamp). In a live mode (long-poll | sse) the primary control is
 * Start tail / Stop, which calls store.startTail(resolveOffset(...)) / stopTail,
 * and the body renders <TailPanel> instead of the paged grid. Either way the
 * "Under the hood" disclosure reflects the current exchange — and, while
 * tailing, the live connection (the GET …&live=… request + status) via the
 * tailDisclosure built from the store's tailOperation / tailStatus.
 *
 * Extensibility seam: add a toolbar control inside .dsui-toolbar (the Segmented
 * helper is reusable for any roving-tabindex picker), or a new section between
 * the toolbar and the grid. Keep reads + the tail lifecycle in the store.
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useLayoutEffect, useRef } from "preact/hooks";
import { relativeTime } from "../lib/format";
import {
	ROW_CAP_OPTIONS,
	type StartMode,
	batchHasTimes,
	compileQuery,
	extractTimestamp,
	formatBytes,
	formatTime,
	matchCompiled,
	resolveOffset,
} from "../lib/messages";
import { offsetChipLabel } from "../lib/readHistory";
import { describeTailMode, isLiveMode } from "../lib/tail";
import type { GridRow, ReadHistoryEntry, TailMode } from "../lib/types";
import { ROW_HEIGHT, WINDOW_THRESHOLD, windowRange } from "../lib/virtual";
import {
	customOffset,
	lastExchange,
	lastRead,
	readFromToolbar,
	readHistory,
	readLoading,
	readNext,
	readSelected,
	rowCap,
	rowsTruncated,
	selectRow,
	selectedRow,
	selectedStream,
	setCustomOffset,
	setRowCap,
	setStartMode,
	setTailMode,
	startMode,
	startTail,
	stopTail,
	tailMode,
	tailOperation,
	tailStartOffset,
	tailStatus,
} from "../state/store";
import { CopyButton } from "./CopyButton";
import { ExportMenu } from "./ExportMenu";
import { ProtocolPanel, type TailDisclosure } from "./ProtocolPanel";
import { PublishComposer } from "./PublishComposer";
import { RowFilter } from "./RowFilter";
import { StreamActionsMenu } from "./StreamActionsMenu";
import { TailPanel } from "./TailPanel";
import {
	IconBroadcast,
	IconChevronDown,
	IconChevronRight,
	IconClock,
	IconClose,
	IconCornerDownRight,
	IconHistory,
	IconPlay,
	IconRefresh,
	IconSearch,
	IconStop,
} from "./icons";

/* ---------------------------------------------------------------------------
 * Toolbar
 * ------------------------------------------------------------------------ */

const START_OPTIONS: readonly { value: StartMode; label: string; title: string }[] = [
	{ value: "earliest", label: "Earliest", title: "Read from the beginning (offset -1)" },
	{ value: "latest", label: "Latest", title: "Read from the current tail (offset now)" },
	{ value: "at", label: "At offset…", title: "Read from an explicit opaque offset cursor" },
];

/** The read-mode choices: paged catch-up vs the two live-tail transports. */
const MODE_OPTIONS: readonly { value: TailMode; label: string; title: string }[] = [
	{ value: "catchup", label: "Catch-up", title: "Read a batch at a time and page forward by hand" },
	{
		value: "long-poll",
		label: "Long-poll",
		title: "Follow the tail by long-polling (GET …&live=long-poll)",
	},
	{ value: "sse", label: "SSE", title: "Follow the tail over Server-Sent Events (GET …&live=sse)" },
];

/**
 * A roving-tabindex segmented control (one tab stop, arrow keys move between
 * segments) shared by the Start-position and the Read-mode pickers. Mirrors the
 * accessible pattern the toolbar already used for the Start control.
 */
function Segmented<T extends string>(props: {
	label: string;
	labelId: string;
	value: T;
	options: readonly { value: T; label: string; title: string }[];
	onSelect: (value: T) => void;
}): JSX.Element {
	const { label, labelId, value, options, onSelect } = props;
	const ref = useRef<HTMLDivElement>(null);

	function activate(index: number): void {
		const count = options.length;
		const wrapped = ((index % count) + count) % count;
		const next = options[wrapped];
		if (next === undefined) return;
		onSelect(next.value);
		ref.current?.querySelectorAll<HTMLButtonElement>("[data-segment]").item(wrapped)?.focus();
	}

	function onKeyDown(e: KeyboardEvent): void {
		const current = options.findIndex((o) => o.value === value);
		if (current < 0) return;
		switch (e.key) {
			case "ArrowRight":
			case "ArrowDown":
				e.preventDefault();
				activate(current + 1);
				break;
			case "ArrowLeft":
			case "ArrowUp":
				e.preventDefault();
				activate(current - 1);
				break;
			case "Home":
				e.preventDefault();
				activate(0);
				break;
			case "End":
				e.preventDefault();
				activate(options.length - 1);
				break;
			default:
				break;
		}
	}

	return (
		<>
			<span class="dsui-toolbar__label" id={labelId}>
				{label}
			</span>
			<div
				class="dsui-segmented"
				// biome-ignore lint/a11y/useSemanticElements: a <fieldset> cannot host an arrow-key roving toolbar segment group; role="group" is the correct ARIA container for these aria-pressed toggle buttons.
				role="group"
				aria-labelledby={labelId}
				ref={ref}
			>
				{options.map((opt) => (
					<button
						key={opt.value}
						type="button"
						aria-pressed={value === opt.value}
						aria-label={`${label}: ${opt.label}`}
						tabIndex={value === opt.value ? 0 : -1}
						data-segment="true"
						class={`dsui-segmented__btn${value === opt.value ? " is-active" : ""}`}
						title={opt.title}
						onClick={() => onSelect(opt.value)}
						onKeyDown={onKeyDown}
					>
						{opt.label}
					</button>
				))}
			</div>
		</>
	);
}

/**
 * The workspace read toolbar. A Mode picker (Catch-up | Long-poll | SSE) chooses
 * between the paged read path and a live tail; the Start picker + offset input
 * choose where to begin (a tail can replay from Earliest, jump to Latest, or
 * start At an offset). The primary control switches with the mode: Read/Refresh
 * for catch-up, Start tail / Stop for a live mode.
 */
function Toolbar(props: { hasRead: boolean }): JSX.Element {
	const start = startMode.value;
	const mode = tailMode.value;
	const loading = readLoading.value;
	const status = tailStatus.value;
	const live = isLiveMode(mode);
	// A live connection is "running" while connecting or connected; closed/error/
	// idle are settled, so the control offers Start again.
	const tailRunning = status.state === "connecting" || status.state === "live";

	function startTailNow(): void {
		startTail(resolveOffset(start, customOffset.value));
	}

	return (
		<div class="dsui-toolbar" role="toolbar" aria-label="Read controls">
			<div class="dsui-toolbar__group">
				<Segmented
					label="Mode"
					labelId="dsui-mode-label"
					value={mode}
					options={MODE_OPTIONS}
					onSelect={setTailMode}
				/>
			</div>

			<div class="dsui-toolbar__group">
				<Segmented
					label="Start"
					labelId="dsui-start-label"
					value={start}
					options={START_OPTIONS}
					onSelect={setStartMode}
				/>
				{start === "at" ? (
					<input
						type="text"
						class="dsui-toolbar__offset"
						placeholder="offset cursor…"
						aria-label="Offset cursor to read from"
						value={customOffset.value}
						autocomplete="off"
						spellcheck={false}
						onInput={(e) => setCustomOffset(e.currentTarget.value)}
						onKeyDown={(e) => {
							if (e.key !== "Enter") return;
							// Gate the read path on !loading, mirroring the Read button / pager /
							// history chips, so repeated Enter cannot launch overlapping reads
							// that resolve out of order and reshuffle the history.
							if (live) startTailNow();
							else if (!loading) void readFromToolbar();
						}}
					/>
				) : null}
			</div>

			{!live ? (
				<div class="dsui-toolbar__group">
					<label class="dsui-toolbar__label" for="dsui-rowcap">
						Rows
					</label>
					<select
						id="dsui-rowcap"
						class="dsui-toolbar__select"
						value={String(rowCap.value)}
						onChange={(e) => setRowCap(Number(e.currentTarget.value))}
					>
						{ROW_CAP_OPTIONS.map((n) => (
							<option key={n} value={String(n)}>
								{n}
							</option>
						))}
					</select>
				</div>
			) : null}

			<div class="dsui-toolbar__spacer" />

			{live ? (
				tailRunning ? (
					<button
						type="button"
						class="dsui-btn dsui-btn--danger"
						title="Stop the live tail and close the connection"
						onClick={() => stopTail()}
					>
						<IconStop size={14} />
						<span>Stop tail</span>
					</button>
				) : (
					<button
						type="button"
						class="dsui-btn dsui-btn--primary"
						title={`Start following the tail with ${describeTailMode(mode)}`}
						onClick={startTailNow}
					>
						<IconBroadcast size={14} />
						<span>Start tail</span>
					</button>
				)
			) : (
				<button
					type="button"
					class="dsui-btn dsui-btn--primary"
					disabled={loading}
					onClick={() => void readFromToolbar()}
				>
					{props.hasRead ? (
						<IconRefresh size={14} class={loading ? "dsui-spin" : undefined} />
					) : (
						<IconPlay size={14} />
					)}
					<span>{loading ? "Reading…" : props.hasRead ? "Refresh" : "Read"}</span>
				</button>
			)}
		</div>
	);
}

/* ---------------------------------------------------------------------------
 * Grid
 * ------------------------------------------------------------------------ */

function GridSkeleton(): JSX.Element {
	return (
		<div class="dsui-grid__skel" aria-hidden="true">
			{[0, 1, 2, 3, 4, 5].map((i) => (
				<div key={i} class="dsui-skel-row">
					<span class="dsui-skel" style={{ inlineSize: "100%" }} />
				</div>
			))}
		</div>
	);
}

function Row(props: {
	row: GridRow;
	active: boolean;
	showTime: boolean;
	tabbable: boolean;
	onKeyDown: (e: KeyboardEvent) => void;
}): JSX.Element {
	const { row, active, showTime, tabbable, onKeyDown } = props;
	const ts = row.kind === "json" ? extractTimestamp(row.value) : null;
	const time = formatTime(ts);
	const rowClass = `dsui-row${showTime ? " dsui-row--timed" : ""}${active ? " is-active" : ""}`;
	const label = `Message ${row.index}, ${formatBytes(row.byteSize)}: ${row.preview}`;
	// The message list is a single-select listbox driving the inspector. Each row
	// is one role="option"; a roving tabindex (exactly one tab stop) plus
	// ArrowUp/Down/Home/End give inter-row navigation matching the streams tree,
	// instead of one Tab stop per row.
	return (
		<button
			type="button"
			// biome-ignore lint/a11y/useSemanticElements: a native <option> is not focusable or clickable as a rich row; this is a roving-tabindex listbox option driving the inspector, so role="option" on a button is correct.
			role="option"
			class={rowClass}
			onClick={() => selectRow(row)}
			tabIndex={tabbable ? 0 : -1}
			data-messagerow="true"
			data-rowindex={row.index}
			aria-selected={active}
			aria-label={label}
			onKeyDown={onKeyDown}
		>
			<span class="dsui-row__index">
				{active ? <IconChevronDown size={12} /> : <IconChevronRight size={12} />}
				<span>{row.index}</span>
			</span>
			<span class="dsui-row__size">{formatBytes(row.byteSize)}</span>
			{showTime ? (
				<span class="dsui-row__time" title={ts === null ? "no timestamp" : new Date(ts).toString()}>
					{time === "" ? "—" : time}
				</span>
			) : null}
			<span class="dsui-row__preview">{row.preview}</span>
		</button>
	);
}

/* ---------------------------------------------------------------------------
 * History strip
 * ------------------------------------------------------------------------ */

/** One clickable breadcrumb chip for a previously-visited read position. */
function HistoryChip(props: {
	entry: ReadHistoryEntry;
	current: boolean;
	disabled: boolean;
}): JSX.Element {
	const { entry, current, disabled } = props;
	const label = offsetChipLabel(entry.requestedOffset);
	const rows = `${entry.rowCount} ${entry.rowCount === 1 ? "row" : "rows"}`;
	const detail = `offset ${entry.requestedOffset} · ${rows} · ${relativeTime(entry.at)}`;
	const title = current ? `Current position · ${detail}` : `Re-read ${detail}`;
	// WCAG 2.5.3 (Label in Name): the accessible name must contain the visible
	// label (e.g. "earliest"/"latest"/the cursor) so speech-control users can
	// target the chip by what they see — so lead the name with that label.
	const ariaLabel = current
		? `${label}, current position · ${detail}`
		: `${label}, re-read · ${detail}`;
	return (
		<button
			type="button"
			class={`dsui-history__chip${current ? " is-current" : ""}`}
			aria-current={current ? "true" : undefined}
			aria-label={ariaLabel}
			title={title}
			disabled={disabled}
			onClick={() => void readSelected(entry.requestedOffset)}
		>
			{label}
		</button>
	);
}

/**
 * A compact strip of the read positions visited for this stream, newest last.
 * Each chip re-reads its cursor in one click — a navigable breadcrumb over the
 * protocol's opaque, forward-only offsets. Renders nothing until a read lands.
 */
function HistoryStrip(): JSX.Element | null {
	const history = readHistory.value;
	if (history.length === 0) return null;
	const loading = readLoading.value;
	const lastIndex = history.length - 1;
	return (
		<nav class="dsui-history" aria-label="Read history">
			<span class="dsui-history__label">
				<IconHistory size={13} />
				History
			</span>
			<div class="dsui-history__chips">
				{history.map((entry, i) => (
					<HistoryChip
						key={`${entry.at}-${i}`}
						entry={entry}
						current={i === lastIndex}
						disabled={loading}
					/>
				))}
			</div>
		</nav>
	);
}

/* ---------------------------------------------------------------------------
 * Workspace
 * ------------------------------------------------------------------------ */

export function MessagesWorkspace(): JSX.Element {
	const stream = selectedStream.value;
	const read = lastRead.value;
	const loading = readLoading.value;
	const active = selectedRow.value;
	const truncated = rowsTruncated.value;
	// gridRef is the scroll container (.dsui-grid__rows); it both reports scroll
	// geometry for windowing and is the root for row focus queries.
	const gridRef = useRef<HTMLDivElement>(null);
	// Scroll geometry driving fixed-height windowing (see the render below).
	const scrollTop = useSignal(0);
	const viewport = useSignal(0);
	// An absolute row index to focus once it scrolls into the rendered window —
	// set when arrow/Home/End navigation targets a row outside the current slice.
	const pendingFocus = useSignal<number | null>(null);

	// Component-local, instant filter over the loaded batch — never touches the
	// store (issue #53). Matching lives in lib/messages: compile the query once,
	// run it per row. row.index is preserved, so the batch-index # stays honest.
	const filter = useSignal("");
	const compiled = useComputed(() => compileQuery(filter.value));
	const visibleRows = useComputed(() => {
		// Read lastRead INSIDE the computed so it stays reactive. The read lands
		// asynchronously after mount, so closing over the render-scoped `read` const
		// would freeze this computed to the mount-time (null/empty) value — the grid
		// would then show "no rows match" forever once a read actually arrived.
		const current = lastRead.value;
		const q = compiled.value;
		const rows = current?.rows ?? [];
		return q.active ? rows.filter((r) => matchCompiled(r, q)) : rows;
	});

	// Show the Time column only when at least one row in the batch has a time.
	const showTime = useComputed(() => {
		const current = lastRead.value;
		return current === null ? false : batchHasTimes(current.rows);
	});

	/** Capture the scroll element's geometry into the windowing signals. */
	function syncMetrics(el: HTMLDivElement): void {
		scrollTop.value = el.scrollTop;
		viewport.value = el.clientHeight;
	}

	function onGridScroll(): void {
		const el = gridRef.current;
		if (el !== null) syncMetrics(el);
	}

	// Measure the viewport whenever a new batch loads, so windowing has a real
	// clientHeight before the first scroll (and tracks the clamped scrollTop when
	// a smaller batch replaces a larger one).
	// biome-ignore lint/correctness/useExhaustiveDependencies: gridRef is a stable handle; re-measure exactly when the loaded batch changes.
	useLayoutEffect(() => {
		const el = gridRef.current;
		if (el !== null) syncMetrics(el);
	}, [read]);

	// Focus a row that navigation targeted but that was outside the rendered
	// window. No dependency array: it runs after every commit so it catches the
	// target as soon as scrolling re-windows it into the DOM, then clears the
	// request (a no-op on the commits where nothing is pending).
	useLayoutEffect(() => {
		const idx = pendingFocus.value;
		if (idx === null) return;
		const el = gridRef.current?.querySelector<HTMLButtonElement>(`[data-rowindex="${idx}"]`);
		if (el !== null && el !== undefined) {
			el.focus();
			pendingFocus.value = null;
		}
	});

	/**
	 * Move roving focus to the row at the given position in the FILTERED list
	 * (clamped). If that row is already rendered, focus it directly; otherwise
	 * scroll its position into view (uniform 30px rows) and let the pendingFocus
	 * effect focus it once windowing mounts it. pendingFocus carries the row's
	 * absolute batch index (its data-rowindex) so the lookup survives re-windowing.
	 */
	function focusRow(pos: number): void {
		const list = visibleRows.value;
		if (list.length === 0) return;
		const clamped = Math.max(0, Math.min(pos, list.length - 1));
		const targetIndex = list[clamped]?.index;
		if (targetIndex === undefined) return;
		const el = gridRef.current?.querySelector<HTMLButtonElement>(
			`[data-rowindex="${targetIndex}"]`,
		);
		if (el !== null && el !== undefined) {
			el.focus();
			return;
		}
		// Outside the window: scroll the position to the top of the viewport,
		// refresh the metrics so the re-render includes it, and queue the focus.
		pendingFocus.value = targetIndex;
		const scroller = gridRef.current;
		if (scroller !== null) {
			scroller.scrollTop = clamped * ROW_HEIGHT;
			syncMetrics(scroller);
		}
	}

	/** Arrow-key roving for a row at the given position within the filtered list. */
	function onRowKeyDown(pos: number): (e: KeyboardEvent) => void {
		return (e) => {
			switch (e.key) {
				case "ArrowDown":
					e.preventDefault();
					focusRow(pos + 1);
					break;
				case "ArrowUp":
					e.preventDefault();
					focusRow(pos - 1);
					break;
				case "Home":
					e.preventDefault();
					focusRow(0);
					break;
				case "End":
					e.preventDefault();
					focusRow(visibleRows.value.length - 1);
					break;
				default:
					break;
			}
		};
	}

	if (stream === null) {
		return (
			<div class="dsui-ws dsui-ws--empty">
				<div class="dsui-empty">
					<IconPlay size={26} class="dsui-empty__icon" />
					<p class="dsui-empty__title">Select a stream</p>
					<p class="dsui-empty__hint">
						Pick a stream from the Navigator to read its messages here.
					</p>
				</div>
			</div>
		);
	}

	const hasRead = read !== null;
	const showTimeCol = showTime.value;
	// In a live mode the workspace shows the TailPanel instead of the paged grid.
	const currentMode = tailMode.value;
	const live = isLiveMode(currentMode);
	// Describe the open live connection for the under-the-hood disclosure, when
	// one exists (the operation is set by startTail and cleared by stopTail).
	const tailOp = tailOperation.value;
	const tailDisclosure: TailDisclosure | null =
		isLiveMode(currentMode) && tailOp !== null
			? {
					operation: tailOp,
					status: tailStatus.value,
					mode: currentMode,
					fromOffset: tailStartOffset.value ?? "now",
				}
			: null;
	// Which row owns the single tab stop: the active row if it is in the VISIBLE
	// subset, otherwise the first visible row, so the grid always has exactly one
	// tabbable cell even when a filter hides the active row.
	const rows = visibleRows.value;
	const filterQuery = compiled.value;
	const activeIsVisible = active !== null && rows.some((r) => r.index === active.index);

	// Fixed-height windowing for the paged grid (issue #58) over the FILTERED rows
	// (issue #53): above the threshold render only the visible slice inside
	// top/bottom spacers (uniform 30px rows), keeping the DOM small for a large
	// batch; at or below it render every row. The spacers preserve scrollHeight so
	// the sticky header and scrollbar are unchanged. Windowing tracks the filtered
	// set, so a query both shrinks the list and re-pins the window to it.
	const gridTotal = rows.length;
	const gridWindowed = gridTotal > WINDOW_THRESHOLD;
	const gridRange = gridWindowed
		? windowRange(scrollTop.value, viewport.value, ROW_HEIGHT, gridTotal)
		: { startIndex: 0, endIndex: gridTotal, padTop: 0, padBottom: 0 };
	// The visible slice plus, for each row, its position in the filtered list, so
	// arrow-key roving moves by filtered position while the # column stays honest.
	const gridVisible = rows
		.slice(gridRange.startIndex, gridRange.endIndex)
		.map((row, i) => ({ row, pos: gridRange.startIndex + i }));
	// The single tab stop must be on a rendered row: the active row (when it
	// survives the filter) else the first row, when that row is in the window,
	// otherwise the first visible (windowed-in) row.
	const tabStopPos = activeIsVisible ? rows.findIndex((r) => r.index === active?.index) : 0;
	const tabStopVisible = tabStopPos >= gridRange.startIndex && tabStopPos < gridRange.endIndex;

	return (
		<div class="dsui-ws">
			<header class="dsui-ws__head">
				<div class="dsui-ws__title">
					<span class="dsui-ws__name">{stream.path}</span>
					<span class={`dsui-kind dsui-kind--${stream.kind}`}>{stream.kind}</span>
					{stream.manual ? <span class="dsui-pill">manual</span> : null}
				</div>
				<div class="dsui-ws__headend">
					{!live && read !== null ? (
						<div class="dsui-ws__offsets" title="Honest batch offset range (no per-element offset)">
							batch&nbsp;
							<code>{read.requestedOffset}</code>
							<CopyButton
								text={read.requestedOffset}
								label="Copy this batch's start cursor"
								copyKey="offset-from"
							/>
							&nbsp;→&nbsp;
							<code>{read.nextOffset ?? "—"}</code>
							{read.nextOffset !== null ? (
								<CopyButton
									text={read.nextOffset}
									label="Copy the next-batch cursor"
									copyKey="offset-next"
								/>
							) : null}
							{read.upToDate ? <span class="dsui-pill dsui-pill--ok">up to date</span> : null}
							{read.closed ? <span class="dsui-pill dsui-pill--warn">closed</span> : null}
						</div>
					) : null}
					<StreamActionsMenu stream={stream} />
				</div>
			</header>

			<Toolbar hasRead={hasRead} />

			<PublishComposer />

			{stream.kind !== "json" ? (
				<p class="dsui-ws__unframed">
					<code>{stream.kind}</code> streams are unframed — the server stores appended bytes with no
					message boundaries, so a refresh reads them back as one concatenated entry. The live view
					shows appends separately only because each is delivered as its own batch; those boundaries
					are not persisted. Use an <code>application/json</code> stream for discrete, persisted
					messages.
				</p>
			) : null}

			{live ? (
				<TailPanel />
			) : (
				<>
					<HistoryStrip />
					<section class="dsui-ws__grid" aria-label="Messages">
						{read !== null && read.rows.length > 0 ? (
							<RowFilter
								value={filter.value}
								matched={rows.length}
								total={read.rows.length}
								active={filterQuery.active}
								error={filterQuery.error}
								label="Filter messages"
								variant="grid"
								onInput={(v) => {
									filter.value = v;
								}}
								onClear={() => {
									filter.value = "";
								}}
							/>
						) : null}
						<div
							class={`dsui-grid__header${showTimeCol ? " dsui-grid__header--timed" : ""}`}
							aria-hidden="true"
						>
							<span>#</span>
							<span>Size</span>
							{showTimeCol ? (
								<span class="dsui-grid__timehead">
									<IconClock size={11} />
									Time
								</span>
							) : null}
							<span>Preview</span>
						</div>
						<div class="dsui-grid__rows" ref={gridRef} onScroll={onGridScroll}>
							{loading && read === null ? (
								<GridSkeleton />
							) : read !== null && read.rows.length > 0 ? (
								rows.length > 0 ? (
									<>
										{/* biome-ignore lint/a11y/useFocusableInteractive: focus lives on the option rows via roving tabindex (one row has tabIndex=0), so the listbox container itself is intentionally not a tab stop. */}
										{/* biome-ignore lint/a11y/useSemanticElements: a native <select> cannot host these rich, focusable message rows; role="listbox" with role="option" children is the correct single-select pattern. */}
										<div
											role="listbox"
											class="dsui-grid__body"
											aria-label="Message rows"
											// Spacers stand in for the windowed-out rows so scrollHeight
											// (and the sticky header) is identical to rendering every row.
											style={{
												paddingBlockStart: gridRange.padTop,
												paddingBlockEnd: gridRange.padBottom,
											}}
										>
											{gridVisible.map(({ row, pos }) => (
												<Row
													key={row.index}
													row={row}
													active={active?.index === row.index}
													showTime={showTimeCol}
													// Roving tabindex: exactly one rendered row owns the tab
													// stop — the active row (when it survives the filter) else
													// the first row, when windowed in, otherwise the first
													// visible row — so the filtered, windowed list is one Tab
													// stop and ArrowUp/Down/Home/End move between rows.
													tabbable={
														tabStopVisible ? pos === tabStopPos : pos === gridRange.startIndex
													}
													onKeyDown={onRowKeyDown(pos)}
												/>
											))}
										</div>
										{truncated ? (
											<p class="dsui-grid__truncated" role="note">
												Showing the first {read.rows.length} of a larger batch. Raise the row cap or
												read the next batch to see more. The full bytes are in the inspector's Raw
												view.
											</p>
										) : null}
									</>
								) : (
									<div class="dsui-empty dsui-empty--inline">
										<IconSearch size={22} class="dsui-empty__icon" />
										<p class="dsui-empty__title">No rows match the filter</p>
										<p class="dsui-empty__hint">
											None of the {read.rows.length}{" "}
											{read.rows.length === 1 ? "loaded row" : "loaded rows"} match{" "}
											<code>{filter.value}</code>. Clear the filter to see them all.
										</p>
										<button
											type="button"
											class="dsui-btn dsui-btn--ghost"
											onClick={() => {
												filter.value = "";
											}}
										>
											<IconClose size={14} />
											<span>Clear filter</span>
										</button>
									</div>
								)
							) : read !== null ? (
								<div class="dsui-empty dsui-empty--inline">
									<p class="dsui-empty__title">
										{read.exchange.status === 0
											? "Could not read this stream"
											: read.exchange.status >= 400
												? `Server responded ${read.exchange.status}`
												: "No messages in this batch"}
									</p>
									<p class="dsui-empty__hint">
										{read.exchange.status === 0
											? (read.exchange.error ?? "The request failed before a response.")
											: read.exchange.status >= 400
												? "The stream may not exist yet, or the offset is out of range."
												: read.upToDate
													? "The read returned an empty body — you are at the tail."
													: "The read returned an empty body."}
									</p>
								</div>
							) : (
								<div class="dsui-empty dsui-empty--inline">
									<IconPlay size={22} class="dsui-empty__icon" />
									<p class="dsui-empty__title">Ready to read</p>
									<p class="dsui-empty__hint">
										Choose a starting position and press Read to load messages.
									</p>
								</div>
							)}
						</div>
						<div class="dsui-ws__pager">
							<span class="dsui-ws__pagerinfo">
								{read !== null && read.rows.length > 0
									? filterQuery.active
										? `${rows.length} of ${read.rows.length} ${read.rows.length === 1 ? "row" : "rows"}`
										: `${read.rows.length} ${read.rows.length === 1 ? "row" : "rows"}`
									: ""}
							</span>
							<div class="dsui-ws__pageractions">
								{read !== null ? (
									<ExportMenu
										rows={read.rows}
										kind={read.kind}
										streamPath={stream.path}
										offset={read.requestedOffset}
										rawBytes={read.rawBytes}
									/>
								) : null}
								<button
									type="button"
									class="dsui-btn dsui-btn--ghost"
									title={
										read?.nextOffset != null
											? `Resume from Stream-Next-Offset ${read.nextOffset}`
											: "No further offset — you are at the tail"
									}
									disabled={read?.nextOffset === null || read?.nextOffset === undefined || loading}
									onClick={() => void readNext()}
								>
									<IconCornerDownRight size={14} />
									<span>Read next batch</span>
									{read?.nextOffset !== null && read?.nextOffset !== undefined ? (
										<code class="dsui-ws__nextoffset">{read.nextOffset}</code>
									) : null}
								</button>
							</div>
						</div>
					</section>
				</>
			)}

			<ProtocolPanel exchange={lastExchange.value} tail={tailDisclosure} />
		</div>
	);
}
