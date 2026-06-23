/**
 * MessagesWorkspace — the center region. It is a vertical stack of <section>s:
 *
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │ head: stream path · kind · honest batch offset range + pills   │
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ toolbar: [Earliest|Latest|At offset…] · rows cap · Read/Refresh│
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ grid: #  · size · time? · preview   (content-type aware)       │
 *   │       pager: Read next batch (Stream-Next-Offset)              │
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ "Under the hood" protocol disclosure (collapsed by default)    │
 *   └──────────────────────────────────────────────────────────────┘
 *
 * Reads go through the store: the toolbar mutates startMode / customOffset /
 * rowCap signals and calls readFromToolbar(), which resolves the toolbar choice
 * into a concrete protocol offset (lib/messages.resolveOffset) via
 * dsClient.readStream. Selecting a row sets store.selectedRow, driving the
 * Inspector. The grid is content-type aware: JSON batches render one row per
 * array element (honest about per-element offsets — there are none, only a
 * batch range), and a Time column appears only when elements carry a timestamp.
 *
 * Extensibility seam: add a toolbar control inside .dsui-toolbar, or a new tab
 * strip / section between the toolbar and the grid. Keep reads in the store.
 */

import { useComputed } from "@preact/signals";
import type { JSX } from "preact";
import { useRef } from "preact/hooks";
import {
	ROW_CAP_OPTIONS,
	type StartMode,
	batchHasTimes,
	extractTimestamp,
	formatBytes,
	formatTime,
} from "../lib/messages";
import type { GridRow } from "../lib/types";
import {
	customOffset,
	lastExchange,
	lastRead,
	readFromToolbar,
	readLoading,
	readNext,
	rowCap,
	rowsTruncated,
	selectRow,
	selectedRow,
	selectedStream,
	setCustomOffset,
	setRowCap,
	setStartMode,
	startMode,
} from "../state/store";
import { ProtocolPanel } from "./ProtocolPanel";
import {
	IconChevronDown,
	IconChevronRight,
	IconClock,
	IconCornerDownRight,
	IconPlay,
	IconRefresh,
} from "./icons";

/* ---------------------------------------------------------------------------
 * Toolbar
 * ------------------------------------------------------------------------ */

const START_OPTIONS: readonly { value: StartMode; label: string; title: string }[] = [
	{ value: "earliest", label: "Earliest", title: "Read from the beginning (offset -1)" },
	{ value: "latest", label: "Latest", title: "Read from the current tail (offset now)" },
	{ value: "at", label: "At offset…", title: "Read from an explicit opaque offset cursor" },
];

/** Starting-position segmented control + custom-offset input + rows cap + Read. */
function Toolbar(props: { hasRead: boolean }): JSX.Element {
	const mode = startMode.value;
	const loading = readLoading.value;
	const segmentedRef = useRef<HTMLDivElement>(null);

	/** Move both selection and focus to the segment at index (wrapping). */
	function activateSegment(index: number): void {
		const count = START_OPTIONS.length;
		const wrapped = ((index % count) + count) % count;
		const next = START_OPTIONS[wrapped];
		if (next === undefined) return;
		setStartMode(next.value);
		segmentedRef.current
			?.querySelectorAll<HTMLButtonElement>("[data-segment]")
			.item(wrapped)
			?.focus();
	}

	function onSegmentKeyDown(e: KeyboardEvent): void {
		const current = START_OPTIONS.findIndex((o) => o.value === mode);
		if (current < 0) return;
		switch (e.key) {
			case "ArrowRight":
			case "ArrowDown":
				e.preventDefault();
				activateSegment(current + 1);
				break;
			case "ArrowLeft":
			case "ArrowUp":
				e.preventDefault();
				activateSegment(current - 1);
				break;
			case "Home":
				e.preventDefault();
				activateSegment(0);
				break;
			case "End":
				e.preventDefault();
				activateSegment(START_OPTIONS.length - 1);
				break;
			default:
				break;
		}
	}

	return (
		<div class="dsui-toolbar" role="toolbar" aria-label="Read controls">
			<div class="dsui-toolbar__group">
				<span class="dsui-toolbar__label" id="dsui-start-label">
					Start
				</span>
				<div
					class="dsui-segmented"
					// biome-ignore lint/a11y/useSemanticElements: <fieldset> cannot host an arrow-key roving toolbar segment group; role="group" is the correct ARIA container for these aria-pressed toggle buttons.
					role="group"
					aria-labelledby="dsui-start-label"
					ref={segmentedRef}
				>
					{START_OPTIONS.map((opt) => (
						<button
							key={opt.value}
							type="button"
							aria-pressed={mode === opt.value}
							aria-label={`Start: ${opt.label}`}
							// Roving tabindex: only the active segment is in the Tab
							// sequence; ArrowLeft/Right move between segments.
							tabIndex={mode === opt.value ? 0 : -1}
							data-segment="true"
							class={`dsui-segmented__btn${mode === opt.value ? " is-active" : ""}`}
							title={opt.title}
							onClick={() => setStartMode(opt.value)}
							onKeyDown={onSegmentKeyDown}
						>
							{opt.label}
						</button>
					))}
				</div>
				{mode === "at" ? (
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
							if (e.key === "Enter") void readFromToolbar();
						}}
					/>
				) : null}
			</div>

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

			<div class="dsui-toolbar__spacer" />

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
 * Workspace
 * ------------------------------------------------------------------------ */

export function MessagesWorkspace(): JSX.Element {
	const stream = selectedStream.value;
	const read = lastRead.value;
	const loading = readLoading.value;
	const active = selectedRow.value;
	const truncated = rowsTruncated.value;
	const gridRef = useRef<HTMLDivElement>(null);

	// Show the Time column only when at least one row in the batch has a time.
	const showTime = useComputed(() => (read === null ? false : batchHasTimes(read.rows)));

	/** Move roving focus to the n-th row button (clamped). */
	function focusRow(index: number): void {
		const cells = gridRef.current?.querySelectorAll<HTMLButtonElement>("[data-messagerow]");
		if (cells === undefined || cells.length === 0) return;
		const clamped = Math.max(0, Math.min(index, cells.length - 1));
		cells.item(clamped)?.focus();
	}

	/** Arrow-key roving for a row at the given position within the batch. */
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
					focusRow((read?.rows.length ?? 1) - 1);
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
	// Which row owns the single tab stop: the active row if it is in this batch,
	// otherwise the first row, so the grid always has exactly one tabbable cell.
	const activeInBatch =
		active !== null && (read?.rows.some((r) => r.index === active.index) ?? false);

	return (
		<div class="dsui-ws">
			<header class="dsui-ws__head">
				<div class="dsui-ws__title">
					<span class="dsui-ws__name">{stream.path}</span>
					<span class={`dsui-kind dsui-kind--${stream.kind}`}>{stream.kind}</span>
					{stream.manual ? <span class="dsui-pill">manual</span> : null}
				</div>
				{read !== null ? (
					<div class="dsui-ws__offsets" title="Honest batch offset range (no per-element offset)">
						batch&nbsp;
						<code>{read.requestedOffset}</code>
						&nbsp;→&nbsp;
						<code>{read.nextOffset ?? "—"}</code>
						{read.upToDate ? <span class="dsui-pill dsui-pill--ok">up to date</span> : null}
						{read.closed ? <span class="dsui-pill dsui-pill--warn">closed</span> : null}
					</div>
				) : null}
			</header>

			<Toolbar hasRead={hasRead} />

			<section class="dsui-ws__grid" aria-label="Messages">
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
				<div class="dsui-grid__rows" ref={gridRef}>
					{loading && read === null ? (
						<GridSkeleton />
					) : read !== null && read.rows.length > 0 ? (
						<>
							{/* biome-ignore lint/a11y/useFocusableInteractive: focus lives on the option rows via roving tabindex (one row has tabIndex=0), so the listbox container itself is intentionally not a tab stop. */}
							{/* biome-ignore lint/a11y/useSemanticElements: a native <select> cannot host these rich, focusable message rows; role="listbox" with role="option" children is the correct single-select pattern. */}
							<div role="listbox" class="dsui-grid__body" aria-label="Message rows">
								{read.rows.map((row, i) => (
									<Row
										key={row.index}
										row={row}
										active={active?.index === row.index}
										showTime={showTimeCol}
										// Roving tabindex: exactly one row owns the tab stop —
										// the active row, else the first row — so the list is one
										// Tab stop and ArrowUp/Down/Home/End move between rows.
										tabbable={activeInBatch ? active?.index === row.index : i === 0}
										onKeyDown={onRowKeyDown(i)}
									/>
								))}
							</div>
							{truncated ? (
								<p class="dsui-grid__truncated" role="note">
									Showing the first {read.rows.length} of a larger batch. Raise the row cap or read
									the next batch to see more. The full bytes are in the inspector's Raw view.
								</p>
							) : null}
						</>
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
							? `${read.rows.length} ${read.rows.length === 1 ? "row" : "rows"}`
							: ""}
					</span>
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
			</section>

			<ProtocolPanel exchange={lastExchange.value} />
		</div>
	);
}
