/**
 * TailPanel — the live-tail view in the Messages workspace (long-poll + SSE).
 *
 * When the toolbar's read mode is Long-poll or SSE, the workspace renders this
 * panel instead of the paged grid. Rows stream in from the store's bounded
 * tail buffer in real time:
 *
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │ ● Live · SSE        atOffset 42      [Pause] [Clear] [Stop]    │  status bar
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ #  · size · time? · preview   (newest at the bottom)          │  live grid
 *   │  … appends as messages arrive; auto-scrolls when stuck …      │
 *   │                                   [▾ Jump to latest] (unstuck) │
 *   └──────────────────────────────────────────────────────────────┘
 *
 * It is layout + thin event wiring over the store's tail seam:
 *  - All tail state lives in the store (tailStatus / tailRows / tailPaused /
 *    tailDropped) and the actions startTail / stopTail / setTailPaused /
 *    clearTailBuffer own the single connection + the bounded buffer.
 *  - All status text/tone/announcement copy comes from lib/tail (pure).
 * The component only arranges them and owns the scroll "stick to bottom"
 * behavior, which is a purely visual concern.
 *
 * Teardown: the panel does not own the connection (the store does), but it
 * stops the tail on unmount as a belt-and-braces guard so navigating away never
 * leaks an EventSource / long-poll loop. Stream / connection / mode changes also
 * stop the tail in the store, so this only adds the unmount case.
 *
 * Accessibility: the status is announced through an aria-live region whose
 * politeness follows the status urgency (errors/reconnects assertive, the rest
 * polite — lib/tail.tailAnnouncePolite), the controls are labelled buttons, and
 * the live list is a labelled listbox of options driving the inspector, matching
 * the paged grid. Motion (the auto-scroll) honors prefers-reduced-motion.
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useEffect, useRef } from "preact/hooks";
import {
	batchHasTimes,
	compileQuery,
	extractTimestamp,
	formatBytes,
	formatTime,
	matchCompiled,
} from "../lib/messages";
import {
	describeTailMode,
	isTerminalTailState,
	tailAnnouncePolite,
	tailStatusDetail,
	tailStatusLabel,
	tailTone,
} from "../lib/tail";
import type { GridRow, TailStatus } from "../lib/types";
import {
	TAIL_BUFFER_CAP,
	clearTailBuffer,
	selectRow,
	selectedRow,
	setTailPaused,
	startTail,
	stopTail,
	tailDropped,
	tailMode,
	tailPaused,
	tailRows,
	tailStatus,
} from "../state/store";
import { RowFilter } from "./RowFilter";
import {
	IconArrowDownToLine,
	IconBroadcast,
	IconClose,
	IconLoader,
	IconPause,
	IconPlay,
	IconSearch,
	IconStop,
	IconTrash,
} from "./icons";

/* ---------------------------------------------------------------------------
 * Status badge (aria-live)
 * ------------------------------------------------------------------------ */

/** A small live-status dot + label, announced via aria-live by tone urgency. */
function StatusBadge(props: { status: TailStatus }): JSX.Element {
	const { status } = props;
	const tone = tailTone(status);
	const label = tailStatusLabel(status);
	const detail = tailStatusDetail(status);
	const polite = tailAnnouncePolite(status);
	const pulse = status.state === "connecting" || status.state === "reconnecting";
	return (
		<div
			class={`dsui-tail__status dsui-tail__status--${tone}`}
			// biome-ignore lint/a11y/useSemanticElements: role="status" is the correct ARIA live-region for a connection-status readout; <output> carries form-output semantics that do not apply here.
			role="status"
			aria-live={polite ? "polite" : "assertive"}
		>
			<span class={`dsui-tail__dot${pulse ? " is-pulsing" : ""}`} aria-hidden="true" />
			<span class="dsui-tail__statuslabel">{label}</span>
			{detail !== "" ? <span class="dsui-tail__statusdetail">{detail}</span> : null}
		</div>
	);
}

/* ---------------------------------------------------------------------------
 * Live grid row (mirrors the paged grid's role=option rows)
 * ------------------------------------------------------------------------ */

function TailRow(props: {
	row: GridRow;
	seq: number;
	active: boolean;
	showTime: boolean;
}): JSX.Element {
	const { row, seq, active, showTime } = props;
	const ts = row.kind === "json" ? extractTimestamp(row.value) : null;
	const time = formatTime(ts);
	const rowClass = `dsui-row${showTime ? " dsui-row--timed" : ""}${active ? " is-active" : ""}`;
	const label = `Live message ${seq}, ${formatBytes(row.byteSize)}: ${row.preview}`;
	return (
		<button
			type="button"
			// biome-ignore lint/a11y/useSemanticElements: a native <option> cannot host a rich, clickable live row driving the inspector; role="option" on a button is the correct single-select pattern, matching the paged grid.
			role="option"
			class={rowClass}
			onClick={() => selectRow(row)}
			aria-selected={active}
			aria-label={label}
		>
			<span class="dsui-row__index">
				<IconBroadcast size={11} class="dsui-tail__rowmark" />
				<span>{seq}</span>
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
 * The panel
 * ------------------------------------------------------------------------ */

/** How close to the bottom (px) still counts as "stuck to bottom". */
const STICK_THRESHOLD = 24;

export function TailPanel(): JSX.Element {
	const status = tailStatus.value;
	const rows = tailRows.value;
	const dropped = tailDropped.value;
	const paused = tailPaused.value;
	const mode = tailMode.value;
	const active = selectedRow.value;

	const scrollRef = useRef<HTMLDivElement>(null);
	// "Stuck to bottom": follow new rows. Disengages when the user scrolls up,
	// re-engages when they scroll back to the bottom (or press "Jump to latest").
	const stuck = useSignal(true);

	// Component-local, instant filter over the live buffer — never touches the
	// store, never drops the connection (issue #53). Each visible row keeps its
	// true arrival number (dropped + buffer position), so the # stays honest even
	// when the filter hides earlier rows: number first, then filter.
	const filter = useSignal("");
	const compiled = useComputed(() => compileQuery(filter.value));
	const visible = useComputed(() => {
		const q = compiled.value;
		return rows
			.map((row, i) => ({ row, seq: dropped + i }))
			.filter(({ row }) => matchCompiled(row, q));
	});

	const showTime = useComputed(() => batchHasTimes(rows));
	const idle = status.state === "idle";
	const terminal = isTerminalTailState(status);

	// Stop the tail when the panel unmounts (belt-and-braces; the store also
	// stops on stream / connection / mode change). A bare cleanup, run once.
	useEffect(() => stopTail, []);

	// Auto-scroll to the newest row while stuck. Runs whenever the visible row
	// count changes (so typing a filter re-pins to the bottom of the filtered
	// set); reduced-motion users get an instant jump via the global CSS rule
	// (scroll-behavior is forced to auto), so this stays honest for them.
	const visibleCount = visible.value.length;
	// biome-ignore lint/correctness/useExhaustiveDependencies: scrollRef + the stuck signal are stable handles; the effect is intentionally driven by the visible row count alone.
	useEffect(() => {
		if (!stuck.value) return;
		const el = scrollRef.current;
		if (el === null) return;
		el.scrollTop = el.scrollHeight;
	}, [visibleCount]);

	function onScroll(): void {
		const el = scrollRef.current;
		if (el === null) return;
		const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight <= STICK_THRESHOLD;
		stuck.value = atBottom;
	}

	function jumpToLatest(): void {
		const el = scrollRef.current;
		if (el === null) return;
		el.scrollTop = el.scrollHeight;
		stuck.value = true;
	}

	const modeLabel = describeTailMode(mode);
	const vis = visible.value;
	const filterQuery = compiled.value;

	return (
		<section class="dsui-tail" aria-label={`Live tail (${modeLabel})`}>
			<div class="dsui-tail__bar">
				<StatusBadge status={status} />
				<span class="dsui-tail__mode" title={`Following with ${modeLabel}`}>
					{modeLabel}
				</span>
				{rows.length > 0 ? (
					<RowFilter
						value={filter.value}
						matched={vis.length}
						total={rows.length}
						active={filterQuery.active}
						error={filterQuery.error}
						label="Filter live messages"
						variant="tail"
						onInput={(v) => {
							filter.value = v;
						}}
						onClear={() => {
							filter.value = "";
						}}
					/>
				) : null}
				<span class="dsui-tail__spacer" />
				{!terminal ? (
					<button
						type="button"
						class={`dsui-btn dsui-btn--xs${paused ? " dsui-btn--primary" : ""}`}
						aria-pressed={paused}
						title={
							paused ? "Resume buffering incoming messages" : "Pause buffering (stay connected)"
						}
						onClick={() => setTailPaused(!paused)}
					>
						{paused ? <IconPlay size={13} /> : <IconPause size={13} />}
						<span>{paused ? "Resume" : "Pause"}</span>
					</button>
				) : null}
				<button
					type="button"
					class="dsui-btn dsui-btn--xs"
					disabled={rows.length === 0 && dropped === 0}
					title="Clear the received messages from the buffer"
					onClick={() => clearTailBuffer()}
				>
					<IconTrash size={13} />
					<span>Clear</span>
				</button>
				{terminal ? (
					<button
						type="button"
						class="dsui-btn dsui-btn--xs dsui-btn--primary"
						title={`Start following the tail with ${modeLabel}`}
						onClick={() => startTail("now")}
					>
						<IconBroadcast size={13} />
						<span>{idle ? "Start tail" : "Restart"}</span>
					</button>
				) : (
					<button
						type="button"
						class="dsui-btn dsui-btn--xs dsui-btn--danger"
						title="Stop following the tail and close the connection"
						onClick={() => stopTail()}
					>
						<IconStop size={13} />
						<span>Stop</span>
					</button>
				)}
			</div>

			{paused ? (
				<p class="dsui-tail__paused" role="note">
					Paused — the connection stays open, but incoming messages are not added to the buffer.
				</p>
			) : null}

			<div class="dsui-tail__gridwrap">
				<div
					class={`dsui-grid__header${showTime.value ? " dsui-grid__header--timed" : ""}`}
					aria-hidden="true"
				>
					<span>#</span>
					<span>Size</span>
					{showTime.value ? <span>Time</span> : null}
					<span>Preview</span>
				</div>
				<div class="dsui-tail__rows" ref={scrollRef} onScroll={onScroll}>
					{vis.length > 0 ? (
						// biome-ignore lint/a11y/useFocusableInteractive: focus lives on the option rows; the live listbox container itself is intentionally not a tab stop (matches the paged grid).
						// biome-ignore lint/a11y/useSemanticElements: a native <select> cannot host these rich, clickable live rows; role="listbox" with role="option" children is the correct single-select pattern.
						<div role="listbox" class="dsui-grid__body" aria-label="Live message rows">
							{vis.map(({ row, seq }) => (
								<TailRow
									key={`${seq}-${row.index}-${row.preview}`}
									row={row}
									seq={seq}
									active={active === row}
									showTime={showTime.value}
								/>
							))}
						</div>
					) : rows.length > 0 ? (
						<div class="dsui-empty dsui-empty--inline">
							<IconSearch size={22} class="dsui-empty__icon" />
							<p class="dsui-empty__title">No messages match the filter</p>
							<p class="dsui-empty__hint">
								None of the {rows.length} buffered {rows.length === 1 ? "message" : "messages"}{" "}
								match <code>{filter.value}</code>. New matching messages will appear as they arrive.
							</p>
							<button
								type="button"
								class="dsui-btn dsui-btn--xs"
								onClick={() => {
									filter.value = "";
								}}
							>
								<IconClose size={13} />
								<span>Clear filter</span>
							</button>
						</div>
					) : (
						<div class="dsui-empty dsui-empty--inline">
							{idle ? (
								<>
									<IconBroadcast size={22} class="dsui-empty__icon" />
									<p class="dsui-empty__title">Ready to follow the tail</p>
									<p class="dsui-empty__hint">
										Press Start tail to open a {modeLabel} connection and watch messages arrive
										live.
									</p>
								</>
							) : status.state === "connecting" ? (
								<>
									<IconLoader size={22} class="dsui-empty__icon dsui-spin" />
									<p class="dsui-empty__title">Connecting…</p>
									<p class="dsui-empty__hint">Opening the live connection.</p>
								</>
							) : (
								<>
									<IconBroadcast size={22} class="dsui-empty__icon" />
									<p class="dsui-empty__title">Waiting for messages</p>
									<p class="dsui-empty__hint">
										Connected and at the tail — new messages appear here as they arrive.
									</p>
								</>
							)}
						</div>
					)}
				</div>

				{!stuck.value && vis.length > 0 ? (
					<button
						type="button"
						class="dsui-tail__jump"
						title="Scroll to the newest message and follow the tail"
						onClick={jumpToLatest}
					>
						<IconArrowDownToLine size={13} />
						<span>Jump to latest</span>
					</button>
				) : null}
			</div>

			<div class="dsui-tail__foot">
				<span class="dsui-tail__count">
					{rows.length} {rows.length === 1 ? "message" : "messages"} buffered
					{rows.length >= TAIL_BUFFER_CAP ? ` (capped at ${TAIL_BUFFER_CAP})` : ""}
				</span>
				{dropped > 0 ? (
					<span class="dsui-tail__dropped" title="Oldest messages aged out of the bounded buffer">
						{dropped} aged out
					</span>
				) : null}
				<span class="dsui-tail__spacer" />
				<span class={`dsui-tail__stick${stuck.value ? " is-on" : ""}`}>
					{stuck.value ? "Following tail" : "Paused scroll"}
				</span>
			</div>
		</section>
	);
}
