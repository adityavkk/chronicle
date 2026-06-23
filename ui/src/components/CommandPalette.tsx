/**
 * CommandPalette — the Cmd/Ctrl-K command palette overlay.
 *
 * A keyboard-first launcher reusing the shared {@link Modal} shell (so Escape,
 * the backdrop, focus-move-in + restore, and the Tab focus trap all come for
 * free). It is pure layout over the store: every row calls an existing action
 * (selectStream, selectSubscription, setCenterView, open*Dialog, readFromToolbar,
 * startTail/stopTail, cycleTheme, toggleInspector, openWakeMonitor). The only
 * new logic — ranking the substring matches + highlighting them — lives in the
 * pure, unit-tested `lib/commandPalette.ts`.
 *
 * Keyboard model: this is a combobox over a listbox, not a roving tree. Focus
 * stays in the search input the whole time so the user can keep typing; the
 * active row is tracked with `aria-activedescendant` and moved with
 * ArrowUp/ArrowDown/Home/End, and Enter runs it. (A roving tabindex like the
 * Navigator's stream tree would move DOM focus off the input on every arrow
 * press, which is wrong for a type-to-filter launcher — so we match the
 * Navigator's keyboard *rigor*, with the focus model the widget actually needs.)
 */

import { useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useEffect, useId, useRef } from "preact/hooks";
import { type CommandMatch, type MatchRange, filterCommands } from "../lib/commandPalette";
import {
	activeConnection,
	closeCommandPalette,
	cycleTheme,
	openCreateDialog,
	openCreateSubscriptionDialog,
	openWakeMonitor,
	readFromToolbar,
	selectStream,
	selectSubscription,
	selectedStreamPath,
	selectedSubscriptionId,
	setCenterView,
	setTailMode,
	startTail,
	stopTail,
	streams,
	subscriptionDetails,
	subscriptionIds,
	theme,
	toggleInspector,
} from "../state/store";
import { Modal } from "./Modal";
import {
	IconBell,
	IconBroadcast,
	IconChart,
	IconFilePlus,
	IconPanelRight,
	IconRefresh,
	IconSearch,
	IconStop,
	IconStream,
	IconSun,
} from "./icons";

/** Which section of the palette a command belongs to. */
type CommandGroup = "Actions" | "Streams" | "Subscriptions";

/** A single palette row: searchable text plus the action it runs. */
interface Command {
	/** Stable identity (for keys); not shown. */
	readonly id: string;
	/** The primary text, matched + displayed. */
	readonly label: string;
	/** Extra terms that also match but are not shown (synonyms, group name). */
	readonly keywords?: string | undefined;
	/** The section this row appears under. */
	readonly group: CommandGroup;
	/** Decorative leading icon. */
	readonly icon: JSX.Element;
	/** Secondary right-aligned text (e.g. the current theme, a kind). */
	readonly hint?: string | undefined;
	/** The store action to invoke when the row is chosen. */
	readonly run: () => void;
}

/**
 * Build the live command list from the current store signals. Reading the
 * signals here subscribes the component, so the list re-derives whenever streams,
 * subscriptions, the selection, or the connection change. Sections are emitted
 * in a fixed order — Actions, then Streams, then Subscriptions — which is the
 * order the grouped (unfiltered) view shows.
 */
function buildCommands(): readonly Command[] {
	const out: Command[] = [];
	const conn = activeConnection.value;
	const streamPath = selectedStreamPath.value;
	const subId = selectedSubscriptionId.value;

	// --- Core actions ---------------------------------------------------------
	if (conn !== null) {
		out.push({
			id: "action:new-stream",
			label: "New stream",
			keywords: "create add stream",
			group: "Actions",
			icon: <IconFilePlus size={16} />,
			run: () => openCreateDialog(),
		});
		out.push({
			id: "action:new-subscription",
			label: "New subscription",
			keywords: "create add subscription fan-out webhook pull-wake",
			group: "Actions",
			icon: <IconBell size={16} />,
			run: () => openCreateSubscriptionDialog(),
		});
		out.push({
			id: "action:metrics",
			label: "Go to Metrics",
			keywords: "metrics prometheus monitor scrape",
			group: "Actions",
			icon: <IconChart size={16} />,
			run: () => setCenterView("metrics"),
		});
	}

	out.push({
		id: "action:theme",
		label: "Toggle theme",
		keywords: "dark light system appearance mode color",
		group: "Actions",
		icon: <IconSun size={16} />,
		hint: theme.value,
		run: () => cycleTheme(),
	});
	out.push({
		id: "action:inspector",
		label: "Toggle inspector panel",
		keywords: "details right pane message inspector",
		group: "Actions",
		icon: <IconPanelRight size={16} />,
		run: () => toggleInspector(),
	});

	// Stream-scoped read/tail actions only make sense with a stream selected.
	if (streamPath !== null) {
		out.push({
			id: "action:read",
			label: "Read / refresh messages",
			keywords: "read refresh reload catch-up fetch messages",
			group: "Actions",
			icon: <IconRefresh size={16} />,
			run: () => void readFromToolbar(),
		});
		out.push({
			id: "action:tail-start",
			label: "Start live tail (SSE)",
			keywords: "tail live follow stream sse subscribe",
			group: "Actions",
			icon: <IconBroadcast size={16} />,
			run: () => {
				setTailMode("sse");
				startTail("now");
			},
		});
		out.push({
			id: "action:tail-stop",
			label: "Stop live tail",
			keywords: "tail stop halt end follow",
			group: "Actions",
			icon: <IconStop size={16} />,
			run: () => stopTail(),
		});
	}

	// A pull-wake / webhook subscription can open its wake monitor.
	if (subId !== null) {
		out.push({
			id: "action:wakes",
			label: "Watch wakes",
			keywords: "wake monitor deliveries notifications subscription",
			group: "Actions",
			icon: <IconBroadcast size={16} />,
			run: () => openWakeMonitor(subId),
		});
	}

	// --- Streams --------------------------------------------------------------
	for (const s of streams.value) {
		out.push({
			id: `stream:${s.path}`,
			label: s.path,
			keywords: `stream ${s.kind}`,
			group: "Streams",
			icon: <IconStream size={16} />,
			hint: s.kind,
			run: () => selectStream(s.path),
		});
	}

	// --- Subscriptions --------------------------------------------------------
	for (const id of subscriptionIds.value) {
		const detail = subscriptionDetails.value[id];
		out.push({
			id: `sub:${id}`,
			label: id,
			keywords: "subscription",
			group: "Subscriptions",
			icon: <IconBell size={16} />,
			...(detail !== undefined ? { hint: detail.type } : {}),
			run: () => selectSubscription(id),
		});
	}

	return out;
}

/** Render a label with the matched ranges wrapped in <mark> for highlight. */
function Highlight(props: { label: string; ranges: readonly MatchRange[] }): JSX.Element {
	const { label, ranges } = props;
	if (ranges.length === 0) return <>{label}</>;
	const parts: JSX.Element[] = [];
	let cursor = 0;
	for (const r of ranges) {
		if (r.start > cursor) {
			parts.push(<span key={`t${r.start}`}>{label.slice(cursor, r.start)}</span>);
		}
		parts.push(
			<mark key={`m${r.start}`} class="dsui-cmdk__mark">
				{label.slice(r.start, r.end)}
			</mark>,
		);
		cursor = r.end;
	}
	if (cursor < label.length) parts.push(<span key={`tail${cursor}`}>{label.slice(cursor)}</span>);
	return <>{parts}</>;
}

export function CommandPalette(): JSX.Element {
	const query = useSignal("");
	const activeIndex = useSignal(0);
	const inputRef = useRef<HTMLInputElement>(null);
	const listRef = useRef<HTMLDivElement>(null);
	const base = useId();
	const listId = `${base}-list`;
	const optionId = (i: number): string => `${base}-opt-${i}`;

	const commands = buildCommands();
	const matches = filterCommands(commands, query.value);
	const showGroups = query.value.trim() === "";
	// Clamp the active row into range every render (the list shrinks as you type).
	const active = matches.length === 0 ? -1 : Math.min(activeIndex.value, matches.length - 1);

	// Focus the search input on open. The Modal (a child component) focuses its
	// close button in its own mount effect; this parent effect runs after the
	// child's, so the input wins — which is what a type-to-filter palette needs.
	useEffect(() => {
		inputRef.current?.focus();
	}, []);

	// Keep the active row scrolled into view as it moves.
	useEffect(() => {
		if (active < 0) return;
		const el = listRef.current?.querySelector<HTMLElement>(`[data-cmdk-index="${active}"]`);
		el?.scrollIntoView({ block: "nearest" });
	}, [active]);

	function activate(index: number): void {
		const cmd = matches[index]?.item;
		if (cmd === undefined) return;
		closeCommandPalette();
		cmd.run();
	}

	function onInputKeyDown(e: KeyboardEvent): void {
		switch (e.key) {
			case "ArrowDown":
				e.preventDefault();
				if (matches.length > 0) activeIndex.value = Math.min(active + 1, matches.length - 1);
				break;
			case "ArrowUp":
				e.preventDefault();
				if (matches.length > 0) activeIndex.value = Math.max(active - 1, 0);
				break;
			case "Home":
				e.preventDefault();
				activeIndex.value = 0;
				break;
			case "End":
				e.preventDefault();
				activeIndex.value = matches.length - 1;
				break;
			case "Enter":
				e.preventDefault();
				if (active >= 0) activate(active);
				break;
			default:
				break;
		}
	}

	const activeId = active >= 0 ? optionId(active) : undefined;

	/** The list rows: a presentation group header before each new group, then options. */
	const rows: JSX.Element[] = [];
	let renderedGroup: CommandGroup | null = null;
	matches.forEach((match: CommandMatch<Command>, i) => {
		const cmd = match.item;
		if (showGroups && cmd.group !== renderedGroup) {
			rows.push(
				<div key={`group-${cmd.group}`} class="dsui-cmdk__group" role="presentation">
					{cmd.group}
				</div>,
			);
		}
		renderedGroup = cmd.group;
		rows.push(
			<button
				type="button"
				key={cmd.id}
				id={optionId(i)}
				data-cmdk-index={i}
				tabIndex={-1}
				// biome-ignore lint/a11y/useSemanticElements: a native <option> cannot host a rich, highlighted, icon-bearing row; role="option" is the correct combobox single-select child (matching the message grid).
				role="option"
				aria-selected={i === active}
				class={`dsui-cmdk__option${i === active ? " dsui-cmdk__option--active" : ""}`}
				onClick={() => activate(i)}
				onMouseMove={() => {
					if (activeIndex.value !== i) activeIndex.value = i;
				}}
			>
				<span class="dsui-cmdk__optionicon" aria-hidden="true">
					{cmd.icon}
				</span>
				<span class="dsui-cmdk__optionlabel">
					<Highlight label={cmd.label} ranges={match.ranges} />
				</span>
				{cmd.hint !== undefined ? <span class="dsui-cmdk__optionhint">{cmd.hint}</span> : null}
			</button>,
		);
	});

	return (
		<Modal
			title="Command palette"
			icon={<IconSearch size={18} />}
			// Pass the stable store action directly (NOT a fresh arrow): Modal's
			// focus effect depends on [onClose], so a new identity per render would
			// re-run it on every keystroke and yank focus off this input. The
			// sibling dialogs pass the bare action for the same reason.
			onClose={closeCommandPalette}
		>
			<div class="dsui-cmdk">
				<div class="dsui-cmdk__search">
					<IconSearch size={16} class="dsui-cmdk__searchicon" />
					<input
						ref={inputRef}
						type="text"
						class="dsui-cmdk__input"
						role="combobox"
						aria-expanded={matches.length > 0}
						aria-controls={matches.length > 0 ? listId : undefined}
						aria-activedescendant={activeId}
						aria-autocomplete="list"
						aria-label="Search commands and streams"
						placeholder="Jump to a stream or run a command…"
						value={query.value}
						onInput={(e) => {
							query.value = e.currentTarget.value;
							activeIndex.value = 0;
						}}
						onKeyDown={onInputKeyDown}
					/>
				</div>

				{matches.length === 0 ? (
					// biome-ignore lint/a11y/useSemanticElements: role="status" is the correct ARIA live region for the "no matches" readout; <output> carries form-output semantics that do not apply here.
					<p class="dsui-cmdk__empty" role="status">
						No matching commands
					</p>
				) : (
					// biome-ignore lint/a11y/useFocusableInteractive: focus stays on the controlling combobox input; the active option is tracked with aria-activedescendant (the WAI-ARIA combobox pattern), so the listbox itself is intentionally not a tab stop.
					<div
						class="dsui-cmdk__list"
						// biome-ignore lint/a11y/useSemanticElements: a native <select> cannot host these rich, highlighted command rows; role="listbox" with role="option" children is the correct single-select pattern (matching the message grid).
						role="listbox"
						id={listId}
						aria-label="Commands"
						ref={listRef}
					>
						{rows}
					</div>
				)}

				<footer class="dsui-cmdk__footer">
					<span>
						<kbd class="dsui-cmdk__key">↑</kbd>
						<kbd class="dsui-cmdk__key">↓</kbd> navigate
					</span>
					<span>
						<kbd class="dsui-cmdk__key">↵</kbd> run
					</span>
					<span>
						<kbd class="dsui-cmdk__key">esc</kbd> close
					</span>
				</footer>
			</div>
		</Modal>
	);
}
