/**
 * ConnectionManager — the header's active-connection switcher + theme toggle.
 *
 * The trigger shows the active connection with a live reachability dot. Clicking
 * it opens a popover listing every saved connection (each selectable, with its
 * own status dot and an active marker), plus actions to add a new connection or
 * disconnect back to the start screen. State lives in the store (switcherOpen,
 * connections, probeStatuses); this component only renders and dispatches the
 * sanctioned actions.
 *
 * Accessibility: the trigger is aria-haspopup/aria-expanded; the popover is a
 * menu closed on outside-click and Escape, with focus returned to the trigger.
 *
 * Extensibility seam: header actions live in `.dsui-header__actions`; add a new
 * `<button class="dsui-iconbtn">` there. Popover rows are a single list — add a
 * per-connection action by extending the row.
 */

import { useComputed } from "@preact/signals";
import type { JSX, RefObject } from "preact";
import { useEffect, useRef } from "preact/hooks";
import { compactUrl, dotStatusOf } from "../lib/format";
import {
	activeConnection,
	activeConnectionId,
	connectionProbe,
	connections,
	cycleTheme,
	probeStatuses,
	setActiveConnection,
	switcherOpen,
	theme,
} from "../state/store";
import { StatusDot } from "./StatusDot";
import {
	IconCheck,
	IconChevronDown,
	IconLogout,
	IconMonitor,
	IconMoon,
	IconPlus,
	IconServer,
	IconSun,
} from "./icons";
import { focusFirstMenuItem, handleMenuKeydown } from "./menuKeyboard";

function ThemeToggle(): JSX.Element {
	const t = theme.value;
	const label = t === "system" ? "Theme: system" : t === "light" ? "Theme: light" : "Theme: dark";
	return (
		<button
			type="button"
			class="dsui-iconbtn"
			title={label}
			aria-label={label}
			onClick={() => cycleTheme()}
		>
			{t === "light" ? (
				<IconSun size={16} />
			) : t === "dark" ? (
				<IconMoon size={16} />
			) : (
				<IconMonitor size={16} />
			)}
		</button>
	);
}

export function ConnectionManager(): JSX.Element {
	const conn = activeConnection.value;
	const probe = connectionProbe.value;
	const open = switcherOpen.value;
	const wrapRef = useRef<HTMLDivElement>(null);
	const popRef = useRef<HTMLDivElement>(null);
	const triggerRef = useRef<HTMLButtonElement>(null);

	const status = probe === null ? "unknown" : probe.ok ? "ok" : "down";

	// On open, move focus into the menu (first item); the roving-tabindex helper
	// keeps Arrow/Home/End/Tab inside it from there.
	useEffect(() => {
		if (open) focusFirstMenuItem(popRef.current);
	}, [open]);

	// Close on outside click or Escape; restore focus to the trigger on Escape.
	useEffect(() => {
		if (!open) return;
		function onPointer(e: PointerEvent): void {
			const target = e.target;
			if (wrapRef.current !== null && target instanceof Node && !wrapRef.current.contains(target)) {
				switcherOpen.value = false;
			}
		}
		function onKey(e: KeyboardEvent): void {
			if (e.key === "Escape") {
				switcherOpen.value = false;
				triggerRef.current?.focus();
			}
		}
		document.addEventListener("pointerdown", onPointer);
		document.addEventListener("keydown", onKey);
		return () => {
			document.removeEventListener("pointerdown", onPointer);
			document.removeEventListener("keydown", onKey);
		};
	}, [open]);

	return (
		<div class="dsui-connbar">
			<div class="dsui-switcher" ref={wrapRef}>
				<button
					type="button"
					ref={triggerRef}
					class="dsui-connswitch"
					title="Switch connection"
					aria-haspopup="menu"
					aria-expanded={open}
					onClick={() => {
						switcherOpen.value = !switcherOpen.value;
					}}
				>
					<StatusDot status={status} />
					<IconServer size={15} class="dsui-connswitch__icon" />
					<span class="dsui-connswitch__label">{conn === null ? "No connection" : conn.name}</span>
					{conn !== null ? (
						<span class="dsui-connswitch__url">{compactUrl(conn.baseUrl)}</span>
					) : null}
					<IconChevronDown size={14} class="dsui-connswitch__caret" />
				</button>

				{open ? <SwitcherPopover popRef={popRef} /> : null}
			</div>

			<ThemeToggle />
		</div>
	);
}

function SwitcherPopover(props: { popRef: RefObject<HTMLDivElement> }): JSX.Element {
	const { popRef } = props;
	const list = connections.value;
	const activeId = activeConnectionId.value;

	return (
		<div
			class="dsui-switcher__pop"
			role="menu"
			aria-label="Connections"
			ref={popRef}
			onKeyDown={(e) => handleMenuKeydown(e, popRef.current)}
		>
			<p class="dsui-switcher__heading">Connections</p>
			{list.length === 0 ? (
				<p class="dsui-switcher__empty">No saved connections.</p>
			) : (
				<ul class="dsui-switcher__list">
					{list.map((c) => (
						<SwitcherRow key={c.id} id={c.id} active={c.id === activeId} />
					))}
				</ul>
			)}
			<div class="dsui-switcher__sep" />
			<button
				type="button"
				role="menuitem"
				tabIndex={-1}
				class="dsui-switcher__action"
				onClick={() => setActiveConnection(null)}
			>
				<IconPlus size={15} />
				<span>New connection…</span>
			</button>
			{activeId !== null ? (
				<button
					type="button"
					role="menuitem"
					tabIndex={-1}
					class="dsui-switcher__action"
					onClick={() => setActiveConnection(null)}
				>
					<IconLogout size={15} />
					<span>Disconnect</span>
				</button>
			) : null}
		</div>
	);
}

function SwitcherRow(props: { id: string; active: boolean }): JSX.Element {
	const { id, active } = props;
	const conn = useComputed(() => connections.value.find((c) => c.id === id) ?? null);
	const status = useComputed(() => dotStatusOf(probeStatuses.value[id]));
	const c = conn.value;
	if (c === null) return <li />;
	return (
		<li>
			<button
				type="button"
				role="menuitemradio"
				tabIndex={-1}
				aria-checked={active}
				class={`dsui-switcher__row${active ? " is-active" : ""}`}
				onClick={() => setActiveConnection(id)}
			>
				<StatusDot status={status.value} />
				<span class="dsui-switcher__rowtext">
					<span class="dsui-switcher__rowname">{c.name}</span>
					<span class="dsui-switcher__rowurl">{compactUrl(c.baseUrl)}</span>
				</span>
				{active ? <IconCheck size={14} class="dsui-switcher__check" /> : null}
			</button>
		</li>
	);
}
