/**
 * A tiny, local inline-SVG icon set. No icon package (per the lightweight
 * stack rules). Icons are stroke-based, sized via `size`, and inherit color
 * through `currentColor` so they theme automatically.
 *
 * Adding an icon: add a new exported component that returns an <Icon> with a
 * 24x24 stroke path. Keep them visually consistent (1.6 stroke, round caps).
 */

import type { JSX } from "preact";

export interface IconProps {
	/** Pixel size (width = height). Default 16. */
	readonly size?: number | undefined;
	/** Extra class for layout. */
	readonly class?: string | undefined;
	/** Accessible label; when omitted the icon is aria-hidden. */
	readonly title?: string | undefined;
}

function Icon(props: IconProps & { children: JSX.Element | JSX.Element[] }): JSX.Element {
	const { size = 16, class: cls, title, children } = props;
	const labelled = title !== undefined;
	return (
		<svg
			width={size}
			height={size}
			viewBox="0 0 24 24"
			fill="none"
			stroke="currentColor"
			stroke-width={1.6}
			stroke-linecap="round"
			stroke-linejoin="round"
			class={cls}
			role={labelled ? "img" : undefined}
			aria-hidden={labelled ? undefined : "true"}
			aria-label={labelled ? title : undefined}
		>
			{title !== undefined ? <title>{title}</title> : null}
			{children}
		</svg>
	);
}

export function IconStream(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M3 7c4 0 4 4 8 4s4-4 8-4" />
			<path d="M3 13c4 0 4 4 8 4s4-4 8-4" />
		</Icon>
	);
}

export function IconChevronRight(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="m9 6 6 6-6 6" />
		</Icon>
	);
}

export function IconChevronDown(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="m6 9 6 6 6-6" />
		</Icon>
	);
}

export function IconPlus(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M12 5v14M5 12h14" />
		</Icon>
	);
}

export function IconRefresh(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M3 12a9 9 0 0 1 15-6.7L21 8" />
			<path d="M21 3v5h-5" />
			<path d="M21 12a9 9 0 0 1-15 6.7L3 16" />
			<path d="M3 21v-5h5" />
		</Icon>
	);
}

export function IconSun(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<circle cx="12" cy="12" r="4" />
			<path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
		</Icon>
	);
}

export function IconMoon(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8Z" />
		</Icon>
	);
}

export function IconMonitor(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<rect x="3" y="4" width="18" height="12" rx="2" />
			<path d="M8 20h8M12 16v4" />
		</Icon>
	);
}

export function IconServer(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<rect x="3" y="4" width="18" height="7" rx="2" />
			<rect x="3" y="13" width="18" height="7" rx="2" />
			<path d="M7 7.5h.01M7 16.5h.01" />
		</Icon>
	);
}

export function IconCode(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="m16 18 6-6-6-6M8 6l-6 6 6 6" />
		</Icon>
	);
}

export function IconClose(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M18 6 6 18M6 6l12 12" />
		</Icon>
	);
}

export function IconInfo(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<circle cx="12" cy="12" r="9" />
			<path d="M12 11v5M12 8h.01" />
		</Icon>
	);
}

export function IconCheck(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="m5 12 5 5L20 7" />
		</Icon>
	);
}

export function IconTrash(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M4 7h16M9 7V5a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2M6 7l1 12a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2l1-12" />
			<path d="M10 11v6M14 11v6" />
		</Icon>
	);
}

export function IconPencil(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M12 20h9" />
			<path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4Z" />
		</Icon>
	);
}

export function IconPlug(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M9 2v6M15 2v6" />
			<path d="M6 8h12v3a6 6 0 0 1-12 0Z" />
			<path d="M12 17v5" />
		</Icon>
	);
}

export function IconLogout(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
			<path d="M16 17l5-5-5-5M21 12H9" />
		</Icon>
	);
}

export function IconLoader(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M12 3a9 9 0 1 0 9 9" />
		</Icon>
	);
}

export function IconArrowLeft(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M19 12H5M12 19l-7-7 7-7" />
		</Icon>
	);
}

export function IconSearch(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<circle cx="11" cy="11" r="7" />
			<path d="m21 21-4.3-4.3" />
		</Icon>
	);
}

export function IconBell(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M6 9a6 6 0 0 1 12 0c0 5 2 6 2 6H4s2-1 2-6Z" />
			<path d="M10 20a2 2 0 0 0 4 0" />
		</Icon>
	);
}

export function IconChart(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M3 3v18h18" />
			<path d="M7 14l3-4 3 3 4-6" />
		</Icon>
	);
}

export function IconPlay(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M6 4.5v15l13-7.5-13-7.5Z" />
		</Icon>
	);
}

export function IconClock(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<circle cx="12" cy="12" r="9" />
			<path d="M12 7v5l3.5 2" />
		</Icon>
	);
}

export function IconCopy(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<rect x="9" y="9" width="11" height="11" rx="2" />
			<path d="M5 15a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2" />
		</Icon>
	);
}

export function IconTerminal(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="m5 8 4 4-4 4" />
			<path d="M12 16h6" />
		</Icon>
	);
}

export function IconCornerDownRight(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M6 4v8a2 2 0 0 0 2 2h10" />
			<path d="m14 10 4 4-4 4" />
		</Icon>
	);
}

export function IconArrowUpRight(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M7 17 17 7M8 7h9v9" />
		</Icon>
	);
}

export function IconAlertTriangle(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M12 3 2.5 19.5h19L12 3Z" />
			<path d="M12 10v4M12 17h.01" />
		</Icon>
	);
}

export function IconFork(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<circle cx="6" cy="5" r="2.4" />
			<circle cx="18" cy="5" r="2.4" />
			<circle cx="12" cy="19" r="2.4" />
			<path d="M6 7.4v3a3 3 0 0 0 3 3h6a3 3 0 0 0 3-3v-3" />
			<path d="M12 13.4v3.2" />
		</Icon>
	);
}

export function IconSend(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M21 3 10.5 13.5" />
			<path d="M21 3 14.5 21l-4-7.5L3 9.5 21 3Z" />
		</Icon>
	);
}

export function IconLock(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<rect x="5" y="11" width="14" height="9" rx="2" />
			<path d="M8 11V8a4 4 0 0 1 8 0v3" />
		</Icon>
	);
}

export function IconMore(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<circle cx="5" cy="12" r="1" />
			<circle cx="12" cy="12" r="1" />
			<circle cx="19" cy="12" r="1" />
		</Icon>
	);
}

export function IconSparkles(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M12 4c.6 2.8 1.2 3.4 4 4-2.8.6-3.4 1.2-4 4-.6-2.8-1.2-3.4-4-4 2.8-.6 3.4-1.2 4-4Z" />
			<path d="M18 14c.3 1.4.6 1.7 2 2-1.4.3-1.7.6-2 2-.3-1.4-.6-1.7-2-2 1.4-.3 1.7-.6 2-2Z" />
		</Icon>
	);
}

export function IconFilePlus(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8Z" />
			<path d="M14 3v5h5" />
			<path d="M12 12v5M9.5 14.5h5" />
		</Icon>
	);
}

export function IconZap(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M13 3 4 14h7l-1 7 9-11h-7l1-7Z" />
		</Icon>
	);
}

export function IconPause(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M8 5v14M16 5v14" />
		</Icon>
	);
}

export function IconStop(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<rect x="6" y="6" width="12" height="12" rx="2" />
		</Icon>
	);
}

export function IconBroadcast(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<circle cx="12" cy="12" r="2" />
			<path d="M6.3 6.3a8 8 0 0 0 0 11.4M17.7 6.3a8 8 0 0 1 0 11.4" />
			<path d="M9.2 9.2a4 4 0 0 0 0 5.6M14.8 9.2a4 4 0 0 1 0 5.6" />
		</Icon>
	);
}

export function IconArrowDownToLine(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M12 4v10M8 10l4 4 4-4M5 20h14" />
		</Icon>
	);
}

/** A rounded rect with a divided right column — the inspector-panel toggle. */
export function IconPanelRight(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<rect x="3" y="4" width="18" height="16" rx="2" />
			<path d="M15 4v16" />
		</Icon>
	);
}

export function IconWebhook(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M9 8a3 3 0 1 1 4.5 2.6l2.5 4.4" />
			<path d="M15 16a3 3 0 1 1-3 3" />
			<path d="M12 19H7a3 3 0 0 1-1-5.8" />
			<path d="M8 13.5 5.5 9" />
			<path d="M16 13.5h-3" />
		</Icon>
	);
}

export function IconKey(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<circle cx="7.5" cy="15.5" r="3.5" />
			<path d="m10 13 9-9M16 7l2 2M13.5 9.5l2 2" />
		</Icon>
	);
}

export function IconLink(props: IconProps): JSX.Element {
	return (
		<Icon {...props}>
			<path d="M10 13a4 4 0 0 0 5.66 0l2-2a4 4 0 0 0-5.66-5.66l-1 1" />
			<path d="M14 11a4 4 0 0 0-5.66 0l-2 2a4 4 0 0 0 5.66 5.66l1-1" />
		</Icon>
	);
}
