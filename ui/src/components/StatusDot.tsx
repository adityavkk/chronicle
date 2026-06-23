/**
 * StatusDot — a tiny reachability indicator shared by the start-screen cards
 * and the header connection switcher. Renders one of four states; "checking"
 * pulses. The dot is decorative (aria-hidden) — callers carry the accessible
 * label on the surrounding control.
 */

import type { JSX } from "preact";
import type { DotStatus } from "../lib/format";

export interface StatusDotProps {
	readonly status: DotStatus;
	/** Optional extra class for layout. */
	readonly class?: string | undefined;
}

export function StatusDot(props: StatusDotProps): JSX.Element {
	const cls = `dsui-dot dsui-dot--${props.status}${props.class === undefined ? "" : ` ${props.class}`}`;
	return <span class={cls} aria-hidden="true" />;
}
