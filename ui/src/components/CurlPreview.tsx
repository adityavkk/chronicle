/**
 * CurlPreview — the shared "equivalent curl" affordance for write operations.
 *
 * Every write control (create, publish, close, delete, fork, and each
 * Playground preset) is described as an {@link Operation} BEFORE it runs. This
 * component turns that descriptor into the exact equivalent curl via the pure
 * lib/curl.toCurl and presents it as a collapsed-by-default `<details>` with a
 * one-click copy. It is pure layout over a descriptor — no store, no I/O — so it
 * drops in next to any action that has an Operation in hand.
 *
 *   ┌──────────────────────────────────────────────┐
 *   │ ▸ Equivalent curl                  [Copy]      │  summary
 *   ├──────────────────────────────────────────────┤
 *   │ curl -X POST -H '…' --data-raw '…' '…'         │  the command
 *   └──────────────────────────────────────────────┘
 *
 * Accessibility: a native disclosure (keyboard + screen-reader friendly), the
 * caret rotates on open, and the copy control carries its own label. Motion is
 * suppressed under prefers-reduced-motion via the shared app.css rules.
 */

import type { JSX } from "preact";
import { toCurl } from "../lib/curl";
import type { Operation } from "../lib/types";
import { CopyButton } from "./CopyButton";
import { IconChevronRight, IconTerminal } from "./icons";

export interface CurlPreviewProps {
	/** The operation to reproduce as curl, or null to render nothing. */
	readonly operation: Operation | null;
	/** A stable key so this preview's copy confirmation is independent. */
	readonly copyKey: string;
	/** Start expanded instead of collapsed. Defaults to collapsed. */
	readonly open?: boolean | undefined;
	/** Override the summary label. Defaults to "Equivalent curl". */
	readonly label?: string | undefined;
	/**
	 * Override the rendered curl command. Use when an operation needs a transport
	 * flag the generic toCurl cannot infer (e.g. SSE's `-N`, via lib/tail.tailToCurl).
	 * When omitted, the command is toCurl(operation).
	 */
	readonly command?: string | undefined;
}

/**
 * A collapsed disclosure showing the equivalent curl for an operation. Renders
 * nothing when there is no operation (e.g. the form is not yet valid), so a
 * control can pass the previewed Operation directly and let this hide itself.
 */
export function CurlPreview(props: CurlPreviewProps): JSX.Element | null {
	const { operation, copyKey, open = false, label = "Equivalent curl" } = props;
	if (operation === null) return null;
	const command = props.command ?? toCurl(operation);
	return (
		<details class="dsui-curl" open={open}>
			<summary class="dsui-curl__summary">
				<IconChevronRight size={13} class="dsui-curl__caret" />
				<IconTerminal size={14} class="dsui-curl__icon" />
				<span class="dsui-curl__label">{label}</span>
			</summary>
			<div class="dsui-curl__body">
				<pre class="dsui-curl__cmd">{command}</pre>
				<div class="dsui-curl__copy">
					<CopyButton
						text={command}
						label="Copy the equivalent curl command"
						copyKey={copyKey}
						variant="pill"
						pillLabel="Copy"
					/>
				</div>
			</div>
		</details>
	);
}
