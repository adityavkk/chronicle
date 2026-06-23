/**
 * CopyButton — a small, best-effort copy-to-clipboard control shared by the
 * inspector and the protocol disclosure. It degrades silently where the
 * Clipboard API is unavailable (it simply does nothing), and shows a brief
 * checkmark confirmation after a successful copy.
 *
 * A single module-level signal tracks which button most recently copied (keyed
 * by `copyKey`), so only the button that was clicked flips to its "copied"
 * state — there is never more than one confirmation visible at a time.
 */

import { signal } from "@preact/signals";
import type { JSX } from "preact";
import { IconCheck, IconCopy } from "./icons";

const copiedKey = signal<string | null>(null);

export interface CopyButtonProps {
	/** The text placed on the clipboard. */
	readonly text: string;
	/** Accessible label / tooltip for the idle state. */
	readonly label: string;
	/** Stable key distinguishing this button's confirmation from others'. */
	readonly copyKey: string;
	/** Render a labelled pill ("Copy") instead of an icon-only button. */
	readonly variant?: "icon" | "pill";
	/** Text shown next to the icon in the pill variant. Defaults to "Copy". */
	readonly pillLabel?: string;
}

export function CopyButton(props: CopyButtonProps): JSX.Element {
	const { text, label, copyKey, variant = "icon", pillLabel = "Copy" } = props;
	const copied = copiedKey.value === copyKey;

	function onClick(): void {
		const clip = globalThis.navigator?.clipboard;
		if (clip === undefined) return;
		void clip.writeText(text).then(() => {
			copiedKey.value = copyKey;
			globalThis.setTimeout(() => {
				if (copiedKey.value === copyKey) copiedKey.value = null;
			}, 1200);
		});
	}

	if (variant === "pill") {
		return (
			<button
				type="button"
				class={`dsui-copypill${copied ? " is-copied" : ""}`}
				title={copied ? "Copied" : label}
				aria-label={label}
				onClick={onClick}
			>
				{copied ? <IconCheck size={13} /> : <IconCopy size={13} />}
				<span>{copied ? "Copied" : pillLabel}</span>
			</button>
		);
	}

	return (
		<button
			type="button"
			class="dsui-copy"
			title={copied ? "Copied" : label}
			aria-label={label}
			onClick={onClick}
		>
			{copied ? <IconCheck size={13} /> : <IconCopy size={13} />}
		</button>
	);
}
