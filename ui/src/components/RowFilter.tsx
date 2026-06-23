/**
 * RowFilter — the shared, instant, client-side filter input for the message
 * grid and the live tail (issue #53). It mirrors the Navigator's
 * `dsui-nav__filter` idiom (IconSearch + input, instant, never touches the
 * store) and adds the bits the in-grid filter needs: a "showing N of M" count,
 * a Clear control, and an inline error chip for a malformed `/regex/`.
 *
 * It is layout only. The owning component holds the component-local `useSignal`
 * for the query text and does the actual matching with the pure helpers in
 * `lib/messages` (compileQuery + matchCompiled); this component just renders the
 * control and reports keystrokes/clears back through callbacks.
 *
 * The matching strategies (substring by default, `/regex/`, and `field:value`)
 * live entirely in `lib/messages`, so the placeholder hint is the only place the
 * syntax is surfaced to the user.
 */

import type { JSX } from "preact";
import { IconClose, IconSearch } from "./icons";

export interface RowFilterProps {
	/** The current query text (owned by the caller's component-local signal). */
	readonly value: string;
	/** How many rows match the active filter. */
	readonly matched: number;
	/** How many rows are loaded in total (the batch / tail buffer size). */
	readonly total: number;
	/** True when the query actually narrows the rows (drives the count + Clear). */
	readonly active: boolean;
	/** A message when the query is a malformed regex, else null. */
	readonly error: string | null;
	/** Accessible label for the input (e.g. "Filter messages"). */
	readonly label: string;
	/** Called on every keystroke with the new query text. */
	readonly onInput: (value: string) => void;
	/** Called when the user clears the filter (button or Escape). */
	readonly onClear: () => void;
	/** Visual variant: the grid header or the denser tail bar. */
	readonly variant?: "grid" | "tail";
}

/**
 * The substring / regex / field syntax hint, kept short so it reads as a
 * placeholder rather than documentation.
 */
const PLACEHOLDER = "Filter rows — text, /regex/, or field:value";

export function RowFilter(props: RowFilterProps): JSX.Element {
	const { value, matched, total, active, error, label, onInput, onClear, variant = "grid" } = props;
	const hasError = error !== null;

	return (
		<div class={`dsui-rowfilter dsui-rowfilter--${variant}${hasError ? " is-error" : ""}`}>
			<div class="dsui-rowfilter__box">
				<IconSearch size={14} class="dsui-rowfilter__icon" />
				<input
					type="search"
					class="dsui-rowfilter__input"
					placeholder={PLACEHOLDER}
					aria-label={label}
					aria-invalid={hasError}
					value={value}
					autocomplete="off"
					spellcheck={false}
					onInput={(e) => onInput(e.currentTarget.value)}
					onKeyDown={(e) => {
						if (e.key === "Escape" && value !== "") {
							e.preventDefault();
							onClear();
						}
					}}
				/>
				{active ? (
					<button
						type="button"
						class="dsui-rowfilter__clear"
						title="Clear filter"
						aria-label="Clear filter"
						onClick={onClear}
					>
						<IconClose size={13} />
					</button>
				) : null}
			</div>
			{/* The match count + any regex error are announced politely so screen
			    readers hear the result of typing without it stealing focus. A bare
			    aria-live region (no explicit role) is the live region here. */}
			<span class="dsui-rowfilter__status" aria-live="polite">
				{hasError ? (
					<span class="dsui-rowfilter__error" title={error ?? undefined}>
						invalid regex
					</span>
				) : active ? (
					<span class="dsui-rowfilter__count">
						showing {matched} of {total}
					</span>
				) : null}
			</span>
		</div>
	);
}
