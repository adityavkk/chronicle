/**
 * roving — the index math for roving-tabindex keyboard groups.
 *
 * This is the same idiom already used inline by the Inspector tab strip
 * (`activateAt`) and the Navigator tree (`focusItem`): a single tab stop, with
 * ArrowUp/ArrowDown moving focus between items (wrapping at the ends) and
 * Home/End jumping to the first/last item. Extracted here as a pure, tested
 * helper so the role="menu" popovers can reuse exactly that behaviour rather
 * than inventing a new one.
 *
 * The helpers are deliberately DOM-free: a component maps its focusable
 * elements to indices, asks for the next index on a keydown, and moves focus.
 */

/** Wrap `index` into the range [0, count). Returns 0 for an empty list. */
export function wrapIndex(index: number, count: number): number {
	if (count <= 0) return 0;
	return ((index % count) + count) % count;
}

/**
 * Resolve the next focused index for a vertical roving group given a key.
 *
 * - ArrowDown / ArrowUp step by one and wrap around the ends.
 * - Home / End jump to the first / last item.
 * - Any other key (and an empty list) returns null, meaning "not a navigation
 *   key — leave the event alone".
 *
 * `current` is the index of the currently focused item (use -1 when focus is
 * not yet on a known item; ArrowDown then lands on the first item).
 */
export function nextRovingIndex(key: string, current: number, count: number): number | null {
	if (count <= 0) return null;
	switch (key) {
		case "ArrowDown":
			return wrapIndex(current + 1, count);
		case "ArrowUp":
			return wrapIndex(current - 1, count);
		case "Home":
			return 0;
		case "End":
			return count - 1;
		default:
			return null;
	}
}
