/**
 * virtual.ts — fixed-height list windowing math (pure, no DOM).
 *
 * Both the paged message grid and the live tail render uniform 30px rows
 * (`block-size: 30px` on `.dsui-row`). A fast stream can fill the bounded
 * buffer to ~1000 rows; rendering all of them is ~1000 row buttons (×4–5 cells
 * each) in one scroller, recreated on every tail batch. Windowing renders only
 * the rows that fall in (or just outside) the viewport and pads the rest with
 * two spacer regions, so the DOM stays small while the scrollbar stays honest.
 *
 * This module is the geometry only: given the scroll position, the viewport
 * height, the row height, and the total row count, it returns the half-open
 * slice `[startIndex, endIndex)` to render plus the top/bottom padding (in px)
 * that stands in for the un-rendered rows. The component applies the slice and
 * the padding; nothing here touches the DOM, which is why it is unit-tested
 * directly. See `windowRange.test.ts`.
 */

/** The uniform row height in px — mirrors `block-size: 30px` on `.dsui-row`. */
export const ROW_HEIGHT = 30;

/**
 * Row count below which windowing is skipped and every row renders. Small reads
 * (the common case) stay simple — no spacers, no slice — and only a fast stream
 * or a large catch-up batch crosses into the windowed path.
 */
export const WINDOW_THRESHOLD = 120;

/**
 * Extra rows rendered above and below the visible band so a small scroll (or a
 * single arrow-key step) lands on an already-mounted row rather than a blank
 * gap. A few rows is plenty at 30px each.
 */
export const OVERSCAN = 8;

/** The slice to render plus the spacer padding that replaces the rest. */
export interface WindowRange {
	/** First row to render (inclusive). */
	readonly startIndex: number;
	/** One past the last row to render (exclusive) — use with `slice`. */
	readonly endIndex: number;
	/** Padding above the slice, in px (stands in for rows `[0, startIndex)`). */
	readonly padTop: number;
	/** Padding below the slice, in px (stands in for rows `[endIndex, total)`). */
	readonly padBottom: number;
}

/**
 * Compute the rows to render for a fixed-height scroller.
 *
 * The half-open range `[startIndex, endIndex)` covers every row touching the
 * viewport plus {@link OVERSCAN} on each side, clamped to `[0, total]`.
 * `padTop`/`padBottom` are the heights of the un-rendered runs, so
 * `padTop + (endIndex - startIndex) * rowHeight + padBottom === total *
 * rowHeight` and the scrollbar geometry is identical to rendering every row.
 *
 * Degenerate inputs render everything (no windowing): a non-positive total is
 * an empty range, and a non-measurable viewport (`clientHeight <= 0`, e.g. pre-
 * layout or jsdom where elements have no size) returns the full `[0, total)` so
 * the list is never blank before the first real measurement.
 */
export function windowRange(
	scrollTop: number,
	clientHeight: number,
	rowHeight: number,
	total: number,
	overscan: number = OVERSCAN,
): WindowRange {
	if (total <= 0) return { startIndex: 0, endIndex: 0, padTop: 0, padBottom: 0 };
	// No measurable viewport (or row height): render everything rather than risk
	// a blank list. This is also the jsdom path, where layout is never computed.
	if (rowHeight <= 0 || clientHeight <= 0) {
		return { startIndex: 0, endIndex: total, padTop: 0, padBottom: 0 };
	}
	const top = scrollTop > 0 ? scrollTop : 0;
	const firstVisible = Math.floor(top / rowHeight);
	// +1 covers the partial row at the bottom edge of the viewport.
	const visibleCount = Math.ceil(clientHeight / rowHeight) + 1;
	const startIndex = Math.max(0, firstVisible - overscan);
	const endIndex = Math.min(total, firstVisible + visibleCount + overscan);
	return {
		startIndex,
		endIndex,
		padTop: startIndex * rowHeight,
		padBottom: (total - endIndex) * rowHeight,
	};
}
