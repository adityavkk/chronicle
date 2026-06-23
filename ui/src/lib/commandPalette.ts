/**
 * commandPalette — the pure matcher behind the Cmd/Ctrl-K command palette.
 *
 * No DOM, no store, no I/O (it lives in `lib/` for exactly that reason): given a
 * list of searchable items and a query, it returns the matching items ranked
 * best-first, each with the [start, end) ranges in the label that matched so the
 * component can highlight them.
 *
 * Matching is PLAIN, CASE-INSENSITIVE SUBSTRING — the whole trimmed query must
 * appear as one contiguous run in the label (or, as a weaker fallback, anywhere
 * in the item's `keywords`). There is deliberately NO fuzzy / subsequence
 * algorithm and no search dependency: this mirrors the Navigator's
 * `path.toLowerCase().includes(q)` filter, adding only ranking + highlight on
 * top of the same substring test.
 */

/** An item the matcher can rank: its primary text plus optional synonyms. */
export interface Searchable {
	/** The primary text that is matched and displayed. */
	readonly label: string;
	/**
	 * Extra terms that also satisfy a match but are not shown as the label
	 * (synonyms, the group name, abbreviations). A keywords-only hit ranks below
	 * every label hit and produces no highlight ranges.
	 */
	readonly keywords?: string | undefined;
}

/** A contiguous half-open `[start, end)` slice of the label that matched. */
export interface MatchRange {
	readonly start: number;
	readonly end: number;
}

/** One ranked match: the original item, its score, and the label ranges to mark. */
export interface CommandMatch<T extends Searchable> {
	readonly item: T;
	readonly score: number;
	readonly ranges: readonly MatchRange[];
}

/**
 * A character that ends one "word" so the next character starts a new one. A
 * match at a word start ("orders/**created**" for "cre") ranks above a hit in
 * the middle of a word.
 */
const WORD_BOUNDARY = /[\s/_\-:.]/;

/*
 * Score bands. They are spaced far wider than any realistic label length so
 * that the small "shorter label wins" tie-break (subtracting the label length)
 * can never push one band into another: a prefix hit always beats a
 * word-boundary hit, which always beats a plain substring hit, which always
 * beats a keywords-only hit.
 */
const SCORE_PREFIX = 4000;
const SCORE_BOUNDARY = 3000;
const SCORE_SUBSTRING = 2000;
const SCORE_KEYWORD = 1000;

/**
 * Score one item against a query. Returns `null` when the item does not match.
 * An empty (or whitespace-only) query matches everything with score 0 and no
 * ranges, so the palette shows its full list before the user types.
 */
export function scoreCommand<T extends Searchable>(item: T, query: string): CommandMatch<T> | null {
	const q = query.trim().toLowerCase();
	if (q === "") return { item, score: 0, ranges: [] };

	const label = item.label;
	const hay = label.toLowerCase();
	const idx = hay.indexOf(q);
	if (idx >= 0) {
		let band = SCORE_SUBSTRING;
		if (idx === 0) {
			band = SCORE_PREFIX;
		} else {
			const prev = hay[idx - 1];
			if (prev !== undefined && WORD_BOUNDARY.test(prev)) band = SCORE_BOUNDARY;
		}
		return { item, score: band - label.length, ranges: [{ start: idx, end: idx + q.length }] };
	}

	if (item.keywords?.toLowerCase().includes(q) === true) {
		return { item, score: SCORE_KEYWORD - label.length, ranges: [] };
	}
	return null;
}

/**
 * Filter and rank a list of items against a query. Matches come back best-first;
 * items with equal scores keep their original input order (a stable sort, made
 * explicit via the index tie-break so the contract holds on any engine).
 */
export function filterCommands<T extends Searchable>(
	items: readonly T[],
	query: string,
): readonly CommandMatch<T>[] {
	const matches: { readonly m: CommandMatch<T>; readonly i: number }[] = [];
	items.forEach((item, i) => {
		const m = scoreCommand(item, query);
		if (m !== null) matches.push({ m, i });
	});
	matches.sort((a, b) => b.m.score - a.m.score || a.i - b.i);
	return matches.map(({ m }) => m);
}

/**
 * Whether a keydown is the command-palette hotkey (Cmd-K on macOS, Ctrl-K
 * elsewhere). Pure over the event's modifier + key fields so it is unit-testable
 * without a DOM; the "ignore while typing in an input" guard lives at the call
 * site in `app.tsx`, where the event target is available.
 */
export function isPaletteHotkey(e: {
	readonly key: string;
	readonly metaKey: boolean;
	readonly ctrlKey: boolean;
}): boolean {
	return (e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K");
}
