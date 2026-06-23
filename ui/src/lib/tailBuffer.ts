/**
 * tailBuffer.ts — bounded append for the live-tail row buffer (pure, no DOM).
 *
 * A fast producer can deliver one row per message indefinitely, so the tail
 * keeps only the most recent {@link TailBuffer.cap} rows. The naive append —
 * `[...current, ...incoming]` then `.slice(overflow)` once over cap — allocates
 * (and copies) the whole buffer on *every* message once it is full: two
 * full-length array passes per row at steady state.
 *
 * {@link appendCapped} fixes both halves:
 *  - It never builds an oversized intermediate: under cap it concatenates once;
 *    over cap it slices the front of `current` first, then concatenates, so the
 *    result is built in a single pass at its final size.
 *  - It evicts in blocks (`evictBlock`, ~10% of the cap) rather than trimming to
 *    exactly the cap each time. After an eviction the buffer sits a block below
 *    the cap, so the next ~`evictBlock` appends fall through the cheap, no-evict
 *    branch. Eviction work is amortized to once per block instead of per row.
 *
 * The `dropped` count it returns is exactly the number of rows removed from the
 * front this call, which keeps the running `tailDropped` total honest: at all
 * times `tailDropped + bufferedRows === rowsEverReceived`.
 */

/** The outcome of one bounded append. */
export interface AppendResult<T> {
	/** The new buffer (most recent last), length ≤ `cap`. */
	readonly rows: readonly T[];
	/** How many rows aged out of the front this call (add to the running total). */
	readonly dropped: number;
}

/**
 * Append `incoming` to `current`, keeping at most `cap` rows by evicting the
 * oldest from the front. Evicts in chunks of at least `evictBlock` so a
 * steady one-row-per-call stream only pays the eviction cost once per block.
 *
 * Invariant for the caller's accounting: the returned `dropped` is precisely
 * the number of `current` rows not carried into `rows`, so adding it to a
 * running counter keeps `counter + rows.length` equal to the total ever seen.
 */
export function appendCapped<T>(
	current: readonly T[],
	incoming: readonly T[],
	cap: number,
	evictBlock: number,
): AppendResult<T> {
	if (incoming.length === 0) return { rows: current, dropped: 0 };

	// A single burst larger than the whole cap: keep only its newest `cap` rows.
	// Everything currently buffered, and the older part of the burst, ages out.
	if (incoming.length >= cap) {
		const keepFrom = incoming.length - cap;
		return {
			rows: incoming.slice(keepFrom),
			dropped: current.length + keepFrom,
		};
	}

	const total = current.length + incoming.length;
	// Cheap branch: still within cap, so just concatenate once. At steady state
	// (post-eviction the buffer sits ~one block below cap) most appends land here.
	if (total <= cap) return { rows: current.concat(incoming), dropped: 0 };

	// Over cap: drop at least one block from the front, but always enough to fit.
	// `overflow` is the minimum we must shed; rounding up to a block leaves room
	// so the following appends skip this branch. Never drop more than we hold.
	const overflow = total - cap;
	const evict = Math.min(current.length, Math.max(overflow, evictBlock));
	return { rows: current.slice(evict).concat(incoming), dropped: evict };
}
