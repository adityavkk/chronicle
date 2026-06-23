/**
 * Loads the runtime config the Go binary serves at /dsui-config.json.
 *
 * The binary encodes `{"defaultServer": "<url>"}` (the value may be an empty
 * string when no --server flag was passed). Under pure `vite dev` the file is
 * absent and the fetch 404s — both cases resolve to a config with a null
 * defaultServer rather than throwing, so callers never need a try/catch.
 */

import { isRecord } from "./guards";
import type { DsuiConfig } from "./types";

/** The path the Go binary registers for the runtime config. */
export const CONFIG_PATH = "/dsui-config.json";

/** A config with no prefill — the safe fallback for every failure mode. */
const EMPTY_CONFIG: DsuiConfig = { defaultServer: null };

/**
 * Narrow an unknown parsed body into a {@link DsuiConfig}. An empty-string
 * defaultServer (the binary's "no --server" encoding) is treated as null so
 * callers get a single "nothing to prefill" sentinel.
 */
export function coerceConfig(raw: unknown): DsuiConfig {
	if (!isRecord(raw)) return EMPTY_CONFIG;
	const ds = raw.defaultServer;
	if (typeof ds !== "string") return EMPTY_CONFIG;
	const trimmed = ds.trim();
	return { defaultServer: trimmed === "" ? null : trimmed };
}

/**
 * Fetch and parse the runtime config. Never throws: a missing file, non-OK
 * status, network error, or malformed body all resolve to {@link EMPTY_CONFIG}.
 */
export async function loadConfig(signal?: AbortSignal): Promise<DsuiConfig> {
	try {
		const init: RequestInit = { headers: { Accept: "application/json" } };
		if (signal !== undefined) init.signal = signal;
		const res = await fetch(CONFIG_PATH, init);
		if (!res.ok) return EMPTY_CONFIG;
		const body: unknown = await res.json();
		return coerceConfig(body);
	} catch {
		return EMPTY_CONFIG;
	}
}
