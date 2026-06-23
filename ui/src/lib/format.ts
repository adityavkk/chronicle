/**
 * Small, pure formatting helpers shared by the connection UI. No DOM, no store.
 * Kept dependency-free so they are easy to test and reuse.
 */

import type { ConnectionProbe, ProbeStatus } from "./types";

/** Coarse reachability state for a status dot. */
export type DotStatus = "ok" | "down" | "unknown" | "checking";

/**
 * Derive a status-dot state from a per-connection {@link ProbeStatus}. Absent
 * (never probed) renders as "unknown"; an in-flight probe as "checking".
 */
export function dotStatusOf(status: ProbeStatus | undefined): DotStatus {
	if (status === undefined) return "unknown";
	if (status.state === "checking") return "checking";
	return status.probe.ok ? "ok" : "down";
}

/** A short human label for a completed probe, for tooltips / secondary text. */
export function describeProbe(probe: ConnectionProbe | null): string {
	if (probe === null) return "Not checked yet";
	if (!probe.ok) return probe.error ?? "Unreachable";
	return `Reachable · HTTP ${probe.status} · ${probe.latencyMs} ms`;
}

/**
 * Render a wall-clock timestamp as a compact "time ago" string. `null` (never
 * used) becomes "never". Coarse buckets are enough for a last-used hint.
 */
export function relativeTime(ms: number | null, now: number = Date.now()): string {
	if (ms === null) return "never";
	const delta = Math.max(0, now - ms);
	const sec = Math.round(delta / 1000);
	if (sec < 45) return "just now";
	const min = Math.round(sec / 60);
	if (min < 60) return `${min}m ago`;
	const hr = Math.round(min / 60);
	if (hr < 24) return `${hr}h ago`;
	const day = Math.round(hr / 24);
	if (day < 7) return `${day}d ago`;
	const wk = Math.round(day / 7);
	if (wk < 5) return `${wk}w ago`;
	const mo = Math.round(day / 30);
	if (mo < 12) return `${mo}mo ago`;
	return `${Math.round(day / 365)}y ago`;
}

/**
 * Strip the scheme from a URL for a denser display label, keeping host + port +
 * path. Falls back to the input on a parse failure.
 */
export function compactUrl(url: string): string {
	try {
		const u = new URL(url);
		const tail = `${u.host}${u.pathname === "/" ? "" : u.pathname}`;
		return tail === "" ? url : tail;
	} catch {
		return url.replace(/^https?:\/\//, "");
	}
}
