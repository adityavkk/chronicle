/**
 * A pure parser for the Prometheus text-exposition format (the body of GET
 * /metrics on chronicle's --metrics-listen address). No client library: the
 * format is line-oriented and small enough to parse by hand, which keeps the
 * runtime dependency surface at preact + signals.
 *
 * The grammar handled (the 0.0.4 text format):
 *   - `# HELP <name> <help text>`   declares a family's help.
 *   - `# TYPE <name> <type>`        declares a family's type.
 *   - `# <anything else>`           a comment, ignored.
 *   - `<series>{<labels>} <value> [<timestamp>]`  one sample.
 *   - `<series> <value> [<timestamp>]`            one sample (no labels).
 *
 * Series carry suffixes for composite families: a histogram emits
 * `<name>_bucket`, `<name>_sum`, `<name>_count`; a summary emits `<name>` (with a
 * `quantile` label), `<name>_sum`, `<name>_count`. We group samples back under
 * their base family name so the UI sees one {@link Metric} per family.
 *
 * Tolerant by design: unparseable lines are skipped, not fatal; a sample whose
 * family had no `# TYPE` becomes "untyped". Pure — no DOM, no store, no I/O.
 */

import type { Metric, MetricSample, MetricType, MetricsSnapshot } from "./types";

/** The HELP/TYPE declarations accumulated for a base metric name. */
interface FamilyMeta {
	type: MetricType;
	help: string | null;
}

/** The known declared metric types; anything else is treated as "untyped". */
const KNOWN_TYPES: ReadonlySet<string> = new Set([
	"counter",
	"gauge",
	"histogram",
	"summary",
	"untyped",
]);

/**
 * Parse a Prometheus text-exposition document into a typed {@link MetricsSnapshot}.
 * Families appear in first-seen order; within a family, samples are in document
 * order.
 */
export function parseMetrics(text: string): MetricsSnapshot {
	const metas = new Map<string, FamilyMeta>();
	// Base name -> its accumulated samples. Insertion order is first-seen order.
	const samples = new Map<string, MetricSample[]>();

	for (const rawLine of text.split("\n")) {
		const line = rawLine.trim();
		if (line === "") continue;

		if (line.startsWith("#")) {
			applyComment(line, metas);
			continue;
		}

		const sample = parseSampleLine(line);
		if (sample === null) continue;
		const existing = samples.get(sample.name);
		if (existing === undefined) {
			samples.set(sample.name, [sample]);
		} else {
			existing.push(sample);
		}
	}

	const metrics: Metric[] = [];
	for (const [name, familySamples] of samples) {
		const meta = metas.get(name);
		metrics.push({
			name,
			type: meta?.type ?? "untyped",
			help: meta?.help ?? null,
			samples: familySamples,
		});
	}

	return { metrics, parsedAt: Date.now() };
}

/** Apply a `# HELP …` or `# TYPE …` comment line to the family-meta map. */
function applyComment(line: string, metas: Map<string, FamilyMeta>): void {
	// "# HELP name help text" / "# TYPE name kind". Split into at most 4 tokens so
	// the help text (which may contain spaces) survives as a single trailing token.
	const head = line.slice(1).trim();
	if (head.startsWith("HELP ")) {
		const rest = head.slice("HELP ".length).trimStart();
		const space = rest.indexOf(" ");
		if (space < 0) {
			ensureMeta(metas, rest).help = "";
			return;
		}
		const name = rest.slice(0, space);
		const help = rest.slice(space + 1);
		ensureMeta(metas, name).help = unescapeHelp(help);
		return;
	}
	if (head.startsWith("TYPE ")) {
		const rest = head.slice("TYPE ".length).trim();
		const space = rest.indexOf(" ");
		if (space < 0) return;
		const name = rest.slice(0, space);
		const kind = rest.slice(space + 1).trim();
		ensureMeta(metas, name).type = KNOWN_TYPES.has(kind) ? (kind as MetricType) : "untyped";
		return;
	}
	// Any other comment line is ignored.
}

/** Get-or-create the meta record for a base metric name. */
function ensureMeta(metas: Map<string, FamilyMeta>, name: string): FamilyMeta {
	const existing = metas.get(name);
	if (existing !== undefined) return existing;
	const created: FamilyMeta = { type: "untyped", help: null };
	metas.set(name, created);
	return created;
}

/**
 * Parse one sample line into a {@link MetricSample}, or null when it does not
 * look like a sample. Handles the labelled (`name{a="b"} 1`) and unlabelled
 * (`name 1`) forms, an optional trailing timestamp, and `NaN` / `+Inf` / `-Inf`.
 */
export function parseSampleLine(line: string): MetricSample | null {
	const braceOpen = line.indexOf("{");
	let series: string;
	let labels: Record<string, string>;
	let rest: string;

	if (braceOpen >= 0) {
		const braceClose = line.lastIndexOf("}");
		if (braceClose < braceOpen) return null;
		series = line.slice(0, braceOpen).trim();
		labels = parseLabels(line.slice(braceOpen + 1, braceClose));
		rest = line.slice(braceClose + 1).trim();
	} else {
		const space = line.indexOf(" ");
		if (space < 0) return null;
		series = line.slice(0, space).trim();
		labels = {};
		rest = line.slice(space + 1).trim();
	}

	if (series === "") return null;

	// The value is the first whitespace-delimited token of the remainder; an
	// optional scrape timestamp may follow and is ignored.
	const valueToken = rest.split(/\s+/)[0] ?? "";
	const value = parseMetricValue(valueToken);
	if (value === null) return null;

	return { name: baseName(series), series, labels, value };
}

/**
 * Parse the inside of a `{…}` label block into a record. Tolerant of trailing
 * commas and spacing. Values are double-quoted with `\\`, `\"`, `\n` escapes.
 */
function parseLabels(inner: string): Record<string, string> {
	const labels: Record<string, string> = {};
	let i = 0;
	const n = inner.length;
	while (i < n) {
		// Skip separators / whitespace.
		while (i < n && (inner[i] === "," || inner[i] === " " || inner[i] === "\t")) i++;
		if (i >= n) break;

		// Read the label name up to '='.
		const eq = inner.indexOf("=", i);
		if (eq < 0) break;
		const name = inner.slice(i, eq).trim();
		i = eq + 1;

		// The value must be a double-quoted string.
		if (inner[i] !== '"') break;
		i++;
		let value = "";
		while (i < n && inner[i] !== '"') {
			const ch = inner[i];
			if (ch === "\\" && i + 1 < n) {
				const next = inner[i + 1];
				value += next === "n" ? "\n" : (next ?? "");
				i += 2;
				continue;
			}
			value += ch ?? "";
			i++;
		}
		i++; // consume the closing quote
		if (name !== "") labels[name] = value;
	}
	return labels;
}

/** Parse a metric value token, preserving NaN / +Inf / -Inf. */
function parseMetricValue(token: string): number | null {
	if (token === "") return null;
	if (token === "NaN") return Number.NaN;
	if (token === "+Inf" || token === "Inf") return Number.POSITIVE_INFINITY;
	if (token === "-Inf") return Number.NEGATIVE_INFINITY;
	const n = Number(token);
	return Number.isNaN(n) ? null : n;
}

/**
 * Strip the composite-family suffix from a series name to get the base family
 * name. A histogram/summary emits `<name>_bucket` / `<name>_sum` / `<name>_count`;
 * everything else is its own base. Only strips when a suffix is present.
 */
function baseName(series: string): string {
	for (const suffix of ["_bucket", "_sum", "_count"] as const) {
		if (series.endsWith(suffix) && series.length > suffix.length) {
			return series.slice(0, series.length - suffix.length);
		}
	}
	return series;
}

/** Un-escape a HELP text per the format (`\\` and `\n`). */
function unescapeHelp(s: string): string {
	let out = "";
	for (let i = 0; i < s.length; i++) {
		const ch = s[i];
		if (ch === "\\" && i + 1 < s.length) {
			const next = s[i + 1];
			out += next === "n" ? "\n" : (next ?? "");
			i++;
			continue;
		}
		out += ch ?? "";
	}
	return out;
}

/* ----------------------------------------------------------------------------
 * Convenience accessors (pure, for the metrics view + tests)
 * ------------------------------------------------------------------------- */

/** Find a metric family by base name, or null. */
export function findMetric(snapshot: MetricsSnapshot, name: string): Metric | null {
	return snapshot.metrics.find((m) => m.name === name) ?? null;
}

/**
 * Sum the samples of a counter/gauge family across all label sets (the `_count`
 * series for a histogram, the bare series otherwise). Returns 0 when the family
 * is absent. Ignores non-finite values so an +Inf bucket does not poison a sum.
 */
export function sumSamples(snapshot: MetricsSnapshot, name: string): number {
	const metric = findMetric(snapshot, name);
	if (metric === null) return 0;
	const wantCount = metric.type === "histogram" || metric.type === "summary";
	let total = 0;
	for (const s of metric.samples) {
		const isCount = s.series.endsWith("_count");
		if (wantCount ? !isCount : isCount) continue;
		if (wantCount && s.series.endsWith("_bucket")) continue;
		if (Number.isFinite(s.value)) total += s.value;
	}
	return total;
}
