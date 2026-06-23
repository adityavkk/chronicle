/**
 * MetricsWorkspace — the center region for the server's Prometheus metrics.
 *
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │ head: "Metrics" · the --metrics-listen URL input · Scrape      │
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ curated key metrics (sweep / wake / fan-out / claim contention)│
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ full families table (name · type · samples · help)             │
 *   ├──────────────────────────────────────────────────────────────┤
 *   │ "Under the hood" — the scrape exchange + copy-as-curl          │
 *   └──────────────────────────────────────────────────────────────┘
 *
 * Metrics are served on a SEPARATE listener (the --metrics-listen address, e.g.
 * :9090), a different origin from the stream handler, so the URL is entered by
 * the user (persisted per connection via store.setMetricsUrl) rather than
 * derived. The scrape itself is the only fetch and lives in dsClient.fetchMetrics
 * (parsed by lib/metrics); this component lays out the typed snapshot and reuses
 * the pure accessors (findMetric / sumSamples) for the curated summary.
 *
 * Extensibility seam: add a tile to KEY_METRICS or a column to the families
 * table. Parsing + accessors stay in lib/metrics; the fetch stays in dsClient.
 */

import { useComputed } from "@preact/signals";
import type { JSX } from "preact";
import { findMetric, sumSamples } from "../lib/metrics";
import type { Metric, Operation } from "../lib/types";
import {
	activeConnection,
	lastExchange,
	metrics,
	metricsError,
	metricsLoading,
	metricsUrl,
	refreshMetrics,
	setMetricsUrl,
} from "../state/store";
import { CurlPreview } from "./CurlPreview";
import { ProtocolPanel } from "./ProtocolPanel";
import { IconChart, IconLoader, IconRefresh } from "./icons";

/** A curated tile: a base metric name + a label and a short note. */
interface KeyMetric {
	readonly name: string;
	readonly label: string;
	readonly note: string;
}

/**
 * The fixed-value, low-cardinality families worth a glance (mirrors the gate
 * metrics in the contract). Each is summed across its label sets via sumSamples.
 */
const KEY_METRICS: readonly KeyMetric[] = [
	{
		name: "chronicle_sweep_wakes_total",
		label: "Sweep wakes",
		note: "Total wakes issued by the recovery sweep.",
	},
	{
		name: "chronicle_wake_delivery_seconds",
		label: "Webhook deliveries",
		note: "Webhook POST round-trips (count across outcomes).",
	},
	{
		name: "chronicle_wake_event_seconds",
		label: "Pull-wake appends",
		note: "Wake-event appends to wake streams (count).",
	},
	{
		name: "chronicle_fanout_seconds",
		label: "Fan-outs",
		note: "OnStreamAppend fan-out passes (count).",
	},
	{
		name: "chronicle_due_set_mutations_total",
		label: "Due-set mutations",
		note: "arm / ack / expire / release across subscriptions.",
	},
	{
		name: "chronicle_claim_contention_total",
		label: "Claim outcomes",
		note: "claimed / already_claimed / fenced / ok / nosub.",
	},
];

/** Format a possibly non-integer metric total compactly. */
function formatTotal(n: number): string {
	if (!Number.isFinite(n)) return "—";
	if (Number.isInteger(n)) return n.toLocaleString();
	return n.toFixed(2);
}

function KeyMetricTile(props: { metric: KeyMetric; total: number; present: boolean }): JSX.Element {
	const { metric, total, present } = props;
	return (
		<div class={`dsui-metrictile${present ? "" : " is-absent"}`}>
			<span class="dsui-metrictile__value">{present ? formatTotal(total) : "—"}</span>
			<span class="dsui-metrictile__label">{metric.label}</span>
			<span class="dsui-metrictile__note">
				{present ? metric.note : "not exposed by this server"}
			</span>
		</div>
	);
}

function FamiliesTable(props: { families: readonly Metric[] }): JSX.Element {
	const { families } = props;
	return (
		<table class="dsui-metrictable" aria-label="All metric families">
			<thead class="dsui-metrictable__head">
				<tr>
					<th scope="col">Metric</th>
					<th scope="col">Type</th>
					<th scope="col">Series</th>
				</tr>
			</thead>
			<tbody>
				{families.map((m) => (
					<tr class="dsui-metrictable__row" key={m.name}>
						<td class="dsui-metrictable__name">
							<code>{m.name}</code>
							{m.help !== null ? <span class="dsui-metrictable__help">{m.help}</span> : null}
						</td>
						<td>
							<span class={`dsui-mtype dsui-mtype--${m.type}`}>{m.type}</span>
						</td>
						<td class="dsui-metrictable__count">{m.samples.length}</td>
					</tr>
				))}
			</tbody>
		</table>
	);
}

export function MetricsWorkspace(): JSX.Element {
	const conn = activeConnection.value;
	const url = metricsUrl.value;
	const loading = metricsLoading.value;
	const error = metricsError.value;
	const snapshot = metrics.value;

	// The scrape is a plain GET to the entered metrics URL; build the preview so
	// the curl is honest even before the first scrape runs.
	const scrapeOp = useComputed<Operation | null>(() =>
		url.trim() === ""
			? null
			: { method: "GET", url: url.trim(), headers: { Accept: "text/plain" } },
	);

	const tiles = useComputed(() =>
		snapshot === null
			? []
			: KEY_METRICS.map((k) => ({
					metric: k,
					present: findMetric(snapshot, k.name) !== null,
					total: sumSamples(snapshot, k.name),
				})),
	);

	return (
		<div class="dsui-ws">
			<header class="dsui-ws__head">
				<div class="dsui-ws__title">
					<IconChart size={15} />
					<span class="dsui-ws__name">Metrics</span>
					{snapshot !== null ? (
						<span class="dsui-pill dsui-pill--ok">{snapshot.metrics.length} families</span>
					) : null}
				</div>
			</header>

			<div class="dsui-toolbar" role="toolbar" aria-label="Metrics controls">
				<div class="dsui-toolbar__group dsui-toolbar__group--grow">
					<label class="dsui-toolbar__label" for="dsui-metrics-url">
						Endpoint
					</label>
					<input
						id="dsui-metrics-url"
						type="text"
						class="dsui-input dsui-input--mono dsui-metrics__url"
						placeholder="http://localhost:9090/metrics"
						aria-label="Metrics endpoint URL (the --metrics-listen address)"
						value={url}
						autocomplete="off"
						spellcheck={false}
						disabled={conn === null}
						onInput={(e) => setMetricsUrl(e.currentTarget.value)}
						onKeyDown={(e) => {
							if (e.key === "Enter") void refreshMetrics();
						}}
					/>
				</div>
				<div class="dsui-toolbar__spacer" />
				<button
					type="button"
					class="dsui-btn dsui-btn--primary"
					disabled={conn === null || loading || url.trim() === ""}
					onClick={() => void refreshMetrics()}
				>
					{loading ? (
						<IconLoader size={14} class="dsui-spin" />
					) : (
						<IconRefresh size={14} class={loading ? "dsui-spin" : undefined} />
					)}
					<span>{loading ? "Scraping…" : "Scrape"}</span>
				</button>
			</div>

			<div class="dsui-ws__scroll">
				{error !== null ? (
					<div class="dsui-empty dsui-empty--inline" role="alert">
						<IconChart size={24} class="dsui-empty__icon" />
						<p class="dsui-empty__title">Could not scrape metrics</p>
						<p class="dsui-empty__hint">{error}</p>
					</div>
				) : null}

				{snapshot === null && error === null ? (
					<div class="dsui-empty">
						<IconChart size={26} class="dsui-empty__icon" />
						<p class="dsui-empty__title">No metrics yet</p>
						<p class="dsui-empty__hint">
							Metrics are served on the separate <code>--metrics-listen</code> address (e.g.{" "}
							<code>:9090</code>), a different origin from the stream handler. Enter that{" "}
							<code>/metrics</code> URL above and Scrape.
						</p>
					</div>
				) : null}

				{snapshot !== null ? (
					<>
						<section class="dsui-subsection" aria-label="Key metrics">
							<header class="dsui-subsection__head">
								<span class="dsui-subsection__title">Key metrics</span>
							</header>
							<div class="dsui-metrictiles">
								{tiles.value.map((t) => (
									<KeyMetricTile
										key={t.metric.name}
										metric={t.metric}
										total={t.total}
										present={t.present}
									/>
								))}
							</div>
						</section>

						<section class="dsui-subsection" aria-label="All metric families">
							<header class="dsui-subsection__head">
								<span class="dsui-subsection__title">All families</span>
								<span class="dsui-nav__count">{snapshot.metrics.length}</span>
							</header>
							<FamiliesTable families={snapshot.metrics} />
						</section>
					</>
				) : null}

				<CurlPreview operation={scrapeOp.value} copyKey="metrics-scrape-curl" />
			</div>

			<ProtocolPanel exchange={lastExchange.value} tail={null} />
		</div>
	);
}
