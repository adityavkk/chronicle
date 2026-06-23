/**
 * StartScreen — the connection manager's landing view, shown by the app shell
 * whenever there is no active connection. It lists saved connections as cards
 * (name, url:port, a live reachability dot, last-used) and offers a polished
 * "New connection" form. Selecting a card makes that connection active; the
 * shell then swaps to the workspace.
 *
 * Data + actions all come from the store (the single mutation seam). On mount
 * we re-probe every saved connection so the dots are fresh, and the runtime
 * /dsui-config.json prefill has already been kicked off by initStore().
 *
 * Extensibility: a card's action row is a flex container — add a per-connection
 * action (duplicate, export…) by dropping a button into `.dsui-card__actions`.
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useEffect } from "preact/hooks";
import { compactUrl, describeProbe, dotStatusOf, relativeTime } from "../lib/format";
import type { Connection } from "../lib/types";
import {
	connections,
	cycleTheme,
	probeAllConnections,
	probeConnection,
	probeStatuses,
	removeConnection,
	setActiveConnection,
	theme,
} from "../state/store";
import { ConnectionForm } from "./ConnectionForm";
import { StatusDot } from "./StatusDot";
import {
	IconMonitor,
	IconMoon,
	IconPlus,
	IconServer,
	IconStream,
	IconSun,
	IconTrash,
} from "./icons";

export function StartScreen(): JSX.Element {
	const saved = connections.value;
	// Open the form by default when there is nothing saved; otherwise show cards.
	const showForm = useSignal(saved.length === 0);

	// Freshen reachability dots when the start screen first appears. The cards
	// subscribe to probeStatuses for live updates, so this only runs on mount.
	useEffect(() => {
		void probeAllConnections();
	}, []);

	const hasSaved = saved.length > 0;

	return (
		<div class="dsui-start">
			<div class="dsui-start__panel">
				<header class="dsui-start__hero">
					<div class="dsui-start__brand">
						<IconStream size={26} class="dsui-start__mark" />
						<div>
							<h1 class="dsui-start__title">Durable Streams</h1>
							<p class="dsui-start__tagline">
								Connect to a Durable Streams server to browse, read, and inspect streams.
							</p>
						</div>
					</div>
					<StartThemeToggle />
				</header>

				{showForm.value ? (
					<section class="dsui-start__section" aria-label="New connection">
						<div class="dsui-start__sectionhead">
							<h2 class="dsui-start__heading">New connection</h2>
							{hasSaved ? (
								<button
									type="button"
									class="dsui-btn dsui-btn--ghost"
									onClick={() => {
										showForm.value = false;
									}}
								>
									Back to saved
								</button>
							) : null}
						</div>
						<ConnectionForm
							onConnected={() => {
								/* shell swaps to workspace once activeConnection is set */
							}}
							{...(hasSaved
								? {
										onCancel: () => {
											showForm.value = false;
										},
									}
								: {})}
						/>
					</section>
				) : (
					<section class="dsui-start__section" aria-label="Saved connections">
						<div class="dsui-start__sectionhead">
							<h2 class="dsui-start__heading">Saved connections</h2>
							<button
								type="button"
								class="dsui-btn"
								onClick={() => {
									showForm.value = true;
								}}
							>
								<IconPlus size={15} />
								<span>New connection</span>
							</button>
						</div>
						<ul class="dsui-cards" aria-label="Saved connections">
							{saved.map((conn) => (
								<ConnectionCard key={conn.id} conn={conn} />
							))}
						</ul>
					</section>
				)}
			</div>
		</div>
	);
}

function ConnectionCard(props: { conn: Connection }): JSX.Element {
	const { conn } = props;
	const confirming = useSignal(false);

	// Live status pulled from the per-connection probe map.
	const status = useComputed(() => dotStatusOf(probeStatuses.value[conn.id]));
	const probeText = useComputed(() => {
		const s = probeStatuses.value[conn.id];
		if (s === undefined) return "Not checked yet";
		if (s.state === "checking") return "Checking…";
		return describeProbe(s.probe);
	});

	function connect(): void {
		setActiveConnection(conn.id);
	}

	return (
		<li class="dsui-card">
			<button
				type="button"
				class="dsui-card__main"
				onClick={connect}
				aria-label={`Connect to ${conn.name} (${conn.baseUrl})`}
			>
				<span class="dsui-card__top">
					<StatusDot status={status.value} />
					<span class="dsui-card__name">{conn.name}</span>
				</span>
				<span class="dsui-card__url" title={conn.baseUrl}>
					<IconServer size={13} class="dsui-card__urlicon" />
					{compactUrl(conn.baseUrl)}
				</span>
				<span class="dsui-card__meta">
					<span class="dsui-card__probe" title={probeText.value}>
						{probeText.value}
					</span>
					<span class="dsui-card__used">Used {relativeTime(conn.lastUsedAt)}</span>
				</span>
			</button>

			<div class="dsui-card__actions">
				<button
					type="button"
					class="dsui-iconbtn dsui-iconbtn--sm"
					title="Re-check reachability"
					aria-label={`Re-check ${conn.name}`}
					onClick={() => void probeConnection(conn.id)}
				>
					<StatusDot status={status.value} class="dsui-dot--inbtn" />
				</button>
				{confirming.value ? (
					<span class="dsui-card__confirm">
						<button
							type="button"
							class="dsui-btn dsui-btn--danger dsui-btn--xs"
							onClick={() => removeConnection(conn.id)}
						>
							Delete
						</button>
						<button
							type="button"
							class="dsui-btn dsui-btn--ghost dsui-btn--xs"
							onClick={() => {
								confirming.value = false;
							}}
						>
							Keep
						</button>
					</span>
				) : (
					<button
						type="button"
						class="dsui-iconbtn dsui-iconbtn--sm"
						title="Delete connection"
						aria-label={`Delete ${conn.name}`}
						onClick={() => {
							confirming.value = true;
						}}
					>
						<IconTrash size={14} />
					</button>
				)}
			</div>
		</li>
	);
}

/** A small theme cycle control for the start screen header. */
function StartThemeToggle(): JSX.Element {
	const t = theme.value;
	const label = t === "system" ? "Theme: system" : t === "light" ? "Theme: light" : "Theme: dark";
	return (
		<button
			type="button"
			class="dsui-iconbtn"
			title={label}
			aria-label={label}
			onClick={() => cycleTheme()}
		>
			{t === "light" ? (
				<IconSun size={16} />
			) : t === "dark" ? (
				<IconMoon size={16} />
			) : (
				<IconMonitor size={16} />
			)}
		</button>
	);
}
