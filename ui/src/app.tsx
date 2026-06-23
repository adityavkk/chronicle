/**
 * App shell — the top-level layout. Four regions on a CSS grid:
 *
 *   ┌───────────────────────── header ─────────────────────────┐
 *   │ brand · ConnectionManager (switcher + theme)              │
 *   ├──────────┬─────────────────────────────┬─────────────────┤
 *   │ Navigator│  MessagesWorkspace          │  Inspector      │
 *   │  (left)  │  (center)                   │  (right)        │
 *   └──────────┴─────────────────────────────┴─────────────────┘
 *
 * The grid collapses responsively via container queries (see app.css). Each
 * region is a typed component slot, so adding a new tab/panel is a localized
 * change — swap the component rendered in a slot, or add a region to the grid
 * template and drop a component into it.
 */

import type { JSX } from "preact";
import { ConnectionManager } from "./components/ConnectionManager";
import { CreateStreamDialog } from "./components/CreateStreamDialog";
import { CreateSubscriptionDialog } from "./components/CreateSubscriptionDialog";
import { ForkDialog } from "./components/ForkDialog";
import { Inspector } from "./components/Inspector";
import { MessagesWorkspace } from "./components/MessagesWorkspace";
import { MetricsWorkspace } from "./components/MetricsWorkspace";
import { Navigator } from "./components/Navigator";
import { StartScreen } from "./components/StartScreen";
import { SubscriptionWorkspace } from "./components/SubscriptionWorkspace";
import { Toaster } from "./components/Toaster";
import { WakeMonitorWorkspace } from "./components/WakeMonitorWorkspace";
import { IconPanelRight, IconStream } from "./components/icons";
import {
	activeConnection,
	activeDialog,
	centerView,
	dismissError,
	errorMessage,
	inspectorCollapsed,
	toggleInspector,
} from "./state/store";

function ErrorBanner(): JSX.Element | null {
	const msg = errorMessage.value;
	if (msg === null) return null;
	return (
		<div class="dsui-banner dsui-banner--error" role="alert">
			<span>{msg}</span>
			<button type="button" class="dsui-banner__close" onClick={() => dismissError()}>
				Dismiss
			</button>
		</div>
	);
}

/** The center workspace for the active view. The single center-pane routing seam. */
function CenterWorkspace(): JSX.Element {
	switch (centerView.value) {
		case "subscription":
			return <SubscriptionWorkspace />;
		case "wakes":
			return <WakeMonitorWorkspace />;
		case "metrics":
			return <MetricsWorkspace />;
		case "messages":
			return <MessagesWorkspace />;
	}
}

/** The routed content: the start screen, or the three-pane workspace shell. */
function Shell(): JSX.Element {
	// No active connection -> the connection manager's start screen. Otherwise
	// the three-pane workspace shell. This is the top-level routing seam.
	if (activeConnection.value === null) {
		return <StartScreen />;
	}

	// The right-hand inspector is the message inspector; it is meaningful only in
	// the messages view. The subscription + metrics views carry their own detail
	// in the center pane, so the inspector folds away for them. Within the
	// messages view the user can also collapse it by hand (the header toggle).
	const collapsed = inspectorCollapsed.value;
	const inMessages = centerView.value === "messages";
	const showInspector = inMessages && !collapsed;

	return (
		<div class={`dsui-shell${showInspector ? "" : " dsui-shell--noinspector"}`}>
			<header class="dsui-header">
				<div class="dsui-brand">
					<span class="dsui-brand__badge" aria-hidden="true">
						<IconStream size={16} class="dsui-brand__mark" />
					</span>
					<span class="dsui-brand__name">dsui</span>
					<span class="dsui-brand__sub">Durable Streams console</span>
				</div>
				<div class="dsui-header__actions">
					{inMessages ? (
						<button
							type="button"
							class="dsui-iconbtn"
							aria-pressed={!collapsed}
							aria-label="Toggle inspector panel"
							title={collapsed ? "Show inspector panel" : "Hide inspector panel"}
							onClick={() => toggleInspector()}
						>
							<IconPanelRight size={16} />
						</button>
					) : null}
					<ConnectionManager />
				</div>
			</header>

			<ErrorBanner />

			<main class={`dsui-main${showInspector ? "" : " dsui-main--noinspector"}`}>
				<aside class="dsui-region dsui-region--nav">
					<Navigator />
				</aside>
				<section class="dsui-region dsui-region--workspace">
					<CenterWorkspace />
				</section>
				{showInspector ? (
					<aside class="dsui-region dsui-region--inspector">
						<Inspector />
					</aside>
				) : null}
			</main>
		</div>
	);
}

/** The active write dialog (create / fork), or nothing. Driven by the store. */
function Dialogs(): JSX.Element | null {
	switch (activeDialog.value) {
		case "create":
			return <CreateStreamDialog />;
		case "fork":
			return <ForkDialog />;
		case "subscription":
			return <CreateSubscriptionDialog />;
		case null:
			return null;
	}
}

export function App(): JSX.Element {
	// The Toaster + write dialogs are mounted above the routing seam so they
	// survive a start-screen ↔ workspace transition and overlay either view.
	return (
		<>
			<Shell />
			<Dialogs />
			<Toaster />
		</>
	);
}
