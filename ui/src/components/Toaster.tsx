/**
 * Toaster — the transient-notification primitive.
 *
 * A small, accessible toast stack rendered once at the app shell. It reads the
 * `toasts` signal and renders each {@link Toast}; the store owns creation and
 * auto-dismiss (addToast / dismissToast), so this component is pure layout —
 * the established "components lay out, they do not compute" seam.
 *
 *   ┌─────────────────────────────────────────┐
 *   │ ✓  Published                         ✕   │   one toast
 *   │    playground/demo                       │
 *   │                              [Copy curl] │   optional action
 *   └─────────────────────────────────────────┘
 *
 * Accessibility:
 *  - The region is an `aria-live` log: "assertive" for errors/warnings (they
 *    interrupt), "polite" for info/success (they wait their turn). The region
 *    is what gets announced, so each card needs no redundant live role.
 *  - The kind is named for screen readers via an sr-only prefix on the title.
 *  - A labelled dismiss button per toast; the optional action is a real button.
 *
 * Motion: the slide/fade-in is suppressed under prefers-reduced-motion (handled
 * in app.css), so the stack appears without movement for users who ask for it.
 */

import type { JSX } from "preact";
import type { Toast, ToastKind } from "../lib/types";
import { dismissToast, toasts } from "../state/store";
import { IconAlertTriangle, IconCheck, IconClose, IconInfo } from "./icons";

/** The icon for a toast kind. */
function ToastIcon(props: { kind: ToastKind }): JSX.Element {
	switch (props.kind) {
		case "success":
			return <IconCheck size={16} />;
		case "warning":
		case "error":
			return <IconAlertTriangle size={16} />;
		case "info":
			return <IconInfo size={16} />;
	}
}

/** A spoken word for the kind, used in the screen-reader label. */
function kindLabel(kind: ToastKind): string {
	switch (kind) {
		case "success":
			return "Success";
		case "warning":
			return "Warning";
		case "error":
			return "Error";
		case "info":
			return "Notice";
	}
}

function ToastCard(props: { toast: Toast }): JSX.Element {
	const { toast } = props;
	return (
		<div class={`dsui-toast dsui-toast--${toast.kind}`}>
			<span class="dsui-toast__icon" aria-hidden="true">
				<ToastIcon kind={toast.kind} />
			</span>
			<div class="dsui-toast__body">
				<p class="dsui-toast__title">
					<span class="sr-only">{kindLabel(toast.kind)}: </span>
					{toast.title}
				</p>
				{toast.message !== undefined ? <p class="dsui-toast__message">{toast.message}</p> : null}
				{toast.action !== undefined ? (
					<div class="dsui-toast__actions">
						<button
							type="button"
							class="dsui-btn dsui-btn--xs"
							onClick={() => {
								toast.action?.onAction();
								dismissToast(toast.id);
							}}
						>
							{toast.action.label}
						</button>
					</div>
				) : null}
			</div>
			<button
				type="button"
				class="dsui-toast__close"
				aria-label="Dismiss notification"
				title="Dismiss"
				onClick={() => dismissToast(toast.id)}
			>
				<IconClose size={14} />
			</button>
		</div>
	);
}

/**
 * The toast stack. Renders into two live regions split by urgency so assistive
 * tech announces errors/warnings assertively and the rest politely. Renders
 * nothing visible when empty (the live regions stay mounted for announcements).
 */
export function Toaster(): JSX.Element {
	const all = toasts.value;
	const assertive = all.filter((t) => t.kind === "error" || t.kind === "warning");
	const polite = all.filter((t) => t.kind === "info" || t.kind === "success");
	return (
		<div class="dsui-toaster" aria-label="Notifications">
			<div class="dsui-toaster__region" role="log" aria-live="assertive" aria-relevant="additions">
				{assertive.map((t) => (
					<ToastCard key={t.id} toast={t} />
				))}
			</div>
			<div class="dsui-toaster__region" role="log" aria-live="polite" aria-relevant="additions">
				{polite.map((t) => (
					<ToastCard key={t.id} toast={t} />
				))}
			</div>
		</div>
	);
}
