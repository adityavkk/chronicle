/**
 * Modal — the shared dialog shell for the write-operation forms (create, fork).
 *
 * A small, accessible modal: a backdrop plus a labelled `role="dialog"` panel
 * with `aria-modal`. It owns the cross-cutting dialog behavior so each form
 * stays focused on its fields:
 *  - Escape closes it; a backdrop click closes it; the close button closes it.
 *  - Focus moves into the panel on open and is restored to the previously
 *    focused element on close.
 *  - Focus is trapped: Tab / Shift+Tab cycle within the panel.
 *  - The title is wired via aria-labelledby; an optional description via
 *    aria-describedby.
 *
 * It is presentation + behavior only — it raises onClose and renders children;
 * the store owns which dialog is open. Motion (the pop-in) is suppressed under
 * prefers-reduced-motion through the shared app.css rules.
 */

import type { ComponentChildren, JSX } from "preact";
import { useEffect, useId, useRef } from "preact/hooks";
import { IconClose } from "./icons";

/** A typed ref to the native dialog element used by the modal panel. */
type DialogRef = HTMLDialogElement;

export interface ModalProps {
	/** The dialog title, shown in the header and wired as aria-labelledby. */
	readonly title: string;
	/** Optional icon rendered before the title. */
	readonly icon?: JSX.Element | undefined;
	/** Optional one-line description under the title (aria-describedby). */
	readonly description?: string | undefined;
	/** Called when the user dismisses the dialog (Escape / backdrop / close). */
	readonly onClose: () => void;
	/** The dialog body. */
	readonly children: ComponentChildren;
}

/** Selector for the tabbable elements used by the focus trap. */
const FOCUSABLE =
	'a[href],button:not([disabled]),textarea:not([disabled]),input:not([disabled]),select:not([disabled]),[tabindex]:not([tabindex="-1"])';

export function Modal(props: ModalProps): JSX.Element {
	const { title, icon, description, onClose, children } = props;
	const panelRef = useRef<DialogRef>(null);
	const base = useId();
	const titleId = `${base}-title`;
	const descId = `${base}-desc`;

	// On mount: remember the previously focused element, move focus into the
	// panel, and wire Escape + a focus trap. On unmount: restore focus.
	useEffect(() => {
		const previouslyFocused =
			document.activeElement instanceof HTMLElement ? document.activeElement : null;

		const panel = panelRef.current;
		// Focus the first focusable control, else the panel itself.
		const first = panel?.querySelector<HTMLElement>(FOCUSABLE);
		(first ?? panel)?.focus();

		function onKey(e: KeyboardEvent): void {
			if (e.key === "Escape") {
				e.preventDefault();
				onClose();
				return;
			}
			if (e.key !== "Tab" || panel === null) return;
			const items = Array.from(panel.querySelectorAll<HTMLElement>(FOCUSABLE)).filter(
				(el) => el.offsetParent !== null || el === document.activeElement,
			);
			if (items.length === 0) {
				e.preventDefault();
				panel.focus();
				return;
			}
			const firstItem = items[0];
			const lastItem = items[items.length - 1];
			const activeEl = document.activeElement;
			if (firstItem === undefined || lastItem === undefined) return;
			if (e.shiftKey && activeEl === firstItem) {
				e.preventDefault();
				lastItem.focus();
			} else if (!e.shiftKey && activeEl === lastItem) {
				e.preventDefault();
				firstItem.focus();
			}
		}

		document.addEventListener("keydown", onKey);
		return () => {
			document.removeEventListener("keydown", onKey);
			previouslyFocused?.focus();
		};
	}, [onClose]);

	/** Dismiss only when the backdrop itself (not the panel) is the click target. */
	function onBackdropClick(e: MouseEvent): void {
		if (e.target === e.currentTarget) onClose();
	}

	return (
		// biome-ignore lint/a11y/useKeyWithClickEvents: the backdrop is a supplementary pointer affordance; Escape (wired on document) and the labelled close button are the keyboard paths, so the backdrop does not need its own key handler.
		<div class="dsui-modal" onClick={onBackdropClick}>
			{/* A native <dialog open> (rendered in-flow, no showModal) so the
			    semantics are native; aria-modal + the manual focus trap above make
			    it behave as a true modal over our own backdrop. */}
			<dialog
				open
				class="dsui-modal__panel"
				aria-modal="true"
				aria-labelledby={titleId}
				aria-describedby={description !== undefined ? descId : undefined}
				ref={panelRef}
			>
				<header class="dsui-modal__head">
					<div class="dsui-modal__titlewrap">
						{icon !== undefined ? (
							<span class="dsui-modal__icon" aria-hidden="true">
								{icon}
							</span>
						) : null}
						<div class="dsui-modal__titletext">
							<h2 class="dsui-modal__title" id={titleId}>
								{title}
							</h2>
							{description !== undefined ? (
								<p class="dsui-modal__desc" id={descId}>
									{description}
								</p>
							) : null}
						</div>
					</div>
					<button
						type="button"
						class="dsui-iconbtn dsui-iconbtn--sm"
						aria-label="Close dialog"
						title="Close"
						onClick={onClose}
					>
						<IconClose size={16} />
					</button>
				</header>
				<div class="dsui-modal__body">{children}</div>
			</dialog>
		</div>
	);
}
