/**
 * StreamActionsMenu — Feature 3: the lifecycle toolbar on the workspace header.
 *
 * A popover menu of per-stream operations for the selected stream:
 *  - Fork…            opens the fork dialog seeded from the current offset.
 *  - Refresh metadata HEAD the stream (updates Content-Type/kind + the protocol
 *                     disclosure), via the store's refreshMeta.
 *  - Close stream     POST Stream-Closed: true (store.closeStream).
 *  - Delete stream    DELETE, behind an inline confirm (store.deleteStream);
 *                     clears selection + updates the navigator/registry.
 *
 * Each destructive action shows the exact equivalent curl inside the menu before
 * it runs (from lib/streamForm previews), and the underlying store action raises
 * a toast and refreshes. The popover follows the established pattern: closed on
 * outside-click + Escape, focus restored to the trigger.
 *
 * Seam: add a per-stream action by adding a row (and, if it is a write, a
 * preview Operation + a store action). Reads/writes never live in this layout.
 */

import type { JSX } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";
import { previewCloseOperation, previewDeleteOperation } from "../lib/streamForm";
import type { StreamInfo } from "../lib/types";
import {
	activeConnection,
	closeStream,
	deleteStream,
	lastRead,
	metaLoading,
	openForkDialog,
	operationInFlight,
	refreshMeta,
} from "../state/store";
import { CurlPreview } from "./CurlPreview";
import { IconFork, IconLoader, IconLock, IconMore, IconRefresh, IconTrash } from "./icons";
import { focusFirstMenuItem, focusMenuItem, handleMenuKeydown } from "./menuKeyboard";

export function StreamActionsMenu(props: { stream: StreamInfo }): JSX.Element {
	const { stream } = props;
	const conn = activeConnection.value;
	const inFlight = operationInFlight.value;
	const metaBusy = metaLoading.value;
	const read = lastRead.value;

	const [open, setOpen] = useState(false);
	const [confirmingDelete, setConfirmingDelete] = useState(false);
	const wrapRef = useRef<HTMLDivElement>(null);
	const popRef = useRef<HTMLDivElement>(null);
	const triggerRef = useRef<HTMLButtonElement>(null);
	const deleteItemRef = useRef<HTMLButtonElement>(null);
	const confirmCancelRef = useRef<HTMLButtonElement>(null);
	const confirmWasOpen = useRef(false);

	// On open, move focus into the menu (first item); the roving-tabindex helper
	// keeps Arrow/Home/End/Tab inside it from there.
	useEffect(() => {
		if (open) focusFirstMenuItem(popRef.current);
	}, [open]);

	// The delete-confirm sub-flow swaps the "Delete stream…" menuitem for a
	// Cancel/Delete pair. Keep focus inside the open menu across that swap: focus
	// Cancel when the confirm opens, and return focus to the Delete item when it
	// closes — otherwise focus would fall back to <body> and the popover's
	// Arrow/Tab handling (which is wired on the popover) would stop firing.
	useEffect(() => {
		if (confirmingDelete) {
			confirmCancelRef.current?.focus();
			confirmWasOpen.current = true;
		} else if (confirmWasOpen.current) {
			confirmWasOpen.current = false;
			const item = deleteItemRef.current;
			if (item !== null && popRef.current !== null) focusMenuItem(popRef.current, item);
		}
	}, [confirmingDelete]);

	useEffect(() => {
		if (!open) return;
		function onPointer(e: PointerEvent): void {
			const target = e.target;
			if (wrapRef.current !== null && target instanceof Node && !wrapRef.current.contains(target)) {
				setOpen(false);
				setConfirmingDelete(false);
			}
		}
		function onKey(e: KeyboardEvent): void {
			if (e.key === "Escape") {
				setOpen(false);
				setConfirmingDelete(false);
				triggerRef.current?.focus();
			}
		}
		document.addEventListener("pointerdown", onPointer);
		document.addEventListener("keydown", onKey);
		return () => {
			document.removeEventListener("pointerdown", onPointer);
			document.removeEventListener("keydown", onKey);
		};
	}, [open]);

	// Default fork offset: resume from where the current read ended, else "now".
	const forkOffset = read?.nextOffset ?? "now";

	const closeOp =
		conn === null ? null : previewCloseOperation(conn.baseUrl, conn.streamRoot, stream.path);
	const deleteOp =
		conn === null ? null : previewDeleteOperation(conn.baseUrl, conn.streamRoot, stream.path);

	function close(): void {
		setOpen(false);
		setConfirmingDelete(false);
	}

	return (
		<div class="dsui-actions" ref={wrapRef}>
			<button
				type="button"
				ref={triggerRef}
				class="dsui-iconbtn dsui-iconbtn--sm"
				title="Stream actions"
				aria-label="Stream actions"
				aria-haspopup="menu"
				aria-expanded={open}
				onClick={() => setOpen((v) => !v)}
			>
				<IconMore size={16} />
			</button>

			{open ? (
				<div
					class="dsui-actions__pop"
					role="menu"
					aria-label={`Actions for ${stream.path}`}
					ref={popRef}
					onKeyDown={(e) => handleMenuKeydown(e, popRef.current)}
				>
					<button
						type="button"
						role="menuitem"
						tabIndex={-1}
						class="dsui-actions__item"
						onClick={() => {
							openForkDialog(stream.path, forkOffset);
							close();
						}}
					>
						<IconFork size={15} />
						<span class="dsui-actions__label">Fork…</span>
						<code class="dsui-actions__meta">@ {forkOffset}</code>
					</button>

					<button
						type="button"
						role="menuitem"
						tabIndex={-1}
						class="dsui-actions__item"
						disabled={metaBusy}
						onClick={() => {
							void refreshMeta(stream.path);
							close();
						}}
					>
						<IconRefresh size={15} class={metaBusy ? "dsui-spin" : undefined} />
						<span class="dsui-actions__label">Refresh metadata (HEAD)</span>
					</button>

					<div class="dsui-actions__sep" />

					<button
						type="button"
						role="menuitem"
						tabIndex={-1}
						class="dsui-actions__item"
						disabled={inFlight}
						onClick={() => {
							void closeStream(stream.path);
							close();
						}}
					>
						<IconLock size={15} />
						<span class="dsui-actions__label">Close stream</span>
					</button>
					<CurlPreview operation={closeOp} copyKey="action-close-curl" label="curl for close" />

					<div class="dsui-actions__sep" />

					{confirmingDelete ? (
						<div class="dsui-actions__confirm">
							<p class="sr-only">Confirm deleting this stream.</p>
							<p class="dsui-actions__confirmtext">
								Delete <code>{stream.path}</code>? Soft-deletes if forks exist, else removed.
							</p>
							<div class="dsui-actions__confirmrow">
								<button
									type="button"
									ref={confirmCancelRef}
									class="dsui-btn dsui-btn--xs dsui-btn--ghost"
									onClick={() => setConfirmingDelete(false)}
								>
									Cancel
								</button>
								<button
									type="button"
									class="dsui-btn dsui-btn--xs dsui-btn--danger"
									disabled={inFlight}
									onClick={() => {
										void deleteStream(stream.path);
										close();
									}}
								>
									{inFlight ? <IconLoader size={13} class="dsui-spin" /> : <IconTrash size={13} />}
									<span>Delete</span>
								</button>
							</div>
						</div>
					) : (
						<button
							type="button"
							ref={deleteItemRef}
							role="menuitem"
							tabIndex={-1}
							class="dsui-actions__item dsui-actions__item--danger"
							onClick={() => setConfirmingDelete(true)}
						>
							<IconTrash size={15} />
							<span class="dsui-actions__label">Delete stream…</span>
						</button>
					)}
					<CurlPreview operation={deleteOp} copyKey="action-delete-curl" label="curl for delete" />
				</div>
			) : null}
		</div>
	);
}
