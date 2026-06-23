/**
 * ExportMenu — download the currently-loaded rows (the paged batch or the live
 * tail buffer) without leaving the page. A small popover offering:
 *
 *   - Export NDJSON  one decoded value per line (lib/export.rowsToNdjson)
 *   - Export CSV     flattened, RFC-4180-escaped (lib/export.rowsToCsv)
 *   - Save raw body  the exact response bytes, kind-appropriate extension
 *                    (paged grid only — the tail delivers rows, not one body)
 *
 * It is layout only: the serialization is pure in `lib/export`, and the download
 * itself is `lib/export.downloadBlob` (a Blob + an `<a download>`, guarded for a
 * missing DOM). No new dependency — the menu mirrors StreamActionsMenu's popover
 * (closed on outside-click + Escape, focus restored to the trigger). Filenames
 * come from the stream path + offset so successive exports are distinguishable.
 *
 * Used in two places: the MessagesWorkspace pager (with `rawBytes`) and the
 * TailPanel footer (rows only). Both anchor at the bottom of their container, so
 * the popover opens upward by default.
 */

import type { JSX } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";
import {
	CSV_MIME,
	NDJSON_MIME,
	buildExportFilename,
	downloadBlob,
	rawExtensionForKind,
	rawMimeForKind,
	rowsToCsv,
	rowsToNdjson,
} from "../lib/export";
import type { GridRow, StreamKind } from "../lib/types";
import { IconDownload } from "./icons";

export interface ExportMenuProps {
	/** The rows to serialize for NDJSON / CSV (the paged batch or tail buffer). */
	readonly rows: readonly GridRow[];
	/** Stream content kind — picks the raw-body extension + MIME. */
	readonly kind: StreamKind;
	/** Stream path, for the filename. */
	readonly streamPath: string;
	/** The offset this view started from, for the filename (e.g. "-1", "now"). */
	readonly offset: string;
	/** Exact response bytes for "Save raw body". Omitted for the tail buffer. */
	readonly rawBytes?: Uint8Array;
	/** Open the popover upward (default true — both call sites are bottom-anchored). */
	readonly openUp?: boolean;
	/** Trigger size: "xs" matches the dense tail footer; "sm" the pager. */
	readonly size?: "xs" | "sm";
}

export function ExportMenu(props: ExportMenuProps): JSX.Element {
	const { rows, kind, streamPath, offset, rawBytes, openUp = true, size = "sm" } = props;
	const [open, setOpen] = useState(false);
	const wrapRef = useRef<HTMLDivElement>(null);
	const triggerRef = useRef<HTMLButtonElement>(null);

	useEffect(() => {
		if (!open) return;
		function onPointer(e: PointerEvent): void {
			const target = e.target;
			if (wrapRef.current !== null && target instanceof Node && !wrapRef.current.contains(target)) {
				setOpen(false);
			}
		}
		function onKey(e: KeyboardEvent): void {
			if (e.key === "Escape") {
				setOpen(false);
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

	const hasRows = rows.length > 0;
	const hasRaw = rawBytes !== undefined && rawBytes.byteLength > 0;
	const disabled = !hasRows && !hasRaw;

	// If there is suddenly nothing to export while the menu is open — e.g. the
	// live tail buffer is cleared (Clear / a stream switch) with the popover up —
	// close it. An all-disabled popover is a keyboard dead-end, and the Escape
	// handler's focus-restore would otherwise target a now-disabled, unfocusable
	// trigger and silently strand focus.
	useEffect(() => {
		if (disabled && open) setOpen(false);
	}, [disabled, open]);

	function saveNdjson(): void {
		downloadBlob(
			buildExportFilename(streamPath, offset, "ndjson"),
			NDJSON_MIME,
			rowsToNdjson(rows),
		);
		setOpen(false);
	}
	function saveCsv(): void {
		downloadBlob(buildExportFilename(streamPath, offset, "csv"), CSV_MIME, rowsToCsv(rows));
		setOpen(false);
	}
	function saveRaw(): void {
		if (rawBytes === undefined) return;
		downloadBlob(
			buildExportFilename(streamPath, offset, rawExtensionForKind(kind)),
			rawMimeForKind(kind),
			rawBytes,
		);
		setOpen(false);
	}

	const rowCount = `${rows.length} ${rows.length === 1 ? "row" : "rows"}`;

	return (
		<div class="dsui-export" ref={wrapRef}>
			<button
				type="button"
				ref={triggerRef}
				class={`dsui-btn dsui-btn--ghost${size === "xs" ? " dsui-btn--xs" : ""}`}
				title="Export the loaded rows"
				aria-label="Export the loaded rows"
				aria-haspopup="menu"
				aria-expanded={open}
				disabled={disabled}
				onClick={() => setOpen((v) => !v)}
			>
				<IconDownload size={size === "xs" ? 13 : 14} />
				<span>Export</span>
			</button>

			{open ? (
				<div
					class={`dsui-export__pop${openUp ? " dsui-export__pop--up" : ""}`}
					role="menu"
					aria-label="Export the loaded rows"
				>
					<p class="dsui-export__hint">Export {rowCount}</p>
					<button
						type="button"
						role="menuitem"
						class="dsui-export__item"
						disabled={!hasRows}
						onClick={saveNdjson}
					>
						<IconDownload size={15} />
						<span class="dsui-export__label">NDJSON</span>
						<code class="dsui-export__meta">.ndjson</code>
					</button>
					<button
						type="button"
						role="menuitem"
						class="dsui-export__item"
						disabled={!hasRows}
						onClick={saveCsv}
					>
						<IconDownload size={15} />
						<span class="dsui-export__label">CSV</span>
						<code class="dsui-export__meta">.csv</code>
					</button>
					{hasRaw ? (
						<>
							<div class="dsui-export__sep" />
							<button type="button" role="menuitem" class="dsui-export__item" onClick={saveRaw}>
								<IconDownload size={15} />
								<span class="dsui-export__label">Save raw body</span>
								<code class="dsui-export__meta">.{rawExtensionForKind(kind)}</code>
							</button>
						</>
					) : null}
				</div>
			) : null}
		</div>
	);
}
