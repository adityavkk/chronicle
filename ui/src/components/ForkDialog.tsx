/**
 * ForkDialog — Feature 4: fork a stream (a CREATE with Stream-Forked-From +
 * Stream-Fork-Offset). A fork inherits the source's data up to the fork offset,
 * then diverges as its own stream.
 *
 * The dialog is seeded from the store's forkSeed: the source path and a default
 * offset (usually the current read's Stream-Next-Offset or "now"). The user
 * names the new fork path, can adjust the fork offset, and may add a sub-offset
 * (a batch element index). It shows the exact equivalent curl and, on submit,
 * calls the store's forkStream — which PUTs the fork, toasts, refreshes the
 * navigator, and selects the new fork. Validation + the preview come from
 * lib/streamForm; this component only lays out the controls.
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useId } from "preact/hooks";
import {
	parseSubOffset,
	previewCreateOperation,
	validateStreamPath,
	validateSubOffset,
} from "../lib/streamForm";
import {
	activeConnection,
	closeDialog,
	forkSeed,
	forkStream,
	operationInFlight,
} from "../state/store";
import { CurlPreview } from "./CurlPreview";
import { Modal } from "./Modal";
import { IconFork, IconLoader } from "./icons";

export function ForkDialog(): JSX.Element | null {
	const conn = activeConnection.value;
	const seed = forkSeed.value;
	const inFlight = operationInFlight.value;

	const newPath = useSignal("");
	const offset = useSignal(seed?.offset ?? "now");
	const subOffset = useSignal("");
	const showErrors = useSignal(false);

	const idBase = useId();
	const ids = {
		newPath: `${idBase}-path`,
		offset: `${idBase}-offset`,
		sub: `${idBase}-sub`,
	};

	const pathError = useComputed(() => validateStreamPath(newPath.value));
	const offsetError = useComputed(() =>
		offset.value.trim() === "" ? "A fork offset is required." : null,
	);
	const subError = useComputed(() => validateSubOffset(subOffset.value));
	const valid = useComputed(
		() => pathError.value === null && offsetError.value === null && subError.value === null,
	);

	const previewOp = useComputed(() => {
		if (conn === null || seed === null || !valid.value || newPath.value.trim() === "") return null;
		const sub = parseSubOffset(subOffset.value);
		const fork =
			sub === undefined
				? { fromPath: seed.fromPath, offset: offset.value.trim() }
				: { fromPath: seed.fromPath, offset: offset.value.trim(), subOffset: sub };
		return previewCreateOperation(conn.baseUrl, conn.streamRoot, {
			path: newPath.value.trim(),
			contentType: "application/octet-stream",
			fork,
		});
	});

	if (seed === null) return null;

	function onSubmit(e: Event): void {
		e.preventDefault();
		showErrors.value = true;
		if (!valid.value || seed === null) return;
		void forkStream(
			newPath.value.trim(),
			seed.fromPath,
			offset.value.trim(),
			parseSubOffset(subOffset.value),
		).then((ok) => {
			if (ok) closeDialog();
		});
	}

	const showPathErr = showErrors.value && pathError.value !== null;
	const showOffsetErr = showErrors.value && offsetError.value !== null;
	const showSubErr = subError.value !== null;

	return (
		<Modal
			title="Fork stream"
			icon={<IconFork size={18} />}
			description="Create a new stream that inherits this one's data up to an offset, then diverges."
			onClose={closeDialog}
		>
			<form class="dsui-form" onSubmit={onSubmit} noValidate>
				<div class="dsui-forksource">
					<span class="dsui-forksource__label">Forking from</span>
					<code class="dsui-forksource__path">{seed.fromPath}</code>
				</div>

				<div class="dsui-field">
					<label class="dsui-field__label" for={ids.newPath}>
						New fork path
						<span class="dsui-field__req" aria-hidden="true">
							{" *"}
						</span>
					</label>
					<div class="dsui-field__control">
						<input
							id={ids.newPath}
							class="dsui-input dsui-input--mono"
							type="text"
							placeholder={`${seed.fromPath}-fork`}
							autocomplete="off"
							spellcheck={false}
							value={newPath.value}
							aria-invalid={showPathErr}
							aria-describedby={showPathErr ? `${ids.newPath}-err` : `${ids.newPath}-hint`}
							aria-required="true"
							onInput={(e) => {
								newPath.value = e.currentTarget.value;
							}}
						/>
					</div>
					{showPathErr ? (
						<p class="dsui-field__error" id={`${ids.newPath}-err`} role="alert">
							{pathError.value}
						</p>
					) : (
						<p class="dsui-field__hint" id={`${ids.newPath}-hint`}>
							The path of the new fork stream.
						</p>
					)}
				</div>

				<div class="dsui-formrow">
					<div class="dsui-field">
						<label class="dsui-field__label" for={ids.offset}>
							Fork offset
							<span class="dsui-field__req" aria-hidden="true">
								{" *"}
							</span>
						</label>
						<div class="dsui-field__control">
							<input
								id={ids.offset}
								class="dsui-input dsui-input--mono"
								type="text"
								placeholder="now"
								autocomplete="off"
								spellcheck={false}
								value={offset.value}
								aria-invalid={showOffsetErr}
								aria-describedby={showOffsetErr ? `${ids.offset}-err` : `${ids.offset}-hint`}
								aria-required="true"
								onInput={(e) => {
									offset.value = e.currentTarget.value;
								}}
							/>
						</div>
						{showOffsetErr ? (
							<p class="dsui-field__error" id={`${ids.offset}-err`} role="alert">
								{offsetError.value}
							</p>
						) : (
							<p class="dsui-field__hint" id={`${ids.offset}-hint`}>
								<code>Stream-Fork-Offset</code> — data up to here is inherited.
							</p>
						)}
					</div>

					<div class="dsui-field">
						<label class="dsui-field__label" for={ids.sub}>
							Sub-offset
						</label>
						<div class="dsui-field__control">
							<input
								id={ids.sub}
								class="dsui-input dsui-input--mono"
								type="text"
								inputMode="numeric"
								placeholder="optional"
								autocomplete="off"
								spellcheck={false}
								value={subOffset.value}
								aria-invalid={showSubErr}
								aria-describedby={showSubErr ? `${ids.sub}-err` : `${ids.sub}-hint`}
								onInput={(e) => {
									subOffset.value = e.currentTarget.value;
								}}
							/>
						</div>
						{showSubErr ? (
							<p class="dsui-field__error" id={`${ids.sub}-err`} role="alert">
								{subError.value}
							</p>
						) : (
							<p class="dsui-field__hint" id={`${ids.sub}-hint`}>
								<code>Stream-Fork-Sub-Offset</code> — a batch element index.
							</p>
						)}
					</div>
				</div>

				<CurlPreview operation={previewOp.value} copyKey="fork-curl" />

				<div class="dsui-form__actions">
					<button type="button" class="dsui-btn dsui-btn--ghost" onClick={closeDialog}>
						Cancel
					</button>
					<button
						type="submit"
						class="dsui-btn dsui-btn--primary"
						disabled={inFlight || (showErrors.value && !valid.value)}
					>
						{inFlight ? <IconLoader size={15} class="dsui-spin" /> : <IconFork size={15} />}
						<span>{inFlight ? "Forking…" : "Create fork"}</span>
					</button>
				</div>
			</form>
		</Modal>
	);
}
