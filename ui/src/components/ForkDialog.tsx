/**
 * ForkDialog — Feature 4: fork a stream (a CREATE with Stream-Forked-From +
 * Stream-Fork-Offset). A fork inherits the source's data up to the fork point,
 * then diverges as its own stream.
 *
 * The dialog is seeded from the store's forkSeed (the source path + a default
 * offset). Rather than make the user hand-enter the coupled Fork-Offset /
 * Sub-Offset pair — which produced confusing 400s when a tail offset was paired
 * with a sub-offset that overshoots — the common cases are a friendly,
 * message-centric "Fork point" choice:
 *   - Everything (fork at the current tail) — the default.
 *   - Nothing (an empty fork).
 *   - First N messages — only for a JSON source — keep the first N and diverge.
 * lib/streamForm.forkSelection maps the choice to the correct (offset, subOffset)
 * so the user never couples them by hand. An "Advanced" disclosure still exposes
 * the raw offset + sub-offset for power users; when it is open the dialog submits
 * those raw values instead. Either way it calls the store's forkStream (which
 * resolves a blank/"now" offset to the source tail), shows the exact equivalent
 * curl, and on success toasts, refreshes the navigator, and selects the fork.
 * Validation + the preview come from lib/streamForm; this component lays out.
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useId } from "preact/hooks";
import {
	type ForkPoint,
	type ForkSelection,
	forkSelection,
	parseSubOffset,
	previewCreateOperation,
	validateFirstN,
	validateStreamPath,
	validateSubOffset,
} from "../lib/streamForm";
import type { ForkSource, StreamKind } from "../lib/types";
import {
	activeConnection,
	closeDialog,
	forkSeed,
	forkStream,
	lastRead,
	operationInFlight,
	streams,
} from "../state/store";
import { CurlPreview } from "./CurlPreview";
import { Modal } from "./Modal";
import { IconFork, IconLoader } from "./icons";

/** The friendly fork-point choices, in display order, with their plain copy. */
const FORK_POINTS: readonly { value: ForkPoint; label: string; hint: string }[] = [
	{
		value: "everything",
		label: "Everything",
		hint: "Fork at the current tail — inherit all messages.",
	},
	{ value: "nothing", label: "Nothing", hint: "Start an empty fork — inherit none of the source." },
	{
		value: "first-n",
		label: "First N messages",
		hint: "Inherit the first N messages from the start, then diverge.",
	},
];

export function ForkDialog(): JSX.Element | null {
	const conn = activeConnection.value;
	const seed = forkSeed.value;
	const inFlight = operationInFlight.value;

	const newPath = useSignal("");
	const point = useSignal<ForkPoint>("everything");
	const firstN = useSignal("");
	// Advanced (raw) mode mirrors the previous dialog: a free Fork-Offset and an
	// optional Sub-Offset. `advanced` follows the disclosure's open state and, when
	// open, takes over both the submitted values and the curl preview.
	const advanced = useSignal(false);
	const offset = useSignal(seed?.offset ?? "now");
	const subOffset = useSignal("");
	const showErrors = useSignal(false);

	const idBase = useId();
	const ids = {
		newPath: `${idBase}-path`,
		firstN: `${idBase}-firstn`,
		offset: `${idBase}-offset`,
		sub: `${idBase}-sub`,
	};

	// Classify the source stream: prefer its StreamInfo kind, then the in-hand
	// read's kind, else treat as non-JSON (so "First N messages" stays hidden).
	const sourceKind = useComputed<StreamKind | null>(() => {
		if (seed === null) return null;
		const info = streams.value.find((s) => s.path === seed.fromPath);
		if (info !== undefined) return info.kind;
		return lastRead.value?.kind ?? null;
	});
	const sourceIsJson = useComputed(() => sourceKind.value === "json");

	// The known message count, only when the in-hand read is THIS JSON source.
	// null means "unknown" — any N ≥ 0 is allowed and the server is the arbiter.
	const knownCount = useComputed<number | null>(() => {
		const read = lastRead.value;
		if (seed === null || read === null) return null;
		if (read.path !== seed.fromPath || read.kind !== "json") return null;
		return read.rows.length;
	});

	const pathError = useComputed(() => validateStreamPath(newPath.value));
	// "First N" only applies (and is only shown) for a JSON source in friendly mode.
	const firstNApplies = useComputed(
		() => !advanced.value && point.value === "first-n" && sourceIsJson.value,
	);
	const firstNError = useComputed(() =>
		firstNApplies.value ? validateFirstN(firstN.value, knownCount.value) : null,
	);
	const offsetError = useComputed(() =>
		advanced.value && offset.value.trim() === "" ? "A fork offset is required." : null,
	);
	const subError = useComputed(() => (advanced.value ? validateSubOffset(subOffset.value) : null));

	const valid = useComputed(
		() =>
			pathError.value === null &&
			firstNError.value === null &&
			offsetError.value === null &&
			subError.value === null,
	);

	// Resolve the (offset, subOffset) pair that WILL be sent, for both submit and
	// the curl preview, from whichever mode is active.
	const selection = useComputed<ForkSelection>(() => {
		if (advanced.value) {
			return { offset: offset.value.trim(), subOffset: parseSubOffset(subOffset.value) };
		}
		// In friendly mode, a JSON-only "First N" falls back to "everything" if the
		// source is not JSON (the option is hidden in that case).
		const effective: ForkPoint =
			point.value === "first-n" && !sourceIsJson.value ? "everything" : point.value;
		return forkSelection(effective, parseSubOffset(firstN.value) ?? 0);
	});

	const previewOp = useComputed(() => {
		if (conn === null || seed === null || !valid.value || newPath.value.trim() === "") return null;
		const sel = selection.value;
		const fork: ForkSource =
			sel.subOffset === undefined
				? { fromPath: seed.fromPath, offset: sel.offset }
				: { fromPath: seed.fromPath, offset: sel.offset, subOffset: sel.subOffset };
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
		const sel = selection.value;
		void forkStream(newPath.value.trim(), seed.fromPath, sel.offset, sel.subOffset).then((ok) => {
			if (ok) closeDialog();
		});
	}

	const showPathErr = showErrors.value && pathError.value !== null;
	const showFirstNErr = firstNError.value !== null;
	const showOffsetErr = showErrors.value && offsetError.value !== null;
	const showSubErr = subError.value !== null;

	return (
		<Modal
			title="Fork stream"
			icon={<IconFork size={18} />}
			description="Create a new stream that starts as a copy of this one, then goes its own way. Choose how much of the source to keep."
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

				<fieldset class="dsui-radioset" disabled={advanced.value}>
					<legend class="dsui-field__label">Fork point — how much to inherit</legend>
					<div class="dsui-radiorow">
						{FORK_POINTS.map((opt) =>
							opt.value === "first-n" && !sourceIsJson.value ? null : (
								<label
									key={opt.value}
									class={`dsui-radio${point.value === opt.value ? " is-checked" : ""}`}
								>
									<input
										type="radio"
										name={`${idBase}-point`}
										value={opt.value}
										checked={point.value === opt.value}
										onChange={() => {
											point.value = opt.value;
										}}
									/>
									<span class="dsui-radio__label">{opt.label}</span>
									<span class="dsui-radio__hint">{opt.hint}</span>
								</label>
							),
						)}
					</div>

					{firstNApplies.value ? (
						<div class="dsui-field">
							<label class="dsui-field__label" for={ids.firstN}>
								Messages to keep
								<span class="dsui-field__req" aria-hidden="true">
									{" *"}
								</span>
							</label>
							<div class="dsui-field__control">
								<input
									id={ids.firstN}
									class="dsui-input dsui-input--mono"
									type="text"
									inputMode="numeric"
									placeholder="0"
									autocomplete="off"
									spellcheck={false}
									value={firstN.value}
									aria-invalid={showFirstNErr}
									aria-describedby={`${ids.firstN}-hint${showFirstNErr ? ` ${ids.firstN}-err` : ""}`}
									aria-required="true"
									onInput={(e) => {
										firstN.value = e.currentTarget.value;
									}}
								/>
							</div>
							{showFirstNErr ? (
								<p class="dsui-field__error" id={`${ids.firstN}-err`} role="alert">
									{firstNError.value}
								</p>
							) : null}
							<p class="dsui-field__hint" id={`${ids.firstN}-hint`}>
								{knownCount.value === null
									? "Counted from the start of the source. The source message count is unknown here, so the server will reject a number that overshoots."
									: `Counted from the start of the source. Of ${knownCount.value} message${knownCount.value === 1 ? "" : "s"} read — a larger number overshoots and is rejected.`}
							</p>
						</div>
					) : null}
				</fieldset>

				{/* Controlled disclosure (the playgroundOpen idiom): a real button is the
				    single source of truth for which mode is active, so it is reliably
				    keyboard-operable and testable. Open => the fork submits the raw values
				    below instead of the Fork point above. */}
				<div class={`dsui-disclose${advanced.value ? " is-open" : ""}`}>
					<button
						type="button"
						class="dsui-disclose__summary dsui-disclose__summary--btn"
						aria-expanded={advanced.value}
						aria-controls={`${idBase}-adv`}
						onClick={() => {
							advanced.value = !advanced.value;
						}}
					>
						Advanced — set the raw offset &amp; sub-offset
					</button>
					{advanced.value ? (
						<div class="dsui-disclose__body" id={`${idBase}-adv`}>
							<p class="dsui-field__hint">
								Active: the fork will use the raw values below instead of the Fork point above.
							</p>
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
											A batch-boundary offset in <code>-1</code> (beginning) … <code>now</code>{" "}
											(tail). Everything up to here is inherited. Blank or <code>now</code> means
											the tail.
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
											Optional. For a JSON source, the number of messages PAST the fork offset to
											also inherit. Pairing it with the tail overshoots (nothing past the tail) and
											is rejected.
										</p>
									)}
								</div>
							</div>
						</div>
					) : null}
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
