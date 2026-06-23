/**
 * CreateStreamDialog — Feature 1: create a new stream (PUT /v1/stream/{path}).
 *
 * A modal form with the essentials up top (path + content-type) and an
 * "Advanced" disclosure for TTL, Expires-At, and "create closed". It validates
 * the path live, shows the exact equivalent curl for what it will send, and on
 * submit calls the store's createStream action — which PUTs the stream, toasts,
 * refreshes the navigator (discovery re-reads __registry__), and selects the new
 * stream. All validation + the curl preview come from lib/streamForm (pure);
 * this component only lays out the controls.
 *
 * Seam: the content-type radios drive the wire Content-Type the server fixes the
 * stream's type to. Add a field by extending the local signals + the previewed
 * CreateStreamOptions and dropping a <Field> in.
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useId } from "preact/hooks";
import {
	type CreateKind,
	contentTypeForKind,
	previewCreateOperation,
	validateExpiresAt,
	validateStreamPath,
	validateTtl,
} from "../lib/streamForm";
import type { CreateStreamOptions } from "../lib/types";
import { activeConnection, closeDialog, createStream, operationInFlight } from "../state/store";
import { CurlPreview } from "./CurlPreview";
import { Modal } from "./Modal";
import { IconFilePlus, IconLoader, IconPlus } from "./icons";

const KIND_OPTIONS: readonly { value: CreateKind; label: string; hint: string }[] = [
	{ value: "text", label: "Text", hint: "text/plain" },
	{ value: "json", label: "JSON", hint: "application/json" },
	{ value: "binary", label: "Binary", hint: "application/octet-stream" },
];

export function CreateStreamDialog(): JSX.Element {
	const conn = activeConnection.value;
	const inFlight = operationInFlight.value;

	const path = useSignal("");
	const kind = useSignal<CreateKind>("json");
	const ttl = useSignal("");
	const expiresAt = useSignal("");
	const closed = useSignal(false);
	const showErrors = useSignal(false);

	const idBase = useId();
	const ids = {
		path: `${idBase}-path`,
		ttl: `${idBase}-ttl`,
		expires: `${idBase}-expires`,
	};

	const pathError = useComputed(() => validateStreamPath(path.value));
	const ttlError = useComputed(() => validateTtl(ttl.value));
	const expiresError = useComputed(() => validateExpiresAt(expiresAt.value));
	const valid = useComputed(
		() => pathError.value === null && ttlError.value === null && expiresError.value === null,
	);

	// Build the typed CreateStreamOptions from current fields (omitting blank
	// optionals so they satisfy exactOptionalPropertyTypes — see ConnectionForm).
	function buildOptions(): CreateStreamOptions {
		const opts: {
			path: string;
			contentType: ReturnType<typeof contentTypeForKind>;
			ttl?: string;
			expiresAt?: string;
			closed?: boolean;
		} = {
			path: path.value.trim(),
			contentType: contentTypeForKind(kind.value),
		};
		if (ttl.value.trim() !== "") opts.ttl = ttl.value.trim();
		if (expiresAt.value.trim() !== "") opts.expiresAt = expiresAt.value.trim();
		if (closed.value) opts.closed = true;
		return opts;
	}

	// Live curl preview, only once the form is valid (so the URL is well-formed).
	const previewOp = useComputed(() => {
		if (conn === null || !valid.value || path.value.trim() === "") return null;
		return previewCreateOperation(conn.baseUrl, conn.streamRoot, buildOptions());
	});

	function onSubmit(e: Event): void {
		e.preventDefault();
		showErrors.value = true;
		if (!valid.value || conn === null) return;
		void createStream(buildOptions()).then((ok) => {
			if (ok) closeDialog();
		});
	}

	const showPathErr = showErrors.value && pathError.value !== null;
	const showTtlErr = ttlError.value !== null;
	const showExpiresErr = expiresError.value !== null;

	return (
		<Modal
			title="New stream"
			icon={<IconFilePlus size={18} />}
			description="Create a stream on this server. Its content type is fixed at creation."
			onClose={closeDialog}
		>
			<form class="dsui-form" onSubmit={onSubmit} noValidate>
				<div class="dsui-field">
					<label class="dsui-field__label" for={ids.path}>
						Stream path
						<span class="dsui-field__req" aria-hidden="true">
							{" *"}
						</span>
					</label>
					<div class="dsui-field__control">
						<input
							id={ids.path}
							class="dsui-input dsui-input--mono"
							type="text"
							placeholder="orders/created"
							autocomplete="off"
							spellcheck={false}
							value={path.value}
							aria-invalid={showPathErr}
							aria-describedby={showPathErr ? `${ids.path}-err` : `${ids.path}-hint`}
							aria-required="true"
							onInput={(e) => {
								path.value = e.currentTarget.value;
							}}
						/>
					</div>
					{showPathErr ? (
						<p class="dsui-field__error" id={`${ids.path}-err`} role="alert">
							{pathError.value}
						</p>
					) : (
						<p class="dsui-field__hint" id={`${ids.path}-hint`}>
							Segments joined by slashes, e.g. <code>orders/created</code>. No leading slash.
						</p>
					)}
				</div>

				<fieldset class="dsui-radioset">
					<legend class="dsui-field__label">Content type</legend>
					<div class="dsui-radiorow">
						{KIND_OPTIONS.map((opt) => (
							<label
								key={opt.value}
								class={`dsui-radio${kind.value === opt.value ? " is-checked" : ""}`}
							>
								<input
									type="radio"
									name={`${idBase}-kind`}
									value={opt.value}
									checked={kind.value === opt.value}
									onChange={() => {
										kind.value = opt.value;
									}}
								/>
								<span class="dsui-radio__label">{opt.label}</span>
								<span class="dsui-radio__hint">{opt.hint}</span>
							</label>
						))}
					</div>
				</fieldset>

				<details class="dsui-disclose">
					<summary class="dsui-disclose__summary">Advanced — lifetime &amp; initial state</summary>
					<div class="dsui-disclose__body">
						<div class="dsui-formrow">
							<div class="dsui-field">
								<label class="dsui-field__label" for={ids.ttl}>
									TTL
								</label>
								<div class="dsui-field__control">
									<input
										id={ids.ttl}
										class="dsui-input dsui-input--mono"
										type="text"
										placeholder="1h"
										autocomplete="off"
										spellcheck={false}
										value={ttl.value}
										aria-invalid={showTtlErr}
										aria-describedby={showTtlErr ? `${ids.ttl}-err` : `${ids.ttl}-hint`}
										onInput={(e) => {
											ttl.value = e.currentTarget.value;
										}}
									/>
								</div>
								{showTtlErr ? (
									<p class="dsui-field__error" id={`${ids.ttl}-err`} role="alert">
										{ttlError.value}
									</p>
								) : (
									<p class="dsui-field__hint" id={`${ids.ttl}-hint`}>
										<code>Stream-TTL</code>, e.g. 1h, 30m.
									</p>
								)}
							</div>

							<div class="dsui-field">
								<label class="dsui-field__label" for={ids.expires}>
									Expires at
								</label>
								<div class="dsui-field__control">
									<input
										id={ids.expires}
										class="dsui-input dsui-input--mono"
										type="text"
										placeholder="2030-01-01T00:00:00Z"
										autocomplete="off"
										spellcheck={false}
										value={expiresAt.value}
										aria-invalid={showExpiresErr}
										aria-describedby={showExpiresErr ? `${ids.expires}-err` : `${ids.expires}-hint`}
										onInput={(e) => {
											expiresAt.value = e.currentTarget.value;
										}}
									/>
								</div>
								{showExpiresErr ? (
									<p class="dsui-field__error" id={`${ids.expires}-err`} role="alert">
										{expiresError.value}
									</p>
								) : (
									<p class="dsui-field__hint" id={`${ids.expires}-hint`}>
										<code>Stream-Expires-At</code>, RFC3339.
									</p>
								)}
							</div>
						</div>

						<label class="dsui-check">
							<input
								type="checkbox"
								checked={closed.value}
								onChange={(e) => {
									closed.value = e.currentTarget.checked;
								}}
							/>
							<span>
								Create already closed <code>(Stream-Closed: true)</code>
							</span>
						</label>
					</div>
				</details>

				<CurlPreview operation={previewOp.value} copyKey="create-curl" />

				<div class="dsui-form__actions">
					<button type="button" class="dsui-btn dsui-btn--ghost" onClick={closeDialog}>
						Cancel
					</button>
					<button
						type="submit"
						class="dsui-btn dsui-btn--primary"
						disabled={inFlight || (showErrors.value && !valid.value)}
					>
						{inFlight ? <IconLoader size={15} class="dsui-spin" /> : <IconPlus size={15} />}
						<span>{inFlight ? "Creating…" : "Create stream"}</span>
					</button>
				</div>
			</form>
		</Modal>
	);
}
