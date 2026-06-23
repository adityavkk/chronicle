/**
 * PublishComposer — Feature 2: the append/publish composer in the workspace.
 *
 * A content-type-aware editor for POST /v1/stream/{path}:
 *  - JSON streams get a batch editor (a JSON array; each element is one
 *    message) with live validation + an element count, and a "format" helper.
 *  - text streams get a plain textarea sent as raw bytes.
 *  - binary streams get a text/base64 input; base64 is decoded to real bytes so
 *    the body is the exact payload (and the curl preview is honest about bytes).
 *
 * An "Idempotent producer" disclosure carries Producer-Id / Epoch / Seq (fenced
 * + deduped server-side), and a "close stream after sending" checkbox appends
 * and closes atomically. It shows the exact equivalent curl and, on send, calls
 * the store's appendMessages — which POSTs, advances the producer seq on
 * success, surfaces a producer-conflict toast, and refreshes the read. All
 * validation + the preview come from lib/streamForm (pure); this lays it out.
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useId } from "preact/hooks";
import {
	isProducerValid,
	previewAppendOperation,
	toProducerIdentity,
	validateJsonBatch,
	validateProducer,
} from "../lib/streamForm";
import type { AppendOptions, ProducerIdentity, StreamContentType, StreamKind } from "../lib/types";
import {
	activeConnection,
	appendMessages,
	operationInFlight,
	selectedStream,
	setProducerIdentity,
} from "../state/store";
import { CurlPreview } from "./CurlPreview";
import { IconLoader, IconSend } from "./icons";

/** Decode a base64 string to bytes; returns null on malformed input. */
function decodeBase64(text: string): Uint8Array | null {
	const cleaned = text.replace(/\s+/g, "");
	if (cleaned === "") return new Uint8Array(0);
	try {
		const binary = globalThis.atob(cleaned);
		const bytes = new Uint8Array(binary.length);
		for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
		return bytes;
	} catch {
		return null;
	}
}

/** A placeholder JSON batch shown the first time a JSON composer opens. */
const JSON_PLACEHOLDER = '[\n  { "id": 1, "event": "created" }\n]';

export function PublishComposer(): JSX.Element | null {
	const stream = selectedStream.value;
	const conn = activeConnection.value;
	const inFlight = operationInFlight.value;

	const text = useSignal("");
	const binaryMode = useSignal<"text" | "base64">("text");
	const closeAfter = useSignal(false);
	const useProducer = useSignal(false);
	const producerId = useSignal("");
	const producerEpoch = useSignal("0");
	const producerSeq = useSignal("0");
	const showErrors = useSignal(false);

	const idBase = useId();

	const kind: StreamKind = stream?.kind ?? "binary";

	// Validate the producer block only when it is enabled.
	const producerErrors = useComputed(() =>
		useProducer.value
			? validateProducer({
					id: producerId.value,
					epoch: producerEpoch.value,
					seq: producerSeq.value,
				})
			: {},
	);
	const producerOk = useComputed(() => isProducerValid(producerErrors.value));

	// Validate the body per kind, and produce the body + a count summary.
	const bodyState = useComputed<
		{ ok: true; body: string | Uint8Array; summary: string } | { ok: false; error: string }
	>(() => {
		if (kind === "json") {
			const out = validateJsonBatch(text.value);
			if (!out.ok) return { ok: false, error: out.error };
			return {
				ok: true,
				body: out.normalized,
				summary: `${out.count} ${out.count === 1 ? "message" : "messages"}`,
			};
		}
		if (kind === "text") {
			if (text.value === "") return { ok: false, error: "Enter some text to publish." };
			return { ok: true, body: text.value, summary: `${new Blob([text.value]).size} bytes` };
		}
		// binary
		if (binaryMode.value === "base64") {
			const bytes = decodeBase64(text.value);
			if (bytes === null) return { ok: false, error: "Invalid base64." };
			if (bytes.byteLength === 0)
				return { ok: false, error: "Enter some base64 bytes to publish." };
			return { ok: true, body: bytes, summary: `${bytes.byteLength} bytes (from base64)` };
		}
		if (text.value === "") return { ok: false, error: "Enter some text to publish." };
		const bytes = new TextEncoder().encode(text.value);
		return { ok: true, body: bytes, summary: `${bytes.byteLength} bytes` };
	});

	const valid = useComputed(() => bodyState.value.ok && producerOk.value);

	function currentProducer(): ProducerIdentity | undefined {
		if (!useProducer.value) return undefined;
		const id = toProducerIdentity({
			id: producerId.value,
			epoch: producerEpoch.value,
			seq: producerSeq.value,
		});
		return id ?? undefined;
	}

	// Live curl preview once the body validates.
	const previewOp = useComputed(() => {
		const bs = bodyState.value;
		if (conn === null || stream === null || !bs.ok) return null;
		const producer = currentProducer();
		const contentType: StreamContentType = stream.contentType ?? "application/octet-stream";
		const opts: AppendOptions = {
			body: bs.body,
			contentType,
			...(producer !== undefined ? { producer } : {}),
			...(closeAfter.value ? { closeAfter: true } : {}),
		};
		return previewAppendOperation(conn.baseUrl, conn.streamRoot, stream.path, opts);
	});

	if (stream === null) return null;

	function onSend(e: Event): void {
		e.preventDefault();
		showErrors.value = true;
		const bs = bodyState.value;
		if (stream === null || !bs.ok || !producerOk.value) return;
		// Mirror the producer identity into the store so the seq advances on success.
		setProducerIdentity(currentProducer() ?? null);
		void appendMessages(stream.path, bs.body, {
			...(closeAfter.value ? { closeAfter: true } : {}),
			...(stream.contentType !== null ? { contentType: stream.contentType } : {}),
		}).then((ok) => {
			if (ok) {
				text.value = "";
				showErrors.value = false;
				// Pull the advanced seq back into the field for the next publish.
				const next = currentProducer();
				if (next !== undefined) producerSeq.value = String(next.seq + 1);
			}
		});
	}

	const bs = bodyState.value;
	const bodyError = showErrors.value && !bs.ok ? bs.error : null;
	const summary = bs.ok ? bs.summary : "";
	const pe = producerErrors.value;

	return (
		<details class="dsui-publish">
			<summary class="dsui-publish__summary">
				<IconSend size={14} class="dsui-publish__icon" />
				<span class="dsui-publish__title">Publish to this stream</span>
				<span class={`dsui-kind dsui-kind--${kind}`}>{kind}</span>
				<span class="dsui-publish__hint">append a message batch</span>
			</summary>

			<form class="dsui-publish__body" onSubmit={onSend} noValidate>
				{kind === "binary" ? (
					<fieldset class="dsui-publish__modes">
						<legend class="sr-only">Binary input mode</legend>
						{(["text", "base64"] as const).map((m) => (
							<label
								key={m}
								class={`dsui-radio dsui-radio--inline${binaryMode.value === m ? " is-checked" : ""}`}
							>
								<input
									type="radio"
									name={`${idBase}-binmode`}
									checked={binaryMode.value === m}
									onChange={() => {
										binaryMode.value = m;
									}}
								/>
								<span class="dsui-radio__label">{m === "text" ? "UTF-8 text" : "Base64"}</span>
							</label>
						))}
					</fieldset>
				) : null}

				<div class="dsui-field">
					<label class="dsui-field__label" for={`${idBase}-body`}>
						{kind === "json" ? "JSON batch" : kind === "text" ? "Text" : "Payload"}
					</label>
					<textarea
						id={`${idBase}-body`}
						class={`dsui-textarea${kind === "binary" || kind === "json" ? " dsui-textarea--mono" : ""}`}
						rows={kind === "json" ? 6 : 4}
						placeholder={
							kind === "json"
								? JSON_PLACEHOLDER
								: kind === "text"
									? "your message text…"
									: binaryMode.value === "base64"
										? "SGVsbG8gd29ybGQ="
										: "bytes as UTF-8 text…"
						}
						spellcheck={false}
						value={text.value}
						aria-invalid={bodyError !== null}
						aria-describedby={`${idBase}-bodymsg`}
						onInput={(e) => {
							text.value = e.currentTarget.value;
						}}
					/>
					<p
						class={`dsui-publish__status${bodyError !== null ? " is-error" : ""}`}
						id={`${idBase}-bodymsg`}
						role={bodyError !== null ? "alert" : undefined}
					>
						{bodyError !== null ? bodyError : summary !== "" ? `Ready · ${summary}` : ""}
					</p>
					{kind === "json" ? (
						<div class="dsui-publish__tools">
							<button
								type="button"
								class="dsui-btn dsui-btn--xs"
								onClick={() => {
									const out = validateJsonBatch(text.value);
									if (out.ok) {
										try {
											text.value = JSON.stringify(JSON.parse(out.normalized), null, 2);
										} catch {
											/* leave as-is */
										}
									}
								}}
							>
								Format JSON
							</button>
						</div>
					) : null}
				</div>

				<details class="dsui-disclose">
					<summary class="dsui-disclose__summary">Idempotent producer (optional)</summary>
					<div class="dsui-disclose__body">
						<label class="dsui-check">
							<input
								type="checkbox"
								checked={useProducer.value}
								onChange={(e) => {
									useProducer.value = e.currentTarget.checked;
								}}
							/>
							<span>
								Send <code>Producer-*</code> headers — epoch fences old producers, seq dedupes.
							</span>
						</label>
						{useProducer.value ? (
							<div class="dsui-formrow dsui-formrow--three">
								<div class="dsui-field">
									<label class="dsui-field__label" for={`${idBase}-pid`}>
										Producer id
									</label>
									<div class="dsui-field__control">
										<input
											id={`${idBase}-pid`}
											class="dsui-input dsui-input--mono"
											type="text"
											placeholder="producer-1"
											autocomplete="off"
											spellcheck={false}
											value={producerId.value}
											aria-invalid={pe.id !== undefined}
											onInput={(e) => {
												producerId.value = e.currentTarget.value;
											}}
										/>
									</div>
									{pe.id !== undefined ? (
										<p class="dsui-field__error" role="alert">
											{pe.id}
										</p>
									) : null}
								</div>
								<div class="dsui-field">
									<label class="dsui-field__label" for={`${idBase}-pepoch`}>
										Epoch
									</label>
									<div class="dsui-field__control">
										<input
											id={`${idBase}-pepoch`}
											class="dsui-input dsui-input--mono"
											type="text"
											inputMode="numeric"
											value={producerEpoch.value}
											aria-invalid={pe.epoch !== undefined}
											onInput={(e) => {
												producerEpoch.value = e.currentTarget.value;
											}}
										/>
									</div>
									{pe.epoch !== undefined ? (
										<p class="dsui-field__error" role="alert">
											{pe.epoch}
										</p>
									) : null}
								</div>
								<div class="dsui-field">
									<label class="dsui-field__label" for={`${idBase}-pseq`}>
										Seq
									</label>
									<div class="dsui-field__control">
										<input
											id={`${idBase}-pseq`}
											class="dsui-input dsui-input--mono"
											type="text"
											inputMode="numeric"
											value={producerSeq.value}
											aria-invalid={pe.seq !== undefined}
											onInput={(e) => {
												producerSeq.value = e.currentTarget.value;
											}}
										/>
									</div>
									{pe.seq !== undefined ? (
										<p class="dsui-field__error" role="alert">
											{pe.seq}
										</p>
									) : null}
								</div>
							</div>
						) : null}
					</div>
				</details>

				<CurlPreview operation={previewOp.value} copyKey="publish-curl" />

				<div class="dsui-publish__foot">
					<label class="dsui-check dsui-check--inline">
						<input
							type="checkbox"
							checked={closeAfter.value}
							onChange={(e) => {
								closeAfter.value = e.currentTarget.checked;
							}}
						/>
						<span>Close stream after sending</span>
					</label>
					<button
						type="submit"
						class="dsui-btn dsui-btn--primary"
						disabled={inFlight || (showErrors.value && !valid.value)}
					>
						{inFlight ? <IconLoader size={15} class="dsui-spin" /> : <IconSend size={15} />}
						<span>
							{inFlight ? "Publishing…" : closeAfter.value ? "Publish & close" : "Publish"}
						</span>
					</button>
				</div>
			</form>
		</details>
	);
}
