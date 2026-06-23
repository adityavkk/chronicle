/**
 * CreateSubscriptionDialog — create (or re-confirm) a subscription on the
 * reserved /__ds/* control plane (PUT /__ds/subscriptions/{id}).
 *
 * A modal form mirroring CreateStreamDialog: a delivery-type picker (webhook |
 * pull-wake) drives which target field shows (a webhook URL vs a wake stream),
 * the stream set is given as a glob pattern AND/OR an explicit list, and an
 * Advanced disclosure carries the lease TTL + a description. It validates live
 * (lib/subscriptionForm, pure), shows the exact equivalent curl for the PUT it
 * will send (lib/subscriptions.previewCreateSubscriptionOperation), and on submit
 * calls the store's createSubscription action — which PUTs, toasts, remembers the
 * id client-side (there is no list-all endpoint), and selects it.
 *
 * Seam: the type radios decide the target field + the previewed
 * CreateSubscriptionOptions; add a field by extending the local signals + the
 * form builder and dropping a <Field> in.
 */

import { useComputed, useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useId } from "preact/hooks";
import {
	type SubscriptionFormValues,
	buildSubscriptionOptions,
	isInsecureWebhookUrl,
	isSubscriptionFormValid,
	validateSubscriptionForm,
} from "../lib/subscriptionForm";
import { previewCreateSubscriptionOperation } from "../lib/subscriptions";
import type { SubscriptionType } from "../lib/types";
import {
	activeConnection,
	closeDialog,
	createSubscription,
	subscriptionInFlight,
} from "../state/store";
import { CurlPreview } from "./CurlPreview";
import { Modal } from "./Modal";
import { IconBell, IconLoader, IconWebhook, IconZap } from "./icons";

const TYPE_OPTIONS: readonly {
	value: SubscriptionType;
	label: string;
	hint: string;
	icon: JSX.Element;
}[] = [
	{
		value: "webhook",
		label: "Webhook",
		hint: "Server POSTs a signed wake to your URL",
		icon: <IconWebhook size={15} />,
	},
	{
		value: "pull-wake",
		label: "Pull-wake",
		hint: "Workers claim a wake from a stream",
		icon: <IconZap size={15} />,
	},
];

export function CreateSubscriptionDialog(): JSX.Element {
	const conn = activeConnection.value;
	const inFlight = subscriptionInFlight.value;

	const id = useSignal("");
	const type = useSignal<SubscriptionType>("webhook");
	const pattern = useSignal("");
	const streamsText = useSignal("");
	const webhookUrl = useSignal("");
	const wakeStream = useSignal("");
	const leaseTtl = useSignal("");
	const description = useSignal("");
	const showErrors = useSignal(false);

	const idBase = useId();
	const ids = {
		id: `${idBase}-id`,
		pattern: `${idBase}-pattern`,
		streams: `${idBase}-streams`,
		webhook: `${idBase}-webhook`,
		wake: `${idBase}-wake`,
		ttl: `${idBase}-ttl`,
		desc: `${idBase}-desc`,
	};

	const values = useComputed<SubscriptionFormValues>(() => ({
		id: id.value,
		type: type.value,
		pattern: pattern.value,
		streamsText: streamsText.value,
		webhookUrl: webhookUrl.value,
		wakeStream: wakeStream.value,
		leaseTtl: leaseTtl.value,
		description: description.value,
	}));
	const errors = useComputed(() => validateSubscriptionForm(values.value));
	const valid = useComputed(() => isSubscriptionFormValid(errors.value));
	const insecure = useComputed(
		() => type.value === "webhook" && isInsecureWebhookUrl(webhookUrl.value),
	);

	// Live curl preview, only once the form is valid (so the URL is well-formed).
	const previewOp = useComputed(() => {
		if (conn === null || !valid.value) return null;
		return previewCreateSubscriptionOperation(conn.baseUrl, buildSubscriptionOptions(values.value));
	});

	function onSubmit(e: Event): void {
		e.preventDefault();
		showErrors.value = true;
		if (!valid.value || conn === null) return;
		void createSubscription(buildSubscriptionOptions(values.value)).then((ok) => {
			if (ok) closeDialog();
		});
	}

	const show = showErrors.value;
	const e = errors.value;

	return (
		<Modal
			title="New subscription"
			icon={<IconBell size={18} />}
			description="Link a set of streams and deliver wakes by webhook or pull-wake. Requires the Redis backend."
			onClose={closeDialog}
		>
			<form class="dsui-form" onSubmit={onSubmit} noValidate>
				<div class="dsui-field">
					<label class="dsui-field__label" for={ids.id}>
						Subscription id
						<span class="dsui-field__req" aria-hidden="true">
							{" *"}
						</span>
					</label>
					<div class="dsui-field__control">
						<input
							id={ids.id}
							class="dsui-input dsui-input--mono"
							type="text"
							placeholder="orders-fanout"
							autocomplete="off"
							spellcheck={false}
							value={id.value}
							aria-invalid={show && e.id !== undefined}
							aria-describedby={show && e.id !== undefined ? `${ids.id}-err` : `${ids.id}-hint`}
							aria-required="true"
							onInput={(ev) => {
								id.value = ev.currentTarget.value;
							}}
						/>
					</div>
					{show && e.id !== undefined ? (
						<p class="dsui-field__error" id={`${ids.id}-err`} role="alert">
							{e.id}
						</p>
					) : (
						<p class="dsui-field__hint" id={`${ids.id}-hint`}>
							Client-provided, unique within the <code>__ds</code> namespace.
						</p>
					)}
				</div>

				<fieldset class="dsui-radioset">
					<legend class="dsui-field__label">Delivery type</legend>
					<div class="dsui-radiorow">
						{TYPE_OPTIONS.map((opt) => (
							<label
								key={opt.value}
								class={`dsui-radio${type.value === opt.value ? " is-checked" : ""}`}
							>
								<input
									type="radio"
									name={`${idBase}-type`}
									value={opt.value}
									checked={type.value === opt.value}
									onChange={() => {
										type.value = opt.value;
									}}
								/>
								<span class="dsui-radio__label">{opt.label}</span>
								<span class="dsui-radio__hint">{opt.hint}</span>
							</label>
						))}
					</div>
				</fieldset>

				<div class="dsui-field">
					<label class="dsui-field__label" for={ids.pattern}>
						Glob pattern
					</label>
					<div class="dsui-field__control">
						<input
							id={ids.pattern}
							class="dsui-input dsui-input--mono"
							type="text"
							placeholder="orders/**"
							autocomplete="off"
							spellcheck={false}
							value={pattern.value}
							aria-invalid={e.pattern !== undefined}
							aria-describedby={
								e.pattern !== undefined ? `${ids.pattern}-err` : `${ids.pattern}-hint`
							}
							onInput={(ev) => {
								pattern.value = ev.currentTarget.value;
							}}
						/>
					</div>
					{e.pattern !== undefined ? (
						<p class="dsui-field__error" id={`${ids.pattern}-err`} role="alert">
							{e.pattern}
						</p>
					) : (
						<p class="dsui-field__hint" id={`${ids.pattern}-hint`}>
							<code>*</code> = one segment, <code>**</code> = zero or more. Or list explicit streams
							below.
						</p>
					)}
				</div>

				<div class="dsui-field">
					<label class="dsui-field__label" for={ids.streams}>
						Explicit streams
					</label>
					<div class="dsui-field__control">
						<textarea
							id={ids.streams}
							class="dsui-textarea dsui-textarea--mono"
							rows={2}
							placeholder={"events/abc\nevents/def"}
							spellcheck={false}
							value={streamsText.value}
							aria-invalid={show && e.streams !== undefined}
							aria-describedby={
								show && e.streams !== undefined ? `${ids.streams}-err` : `${ids.streams}-hint`
							}
							onInput={(ev) => {
								streamsText.value = ev.currentTarget.value;
							}}
						/>
					</div>
					{show && e.streams !== undefined ? (
						<p class="dsui-field__error" id={`${ids.streams}-err`} role="alert">
							{e.streams}
						</p>
					) : (
						<p class="dsui-field__hint" id={`${ids.streams}-hint`}>
							One per line or comma-separated. Linked at their current tail (no replay).
						</p>
					)}
				</div>

				{type.value === "webhook" ? (
					<div class="dsui-field">
						<label class="dsui-field__label" for={ids.webhook}>
							Webhook URL
							<span class="dsui-field__req" aria-hidden="true">
								{" *"}
							</span>
						</label>
						<div class="dsui-field__control">
							<input
								id={ids.webhook}
								class="dsui-input dsui-input--mono"
								type="text"
								placeholder="https://hooks.example.com/ds"
								autocomplete="off"
								spellcheck={false}
								value={webhookUrl.value}
								aria-invalid={show && e.webhookUrl !== undefined}
								aria-describedby={
									show && e.webhookUrl !== undefined ? `${ids.webhook}-err` : `${ids.webhook}-hint`
								}
								aria-required="true"
								onInput={(ev) => {
									webhookUrl.value = ev.currentTarget.value;
								}}
							/>
						</div>
						{show && e.webhookUrl !== undefined ? (
							<p class="dsui-field__error" id={`${ids.webhook}-err`} role="alert">
								{e.webhookUrl}
							</p>
						) : insecure.value ? (
							<p class="dsui-field__hint dsui-field__hint--warn" id={`${ids.webhook}-hint`}>
								Plain http — allowed only for localhost in dev. Production requires https.
							</p>
						) : (
							<p class="dsui-field__hint" id={`${ids.webhook}-hint`}>
								Signed with Ed25519; verify via <code>/__ds/jwks.json</code>.
							</p>
						)}
					</div>
				) : (
					<div class="dsui-field">
						<label class="dsui-field__label" for={ids.wake}>
							Wake stream
							<span class="dsui-field__req" aria-hidden="true">
								{" *"}
							</span>
						</label>
						<div class="dsui-field__control">
							<input
								id={ids.wake}
								class="dsui-input dsui-input--mono"
								type="text"
								placeholder="__ds/wakes/orders"
								autocomplete="off"
								spellcheck={false}
								value={wakeStream.value}
								aria-invalid={show && e.wakeStream !== undefined}
								aria-describedby={
									show && e.wakeStream !== undefined ? `${ids.wake}-err` : `${ids.wake}-hint`
								}
								aria-required="true"
								onInput={(ev) => {
									wakeStream.value = ev.currentTarget.value;
								}}
							/>
						</div>
						{show && e.wakeStream !== undefined ? (
							<p class="dsui-field__error" id={`${ids.wake}-err`} role="alert">
								{e.wakeStream}
							</p>
						) : (
							<p class="dsui-field__hint" id={`${ids.wake}-hint`}>
								Workers read this stream for wake events, then claim a lease.
							</p>
						)}
					</div>
				)}

				<details class="dsui-disclose">
					<summary class="dsui-disclose__summary">Advanced — lease &amp; label</summary>
					<div class="dsui-disclose__body">
						<div class="dsui-field">
							<label class="dsui-field__label" for={ids.ttl}>
								Lease TTL (ms)
							</label>
							<div class="dsui-field__control">
								<input
									id={ids.ttl}
									class="dsui-input dsui-input--mono"
									type="text"
									placeholder="30000"
									autocomplete="off"
									spellcheck={false}
									value={leaseTtl.value}
									aria-invalid={e.leaseTtl !== undefined}
									aria-describedby={e.leaseTtl !== undefined ? `${ids.ttl}-err` : `${ids.ttl}-hint`}
									onInput={(ev) => {
										leaseTtl.value = ev.currentTarget.value;
									}}
								/>
							</div>
							{e.leaseTtl !== undefined ? (
								<p class="dsui-field__error" id={`${ids.ttl}-err`} role="alert">
									{e.leaseTtl}
								</p>
							) : (
								<p class="dsui-field__hint" id={`${ids.ttl}-hint`}>
									1000–600000 ms (1s–10m). Blank uses the server default (30000).
								</p>
							)}
						</div>

						<div class="dsui-field">
							<label class="dsui-field__label" for={ids.desc}>
								Description
							</label>
							<div class="dsui-field__control">
								<input
									id={ids.desc}
									class="dsui-input"
									type="text"
									placeholder="Orders fan-out to billing"
									autocomplete="off"
									value={description.value}
									onInput={(ev) => {
										description.value = ev.currentTarget.value;
									}}
								/>
							</div>
							<p class="dsui-field__hint">A human-readable label (optional).</p>
						</div>
					</div>
				</details>

				<CurlPreview operation={previewOp.value} copyKey="create-sub-curl" />

				<div class="dsui-form__actions">
					<button type="button" class="dsui-btn dsui-btn--ghost" onClick={closeDialog}>
						Cancel
					</button>
					<button
						type="submit"
						class="dsui-btn dsui-btn--primary"
						disabled={inFlight || (show && !valid.value)}
					>
						{inFlight ? <IconLoader size={15} class="dsui-spin" /> : <IconBell size={15} />}
						<span>{inFlight ? "Creating…" : "Create subscription"}</span>
					</button>
				</div>
			</form>
		</Modal>
	);
}
