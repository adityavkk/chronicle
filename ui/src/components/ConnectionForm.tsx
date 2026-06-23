/**
 * ConnectionForm — the "New connection" form. A controlled form with inline,
 * per-field validation and an explicit "Test connection" step that probes the
 * candidate (GET/HEAD __registry__, any HTTP <500 = reachable) before the user
 * commits to saving. On submit it adds the connection via the store and makes
 * it active; the caller decides what to render next (typically the workspace).
 *
 * State is local (component-scoped signals via useState-style refs are avoided;
 * we use a small set of `useSignal`-free local signals created per instance).
 * The form mutates the store only through the sanctioned `addConnection` action.
 *
 * Extensibility: the form is field-driven; add a field by extending
 * ConnectionFormValues + validateConnectionForm and dropping a <Field> in.
 */

import { useSignal } from "@preact/signals";
import type { JSX } from "preact";
import { useId } from "preact/hooks";
import { describeProbe } from "../lib/format";
import type { ConnectionProbe } from "../lib/types";
import {
	type ConnectionFormErrors,
	DEFAULT_STREAM_ROOT,
	isFormValid,
	validateConnectionForm,
} from "../lib/validation";
import { addConnection, testCandidate } from "../state/store";
import { IconCheck, IconClose, IconLoader, IconPlug } from "./icons";

export interface ConnectionFormProps {
	/** Called after a connection is saved and made active. */
	readonly onConnected: () => void;
	/** Called when the user backs out (e.g. closes the form). Optional. */
	readonly onCancel?: (() => void) | undefined;
	/** Render compactly (header popover) vs. spaciously (start screen). */
	readonly compact?: boolean | undefined;
}

/** A single completed/in-flight test result, kept local to the form. */
type TestState =
	| { readonly state: "idle" }
	| { readonly state: "testing" }
	| { readonly state: "done"; readonly probe: ConnectionProbe };

export function ConnectionForm(props: ConnectionFormProps): JSX.Element {
	const { onConnected, onCancel, compact = false } = props;

	const name = useSignal("");
	const baseUrl = useSignal("");
	const streamRoot = useSignal("");
	// Track which fields the user has interacted with, so we only surface an
	// error after a field is touched (or after an attempted submit/test).
	const touched = useSignal<Readonly<Record<string, boolean>>>({});
	const showAll = useSignal(false);
	const test = useSignal<TestState>({ state: "idle" });

	const idBase = useId();
	const ids = {
		name: `${idBase}-name`,
		baseUrl: `${idBase}-baseUrl`,
		streamRoot: `${idBase}-streamRoot`,
	};

	function values() {
		return { name: name.value, baseUrl: baseUrl.value, streamRoot: streamRoot.value };
	}

	// Build the store-action input. `streamRoot` is an optional override: when the
	// user leaves it blank we omit the key entirely (rather than pass `undefined`)
	// so it satisfies exactOptionalPropertyTypes and means "use the default root".
	function connectionInput(): { name: string; baseUrl: string; streamRoot?: string } {
		const root = streamRoot.value.trim();
		const base = { name: name.value, baseUrl: baseUrl.value };
		return root === "" ? base : { ...base, streamRoot: root };
	}

	const errors: ConnectionFormErrors = validateConnectionForm(values());
	const valid = isFormValid(errors);

	function markTouched(field: string): void {
		if (touched.value[field] === true) return;
		touched.value = { ...touched.value, [field]: true };
	}

	function errorFor(field: keyof ConnectionFormErrors): string | undefined {
		if (!showAll.value && touched.value[field] !== true) return undefined;
		return errors[field];
	}

	/** The element id that an input should point its aria-describedby at. */
	function describedBy(id: string, field: keyof ConnectionFormErrors): string {
		return errorFor(field) !== undefined ? `${id}-err` : `${id}-hint`;
	}

	// Editing any field invalidates a prior test result (it may no longer apply).
	function onEdit(): void {
		if (test.value.state !== "idle") test.value = { state: "idle" };
	}

	async function runTest(): Promise<void> {
		showAll.value = true;
		if (!valid) return;
		test.value = { state: "testing" };
		const probe = await testCandidate(connectionInput());
		test.value = { state: "done", probe };
	}

	function onSubmit(event: Event): void {
		event.preventDefault();
		showAll.value = true;
		if (!valid) return;
		addConnection(connectionInput());
		onConnected();
	}

	const t = test.value;

	return (
		<form
			class={`dsui-connform${compact ? " dsui-connform--compact" : ""}`}
			onSubmit={onSubmit}
			noValidate
		>
			<Field
				id={ids.name}
				label="Name"
				hint="Optional — defaults to the URL."
				error={errorFor("name")}
			>
				<input
					id={ids.name}
					class="dsui-input"
					type="text"
					placeholder="Local dev"
					autoComplete="off"
					value={name.value}
					aria-invalid={errorFor("name") !== undefined}
					aria-describedby={describedBy(ids.name, "name")}
					onInput={(e) => {
						name.value = (e.currentTarget as HTMLInputElement).value;
						onEdit();
					}}
					onBlur={() => markTouched("name")}
				/>
			</Field>

			<Field
				id={ids.baseUrl}
				label="Base URL"
				hint="Include the port, e.g. http://localhost:4437"
				error={errorFor("baseUrl")}
				required
			>
				<input
					id={ids.baseUrl}
					class="dsui-input"
					type="url"
					inputMode="url"
					placeholder="http://localhost:4437"
					autoComplete="off"
					spellcheck={false}
					value={baseUrl.value}
					aria-invalid={errorFor("baseUrl") !== undefined}
					aria-describedby={describedBy(ids.baseUrl, "baseUrl")}
					aria-required="true"
					onInput={(e) => {
						baseUrl.value = (e.currentTarget as HTMLInputElement).value;
						onEdit();
					}}
					onBlur={() => markTouched("baseUrl")}
				/>
			</Field>

			<Field
				id={ids.streamRoot}
				label="Stream root"
				hint={`Optional — defaults to ${DEFAULT_STREAM_ROOT}`}
				error={errorFor("streamRoot")}
			>
				<input
					id={ids.streamRoot}
					class="dsui-input"
					type="text"
					placeholder={DEFAULT_STREAM_ROOT}
					autoComplete="off"
					spellcheck={false}
					value={streamRoot.value}
					aria-invalid={errorFor("streamRoot") !== undefined}
					aria-describedby={describedBy(ids.streamRoot, "streamRoot")}
					onInput={(e) => {
						streamRoot.value = (e.currentTarget as HTMLInputElement).value;
						onEdit();
					}}
					onBlur={() => markTouched("streamRoot")}
				/>
			</Field>

			{t.state !== "idle" ? (
				// <output> is a polite live region by default (implicit role="status"),
				// so the screen reader announces the test outcome without a manual role.
				<output
					class={`dsui-testresult${
						t.state === "done" ? (t.probe.ok ? " is-ok" : " is-down") : " is-testing"
					}`}
					aria-live="polite"
				>
					{t.state === "testing" ? (
						<>
							<IconLoader size={15} class="dsui-spin" />
							<span>Testing connection…</span>
						</>
					) : t.probe.ok ? (
						<>
							<IconCheck size={15} />
							<span>
								Reachable — HTTP {t.probe.status} in {t.probe.latencyMs} ms
							</span>
						</>
					) : (
						<>
							<IconClose size={15} />
							<span>{describeProbe(t.probe)}</span>
						</>
					)}
				</output>
			) : null}

			<div class="dsui-connform__actions">
				{onCancel !== undefined ? (
					<button type="button" class="dsui-btn dsui-btn--ghost" onClick={() => onCancel()}>
						Cancel
					</button>
				) : null}
				<button
					type="button"
					class="dsui-btn"
					onClick={() => void runTest()}
					disabled={t.state === "testing"}
				>
					{t.state === "testing" ? (
						<IconLoader size={15} class="dsui-spin" />
					) : (
						<IconCheck size={15} />
					)}
					<span>Test connection</span>
				</button>
				<button type="submit" class="dsui-btn dsui-btn--primary">
					<IconPlug size={15} />
					<span>Save &amp; connect</span>
				</button>
			</div>
		</form>
	);
}

/** A labelled form field with hint + inline error wiring (aria-describedby). */
function Field(props: {
	id: string;
	label: string;
	hint: string;
	error?: string | undefined;
	required?: boolean | undefined;
	children: JSX.Element;
}): JSX.Element {
	const { id, label, hint, error, required = false, children } = props;
	const hintId = `${id}-hint`;
	const errId = `${id}-err`;
	return (
		<div class={`dsui-field${error !== undefined ? " is-invalid" : ""}`}>
			<label class="dsui-field__label" for={id}>
				{label}
				{required ? (
					<span class="dsui-field__req" aria-hidden="true">
						{" *"}
					</span>
				) : null}
			</label>
			<div class="dsui-field__control">{children}</div>
			{error !== undefined ? (
				<p class="dsui-field__error" id={errId} role="alert">
					{error}
				</p>
			) : (
				<p class="dsui-field__hint" id={hintId}>
					{hint}
				</p>
			)}
		</div>
	);
}
