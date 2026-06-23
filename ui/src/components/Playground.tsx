/**
 * Playground — one-click presets that bootstrap a newcomer through the whole
 * Durable Streams API on a safe, obvious sample namespace (playground/…).
 *
 * Each preset is a small, clearly-labelled action that drives the SAME store
 * actions the rest of the UI uses (createStream / appendMessages / forkStream /
 * closeStream / deleteStream / startTail / runDemoProducer), so a newcomer can
 * watch the real operations run — toasts, the protocol disclosure, and (for the
 * demo producer + live tail) the grid updating in real time. Nothing here is a
 * special path: it is the public store surface, pre-filled.
 *
 * Each preset also discloses two things before it runs, so a newcomer can learn
 * the protocol by reading rather than only clicking:
 *   - a plain-language line describing exactly what the preset will do, and
 *   - the EXACT equivalent curl (via <CurlPreview>), built from the same pure
 *     preview helpers (lib/streamForm + lib/tail) the rest of the write UI uses,
 *     so the command shown is identical to what dsClient sends.
 *
 * The presets run in sensible order but each is independent and idempotent-ish
 * (re-running "create" over an existing stream just no-ops server-side or
 * re-selects). They operate only on the SAMPLE_STREAM path so they can never
 * touch a user's real streams. When there is no active connection the curl
 * previews fall back to a placeholder origin so they still teach the shape.
 *
 * Seam: add a preset by adding an entry to `buildPresets` — give it a label, a
 * "what it does" line, the store action it runs, and the previewed Operation for
 * its curl. Keep the work in the store; this is layout + a friendly label.
 */

import type { JSX } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";
import {
	previewAppendOperation,
	previewCloseOperation,
	previewCreateOperation,
	previewDeleteOperation,
} from "../lib/streamForm";
import { previewTailOperation, tailToCurl } from "../lib/tail";
import type { Connection, Operation } from "../lib/types";
import {
	WAKE_DEMO_SUB_ID,
	previewWakeDemoCreateStream,
	previewWakeDemoPublish,
	previewWakeDemoRegister,
} from "../lib/wakes";
import {
	activeConnection,
	appendMessages,
	captureBase,
	closeStream,
	createStream,
	deleteStream,
	forkStream,
	lastRead,
	operationInFlight,
	playgroundHighlight,
	playgroundOpen,
	runDemoProducer,
	runWakeDemo,
	selectStream,
	selectedStreamPath,
	setTailMode,
	startTail,
	togglePlayground,
	wakeDemoInFlight,
} from "../state/store";
import { CurlPreview } from "./CurlPreview";
import {
	IconBroadcast,
	IconChevronRight,
	IconFilePlus,
	IconFork,
	IconLoader,
	IconLock,
	IconPlay,
	IconSend,
	IconSparkles,
	IconTrash,
	IconWebhook,
	IconZap,
} from "./icons";

/** The sample stream every preset operates on — obvious + namespaced. */
const SAMPLE_STREAM = "playground/demo";
/** A sample fork target. */
const SAMPLE_FORK = "playground/demo-fork";
/** The sample JSON stream's wire content type. */
const SAMPLE_CONTENT_TYPE = "application/json";

/** A sample JSON batch the "publish a batch" preset sends. */
const SAMPLE_BATCH = JSON.stringify([
	{ id: 1, event: "created", note: "hello from the playground" },
	{ id: 2, event: "updated", note: "second message in the batch" },
	{ id: 3, event: "shipped", note: "third and final" },
]);

/** A single demo-producer message body, for the curl preview (the action loops). */
const DEMO_SAMPLE_BODY = JSON.stringify([{ seq: 1, of: 5, note: "demo message 1" }]);

/**
 * When there is no active connection (curl is still worth showing for teaching),
 * fall back to a placeholder origin + the default stream route so the previewed
 * command is still shape-correct.
 */
const PLACEHOLDER_CONN = {
	baseUrl: "http://localhost:4437",
	streamRoot: "/v1/stream",
} as const;

interface Preset {
	readonly key: string;
	readonly icon: JSX.Element;
	readonly label: string;
	/** The short, mono hint shown on the button (path / count). */
	readonly hint: string;
	/** A plain-language line describing exactly what the preset will do. */
	readonly does: string;
	/** The exact operation this preset issues, for the equivalent-curl preview. */
	readonly operation: Operation;
	/** Override the rendered curl (e.g. SSE's `-N`); else toCurl(operation). */
	readonly command?: string;
	readonly danger?: boolean;
	readonly run: () => void;
}

/**
 * Build the preset list. Pure given a connection origin (or the placeholder) and
 * the current next-offset, so the previewed curl is exact. The `run` closures
 * call the same store actions the rest of the UI uses.
 */
function buildPresets(
	origin: Pick<Connection, "baseUrl" | "streamRoot">,
	nextOffset: string,
	onSample: boolean,
): readonly Preset[] {
	const { baseUrl, streamRoot } = origin;
	return [
		{
			key: "create",
			icon: <IconFilePlus size={15} />,
			label: "Create sample JSON stream",
			hint: SAMPLE_STREAM,
			does: `PUT a new application/json stream at ${SAMPLE_STREAM}.`,
			operation: previewCreateOperation(baseUrl, streamRoot, {
				path: SAMPLE_STREAM,
				contentType: SAMPLE_CONTENT_TYPE,
			}),
			run: () => {
				void createStream({ path: SAMPLE_STREAM, contentType: SAMPLE_CONTENT_TYPE });
			},
		},
		{
			key: "publish",
			icon: <IconSend size={15} />,
			label: "Publish a sample batch",
			hint: "3 JSON messages",
			does: "POST a 3-element JSON array — each element is one message in the batch.",
			operation: previewAppendOperation(baseUrl, streamRoot, SAMPLE_STREAM, {
				body: SAMPLE_BATCH,
				contentType: SAMPLE_CONTENT_TYPE,
			}),
			run: () => {
				void appendMessages(SAMPLE_STREAM, SAMPLE_BATCH, { contentType: SAMPLE_CONTENT_TYPE });
			},
		},
		{
			key: "demo",
			icon: <IconZap size={15} />,
			label: "Run a demo producer",
			hint: "5 messages, ~700ms apart",
			does: "POST five one-message batches, spaced ~700ms apart, so a live tail visibly updates. Each POST looks like the curl below.",
			operation: previewAppendOperation(baseUrl, streamRoot, SAMPLE_STREAM, {
				body: DEMO_SAMPLE_BODY,
				contentType: SAMPLE_CONTENT_TYPE,
			}),
			run: () => {
				if (!onSample) selectStream(SAMPLE_STREAM);
				void runDemoProducer(SAMPLE_STREAM);
			},
		},
		{
			key: "tail",
			icon: <IconPlay size={15} />,
			label: "Tail live (SSE)",
			hint: "follow the tail in real time",
			does: "Open a Server-Sent Events connection from the tail (now) and stream new messages as they arrive.",
			operation: previewTailOperation(baseUrl, streamRoot, SAMPLE_STREAM, "now", "sse"),
			command: tailToCurl(
				previewTailOperation(baseUrl, streamRoot, SAMPLE_STREAM, "now", "sse"),
				"sse",
			),
			run: () => {
				if (!onSample) selectStream(SAMPLE_STREAM);
				setTailMode("sse");
				startTail("now");
			},
		},
		{
			key: "fork",
			icon: <IconFork size={15} />,
			label: "Fork at latest",
			hint: SAMPLE_FORK,
			does: `PUT a new stream at ${SAMPLE_FORK} that inherits ${SAMPLE_STREAM} up to the current offset, then diverges.`,
			operation: previewCreateOperation(baseUrl, streamRoot, {
				path: SAMPLE_FORK,
				contentType: "application/octet-stream",
				fork: { fromPath: SAMPLE_STREAM, offset: nextOffset },
			}),
			run: () => {
				void forkStream(SAMPLE_FORK, SAMPLE_STREAM, nextOffset);
			},
		},
		{
			key: "close",
			icon: <IconLock size={15} />,
			label: "Close stream",
			hint: SAMPLE_STREAM,
			does: `POST Stream-Closed: true to seal ${SAMPLE_STREAM} — no more messages can be appended.`,
			operation: previewCloseOperation(baseUrl, streamRoot, SAMPLE_STREAM),
			run: () => {
				void closeStream(SAMPLE_STREAM);
			},
		},
		{
			key: "delete",
			icon: <IconTrash size={15} />,
			label: "Delete / reset playground",
			hint: SAMPLE_STREAM,
			does: `DELETE ${SAMPLE_STREAM} — soft-deletes if forks exist, else removed entirely. Resets the playground.`,
			operation: previewDeleteOperation(baseUrl, streamRoot, SAMPLE_STREAM),
			danger: true,
			run: () => {
				void deleteStream(SAMPLE_STREAM);
			},
		},
	];
}

/** One preset row: the run button, a "what it does" line, and its curl. */
function PresetRow(props: { preset: Preset; disabled: boolean }): JSX.Element {
	const { preset, disabled } = props;
	return (
		<li class="dsui-playground__row">
			<button
				type="button"
				class={`dsui-playground__btn${preset.danger === true ? " dsui-playground__btn--danger" : ""}`}
				disabled={disabled}
				onClick={preset.run}
			>
				<span class="dsui-playground__btnicon">{preset.icon}</span>
				<span class="dsui-playground__btntext">
					<span class="dsui-playground__btnlabel">{preset.label}</span>
					<span class="dsui-playground__btnhint">{preset.hint}</span>
				</span>
			</button>
			<details class="dsui-playground__detail">
				<summary class="dsui-playground__detailsummary">
					<IconChevronRight size={12} class="dsui-playground__detailcaret" />
					<span>What it does &amp; curl</span>
				</summary>
				<p class="dsui-playground__does">{preset.does}</p>
				<CurlPreview
					operation={preset.operation}
					copyKey={`playground-${preset.key}-curl`}
					label="Equivalent curl"
					{...(preset.command !== undefined ? { command: preset.command } : {})}
					open
				/>
			</details>
		</li>
	);
}

/**
 * The one-click "Wake demo" row: a single button that creates a sample stream,
 * registers a webhook subscription pointed at the built-in capture endpoint, and
 * publishes a message — then opens the Wake Monitor so a newcomer sees the whole
 * publish → wake → hook → ack loop. It discloses all three exact curls (register,
 * create, publish). It needs the dsui binary's capture endpoint (not vite dev);
 * without it the button explains why, and webhooks only fire for real against a
 * redis-backed chronicle with subscriptions enabled.
 */
function WakeDemoRow(props: {
	origin: Pick<Connection, "baseUrl" | "streamRoot">;
	capture: string | null;
	disabled: boolean;
}): JSX.Element {
	const { origin, capture, disabled } = props;
	const busy = wakeDemoInFlight.value;
	const createOp = previewWakeDemoCreateStream(origin.baseUrl, origin.streamRoot);
	const publishOp = previewWakeDemoPublish(origin.baseUrl, origin.streamRoot);
	// The registration curl needs a capture base; fall back to a placeholder so the
	// shape is still teachable when no binary is present.
	const registerOp = previewWakeDemoRegister(origin.baseUrl, capture ?? "http://localhost:4438");

	return (
		<li class="dsui-playground__row dsui-playground__row--wake">
			<button
				type="button"
				class="dsui-playground__btn dsui-playground__btn--wake"
				disabled={disabled || busy || capture === null}
				onClick={() => void runWakeDemo()}
			>
				<span class="dsui-playground__btnicon">
					{busy ? <IconLoader size={15} class="dsui-spin" /> : <IconBroadcast size={15} />}
				</span>
				<span class="dsui-playground__btntext">
					<span class="dsui-playground__btnlabel">
						<IconWebhook size={12} /> Wake demo — capture a webhook
					</span>
					<span class="dsui-playground__btnhint">
						register {WAKE_DEMO_SUB_ID} → publish → watch the wake
					</span>
				</span>
			</button>
			<details class="dsui-playground__detail">
				<summary class="dsui-playground__detailsummary">
					<IconChevronRight size={12} class="dsui-playground__detailcaret" />
					<span>What it does &amp; curl</span>
				</summary>
				<p class="dsui-playground__does">
					Creates the sample stream, registers a <strong>webhook</strong> subscription whose
					webhook_url is this binary's built-in capture endpoint, then publishes a message and opens
					the Wake Monitor. The browser cannot receive a webhook directly, so the dsui binary
					captures it and relays it over SSE.{" "}
					{capture === null ? (
						<strong>No capture endpoint detected — run the dsui binary, not vite dev.</strong>
					) : (
						"Webhooks only fire for real against a redis-backed chronicle with subscriptions enabled."
					)}
				</p>
				<CurlPreview
					operation={registerOp}
					copyKey="wakedemo-register-curl"
					label="1 · register subscription"
					open
				/>
				<CurlPreview
					operation={createOp}
					copyKey="wakedemo-create-curl"
					label="2 · create sample stream"
				/>
				<CurlPreview
					operation={publishOp}
					copyKey="wakedemo-publish-curl"
					label="3 · publish (fires the wake)"
				/>
			</details>
		</li>
	);
}

export function Playground(): JSX.Element {
	const inFlight = operationInFlight.value;
	const conn = activeConnection.value;
	const onSample = selectedStreamPath.value === SAMPLE_STREAM;
	const read = lastRead.value;
	const highlightTick = playgroundHighlight.value;

	const sectionRef = useRef<HTMLElement>(null);
	// A local flag the highlight effect flips on, cleared after the pulse ends, so
	// the first-run hint can briefly draw the eye to this section + scroll it in.
	// useState's setter is stable, so the effect keys off the highlight tick alone.
	const [pulsing, setPulsing] = useState(false);

	useEffect(() => {
		if (highlightTick === 0) return;
		setPulsing(true);
		sectionRef.current?.scrollIntoView({ block: "nearest", behavior: "smooth" });
		const id = globalThis.setTimeout(() => setPulsing(false), 1600);
		return () => globalThis.clearTimeout(id);
	}, [highlightTick]);

	const open = playgroundOpen.value;
	const origin = conn ?? PLACEHOLDER_CONN;
	const capture = captureBase.value;
	const presets = buildPresets(origin, read?.nextOffset ?? "now", onSample);

	return (
		<section
			class={`dsui-nav__section dsui-playground${pulsing ? " is-pulsing" : ""}`}
			aria-label="Playground"
			ref={sectionRef}
		>
			<header class="dsui-nav__head">
				<button
					type="button"
					class="dsui-playground__toggle"
					aria-expanded={open}
					onClick={() => togglePlayground()}
				>
					<span class="dsui-nav__title">
						<IconSparkles size={13} class="dsui-playground__sparkle" />
						Playground
					</span>
					<IconChevronRight size={14} class="dsui-playground__caret" />
				</button>
			</header>
			{open ? (
				<>
					<p class="dsui-playground__lead">
						One-click presets on the sample stream <code>{SAMPLE_STREAM}</code>. Each runs the real
						operation — watch the toasts, the live grid, and the protocol disclosure — and shows the
						equivalent <code>curl</code>.
					</p>
					<ul class="dsui-playground__list">
						{presets.map((p) => (
							<PresetRow key={p.key} preset={p} disabled={inFlight} />
						))}
						<WakeDemoRow origin={origin} capture={capture} disabled={inFlight} />
					</ul>
				</>
			) : null}
		</section>
	);
}
