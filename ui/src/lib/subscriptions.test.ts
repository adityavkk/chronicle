import { describe, expect, it } from "vitest";
import {
	buildAckBody,
	buildCreateBody,
	parseAckResult,
	parseErrorCode,
	parseRefreshedToken,
	parseStreamLink,
	parseSubscription,
	parseWakeClaim,
	parseWakeStreamSnapshot,
	previewAckOperation,
	previewClaimOperation,
	previewCreateSubscriptionOperation,
} from "./subscriptions";

const BASE = "http://localhost:4437";

/* ----------------------------------------------------------------------------
 * parseSubscription — the GET / create response view
 * ------------------------------------------------------------------------- */

describe("parseSubscription", () => {
	it("parses a full webhook subscription view including signing", () => {
		const sub = parseSubscription({
			id: "sub-1",
			subscription_id: "sub-1",
			type: "webhook",
			pattern: "events/**",
			streams: [
				{ path: "events/a", link_type: "glob", acked_offset: "off-1" },
				{ path: "events/b", link_type: "explicit", acked_offset: "" },
			],
			webhook: {
				url: "https://hook.example/x",
				signing: { alg: "ed25519", kid: "ds_abc", jwks_url: "https://srv/streams/__ds/jwks.json" },
			},
			wake_stream: null,
			lease_ttl_ms: 30000,
			created_at: "2026-05-09T00:00:00.000Z",
			status: "active",
			description: "orders fan-out",
		});
		expect(sub).not.toBeNull();
		expect(sub?.id).toBe("sub-1");
		expect(sub?.type).toBe("webhook");
		expect(sub?.pattern).toBe("events/**");
		expect(sub?.streams).toHaveLength(2);
		expect(sub?.streams[0]).toEqual({ path: "events/a", linkType: "glob", ackedOffset: "off-1" });
		expect(sub?.streams[1]?.linkType).toBe("explicit");
		expect(sub?.webhook?.url).toBe("https://hook.example/x");
		expect(sub?.webhook?.signing?.kid).toBe("ds_abc");
		expect(sub?.wakeStream).toBeNull();
		expect(sub?.leaseTtlMs).toBe(30000);
		expect(sub?.status).toBe("active");
	});

	it("parses a pull-wake subscription with a wake stream and no webhook", () => {
		const sub = parseSubscription({
			subscription_id: "sub-pw",
			type: "pull-wake",
			streams: [],
			wake_stream: "wakes/sub-pw",
			lease_ttl_ms: 5000,
			created_at: "2026-05-09T00:00:00.000Z",
			status: "active",
		});
		expect(sub?.id).toBe("sub-pw");
		expect(sub?.type).toBe("pull-wake");
		expect(sub?.webhook).toBeNull();
		expect(sub?.wakeStream).toBe("wakes/sub-pw");
		expect(sub?.pattern).toBeNull();
	});

	it("tolerates missing optional fields and defaults type/status loosely", () => {
		const sub = parseSubscription({ id: "x" });
		expect(sub).not.toBeNull();
		expect(sub?.type).toBe("webhook");
		expect(sub?.status).toBe("active");
		expect(sub?.streams).toEqual([]);
		expect(sub?.leaseTtlMs).toBe(0);
		expect(sub?.createdAt).toBeNull();
		expect(sub?.description).toBeNull();
		// Runtime-only fields are absent unless present (exactOptionalPropertyTypes).
		expect("phase" in (sub ?? {})).toBe(false);
		expect("generation" in (sub ?? {})).toBe(false);
	});

	it("reads optional runtime phase + generation when present", () => {
		const sub = parseSubscription({ id: "x", phase: "live", generation: 7 });
		expect(sub?.phase).toBe("live");
		expect(sub?.generation).toBe(7);
	});

	it("returns null when there is no usable id", () => {
		expect(parseSubscription({ type: "webhook" })).toBeNull();
		expect(parseSubscription(null)).toBeNull();
		expect(parseSubscription("nope")).toBeNull();
	});

	it("skips malformed stream links", () => {
		const sub = parseSubscription({
			id: "x",
			streams: [{ path: "ok", link_type: "glob", acked_offset: "o" }, { nope: true }, 42],
		});
		expect(sub?.streams).toHaveLength(1);
		expect(sub?.streams[0]?.path).toBe("ok");
	});
});

/* ----------------------------------------------------------------------------
 * parseStreamLink / parseWakeStreamSnapshot
 * ------------------------------------------------------------------------- */

describe("parseStreamLink + parseWakeStreamSnapshot", () => {
	it("parses a serialized link (no tail/pending)", () => {
		expect(parseStreamLink({ path: "p", link_type: "explicit", acked_offset: "a" })).toEqual({
			path: "p",
			linkType: "explicit",
			ackedOffset: "a",
		});
	});

	it("parses a wake snapshot with tail + has_pending", () => {
		expect(
			parseWakeStreamSnapshot({
				path: "p",
				link_type: "glob",
				acked_offset: "a",
				tail_offset: "t",
				has_pending: true,
			}),
		).toEqual({ path: "p", linkType: "glob", ackedOffset: "a", tailOffset: "t", hasPending: true });
	});

	it("returns null without a path", () => {
		expect(parseStreamLink({ link_type: "glob" })).toBeNull();
		expect(parseWakeStreamSnapshot({ tail_offset: "t" })).toBeNull();
	});
});

/* ----------------------------------------------------------------------------
 * parseWakeClaim / parseAckResult / parseErrorCode
 * ------------------------------------------------------------------------- */

describe("parseWakeClaim", () => {
	it("parses a successful claim with token, generation, and snapshots", () => {
		const claim = parseWakeClaim({
			wake_id: "w_abc",
			generation: 7,
			token: "eyJ...",
			streams: [
				{
					path: "events/a",
					link_type: "glob",
					acked_offset: "a",
					tail_offset: "t",
					has_pending: true,
				},
			],
			lease_ttl_ms: 30000,
		});
		expect(claim?.wakeId).toBe("w_abc");
		expect(claim?.generation).toBe(7);
		expect(claim?.token).toBe("eyJ...");
		expect(claim?.streams).toHaveLength(1);
		expect(claim?.streams[0]?.hasPending).toBe(true);
		expect(claim?.leaseTtlMs).toBe(30000);
	});

	it("returns null without a wake_id or token (cannot ack/release)", () => {
		expect(parseWakeClaim({ generation: 1, token: "t" })).toBeNull();
		expect(parseWakeClaim({ wake_id: "w" })).toBeNull();
	});
});

describe("parseAckResult", () => {
	it("reads ok + next_wake", () => {
		expect(parseAckResult({ ok: true, next_wake: true })).toEqual({ ok: true, nextWake: true });
		expect(parseAckResult({ ok: true, next_wake: false })).toEqual({ ok: true, nextWake: false });
	});

	it("defaults a non-object / missing fields safely", () => {
		expect(parseAckResult(null)).toEqual({ ok: true, nextWake: false });
		expect(parseAckResult({})).toEqual({ ok: true, nextWake: false });
	});

	it("ignores a refreshed token on the body (surfaced via SubscriptionResult, not AckResult)", () => {
		// The common ack response yields exactly {ok, nextWake}; the token channel
		// is SubscriptionResult.refreshedToken, kept separate from AckResult.
		expect(parseAckResult({ ok: true, next_wake: false, token: "fresh" })).toEqual({
			ok: true,
			nextWake: false,
		});
	});
});

describe("parseRefreshedToken", () => {
	it("reads a rolled token from a 2xx ack/callback body or a 401 error body", () => {
		expect(parseRefreshedToken({ ok: true, next_wake: false, token: "fresh" })).toBe("fresh");
		expect(parseRefreshedToken({ error: { code: "TOKEN_EXPIRED" }, token: "retry" })).toBe("retry");
	});

	it("returns null when no token is present", () => {
		expect(parseRefreshedToken({ ok: true, next_wake: true })).toBeNull();
		expect(parseRefreshedToken({ token: "" })).toBeNull();
		expect(parseRefreshedToken(null)).toBeNull();
		expect(parseRefreshedToken("nope")).toBeNull();
	});
});

describe("parseErrorCode", () => {
	it("extracts the code from the {error:{code}} envelope", () => {
		expect(parseErrorCode({ error: { code: "FENCED" } })).toBe("FENCED");
		expect(parseErrorCode({ error: { code: "ALREADY_CLAIMED", current_holder: "w2" } })).toBe(
			"ALREADY_CLAIMED",
		);
	});

	it("returns null for a body that is not an error envelope", () => {
		expect(parseErrorCode({ ok: true })).toBeNull();
		expect(parseErrorCode(null)).toBeNull();
		expect(parseErrorCode({ error: "oops" })).toBeNull();
	});
});

/* ----------------------------------------------------------------------------
 * Body builders — wire field names + omission rules
 * ------------------------------------------------------------------------- */

describe("buildCreateBody", () => {
	it("includes only the set fields, with the wire field names", () => {
		const body = buildCreateBody({
			type: "webhook",
			pattern: "events/**",
			webhookUrl: "https://hook.example",
			leaseTtlMs: 5000,
			description: "d",
		});
		expect(body).toEqual({
			type: "webhook",
			pattern: "events/**",
			webhook: { url: "https://hook.example" },
			lease_ttl_ms: 5000,
			description: "d",
		});
	});

	it("uses streams + wake_stream for a pull-wake subscription and omits empties", () => {
		const body = buildCreateBody({
			type: "pull-wake",
			streams: ["a", "b"],
			wakeStream: "wakes/x",
			pattern: "",
			description: "",
		});
		expect(body).toEqual({ type: "pull-wake", streams: ["a", "b"], wake_stream: "wakes/x" });
	});
});

describe("buildAckBody", () => {
	it("maps acks to the wire shape and includes done only when set", () => {
		expect(
			buildAckBody({
				wakeId: "w",
				generation: 3,
				acks: [{ stream: "s", offset: "o" }],
				done: true,
			}),
		).toEqual({ wake_id: "w", generation: 3, acks: [{ stream: "s", offset: "o" }], done: true });

		const heartbeat = buildAckBody({ wakeId: "w", generation: 3, acks: [] });
		expect("done" in heartbeat).toBe(false);
	});
});

/* ----------------------------------------------------------------------------
 * Operation previews — mirror what the client sends (drift guard)
 * ------------------------------------------------------------------------- */

describe("operation previews", () => {
	it("previews a create PUT under the stream root (/__ds is stream-root-relative)", () => {
		const op = previewCreateSubscriptionOperation(BASE, "/v1/stream", {
			id: "sub 1",
			type: "webhook",
			pattern: "events/**",
			webhookUrl: "https://hook.example",
		});
		expect(op.method).toBe("PUT");
		// id is URL-encoded as a single segment.
		expect(op.url).toBe("http://localhost:4437/v1/stream/__ds/subscriptions/sub%201");
		expect(op.headers).toMatchObject({ Accept: "*/*", "Content-Type": "application/json" });
		expect(op.body).toBe(
			JSON.stringify({
				type: "webhook",
				pattern: "events/**",
				webhook: { url: "https://hook.example" },
			}),
		);
	});

	it("previews a claim POST", () => {
		const op = previewClaimOperation(BASE, "/v1/stream", "sub-1", "worker-7");
		expect(op.url).toBe("http://localhost:4437/v1/stream/__ds/subscriptions/sub-1/claim");
		expect(op.body).toBe(JSON.stringify({ worker: "worker-7" }));
	});

	it("previews an ack POST carrying the Bearer token", () => {
		const op = previewAckOperation(BASE, "/v1/stream", "sub-1", "tok-9", {
			wakeId: "w",
			generation: 4,
			acks: [{ stream: "s", offset: "o" }],
			done: true,
		});
		expect(op.url).toBe("http://localhost:4437/v1/stream/__ds/subscriptions/sub-1/ack");
		expect(op.headers.Authorization).toBe("Bearer tok-9");
	});
});
