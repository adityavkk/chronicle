import { afterEach, describe, expect, it, vi } from "vitest";
import { REGISTRY_PATH, createClient, streamUrl } from "./dsClient";
import type { Connection } from "./types";

/* ----------------------------------------------------------------------------
 * Fixtures + a thin fetch double
 *
 * The client's only network primitive is the platform `fetch`, so every test
 * stubs `globalThis.fetch` with a recorded queue. Responses are built from a
 * real `Response` so header casing, `.text()`, and `.arrayBuffer()` behave
 * exactly as they do in the browser.
 * ------------------------------------------------------------------------- */

const CONN: Connection = {
	id: "c1",
	name: "Local",
	baseUrl: "http://localhost:4437",
	streamRoot: "/v1/stream",
	createdAt: 0,
	lastUsedAt: null,
};

interface StubResponse {
	readonly status?: number;
	readonly statusText?: string;
	readonly headers?: Record<string, string>;
	readonly body?: string | Uint8Array;
}

/** Build a real Response from a stub spec. */
function makeResponse(spec: StubResponse): Response {
	const status = spec.status ?? 200;
	// A Uint8Array is a valid runtime BodyInit; the DOM lib types are narrower.
	const body: BodyInit | null = spec.body === undefined ? null : (spec.body as BodyInit);
	return new Response(body, {
		status,
		statusText: spec.statusText ?? "",
		headers: spec.headers ?? {},
	});
}

/** Install a fetch stub that returns the given responses in call order. */
function stubFetch(...responses: StubResponse[]): ReturnType<typeof vi.fn> {
	const fn = vi.fn(async () => makeResponse(responses.shift() ?? { status: 200 }));
	vi.stubGlobal("fetch", fn);
	return fn;
}

/** Install a fetch stub that rejects (simulates a network/CORS failure). */
function stubFetchReject(error: unknown): ReturnType<typeof vi.fn> {
	const fn = vi.fn(async () => {
		throw error;
	});
	vi.stubGlobal("fetch", fn);
	return fn;
}

afterEach(() => {
	vi.unstubAllGlobals();
	vi.restoreAllMocks();
});

/* ----------------------------------------------------------------------------
 * streamUrl
 * ------------------------------------------------------------------------- */

describe("streamUrl", () => {
	it("joins base + root + path, strips a leading slash, and encodes segments", () => {
		expect(streamUrl(CONN, "orders/created")).toBe(
			"http://localhost:4437/v1/stream/orders/created",
		);
		expect(streamUrl(CONN, "/leading/slash")).toBe("http://localhost:4437/v1/stream/leading/slash");
		// Slashes are path separators; other reserved chars in a segment are escaped.
		expect(streamUrl(CONN, "a b/c?d")).toBe("http://localhost:4437/v1/stream/a%20b/c%3Fd");
	});

	it("appends an offset query param only when an offset is given", () => {
		expect(streamUrl(CONN, "s")).toBe("http://localhost:4437/v1/stream/s");
		expect(streamUrl(CONN, "s", "-1")).toBe("http://localhost:4437/v1/stream/s?offset=-1");
		expect(streamUrl(CONN, "s", "now")).toBe("http://localhost:4437/v1/stream/s?offset=now");
	});
});

/* ----------------------------------------------------------------------------
 * listStreams — registry reduction over the wire
 * ------------------------------------------------------------------------- */

describe("listStreams", () => {
	it("reads the registry at offset -1 and derives sorted, kind-tagged streams", async () => {
		const body = JSON.stringify([
			{
				key: "orders/created",
				value: {
					path: "orders/created",
					contentType: "application/json",
					createdAt: "2026-01-01T00:00:00Z",
				},
				headers: { operation: "upsert" },
			},
			{
				key: "logs/app",
				value: { path: "logs/app", contentType: "text/plain" },
				headers: { operation: "upsert" },
			},
			{
				key: "blobs/raw",
				value: { path: "blobs/raw", contentType: "application/octet-stream" },
				headers: { operation: "upsert" },
			},
		]);
		const fetchFn = stubFetch({ status: 200, body });

		const client = createClient(CONN);
		const { streams, exchange } = await client.listStreams();

		// Hit the registry stream from the beginning.
		expect(fetchFn).toHaveBeenCalledTimes(1);
		const calledUrl = fetchFn.mock.calls[0]?.[0] as string;
		expect(calledUrl).toBe(streamUrl(CONN, REGISTRY_PATH, "-1"));

		// Sorted by path, with kind derived from each contentType.
		expect(streams.map((s) => s.path)).toEqual(["blobs/raw", "logs/app", "orders/created"]);
		expect(streams.map((s) => s.kind)).toEqual(["binary", "text", "json"]);
		expect(streams.every((s) => s.manual === false)).toBe(true);
		expect(streams.find((s) => s.path === "orders/created")?.createdAt).toBe(
			"2026-01-01T00:00:00Z",
		);

		// The exchange is captured even on the happy path.
		expect(exchange.method).toBe("GET");
		expect(exchange.status).toBe(200);
		expect(exchange.url).toBe(calledUrl);
	});

	it("applies upsert/deleted reduction: a later delete drops a path, later upsert wins", async () => {
		const body = JSON.stringify([
			{
				key: "a",
				value: { path: "a", contentType: "text/plain" },
				headers: { operation: "upsert" },
			},
			{
				key: "b",
				value: { path: "b", contentType: "text/plain" },
				headers: { operation: "upsert" },
			},
			// b is deleted after being created -> should not survive.
			{ key: "b", value: { path: "b" }, headers: { operation: "deleted" } },
			// a is re-upserted with a new content type -> last write wins.
			{
				key: "a",
				value: { path: "a", contentType: "application/json" },
				headers: { operation: "upsert" },
			},
		]);
		stubFetch({ status: 200, body });

		const { streams } = await createClient(CONN).listStreams();
		expect(streams.map((s) => s.path)).toEqual(["a"]);
		expect(streams[0]?.contentType).toBe("application/json");
		expect(streams[0]?.kind).toBe("json");
	});

	it("tolerates newline-delimited registry bodies and skips malformed lines", async () => {
		const body = [
			'{"key":"x","value":{"path":"x"},"headers":{"operation":"upsert"}}',
			"this is not json",
			"{ also broken",
			'{"key":"y","value":{"path":"y"},"headers":{"operation":"upsert"}}',
		].join("\n");
		stubFetch({ status: 200, body });

		const { streams } = await createClient(CONN).listStreams();
		expect(streams.map((s) => s.path)).toEqual(["x", "y"]);
	});

	it("returns an honest empty tree for 404 / 204 / non-ok without throwing", async () => {
		for (const status of [404, 204, 500]) {
			stubFetch({ status });
			const { streams, exchange } = await createClient(CONN).listStreams();
			expect(streams).toEqual([]);
			expect(exchange.status).toBe(status);
			vi.unstubAllGlobals();
		}
	});

	it("returns an empty tree and a status-0 error exchange on a network failure", async () => {
		stubFetchReject(new TypeError("Failed to fetch"));
		const { streams, exchange } = await createClient(CONN).listStreams();
		expect(streams).toEqual([]);
		expect(exchange.status).toBe(0);
		expect(exchange.error).toBe("Failed to fetch");
	});

	it("treats a malformed but ok body as zero streams rather than throwing", async () => {
		stubFetch({ status: 200, body: "<<<not json at all>>>" });
		const { streams } = await createClient(CONN).listStreams();
		expect(streams).toEqual([]);
	});
});

/* ----------------------------------------------------------------------------
 * readStream — JSON-array -> GridRow + captured exchange
 * ------------------------------------------------------------------------- */

describe("readStream", () => {
	it("parses a JSON array body into one GridRow per element with index/size/preview", async () => {
		const elements = [{ id: 1, name: "alpha" }, { id: 2, name: "beta" }, "loose-string"];
		stubFetch({
			status: 200,
			headers: {
				"Content-Type": "application/json",
				"Stream-Next-Offset": "42",
				"Stream-Up-To-Date": "true",
			},
			body: JSON.stringify(elements),
		});

		const result = await createClient(CONN).readStream("orders", "-1");

		expect(result.kind).toBe("json");
		expect(result.rows).toHaveLength(3);
		expect(result.rows.map((r) => r.index)).toEqual([0, 1, 2]);
		// Each row carries the decoded element and a flattened preview.
		expect(result.rows[0]?.value).toEqual({ id: 1, name: "alpha" });
		expect(result.rows[0]?.preview).toBe('{"id":1,"name":"alpha"}');
		expect(result.rows[2]?.value).toBe("loose-string");
		// Byte sizes are positive and measured from the serialized element.
		expect(result.rows[0]?.byteSize).toBeGreaterThan(0);

		// Protocol headers flow through into the typed result.
		expect(result.nextOffset).toBe("42");
		expect(result.upToDate).toBe(true);
		expect(result.closed).toBe(false);
		expect(result.requestedOffset).toBe("-1");
	});

	it("wraps a single JSON object into exactly one row", async () => {
		stubFetch({
			status: 200,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ only: "one" }),
		});
		const result = await createClient(CONN).readStream("s", "-1");
		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]?.value).toEqual({ only: "one" });
	});

	it("falls back to a single text row when a JSON stream returns malformed JSON", async () => {
		stubFetch({
			status: 200,
			headers: { "Content-Type": "application/json" },
			body: "{ truncated",
		});
		const result = await createClient(CONN).readStream("s", "-1");
		// Nothing is lost: the raw text becomes one row so the user still sees it.
		expect(result.kind).toBe("json");
		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]?.kind).toBe("text");
		expect(result.rows[0]?.value).toBe("{ truncated");
	});

	it("decodes a text stream into a single text row", async () => {
		stubFetch({
			status: 200,
			headers: { "Content-Type": "text/plain; charset=utf-8" },
			body: "hello\nworld",
		});
		const result = await createClient(CONN).readStream("s", "-1");
		expect(result.kind).toBe("text");
		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]?.kind).toBe("text");
		expect(result.rows[0]?.value).toBe("hello\nworld");
	});

	it("treats an unknown content type as a single binary row carrying raw bytes", async () => {
		const bytes = new Uint8Array([0, 1, 2, 253, 254, 255]);
		stubFetch({
			status: 200,
			headers: { "Content-Type": "application/octet-stream" },
			body: bytes,
		});
		const result = await createClient(CONN).readStream("s", "-1");
		expect(result.kind).toBe("binary");
		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]?.kind).toBe("binary");
		expect(result.rows[0]?.value).toBeInstanceOf(Uint8Array);
		expect(result.rows[0]?.preview).toContain("binary");
		expect(Array.from(result.rawBytes)).toEqual(Array.from(bytes));
	});

	it("captures the exchange and requests the offset on the URL", async () => {
		const fetchFn = stubFetch({
			status: 200,
			headers: { "Content-Type": "application/json", "Stream-Next-Offset": "7" },
			body: "[]",
		});
		const result = await createClient(CONN).readStream("orders/created", "cursor-9");

		const calledUrl = fetchFn.mock.calls[0]?.[0] as string;
		expect(calledUrl).toBe(streamUrl(CONN, "orders/created", "cursor-9"));
		expect(result.exchange.method).toBe("GET");
		expect(result.exchange.url).toBe(calledUrl);
		expect(result.exchange.status).toBe(200);
		expect(result.exchange.protocol.streamNextOffset).toBe("7");
		expect(result.exchange.requestHeaders).toEqual({ Accept: "*/*" });
		// An empty array body produces no rows, but the exchange is still captured.
		expect(result.rows).toEqual([]);
	});

	it("interprets Stream-Closed / Stream-Up-To-Date booleans loosely", async () => {
		stubFetch({
			status: 200,
			headers: {
				"Content-Type": "application/json",
				"Stream-Closed": "1",
				"Stream-Up-To-Date": "yes",
			},
			body: "[]",
		});
		const result = await createClient(CONN).readStream("s", "-1");
		expect(result.closed).toBe(true);
		expect(result.upToDate).toBe(true);
	});

	it("returns an empty, captured result on a non-ok response without throwing", async () => {
		stubFetch({ status: 404, headers: { "Content-Type": "application/json" } });
		const result = await createClient(CONN).readStream("missing", "-1");
		expect(result.rows).toEqual([]);
		expect(result.rawBytes.byteLength).toBe(0);
		expect(result.exchange.status).toBe(404);
	});

	it("returns an empty result with a status-0 error exchange on a network failure", async () => {
		stubFetchReject(new Error("boom"));
		const result = await createClient(CONN).readStream("s", "-1");
		expect(result.rows).toEqual([]);
		expect(result.exchange.status).toBe(0);
		expect(result.exchange.error).toBe("boom");
	});
});

/* ----------------------------------------------------------------------------
 * testConnection + headStream — probe + capture semantics
 * ------------------------------------------------------------------------- */

describe("testConnection", () => {
	it("treats any HTTP response (even 404) as reachable", async () => {
		const fetchFn = stubFetch({ status: 404 });
		const probe = await createClient(CONN).testConnection();
		expect(probe.ok).toBe(true);
		expect(probe.status).toBe(404);
		// Probes the registry with HEAD.
		expect(fetchFn.mock.calls[0]?.[1]).toMatchObject({ method: "HEAD" });
	});

	it("reports unreachable with the error message on a network failure", async () => {
		stubFetchReject(new TypeError("Failed to fetch"));
		const probe = await createClient(CONN).testConnection();
		expect(probe.ok).toBe(false);
		expect(probe.status).toBe(0);
		expect(probe.error).toBe("Failed to fetch");
	});
});

describe("headStream", () => {
	it("issues a HEAD and returns the captured exchange with protocol headers", async () => {
		const fetchFn = stubFetch({
			status: 200,
			headers: { "Content-Type": "application/json", ETag: 'W/"abc"' },
		});
		const exchange = await createClient(CONN).headStream("orders");
		expect(fetchFn.mock.calls[0]?.[1]).toMatchObject({ method: "HEAD" });
		expect(exchange.method).toBe("HEAD");
		expect(exchange.protocol.etag).toBe('W/"abc"');
		expect(exchange.protocol.contentType).toBe("application/json");
	});
});
