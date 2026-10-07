import assert from "node:assert/strict";
import test from "node:test";
import { awaitCommit } from "./receipts.mjs";

const receipt = "er1.c3ludGhldGlj";
const terminal = (state, status, body = []) => Response.json({ state,
  session: state === "committed" ? "cluster:0:9" : null,
  response: { status, headers: [["content-type", "text/plain"]], body } });

test("awaits pending without mistaking acceptance for successful execution", async () => {
  let calls = 0;
  const result = await awaitCommit("http://replica.test", receipt, { fetch: async (url) => {
    assert.match(url.search, /^\?wait_ms=\d+$/);
    return ++calls === 1 ? Response.json({ state: "pending" }, { status: 202 }) :
      terminal("rejected", 409, [...new TextEncoder().encode("producer sequence gap")]);
  } });
  assert.equal(calls, 2);
  assert.equal(result.state, "rejected");
  assert.equal(result.session, null);
  assert.equal(result.response.status, 409);
  assert.equal(await result.response.text(), "producer sequence gap");
});

test("tries the supplied failover replica and reconstructs empty 204", async () => {
  const seen = [];
  const result = await awaitCommit(["http://down.test", "http://lag.test", "http://new.test"], receipt,
    { fetch: async (url) => {
      seen.push(url.host);
      if (url.host === "down.test") throw new TypeError("connection refused");
      if (url.host === "lag.test") return Response.json({ state: "unknown" }, { status: 404 });
      return terminal("committed", 204);
    } });
  assert.deepEqual(seen, ["down.test", "lag.test", "new.test"]);
  assert.equal(result.state, "committed");
  assert.equal(result.response.status, 204);
  assert.equal(await result.response.text(), "");
});

test("distinguishes unknown, invalidated and deadline/cancellation", async () => {
  for (const [state, status] of [["unknown", 404], ["invalidated", 410]]) {
    assert.equal((await awaitCommit("http://replica.test", receipt,
      { fetch: async () => Response.json({ state }, { status }) })).state, state);
  }
  let calls = 0;
  await assert.rejects(awaitCommit("http://replica.test", receipt, { timeoutMs: 120,
    fetch: async () => { calls++; return new Response("unavailable", { status: 503 }); } }),
    { name: "TimeoutError" });
  assert.ok(calls <= 3, "errors must back off, not spin");
  const controller = new AbortController();
  controller.abort(new Error("application cancellation"));
  await assert.rejects(awaitCommit("http://replica.test", receipt, { signal: controller.signal }),
    /application cancellation/);
});
