/** Await local acceptance's committed outcome, never retry the append itself.
 * `origins` is the caller's trusted replica list, beginning with the accepting
 * origin. Unknown on every reachable replica is still unknown, not data loss.
 * A timeout/abort/network error never means the append was rejected.
 */
export async function awaitCommit(origins, receipt, { timeoutMs = 60_000, signal,
  fetch: request = globalThis.fetch } = {}) {
  if (!Array.isArray(origins)) origins = [origins];
  if (!origins.length || !/^er1\.[A-Za-z0-9_-]+$/.test(receipt)) {
    throw new TypeError("Expected replica origins and a Stream-Receipt");
  }
  const deadline = AbortSignal.timeout(timeoutMs);
  const canceled = signal ? AbortSignal.any([signal, deadline]) : deadline;
  const end = performance.now() + timeoutMs;
  for (;;) {
    canceled.throwIfAborted();
    let unknown;
    let pending = false;
    let unavailable = false;
    for (const origin of origins) {
      canceled.throwIfAborted();
      const wait = Math.max(0, Math.min(30_000, Math.floor(end - performance.now())));
      const url = new URL(`/_receipts/${receipt}?wait_ms=${wait}`, origin);
      let response;
      try {
        response = await request(url, { redirect: "error",
          signal: AbortSignal.any([canceled, AbortSignal.timeout(wait + 3500)]) });
      } catch (error) {
        canceled.throwIfAborted();
        unavailable = true;
        continue;
      }
      if (response.status >= 500) {
        await response.body?.cancel();
        unavailable = true;
        continue;
      }
      if (![200, 202, 404, 410].includes(response.status)) {
        throw new Error(`Receipt lookup HTTP ${response.status}: ${await response.text()}`);
      }
      const result = await response.json();
      if (result.state === "committed" || result.state === "rejected") {
        const { status, headers, body } = result.response;
        return { ...result, response: new Response([204, 205, 304].includes(status) ? null :
          Uint8Array.from(body), { status, headers }) };
      }
      if (result.state === "invalidated") return result;
      if (result.state === "pending") pending = true;
      else if (result.state === "unknown") unknown = result;
      else throw new Error("Unrecognized receipt state");
    }
    if (unknown && !pending && !unavailable) return unknown;
    // Pending normally parks in the server's long-poll. A failing origin or
    // an immediate response must not turn a retry into a busy network loop.
    await new Promise((resolve, reject) => {
      canceled.throwIfAborted();
      const done = () => { canceled.removeEventListener("abort", abort); resolve(); };
      const timer = setTimeout(done, 50);
      const abort = () => { clearTimeout(timer); reject(canceled.reason); };
      canceled.addEventListener("abort", abort, { once: true });
    });
  }
}
