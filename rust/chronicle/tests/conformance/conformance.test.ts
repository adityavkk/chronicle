import { runConformanceTests } from "@durable-streams/server-conformance-tests"

// The upstream suite concatenates /v1/stream/... to this prefix. Supply a
// disposable tenant URL, not the server root; no protocol test is rewritten.
const baseUrl = process.env.CONFORMANCE_TEST_URL
if (!baseUrl) throw new Error("CONFORMANCE_TEST_URL must name a disposable tenant URL")

// Retain transport causes alongside assertion failures without changing results.
const originalFetch = globalThis.fetch
globalThis.fetch = async (...args: Parameters<typeof originalFetch>) => {
  try {
    return await originalFetch(...args)
  } catch (error) {
    console.error("conformance transport failure", String(args[0]), error)
    throw error
  }
}

runConformanceTests({ baseUrl, longPollTimeoutMs: 20_000 })
