import { defineConfig } from "vitest/config"

// The extension suite drives a live chronicle over HTTP: lease lapses, token
// expiries, and webhook deliveries take wall-clock seconds, so the per-test
// budget is generous. client.test.ts enforces its own tighter WRITE_FAILED
// deadline inside the test (design §H.3 / M.1: < 5 s, not a vitest timeout).
export default defineConfig({
  test: {
    include: ["*.test.ts"],
    testTimeout: 20_000,
    hookTimeout: 20_000,
  },
})
