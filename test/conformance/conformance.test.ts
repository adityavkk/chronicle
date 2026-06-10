// Programmatic entry point for the Durable Streams server conformance suite.
//
// The published CLI points vitest at a file inside node_modules, which
// vitest's default exclude swallows ("No test files found"); registering the
// suite from a local test file sidesteps that and gives us access to the
// full options object (e.g. `subscriptions` once chronicle implements the
// reserved __ds APIs).
import { runConformanceTests } from "@durable-streams/server-conformance-tests"

const baseUrl = process.env.CONFORMANCE_TEST_URL ?? "http://localhost:4437"

runConformanceTests({ baseUrl })
