// No test filters or assertion changes. The complete published suite is the oracle.
import { runConformanceTests } from "../../.tmp/electric-conformance/node_modules/@durable-streams/server-conformance-tests/dist/index.js";

runConformanceTests({
  baseUrl: process.env.CONFORMANCE_TEST_URL!,
  subscriptions: true,
});
